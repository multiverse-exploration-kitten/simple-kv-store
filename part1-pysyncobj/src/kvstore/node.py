"""One Raft node of the key-value store: a pysyncobj SyncObj that runs the replicated store."""

import enum
from collections.abc import Callable
from typing import Any

from pysyncobj import FAIL_REASON, SyncObj, SyncObjConf, SyncObjException
from pysyncobj.syncobj import _RAFT_STATE
from pysyncobj.transport import TCPTransport

from kvstore.config import NodeConfig
from kvstore.errors import CommandOutcomeUnknownError, CommandRejectedError, KeyValueStoreError
from kvstore.store import ReplicatedKeyValueStore


class ReadConsistency(enum.StrEnum):
    LINEARIZABLE = "linearizable"  # through the Raft log, always current
    STALE = "stale"  # local memory, can be old (for example on a cut-off follower)


# The command never entered the log, or another entry replaced it at its log index.
_REJECTED_REASONS = {
    FAIL_REASON.QUEUE_FULL: "queue_full",
    FAIL_REASON.MISSING_LEADER: "no_leader",
    FAIL_REASON.NOT_LEADER: "not_leader",
    FAIL_REASON.DISCARDED: "discarded",
    FAIL_REASON.REQUEST_DENIED: "request_denied",
}

# The command may be in a leader's log and may still commit.
# pysyncobj documents LEADER_CHANGED as "similar to NOT_LEADER". But a follower reports it
# when the leader changes after the follower forwarded the command. The old leader may
# already have replicated the entry to a majority, so the new leader can commit it.
_UNKNOWN_OUTCOME_REASONS = {
    FAIL_REASON.LEADER_CHANGED: "leader_changed",
    FAIL_REASON.UNKNOWN_OUTCOME: "unknown_outcome",
    "Timeout": "timeout",
}

_STATE_NAMES = {
    _RAFT_STATE.FOLLOWER: "follower",
    _RAFT_STATE.CANDIDATE: "candidate",
    _RAFT_STATE.LEADER: "leader",
}

JOURNAL_FILE_NAME = "raft.journal"
SNAPSHOT_FILE_NAME = "raft.snapshot"
READY_BARRIER_KEY = ""  # is_ready() reads this key through the log. Its value does not matter.
STATUS_READ_ATTEMPTS = 3


def build_syncobj_conf(config: NodeConfig) -> SyncObjConf:
    timings = config.timings
    data_files: dict[str, str] = {}
    if config.data_dir is not None:
        config.data_dir.mkdir(parents=True, exist_ok=True)
        data_files = {
            "journalFile": str(config.data_dir / JOURNAL_FILE_NAME),
            "fullDumpFile": str(config.data_dir / SNAPSHOT_FILE_NAME),
        }
    return SyncObjConf(
        bindAddress=config.bind_address,
        raftMinTimeout=timings.election_timeout_min,
        raftMaxTimeout=timings.election_timeout_max,
        appendEntriesPeriod=timings.heartbeat_interval,
        connectionTimeout=timings.connection_timeout,
        connectionRetryTime=timings.connection_retry_interval,
        leaderFallbackTimeout=timings.leader_fallback_timeout,
        # Fail fast with "no_leader" instead of queueing commands while there is no leader.
        commandsWaitLeader=False,
        logCompactionMinEntries=config.log_compaction_min_entries,
        logCompactionMinTime=config.log_compaction_min_seconds,
        # fork() in a process with threads (uvicorn, tests) can deadlock. The cost: the
        # snapshot blocks the Raft thread while it writes, so keep the state small.
        useFork=False,
        **data_files,
    )


class KeyValueNode:
    def __init__(
        self, config: NodeConfig, transport_class: type[TCPTransport] = TCPTransport
    ) -> None:
        self._config = config
        self._caught_up = False
        self._store = ReplicatedKeyValueStore()
        self._sync_obj = SyncObj(
            config.self_address,
            list(config.peer_addresses),
            conf=build_syncobj_conf(config),
            consumers=[self._store],
            transportClass=transport_class,
        )

    @property
    def address(self) -> str:
        return self._config.self_address

    def put(self, key: str, value: str) -> None:
        self._run_command(self._store.put, key, value)

    def delete(self, key: str) -> bool:
        return self._run_command(self._store.delete, key)

    def compare_and_set(self, key: str, expected_value: str | None, new_value: str) -> bool:
        return self._run_command(self._store.compare_and_set, key, expected_value, new_value)

    def get(
        self, key: str, consistency: ReadConsistency = ReadConsistency.LINEARIZABLE
    ) -> str | None:
        if consistency is ReadConsistency.STALE:
            return self._store.read_local(key)
        return self._run_command(self._store.read, key)

    def is_leader(self) -> bool:
        return self._sync_obj._isLeader()

    def leader_address(self) -> str | None:
        leader = self._sync_obj._getLeader()
        return None if leader is None else leader.address

    def has_quorum(self) -> bool:
        """True if this node has open connections to a majority (itself included).

        It checks TCP connections only. A link that loses packets but stays open still counts.
        """
        return bool(self._sync_obj.hasQuorum)

    def is_ready(self) -> bool:
        """True when this node has applied every write that was committed before it started.

        pysyncobj's isReady() alone is not enough. After a full cluster restart, a new leader
        reports its old saved commit index until it commits an entry in its own term. So a
        node can be "ready" and still miss committed writes. One linearizable read fixes it:
        when the read returns, this node has applied every entry before it. The check can
        block for up to the command timeout, so call it from health checks, not from /status.
        """
        if self._caught_up:
            return True
        if not self._sync_obj.isReady():
            return False
        try:
            self._run_command(self._store.read, READY_BARRIER_KEY)
        except KeyValueStoreError:
            return False
        self._caught_up = True
        return True

    def status(self) -> dict[str, Any]:
        raw_status = self._read_raw_status()
        peers = {
            peer_address: {
                "connected": raw_status.get(f"partner_node_status_server_{peer_address}") == 2,
                "match_index": raw_status.get(f"match_idx_server_{peer_address}"),
                "next_index": raw_status.get(f"next_node_idx_server_{peer_address}"),
            }
            for peer_address in self._config.peer_addresses
        }
        return {
            "address": self.address,
            "state": _STATE_NAMES.get(raw_status["state"], str(raw_status["state"])),
            "leader": self.leader_address(),
            "term": raw_status["raft_term"],
            "has_quorum": bool(raw_status["has_quorum"]),
            "ready": self._caught_up,  # set by the first successful is_ready() check
            "commit_index": raw_status["commit_idx"],
            "last_applied": raw_status["last_applied"],
            "log_length": raw_status["log_len"],
            "uptime_seconds": raw_status["uptime"],
            "peers": peers,
        }

    def close(self) -> None:
        """Stop the Raft thread, close the sockets and the journal."""
        self._sync_obj.destroy_synchronous()

    def _read_raw_status(self) -> dict[str, Any]:
        # getStatus() loops over pysyncobj dicts that the Raft thread can change at the same
        # time (for example during a leader change). Then it raises RuntimeError. Try again.
        for _ in range(STATUS_READ_ATTEMPTS - 1):
            try:
                return self._sync_obj.getStatus()
            except RuntimeError:
                continue
        return self._sync_obj.getStatus()

    def _run_command(self, replicated_method: Callable[..., Any], *arguments: Any) -> Any:
        # Fail fast (AGENTS.md, Raft rule 6). Without open connections to a majority, no
        # command can commit. Without this check, a follower whose peers died keeps the dead
        # leader's address and waits the full timeout on every command.
        if not self._sync_obj.hasQuorum:
            raise CommandRejectedError("no_quorum")
        try:
            return replicated_method(
                *arguments, sync=True, timeout=self._config.command_timeout_seconds
            )
        except SyncObjException as error:
            raise _translate_failure(error.errorCode) from error


def _translate_failure(error_code: Any) -> Exception:
    if error_code in _REJECTED_REASONS:
        return CommandRejectedError(_REJECTED_REASONS[error_code])
    if error_code in _UNKNOWN_OUTCOME_REASONS:
        return CommandOutcomeUnknownError(_UNKNOWN_OUTCOME_REASONS[error_code])
    return CommandOutcomeUnknownError(f"unexpected pysyncobj error {error_code!r}")
