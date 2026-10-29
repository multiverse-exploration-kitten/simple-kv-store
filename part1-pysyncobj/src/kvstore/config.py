"""Node configuration. Production reads it from environment variables."""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RaftTimings:
    """Raft timing values in seconds. Each field maps to one pysyncobj SyncObjConf option.

    Rule of thumb: network round trip << heartbeat interval << election timeout.
    """

    election_timeout_min: float = 0.4  # raftMinTimeout
    election_timeout_max: float = 1.4  # raftMaxTimeout
    heartbeat_interval: float = 0.1  # appendEntriesPeriod
    connection_timeout: float = 3.5  # connectionTimeout
    connection_retry_interval: float = 1.0  # connectionRetryTime (library default 5.0)
    # Check-quorum: a leader that cannot reach a majority for this long steps down.
    # The library default is 30 s. A shorter value makes a cut-off leader fail fast.
    leader_fallback_timeout: float = 5.0  # leaderFallbackTimeout

    @classmethod
    def for_tests(cls) -> "RaftTimings":
        """Fast values for in-process clusters on 127.0.0.1."""
        return cls(
            election_timeout_min=0.3,
            election_timeout_max=0.8,
            heartbeat_interval=0.05,
            connection_timeout=1.0,
            connection_retry_interval=0.2,
            leader_fallback_timeout=1.5,
        )


@dataclass(frozen=True)
class NodeConfig:
    self_address: str  # host:port that the peers use to reach this node
    peer_addresses: tuple[str, ...]
    bind_address: str | None = None  # host:port to listen on. None means self_address.
    data_dir: Path | None = None  # None keeps the Raft log in memory only (tests).
    http_host: str = "0.0.0.0"
    http_port: int = 8000
    command_timeout_seconds: float = 3.0
    log_compaction_min_entries: int = 5000
    log_compaction_min_seconds: int = 300
    timings: RaftTimings = field(default_factory=RaftTimings)
    # pysyncobj sends peer messages as pickles. Without a password, anyone who can reach the
    # Raft port can run code on the node. With it, every peer message is encrypted and signed.
    raft_password: str | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "NodeConfig":
        environ = os.environ if environ is None else environ
        self_address = environ.get("KV_SELF_ADDRESS", "").strip()
        if not self_address:
            raise ValueError("KV_SELF_ADDRESS is required, for example 'node1:4321'")
        peer_addresses = tuple(
            address.strip()
            for address in environ.get("KV_PEERS", "").split(",")
            if address.strip() and address.strip() != self_address
        )
        data_dir = environ.get("KV_DATA_DIR", "").strip()
        return cls(
            self_address=self_address,
            peer_addresses=peer_addresses,
            bind_address=environ.get("KV_BIND_ADDRESS", "").strip() or None,
            data_dir=Path(data_dir) if data_dir else None,
            http_host=environ.get("KV_HTTP_HOST", "0.0.0.0"),
            http_port=_parse_number(environ, "KV_HTTP_PORT", "8000", int),
            command_timeout_seconds=_parse_number(
                environ, "KV_COMMAND_TIMEOUT_SECONDS", "3.0", float
            ),
            raft_password=environ.get("KV_RAFT_PASSWORD", "") or None,
        )


def _parse_number[T: (int, float)](
    environ: Mapping[str, str], name: str, default: str, number_type: type[T]
) -> T:
    raw_value = environ.get(name, default)
    try:
        return number_type(raw_value)
    except ValueError:
        raise ValueError(f"{name} must be a {number_type.__name__}, got {raw_value!r}") from None
