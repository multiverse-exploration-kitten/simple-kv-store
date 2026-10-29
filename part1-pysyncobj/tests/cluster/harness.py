"""A test harness that runs a real pysyncobj cluster inside the test process."""

import socket
import time
from collections.abc import Callable, Iterable
from pathlib import Path

from kvstore.config import NodeConfig, RaftTimings
from kvstore.node import KeyValueNode
from tests.cluster.faults import NetworkFaults

DEFAULT_WAIT_SECONDS = 15.0
POLL_INTERVAL_SECONDS = 0.02


def wait_until[T](
    probe: Callable[[], T], description: str, timeout: float = DEFAULT_WAIT_SECONDS
) -> T:
    """Call probe until it returns a truthy value. Return that value.

    A failed wait raises AssertionError that names the condition and the last probe error.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return result
        except Exception as error:  # the cluster is in transition, so probes can fail
            last_error = error
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"timed out after {timeout}s waiting for: {description} (last error: {last_error!r})"
    )


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return probe_socket.getsockname()[1]


class Cluster:
    """N nodes on 127.0.0.1. Each node has its own data dir, so it can restart from disk."""

    def __init__(
        self,
        data_root: Path,
        size: int = 3,
        command_timeout_seconds: float = 2.0,
        log_compaction_min_entries: int = 5000,
        timings: RaftTimings | None = None,
        raft_password: str | None = None,
    ) -> None:
        self.faults = NetworkFaults()
        self.addresses = [f"127.0.0.1:{find_free_port()}" for _ in range(size)]
        self._data_root = data_root
        self._command_timeout_seconds = command_timeout_seconds
        self._log_compaction_min_entries = log_compaction_min_entries
        self._timings = timings or RaftTimings.for_tests()
        self._raft_password = raft_password
        self._running_nodes: dict[str, KeyValueNode] = {}
        for address in self.addresses:
            self.start(address)

    # --- node lifecycle ---------------------------------------------------------------

    def start(self, address: str, raft_password: str | None = None) -> KeyValueNode:
        """Start a node. raft_password overrides the cluster password for this node only."""
        assert address not in self._running_nodes, f"{address} is already running"
        config = NodeConfig(
            self_address=address,
            peer_addresses=tuple(peer for peer in self.addresses if peer != address),
            data_dir=self._data_root / address.replace(":", "_"),
            command_timeout_seconds=self._command_timeout_seconds,
            log_compaction_min_entries=self._log_compaction_min_entries,
            timings=self._timings,
            raft_password=raft_password or self._raft_password,
        )
        node = KeyValueNode(config, transport_class=self.faults.transport_class())
        self._running_nodes[address] = node
        return node

    def stop(self, node: KeyValueNode) -> None:
        """Stop a node like a crash of the process. Its data dir stays on disk."""
        self._running_nodes.pop(node.address).close()

    def restart(self, node: KeyValueNode) -> KeyValueNode:
        self.stop(node)
        return self.start(node.address)

    def close(self) -> None:
        for node in list(self._running_nodes.values()):
            self.stop(node)

    @property
    def nodes(self) -> list[KeyValueNode]:
        """The running nodes."""
        return list(self._running_nodes.values())

    def node(self, address: str) -> KeyValueNode:
        return self._running_nodes[address]

    # --- leadership ---------------------------------------------------------------------

    def wait_for_leader(
        self, among: Iterable[KeyValueNode] | None = None, timeout: float = DEFAULT_WAIT_SECONDS
    ) -> KeyValueNode:
        """Wait until exactly one node in the group is leader and the whole group agrees."""
        group = list(among) if among is not None else self.nodes

        def single_agreed_leader() -> KeyValueNode | None:
            leaders = [node for node in group if node.is_leader()]
            if len(leaders) != 1:
                return None
            leader = leaders[0]
            if all(node.leader_address() == leader.address for node in group):
                return leader
            return None

        names = [node.address for node in group]
        return wait_until(single_agreed_leader, f"one agreed leader among {names}", timeout)

    def followers(self, leader: KeyValueNode) -> list[KeyValueNode]:
        return [node for node in self.nodes if node is not leader]

    # --- network faults -----------------------------------------------------------------

    def partition(self, group_a: Iterable[KeyValueNode], group_b: Iterable[KeyValueNode]) -> None:
        self.faults.partition(
            [node.address for node in group_a], [node.address for node in group_b]
        )

    def isolate(self, node: KeyValueNode) -> list[KeyValueNode]:
        """Cut one node off from all others. Return the other nodes."""
        others = [other for other in self.nodes if other is not node]
        self.partition([node], others)
        return others

    def block(self, source: KeyValueNode, destination: KeyValueNode) -> None:
        """One-way fault: messages from source to destination are lost."""
        self.faults.block(source.address, destination.address)

    def heal(self) -> None:
        self.faults.heal()

    # --- data checks --------------------------------------------------------------------

    def wait_for_value_everywhere(
        self, key: str, expected_value: str | None, timeout: float = DEFAULT_WAIT_SECONDS
    ) -> None:
        """Wait until every running node has applied the same value for the key."""
        from kvstore.node import ReadConsistency

        wait_until(
            lambda: all(
                node.get(key, ReadConsistency.STALE) == expected_value for node in self.nodes
            ),
            f"{key}={expected_value!r} on every running node",
            timeout,
        )
