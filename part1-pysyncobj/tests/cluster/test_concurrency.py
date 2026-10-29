import threading
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple

from kvstore.node import KeyValueNode
from tests.cluster.harness import Cluster

WORKERS_PER_NODE = 2
INCREMENTS_PER_WORKER = 5
BARRIER_TIMEOUT_SECONDS = 10.0


class WorkerResult(NamedTuple):
    successful_cas_calls: int
    failed_cas_calls: int


def test_concurrent_cas_increments_lose_no_updates(cluster: Cluster):
    """
    Scenario: 6 threads, 2 on each node, increment one counter at the same time. Each
    increment reads the counter and calls compare-and-set. On a conflict it reads again
    and retries.
    Expected: some CAS calls fail because of conflicts. The final value equals the number
    of successful CAS calls (no lost updates). Every node has the same value.
    Raft lesson: Raft orders the commands, but "read, then write" from a client is two
    commands, and another write can come in between. CAS checks the expected value inside
    the apply function, so the check and the write are one log entry. Use CAS for every
    read-modify-write. A plain put here would lose updates.
    """
    cluster.wait_for_leader()
    cluster.nodes[0].put("counter", "0")
    worker_nodes = [node for node in cluster.nodes for _ in range(WORKERS_PER_NODE)]
    start_together = threading.Barrier(len(worker_nodes), timeout=BARRIER_TIMEOUT_SECONDS)

    def increment_with_cas(node: KeyValueNode) -> WorkerResult:
        start_together.wait()
        successful_cas_calls = 0
        failed_cas_calls = 0
        while successful_cas_calls < INCREMENTS_PER_WORKER:
            current_value = node.get("counter")
            if node.compare_and_set("counter", current_value, str(int(current_value) + 1)):
                successful_cas_calls += 1
            else:
                failed_cas_calls += 1
        return WorkerResult(successful_cas_calls, failed_cas_calls)

    with ThreadPoolExecutor(max_workers=len(worker_nodes)) as executor:
        worker_results = list(executor.map(increment_with_cas, worker_nodes))

    total_successful_cas_calls = sum(result.successful_cas_calls for result in worker_results)
    assert sum(result.failed_cas_calls for result in worker_results) > 0, "no conflict happened"
    assert cluster.nodes[0].get("counter") == str(total_successful_cas_calls)
    cluster.wait_for_value_everywhere("counter", str(total_successful_cas_calls))
