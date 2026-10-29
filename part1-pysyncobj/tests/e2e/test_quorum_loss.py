import time

import pytest

from tests.e2e.docker_cluster import SERVICES, DockerCluster

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]

# The command timeout is 3 s. The extra time covers HTTP and the Docker port proxy.
FAIL_FAST_SECONDS = 5.0


def test_without_majority_writes_fail_fast_and_stale_reads_still_work(
    cluster: DockerCluster, key: str
):
    """
    Scenario: write a key. Stop 2 of the 3 containers. Write and read through the last node.
    Start the 2 nodes again.
    Expected: the write fails with 503 or 504 in less than 5 s. Usually the node sees no
    open connections to a majority and rejects at once (503 "no_quorum"). Just after the
    stop, it can still try and return 504 (outcome unknown). A linearizable read fails in
    the same way. A stale read still returns the old value. After the restart, writes work.
    Raft lesson: without a majority, nothing can commit. The API must fail closed and fast,
    not hang. A 504 write may still commit later, so it is not a failure. A stale read is
    the only read left, and it can be old.
    """
    last_node = cluster.wait_for_leader()
    stopped_nodes = [service for service in SERVICES if service != last_node]
    assert cluster.put(last_node, key, "before").status_code == 200

    cluster.stop(*stopped_nodes)

    write_started = time.monotonic()
    write = cluster.put(last_node, key, "during-outage")
    write_seconds = time.monotonic() - write_started
    assert write.status_code in (503, 504), write.text
    assert write_seconds < FAIL_FAST_SECONDS

    read_started = time.monotonic()
    linearizable_read = cluster.get(last_node, key, "linearizable")
    read_seconds = time.monotonic() - read_started
    assert linearizable_read.status_code in (503, 504), linearizable_read.text
    assert read_seconds < FAIL_FAST_SECONDS

    assert cluster.read_value(last_node, key, "stale") == "before"

    cluster.start(*stopped_nodes)
    cluster.wait_for_leader()
    response = cluster.put(last_node, key, "after")
    assert response.status_code == 200, response.text
    assert cluster.read_value(last_node, key) == "after"
