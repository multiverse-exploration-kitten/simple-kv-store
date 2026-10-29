import pytest

from tests.e2e.docker_cluster import SERVICES, DockerCluster

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


def test_write_on_one_node_is_readable_on_the_other_two(cluster: DockerCluster, key: str):
    """
    Scenario: write a key through a follower container.
    Expected: the other two nodes return the value, with a linearizable read and a stale read.
    Raft lesson: any node accepts a write and forwards it to the leader. A linearizable read
    goes through the log, so it is current. A stale read is local memory, so it can lag.
    """
    leader = cluster.wait_for_leader()
    writer = next(service for service in SERVICES if service != leader)
    readers = [service for service in SERVICES if service != writer]

    response = cluster.put(writer, key, "hello")
    assert response.status_code == 200, response.text

    for reader in readers:
        assert cluster.read_value(reader, key, "linearizable") == "hello"
        cluster.wait_for_value(reader, key, "hello", consistency="stale")
