import pytest

from tests.e2e.docker_cluster import SERVICES, DockerCluster

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


def test_full_cluster_restart_keeps_the_data(cluster: DockerCluster, key: str):
    """
    Scenario: write 3 keys. Stop all 3 containers (docker compose stop). Start them again.
    Expected: the cluster elects a leader again, and every node returns every key.
    Raft lesson: each node keeps its Raft log on its own volume, so committed data survives
    a restart of the whole cluster. Note: pysyncobj writes the log to a memory-mapped file
    and does not fsync it. The data survives a process crash, but not a power loss.
    """
    values = {f"{key}-{number}": f"value-{number}" for number in range(3)}
    leader = cluster.wait_for_leader()
    for item_key, value in values.items():
        assert cluster.put(leader, item_key, value).status_code == 200

    cluster.stop()
    assert [cluster.container_state(service) for service in SERVICES] == ["exited"] * 3
    cluster.start()

    cluster.wait_for_leader()
    for service in SERVICES:
        for item_key, value in values.items():
            assert cluster.read_value(service, item_key) == value, f"{service} lost {item_key}"
