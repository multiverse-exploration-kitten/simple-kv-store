import pytest

from tests.e2e.docker_cluster import SERVICES, DockerCluster

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


def test_disconnected_leader_is_replaced_and_rejoins_as_follower(cluster: DockerCluster, key: str):
    """
    Scenario: disconnect the leader container from the Raft network. Write through the other
    two. Connect the old leader again.
    Expected: the other two elect a new leader and accept the write. After reconnect, the old
    leader is a follower and has the new value.
    Raft lesson: the majority side keeps working without the old leader. When the old leader
    comes back, it sees a higher term, steps down and takes the log of the majority.
    """
    old_leader = cluster.wait_for_leader()
    majority = [service for service in SERVICES if service != old_leader]

    # A disconnected container is not reachable on its host port either.
    # So during the partition, talk only to the majority.
    cluster.disconnect(old_leader)
    new_leader = cluster.wait_for_leader(among=majority)
    response = cluster.put(new_leader, key, "written-during-partition")
    assert response.status_code == 200, response.text

    cluster.connect(old_leader)
    # Its log lacks the new entry, so the others do not vote for it.
    assert cluster.wait_for_leader() != old_leader
    assert cluster.status(old_leader)["state"] == "follower"
    cluster.wait_for_value(old_leader, key, "written-during-partition")
