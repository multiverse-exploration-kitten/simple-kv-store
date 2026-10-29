import pytest

from tests.e2e.docker_cluster import SERVICES, DockerCluster

pytestmark = [pytest.mark.docker, pytest.mark.timeout(300)]


def test_killed_leader_is_replaced_and_catches_up_after_restart(cluster: DockerCluster, key: str):
    """
    Scenario: kill the leader container with SIGKILL. Write through the 2 survivors.
    Start the old leader again.
    Expected: the survivors elect a new leader. The old data is still there, new writes work.
    The old leader comes back as a follower and has the old and the new data.
    Raft lesson: 2 of 3 is a majority, so the cluster survives one crash. The restarted node
    reads its log from its volume and gets only the missing entries from the leader.
    """
    old_leader = cluster.wait_for_leader()
    survivors = [service for service in SERVICES if service != old_leader]
    assert cluster.put(old_leader, f"{key}-before", "old").status_code == 200

    cluster.kill(old_leader)
    new_leader = cluster.wait_for_leader(among=survivors)

    for survivor in survivors:
        assert cluster.read_value(survivor, f"{key}-before") == "old"
    response = cluster.put(new_leader, f"{key}-after", "new")
    assert response.status_code == 200, response.text

    cluster.start(old_leader)
    # Its log lacks the new entry, so the others do not vote for it.
    assert cluster.wait_for_leader() != old_leader
    assert cluster.status(old_leader)["state"] == "follower"
    cluster.wait_for_value(old_leader, f"{key}-after", "new")
    assert cluster.read_value(old_leader, f"{key}-before", "stale") == "old"


def test_paused_follower_does_not_block_writes_and_catches_up(cluster: DockerCluster, key: str):
    """
    Scenario: freeze one follower container with docker pause. Write through the leader.
    Unpause the follower.
    Expected: the write succeeds while the follower is frozen. After unpause, the follower
    has the value.
    Raft lesson: the leader needs a majority, not every node. A frozen node (long GC pause,
    stalled VM) is only a slow follower. It catches up from the leader when it runs again.
    """
    leader = cluster.wait_for_leader()
    paused_follower, active_follower = [service for service in SERVICES if service != leader]

    cluster.pause(paused_follower)
    response = cluster.put(leader, key, "written-while-paused")
    assert response.status_code == 200, response.text
    assert cluster.read_value(active_follower, key) == "written-while-paused"

    cluster.unpause(paused_follower)
    cluster.wait_for_value(paused_follower, key, "written-while-paused")
