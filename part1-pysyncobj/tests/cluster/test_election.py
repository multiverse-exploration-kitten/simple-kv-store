from typing import Any

from kvstore.node import KeyValueNode, ReadConsistency
from tests.cluster.harness import Cluster, wait_until


def _agreed_statuses(nodes: list[KeyValueNode]) -> list[dict[str, Any]] | None:
    """The statuses of all nodes if exactly one is leader and all agree on it and its term."""
    statuses = [node.status() for node in nodes]
    leader_statuses = [status for status in statuses if status["state"] == "leader"]
    if len(leader_statuses) != 1:
        return None
    leader_status = leader_statuses[0]
    if all(
        status["leader"] == leader_status["address"] and status["term"] == leader_status["term"]
        for status in statuses
    ):
        return statuses
    return None


def test_three_nodes_agree_on_one_leader_and_one_term(cluster: Cluster):
    """
    Scenario: start 3 healthy nodes.
    Expected: exactly one leader and two followers. All 3 nodes name the same leader and
    report the same term.
    Raft lesson: Raft allows at most one leader per term. The term is Raft's logical clock.
    A node that sees a higher term adopts it and becomes a follower. In production, export
    the term as a metric. A term that goes up often means unstable elections: election
    timeouts too short for the network, slow disks or lost packets.
    """
    statuses = wait_until(
        lambda: _agreed_statuses(cluster.nodes), "one leader that all 3 nodes agree on"
    )

    states = sorted(status["state"] for status in statuses)
    assert states == ["follower", "follower", "leader"]
    assert len({status["term"] for status in statuses}) == 1
    assert statuses[0]["term"] >= 1


def test_leader_crash_survivors_elect_new_leader_in_higher_term(cluster: Cluster):
    """
    Scenario: write a value, then stop the leader. Write again through each survivor.
    Expected: the 2 survivors elect a new leader in a higher term. The value written before
    the crash is still there. New writes commit.
    Raft lesson: a write is committed when a majority stores it. A node votes only for a
    candidate whose log is at least as up to date as its own. So the new leader always has
    every committed write. Failover takes about one election timeout. During that time
    writes fail, so clients must retry the writes that were rejected.
    """
    old_leader = cluster.wait_for_leader()
    old_leader.put("before-crash", "kept")
    old_term = old_leader.status()["term"]

    cluster.stop(old_leader)
    survivors = cluster.nodes
    new_leader = cluster.wait_for_leader(among=survivors)

    assert new_leader.address != old_leader.address
    assert new_leader.status()["term"] > old_term
    assert new_leader.get("before-crash") == "kept"

    for survivor in survivors:
        survivor.put(f"after-crash-via-{survivor.address}", "written")
    for survivor in survivors:
        cluster.wait_for_value_everywhere(f"after-crash-via-{survivor.address}", "written")
    assert all(node.get("before-crash", ReadConsistency.STALE) == "kept" for node in survivors)
