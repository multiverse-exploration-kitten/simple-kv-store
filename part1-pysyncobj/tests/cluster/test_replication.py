from kvstore.node import ReadConsistency
from tests.cluster.harness import Cluster, wait_until


def test_write_through_follower_is_forwarded_to_leader_and_applied_everywhere(cluster: Cluster):
    """
    Scenario: send a put and two compare-and-set calls to a follower.
    Expected: the leader's commit index moves forward, so the command went into the
    leader's log. The follower can read its own write at once, also with a stale read.
    Each compare-and-set returns the real result of the committed command. All nodes
    apply the same value.
    Raft lesson: only the leader appends to the log. pysyncobj forwards a command from a
    follower to the leader. A synchronous call returns after the calling node applied the
    committed entry. So the caller reads its own write on the same node. Another
    follower can still be behind for a short time.
    """
    leader = cluster.wait_for_leader()
    follower = cluster.followers(leader)[0]
    leader_commit_before = leader.status()["commit_index"]

    follower.put("city", "lisbon")

    assert leader.status()["commit_index"] > leader_commit_before
    assert follower.get("city", ReadConsistency.STALE) == "lisbon"
    assert follower.compare_and_set("city", "lisbon", "porto") is True
    assert follower.compare_and_set("city", "lisbon", "faro") is False
    cluster.wait_for_value_everywhere("city", "porto")


def test_writes_commit_with_one_follower_down(cluster: Cluster):
    """
    Scenario: stop one follower. Write through the leader and through the other follower.
    Expected: both writes commit, because 2 of 3 nodes are a majority. The leader shows the
    stopped follower as disconnected and behind (its match index is below the commit index).
    Raft lesson: a 3-node cluster tolerates 1 failed node. A 5-node cluster tolerates 2.
    The cluster now runs with no spare node: one more failure stops all writes. In
    production, alert on a disconnected or lagging follower (its match index) before a
    second failure takes the quorum away.
    """
    leader = cluster.wait_for_leader()
    stopped_follower, running_follower = cluster.followers(leader)
    cluster.stop(stopped_follower)

    leader.put("written-via-leader", "one")
    running_follower.put("written-via-follower", "two")

    cluster.wait_for_value_everywhere("written-via-leader", "one")
    cluster.wait_for_value_everywhere("written-via-follower", "two")

    def stopped_peer_seen_as_disconnected() -> dict | None:
        stopped_peer = leader.status()["peers"][stopped_follower.address]
        return None if stopped_peer["connected"] else stopped_peer

    stopped_peer = wait_until(
        stopped_peer_seen_as_disconnected, "the leader sees the stopped follower as disconnected"
    )
    assert stopped_peer["match_index"] < leader.status()["commit_index"]


def test_restarted_follower_catches_up_on_missed_writes(cluster: Cluster):
    """
    Scenario: stop one follower. Write 10 values. Start the follower again from its data dir.
    Expected: the follower becomes ready and has all 10 values that it missed.
    Raft lesson: the leader keeps a next index for each follower. When the follower comes
    back, the leader finds the last entry that both logs share and sends every entry after
    it. The follower first replays its own journal from disk. Do not send reads to a node
    before it is ready (is_ready, /health/ready). Before that, its data can be old.
    """
    leader = cluster.wait_for_leader()
    lagging_follower = cluster.followers(leader)[0]
    leader.put("before-stop", "seen")
    cluster.wait_for_value_everywhere("before-stop", "seen")
    cluster.stop(lagging_follower)

    missed_values = {f"missed-{number}": str(number) for number in range(10)}
    for key, value in missed_values.items():
        leader.put(key, value)

    restarted_follower = cluster.start(lagging_follower.address)
    wait_until(restarted_follower.is_ready, "the restarted follower is ready")

    assert restarted_follower.get("before-stop", ReadConsistency.STALE) == "seen"
    for key, value in missed_values.items():
        assert restarted_follower.get(key, ReadConsistency.STALE) == value, key
