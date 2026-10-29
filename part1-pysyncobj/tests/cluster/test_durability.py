from collections.abc import Callable
from pathlib import Path
from typing import Any

from kvstore.node import SNAPSHOT_FILE_NAME, KeyValueNode, ReadConsistency
from tests.cluster.harness import Cluster, wait_until

# pysyncobj compacts the log when it has more entries than this.
COMPACTION_THRESHOLD = 20


def _first_log_index(status: dict[str, Any]) -> int:
    """The index of the oldest entry that is still in the log.

    Correct only when the last log entry is committed (no write in flight).
    """
    return status["commit_index"] - status["log_length"] + 1


def _compacted_status(node: KeyValueNode) -> dict[str, Any] | None:
    status = node.status()
    return status if status["log_length"] <= COMPACTION_THRESHOLD + 1 else None


def test_lagging_follower_catches_up_from_a_snapshot(make_cluster: Callable[..., Cluster]):
    """
    Scenario: compact the log after 20 entries. Stop one follower. Write 30 values, so the
    leader compacts its log. Start the follower again.
    Expected: the leader's log no longer has the entries that the follower missed. The
    follower still gets all 30 values. The logs of both nodes stay short.
    Raft lesson: a log cannot grow forever. pysyncobj saves the state to a snapshot and
    deletes the log entries before it. A follower that needs deleted entries gets the whole
    snapshot instead (Raft's InstallSnapshot). In production, a big state makes snapshots
    slow to write and to send. With useFork=False, the snapshot write blocks the Raft
    thread and stops heartbeats for that time. Keep the state small.
    """
    cluster = make_cluster(log_compaction_min_entries=COMPACTION_THRESHOLD)
    leader = cluster.wait_for_leader()
    lagging_follower = cluster.followers(leader)[0]
    leader.put("before-stop", "seen")
    cluster.wait_for_value_everywhere("before-stop", "seen")
    cluster.stop(lagging_follower)
    # The stopped follower cannot have more entries than the leader had at this moment.
    leader_last_index_at_stop = leader.status()["commit_index"]

    missed_values = {f"missed-{number}": str(number) for number in range(30)}
    for key, value in missed_values.items():
        leader.put(key, value)

    leader_status = wait_until(lambda: _compacted_status(leader), "a compacted leader log")
    assert _first_log_index(leader_status) > leader_last_index_at_stop + 1, (
        "the leader still has the entries that the follower missed, so no snapshot is needed"
    )

    restarted_follower = cluster.start(lagging_follower.address)
    wait_until(restarted_follower.is_ready, "the restarted follower is ready")

    assert restarted_follower.get("before-stop", ReadConsistency.STALE) == "seen"
    for key, value in missed_values.items():
        assert restarted_follower.get(key, ReadConsistency.STALE) == value, key
    wait_until(lambda: _compacted_status(restarted_follower), "a short log on the follower")


def test_full_cluster_restart_keeps_all_committed_data(
    make_cluster: Callable[..., Cluster], tmp_path: Path
):
    """
    Scenario: write 30 values, overwrite one key and delete another, so every node has a
    snapshot and a journal. Stop all 3 nodes. Start them again from their data dirs.
    Expected: a new leader in a higher term. Every node has every committed value,
    the overwrite and the delete.
    Raft lesson: a node must keep its log, its term and its vote on disk. pysyncobj keeps
    them in raft.snapshot, raft.journal and raft.journal.meta. A node that forgets its vote
    can vote twice in one term, and then two leaders can win. Gotcha: pysyncobj saves the
    commit index only once per second. Right after a full restart, a node can be ready
    but still miss recent writes. A linearizable read waits for them. Also, pysyncobj does
    not call fsync, so a power loss can lose recent entries. This test stops nodes cleanly
    and does not show that.
    """
    cluster = make_cluster(log_compaction_min_entries=COMPACTION_THRESHOLD)
    leader = cluster.wait_for_leader()
    expected_values = {f"key-{number}": f"value-{number}" for number in range(30)}
    for key, value in expected_values.items():
        leader.put(key, value)
    leader.put("overwritten", "first")
    leader.put("overwritten", "second")
    leader.put("removed", "temporary")
    assert leader.delete("removed") is True
    term_before_restart = leader.status()["term"]
    wait_until(
        lambda: len(list(tmp_path.rglob(SNAPSHOT_FILE_NAME))) == len(cluster.addresses),
        "a snapshot file on every node",
    )

    for node in cluster.nodes:
        cluster.stop(node)
    for address in cluster.addresses:
        cluster.start(address)

    new_leader = cluster.wait_for_leader()
    assert new_leader.status()["term"] > term_before_restart
    for node in cluster.nodes:
        # A linearizable read goes through the log. When it returns, this node has applied
        # every entry before it, so the stale reads below see all committed data.
        assert node.get("overwritten") == "second"
        for key, value in expected_values.items():
            assert node.get(key, ReadConsistency.STALE) == value, f"{key} on {node.address}"
        assert node.get("removed", ReadConsistency.STALE) is None


def test_ready_after_full_restart_means_all_committed_data_is_applied(cluster: Cluster):
    """
    Scenario: write 10 values. Stop all 3 nodes. Start them again. Wait until each node
    says it is ready.
    Expected: a ready node has every committed value in local memory (stale read).
    Raft lesson: a new leader does not know the real commit index until it commits an
    entry in its own term (Raft paper, section 5.4.2). pysyncobj's own isReady() trusts
    the old saved commit index, so it can say "ready" too early. KeyValueNode.is_ready()
    adds one linearizable read as a barrier. Use such a barrier before a node serves
    stale reads or joins a load balancer.
    """
    leader = cluster.wait_for_leader()
    expected_values = {f"key-{number}": f"value-{number}" for number in range(10)}
    for key, value in expected_values.items():
        leader.put(key, value)

    for node in cluster.nodes:
        cluster.stop(node)
    for address in cluster.addresses:
        cluster.start(address)

    for node in cluster.nodes:
        wait_until(node.is_ready, f"{node.address} is ready")
        for key, value in expected_values.items():
            assert node.get(key, ReadConsistency.STALE) == value, f"{key} on {node.address}"
