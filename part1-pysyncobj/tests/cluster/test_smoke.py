from kvstore.node import ReadConsistency
from tests.cluster.harness import Cluster


def test_three_nodes_elect_one_leader_and_replicate_a_write(cluster: Cluster):
    """
    Scenario: start 3 healthy nodes. Write once through a follower.
    Expected: one leader. The write is visible on every node.
    Raft lesson: any node accepts a write. pysyncobj forwards it to the leader.
    """
    leader = cluster.wait_for_leader()
    follower = cluster.followers(leader)[0]

    follower.put("greeting", "hello")

    assert leader.get("greeting") == "hello"
    cluster.wait_for_value_everywhere("greeting", "hello")
    assert all(node.get("greeting", ReadConsistency.STALE) == "hello" for node in cluster.nodes)


def test_partition_and_heal_moves_leadership(cluster: Cluster):
    """
    Scenario: cut the leader off from the other two nodes, then heal.
    Expected: the other two elect a new leader and commit a write. After heal, all 3 nodes
    agree on one leader and have the write.
    Raft lesson: a majority can always make progress without the old leader.
    """
    old_leader = cluster.wait_for_leader()
    majority = cluster.isolate(old_leader)

    new_leader = cluster.wait_for_leader(among=majority)
    assert new_leader is not old_leader
    new_leader.put("written-during-partition", "yes")

    cluster.heal()
    cluster.wait_for_leader()
    cluster.wait_for_value_everywhere("written-during-partition", "yes")
