"""Peer traffic security: pysyncobj sends pickles, so the password is the only gate."""

import time
from collections.abc import Callable

from kvstore.node import ReadConsistency
from tests.cluster.harness import Cluster, wait_until

PASSWORD = "cluster-secret"
OBSERVATION_SECONDS = 3.0


def test_cluster_with_password_elects_leader_and_replicates(
    make_cluster: Callable[..., Cluster],
):
    """
    Scenario: three nodes share one raft_password.
    Expected: they elect one leader and a write reaches every node.
    Raft lesson: authenticate peer traffic. Raft assumes trusted peers. Real systems use
    mutual TLS, for example etcd --peer-client-cert-auth.
    """
    cluster = make_cluster(raft_password=PASSWORD)
    leader = cluster.wait_for_leader()

    leader.put("greeting", "hello")

    cluster.wait_for_value_everywhere("greeting", "hello")


def test_node_with_wrong_password_cannot_join(make_cluster: Callable[..., Cluster]):
    """
    Scenario: two nodes run with the right password. The third node has a wrong one.
    Expected: the two good nodes elect a leader and commit. The bad node never sees a
    leader and never gets the value.
    Raft lesson: authenticate peer traffic. Raft assumes trusted peers. Real systems use
    mutual TLS, for example etcd --peer-client-cert-auth.
    """
    cluster = make_cluster(raft_password=PASSWORD)
    bad_address = cluster.addresses[2]
    cluster.stop(cluster.node(bad_address))
    bad_node = cluster.start(bad_address, raft_password="wrong-password")
    good_nodes = [node for node in cluster.nodes if node is not bad_node]

    leader = cluster.wait_for_leader(among=good_nodes)
    leader.put("greeting", "hello")

    wait_until(
        lambda: all(node.get("greeting", ReadConsistency.STALE) == "hello" for node in good_nodes),
        "the value on both good nodes",
    )
    deadline = time.monotonic() + OBSERVATION_SECONDS
    while time.monotonic() < deadline:
        assert bad_node.get("greeting", ReadConsistency.STALE) is None
        assert bad_node.leader_address() is None
        time.sleep(0.1)
