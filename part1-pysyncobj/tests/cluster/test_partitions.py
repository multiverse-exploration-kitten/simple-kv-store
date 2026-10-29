import dataclasses
from collections.abc import Callable

import pytest

from kvstore.config import RaftTimings
from kvstore.errors import CommandOutcomeUnknownError, CommandRejectedError
from kvstore.node import ReadConsistency
from tests.cluster.harness import Cluster, wait_until

COMMAND_TIMEOUT_SECONDS = 2.0


def test_heal_discards_the_old_leaders_uncommitted_write(cluster: Cluster):
    """
    Scenario: isolate the leader. It accepts a write that it cannot commit. The majority
    writes another value to the same key. Then heal.
    Expected: the old leader becomes a follower. Its write is discarded. Every node has
    the majority's value.
    Raft lesson: an entry is safe only after a majority stores it. The new leader
    overwrites the uncommitted entry on the old leader. So a 504 can mean "lost", not
    only "late". The client must read the key again (or use CAS) to learn the outcome.
    """
    old_leader = cluster.wait_for_leader()
    old_leader.put("color", "blue")
    cluster.wait_for_value_everywhere("color", "blue")
    majority = cluster.isolate(old_leader)

    with pytest.raises(CommandOutcomeUnknownError):
        old_leader.put("color", "red")
    new_leader = cluster.wait_for_leader(among=majority)
    new_leader.put("color", "green")

    cluster.heal()
    cluster.wait_for_value_everywhere("color", "green")
    healed_leader = cluster.wait_for_leader()
    assert healed_leader is not old_leader
    assert old_leader.leader_address() == healed_leader.address
    assert old_leader.get("color") == "green"


def test_cut_off_follower_serves_stale_reads_but_rejects_linearizable_reads(cluster: Cluster):
    """
    Scenario: isolate a follower. Change a key on the leader.
    Expected: a STALE read on the cut-off follower returns the old value. A LINEARIZABLE
    read on it fails with "no_leader" (503).
    Raft lesson: a local read can be old on any node, also on a node that looks healthy.
    A linearizable read must go through the leader and a majority. pysyncobj has no
    ReadIndex or lease read, so here each linearizable read is a log entry.
    """
    leader = cluster.wait_for_leader()
    leader.put("price", "10")
    cluster.wait_for_value_everywhere("price", "10")
    cut_off_follower = cluster.followers(leader)[0]
    cluster.isolate(cut_off_follower)

    leader.put("price", "20")
    wait_until(
        lambda: cut_off_follower.leader_address() is None,
        "the cut-off follower starts an election and forgets the leader",
    )

    assert cut_off_follower.get("price", ReadConsistency.STALE) == "10"
    with pytest.raises(CommandRejectedError) as rejected_read:
        cut_off_follower.get("price", ReadConsistency.LINEARIZABLE)
    assert rejected_read.value.reason == "no_leader"
    assert leader.get("price", ReadConsistency.LINEARIZABLE) == "20"


def test_write_that_times_out_can_still_commit(make_cluster: Callable[..., Cluster]):
    """
    Scenario: drop only the messages from both followers to the leader (one-way). Write on
    the leader. Then heal. Do not retry the write.
    Expected: the write fails with CommandOutcomeUnknownError (504). After heal, the same
    leader commits it and every node has the value.
    Mechanism: the leader still sends the entry, so each follower stores it. Only the acks
    are lost, so the leader cannot count a majority. The timings are like production
    (command timeout < connection timeout < check-quorum), so the leader keeps its role.
    After heal, the next ack arrives and the leader commits the entry. With the fast test
    timings the leader steps down first, but a new leader still commits the entry,
    because every node has it.
    Raft lesson: a timeout is not a failure. Report it as "outcome unknown" (504), never
    as "failed". A blind retry of a non-idempotent command can apply it twice. Read the
    value again, or use CAS or an idempotency key.
    """
    production_like_timings = dataclasses.replace(
        RaftTimings.for_tests(), connection_timeout=6.0, leader_fallback_timeout=6.0
    )
    cluster = make_cluster(
        command_timeout_seconds=COMMAND_TIMEOUT_SECONDS, timings=production_like_timings
    )
    leader = cluster.wait_for_leader()
    term_before_fault = leader.status()["term"]
    for follower in cluster.followers(leader):
        cluster.block(follower, leader)

    with pytest.raises(CommandOutcomeUnknownError) as unknown_outcome:
        leader.put("payment-42", "charged")
    assert unknown_outcome.value.reason == "timeout"
    assert all(node.get("payment-42", ReadConsistency.STALE) is None for node in cluster.nodes)

    cluster.heal()
    cluster.wait_for_value_everywhere("payment-42", "charged")
    assert cluster.wait_for_leader() is leader
    assert leader.status()["term"] == term_before_fault


def test_rejoining_follower_with_a_higher_term_disrupts_the_leader(cluster: Cluster):
    """
    Scenario: isolate a follower. While it is cut off, it starts election after election.
    Then heal.
    Expected: its term rises, but the majority keeps its leader and term. After heal, its
    higher term forces a new election, and the cluster term moves above it. The rejoining
    node cannot win, because it missed a write.
    Raft lesson: pysyncobj has no pre-vote. A healthy leader steps down when it sees a
    vote request with a higher term. So one flaky node can interrupt the leader on each
    rejoin, and each interruption is a short write outage. etcd/raft fixes this with
    PreVote: a node first asks if it can win, and raises its term only then.
    """
    healthy_leader = cluster.wait_for_leader()
    leader_term = healthy_leader.status()["term"]
    rejoining_follower = cluster.followers(healthy_leader)[0]
    majority = cluster.isolate(rejoining_follower)

    healthy_leader.put("written-while-isolated", "yes")
    wait_until(
        lambda: rejoining_follower.status()["term"] >= leader_term + 3,
        "the isolated follower raises its term with failed elections",
    )
    assert cluster.wait_for_leader(among=majority) is healthy_leader
    assert healthy_leader.status()["term"] == leader_term

    rejoining_term = rejoining_follower.status()["term"]
    cluster.heal()
    wait_until(
        lambda: all(node.status()["term"] > rejoining_term for node in cluster.nodes),
        f"every node moves above the rejoining follower's term {rejoining_term}",
    )
    final_leader = cluster.wait_for_leader()
    assert final_leader is not rejoining_follower
    assert healthy_leader.status()["term"] > leader_term
    cluster.wait_for_value_everywhere("written-while-isolated", "yes")
