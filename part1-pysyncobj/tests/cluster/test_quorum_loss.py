import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from kvstore.config import RaftTimings
from kvstore.errors import CommandOutcomeUnknownError, CommandRejectedError
from kvstore.node import ReadConsistency
from tests.cluster.harness import Cluster, wait_until

COMMAND_TIMEOUT_SECONDS = 2.0
# Time for the Python call around the pysyncobj wait. A slow machine needs more.
CALL_OVERHEAD_MARGIN_SECONDS = 1.0
TEST_TIMINGS = RaftTimings.for_tests()


def assert_fails_within_command_timeout(command: Callable[[], Any]) -> None:
    started = time.monotonic()
    with pytest.raises((CommandRejectedError, CommandOutcomeUnknownError)):
        command()
    elapsed_seconds = time.monotonic() - started
    assert elapsed_seconds < COMMAND_TIMEOUT_SECONDS + CALL_OVERHEAD_MARGIN_SECONDS, (
        f"the command hung for {elapsed_seconds:.2f}s"
    )


@pytest.mark.parametrize("survivor_role", ["leader", "follower"])
def test_majority_down_fails_closed_within_the_command_timeout(
    make_cluster: Callable[..., Cluster], survivor_role: str
):
    """
    Scenario: stop 2 of the 3 nodes. Send a write and a linearizable read to the survivor.
    Expected: both fail with CommandRejectedError (503) or CommandOutcomeUnknownError (504)
    within the command timeout. A stale read still returns the old value.
    The survivor sees the closed TCP connections, so KeyValueNode rejects at once with
    "no_quorum" (503). Without that check, a lone follower keeps the dead leader's address,
    and every command waits the full timeout (504).
    Raft lesson: without a majority, nothing can commit, so the store must fail closed.
    The command timeout turns "hang forever" into an error. A stale read keeps working,
    but its value can be old. Use it only where old data is acceptable.
    """
    cluster = make_cluster(command_timeout_seconds=COMMAND_TIMEOUT_SECONDS)
    leader = cluster.wait_for_leader()
    leader.put("city", "Paris")
    cluster.wait_for_value_everywhere("city", "Paris")
    survivor = leader if survivor_role == "leader" else cluster.followers(leader)[0]
    for node in cluster.nodes:
        if node is not survivor:
            cluster.stop(node)

    assert_fails_within_command_timeout(lambda: survivor.put("city", "Rome"))
    assert_fails_within_command_timeout(lambda: survivor.get("city"))
    assert survivor.get("city", ReadConsistency.STALE) == "Paris"


def test_isolated_leader_cannot_commit_and_steps_down_after_check_quorum(
    make_cluster: Callable[..., Cluster],
):
    """
    Scenario: cut the leader off from both followers. Write on the old leader and on the
    new leader.
    Expected: the followers elect a new leader that commits. The old leader's write times
    out (504). After leader_fallback_timeout the old leader steps down (check-quorum).
    After that it rejects writes at once with "no_leader" (503).
    Raft lesson: for a short time, two nodes think that they are the leader. Only the one
    with a majority can commit, so the data stays safe. But clients of the old leader wait
    and get "outcome unknown". Check-quorum (pysyncobj leaderFallbackTimeout) limits this
    window. The library default is 30 s. Set it to a few election timeouts.
    """
    cluster = make_cluster(command_timeout_seconds=COMMAND_TIMEOUT_SECONDS, timings=TEST_TIMINGS)
    old_leader = cluster.wait_for_leader()
    majority = cluster.isolate(old_leader)
    isolated_at = time.monotonic()

    with ThreadPoolExecutor(max_workers=1) as executor:
        # The old leader is still leader here, so the entry enters its log.
        write_on_old_leader = executor.submit(old_leader.put, "minority-write", "lost")
        new_leader = cluster.wait_for_leader(among=majority)
        new_leader.put("majority-write", "kept")

        wait_until(lambda: not old_leader.is_leader(), "the old leader steps down")
        step_down_seconds = time.monotonic() - isolated_at
        with pytest.raises(CommandOutcomeUnknownError) as stuck_write:
            write_on_old_leader.result()

    assert stuck_write.value.reason == "timeout"
    # Not at once: the old leader waits for leader_fallback_timeout without acks.
    assert step_down_seconds >= TEST_TIMINGS.leader_fallback_timeout - 0.5
    assert old_leader.leader_address() is None

    started = time.monotonic()
    with pytest.raises(CommandRejectedError) as fast_rejection:
        old_leader.put("minority-write", "rejected")
    assert fast_rejection.value.reason == "no_leader"
    assert time.monotonic() - started < COMMAND_TIMEOUT_SECONDS / 2

    assert old_leader.get("majority-write", ReadConsistency.STALE) is None
    assert new_leader.get("majority-write") == "kept"
    assert new_leader.get("minority-write") is None
