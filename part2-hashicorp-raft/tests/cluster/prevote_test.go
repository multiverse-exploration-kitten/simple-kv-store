package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const preVoteWritesDuringIsolation = 5

// preVoteIsolateAndWrite cuts one follower off in both directions and
// writes through the leader, so the cut follower's log falls behind. It
// returns the cut follower's ID.
func preVoteIsolateAndWrite(t *testing.T, cluster *Cluster, leaderID string) string {
	t.Helper()
	isolatedID := cluster.Followers(leaderID)[0].ID()
	cluster.Isolate(isolatedID)
	leader := cluster.Node(leaderID)
	for writeNumber := range preVoteWritesDuringIsolation {
		mustPut(t, leader, fmt.Sprintf("during-isolation-%d", writeNumber), "x")
	}
	return isolatedID
}

func preVoteLastKey() string {
	return fmt.Sprintf("during-isolation-%d", preVoteWritesDuringIsolation-1)
}

// Scenario: cut one follower off in both directions while the majority
// writes. The cut follower hears no leader and campaigns again and again.
// Then heal.
// Expected: every campaign is a pre-vote that fails, so the cut follower
// stays in the old term. After the heal it catches up, and the same leader
// leads in the same term.
// Raft lesson: pre-vote stops the "disruptive rejoin". A node asks first
// whether it could win, and raises its term only then.
//
// The timeouts are 500 ms: during the cut the leader keeps its lease with
// one follower only, and a test process paused for 230 ms under -race load.
func TestPreVoteDisruptiveRejoinKeepsLeaderAndTerm(t *testing.T) {
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond)})
	leader := cluster.WaitForLeader()
	termBefore := leader.Status().Term
	isolatedID := preVoteIsolateAndWrite(t, cluster, leader.ID())

	const failedRounds = 3
	WaitUntil(t, 30*time.Second, "the cut follower lost 3 pre-vote rounds", func() bool {
		return strings.Count(cluster.Logs(isolatedID), "pre-vote campaign failed") >= failedRounds
	})
	if isolatedTerm := cluster.Node(isolatedID).Status().Term; isolatedTerm != termBefore {
		t.Fatalf("the cut follower %s is in term %d after %d failed pre-votes, want %d", isolatedID, isolatedTerm, failedRounds, termBefore)
	}

	cluster.Heal()
	cluster.WaitForValueEverywhere(preVoteLastKey(), "x")
	healedLeader := cluster.WaitForLeader()
	if healedLeader.ID() != leader.ID() {
		t.Fatalf("leader after the heal = %s, want %s", healedLeader.ID(), leader.ID())
	}
	for _, member := range cluster.Nodes() {
		if term := member.Status().Term; term != termBefore {
			t.Fatalf("%s is in term %d after the heal, want %d", member.ID(), term, termBefore)
		}
	}
}

// Scenario: the same cut with PreVoteDisabled. The cut follower runs real
// elections: each one raises its term. Then heal.
// Expected: the cut follower climbs at least 3 terms while the majority
// stays in its term. After the heal the leader steps down: the cut node
// answers with its higher term. The next leader's term is at least the cut
// node's term. The cut node cannot win itself: its log is behind.
// Raft lesson: without pre-vote a node that comes back from a partition
// forces an election and a short time without a leader. Leader stickiness
// (votes are refused while a leader is known) does not help: the step-down
// comes from the higher term in a replication response.
func TestPreVoteDisabledDisruptiveRejoinForcesNewElection(t *testing.T) {
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond), PreVoteDisabled: true})
	leader := cluster.WaitForLeader()
	termBefore := leader.Status().Term
	isolatedID := preVoteIsolateAndWrite(t, cluster, leader.ID())
	isolated := cluster.Node(isolatedID)

	const termJump = 3
	WaitUntil(t, 30*time.Second, fmt.Sprintf("the cut follower climbs from term %d to %d", termBefore, termBefore+termJump), func() bool {
		return isolated.Status().Term >= termBefore+termJump
	})
	isolatedTermAtHeal := isolated.Status().Term
	if !leader.IsLeader() || leader.Status().Term != termBefore {
		t.Fatalf("the majority changed during the cut: %s state %s term %d, want leader in term %d", leader.ID(), leader.Status().State, leader.Status().Term, termBefore)
	}

	cluster.Heal()
	cluster.WaitForValueEverywhere(preVoteLastKey(), "x")
	healedLeader := cluster.WaitForLeader()
	termAfter := healedLeader.Status().Term
	if termAfter < isolatedTermAtHeal {
		t.Fatalf("leader %s after the heal is in term %d, want at least the cut node's term %d", healedLeader.ID(), termAfter, isolatedTermAtHeal)
	}
	if healedLeader.ID() == isolatedID {
		t.Fatalf("the cut node %s won with a log that is behind", isolatedID)
	}
	t.Logf("term %d before the cut, cut node at term %d at the heal, leader %s in term %d after the heal (before: %s)", termBefore, isolatedTermAtHeal, healedLeader.ID(), termAfter, leader.ID())
}
