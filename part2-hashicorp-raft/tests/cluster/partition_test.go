package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// partitionTiming sets heartbeat, election and lease timeouts to base. Tests
// in which a leader keeps its lease with one follower only use 500 ms or
// more: under parallel -race load the test process can pause for 230 ms, and
// a pause longer than the lease makes the leader step down.
func partitionTiming(base time.Duration) node.Timing {
	return node.Timing{
		HeartbeatTimeout:   base,
		ElectionTimeout:    base,
		LeaderLeaseTimeout: base,
		CommitTimeout:      10 * time.Millisecond,
	}
}

// Scenario: cut the leader off from both followers in both directions. At
// once, send a write to the old leader. The majority elects a new leader and
// writes. Then heal.
// Expected: the old leader accepts the write, because it does not know yet
// that it lost the quorum. It steps down when its lease (LeaderLeaseTimeout)
// ends, and the write fails with OutcomeUnknownError "leadership_lost" before
// CommandTimeout. The majority elects a new leader in a higher term. After the
// heal the old leader drops the uncommitted entry: no node has the key.
// Raft lesson: an isolated leader fails closed after one lease period. Its
// uncommitted entries are overwritten by the new leader's log. The client got
// "outcome unknown", and here the true outcome was "not written".
func TestPartitionIsolatedLeaderStepsDownAndLosesItsWrite(t *testing.T) {
	timing := partitionTiming(time.Second)
	const commandTimeout = 5 * time.Second
	cluster := New(t, Options{Timing: timing, CommandTimeout: commandTimeout})
	oldLeader := cluster.WaitForLeader()
	oldTerm := oldLeader.Status().Term
	mustPut(t, oldLeader, "before", "committed")
	cluster.WaitForValueEverywhere("before", "committed")

	majority := cluster.Isolate(oldLeader.ID())
	isolatedAt := time.Now()
	type writeOutcome struct {
		err     error
		elapsed time.Duration // since the cut
	}
	writeOutcomes := make(chan writeOutcome, 1)
	go func() {
		_, err := oldLeader.Put(context.Background(), "lost-write", "never-committed", node.WriteOptions{})
		writeOutcomes <- writeOutcome{err: err, elapsed: time.Since(isolatedAt)}
	}()

	newLeader := cluster.WaitForLeader(majority...)
	if newLeader.ID() == oldLeader.ID() {
		t.Fatalf("the majority %v elected the isolated node %s", majority, oldLeader.ID())
	}
	if newTerm := newLeader.Status().Term; newTerm <= oldTerm {
		t.Fatalf("new leader %s is in term %d, want a term above %d", newLeader.ID(), newTerm, oldTerm)
	}

	var outcome writeOutcome
	select {
	case outcome = <-writeOutcomes:
	case <-time.After(commandTimeout + 2*time.Second):
		t.Fatalf("the write on the isolated old leader did not return within %s", commandTimeout+2*time.Second)
	}
	var unknown *node.OutcomeUnknownError
	if !errors.As(outcome.err, &unknown) || unknown.Reason != node.ReasonLeadershipLost {
		t.Fatalf("write on the isolated old leader: %v, want OutcomeUnknownError %q", outcome.err, node.ReasonLeadershipLost)
	}
	if outcome.elapsed >= commandTimeout {
		t.Fatalf("the write failed %s after the cut: the step-down should end it before CommandTimeout %s", outcome.elapsed, commandTimeout)
	}
	if oldLeader.IsLeader() {
		t.Fatalf("%s is still leader after its write failed with leadership_lost", oldLeader.ID())
	}
	t.Logf("old leader %s stepped down and failed its write %s after the cut (lease %s); new leader %s", oldLeader.ID(), outcome.elapsed.Round(time.Millisecond), timing.LeaderLeaseTimeout, newLeader.ID())

	mustPut(t, newLeader, "during", "majority")
	cluster.Heal()
	cluster.WaitForValueEverywhere("during", "majority")
	for _, member := range cluster.Nodes() {
		read, err := member.Get(context.Background(), "lost-write", node.ReadOptions{Consistency: node.Stale})
		if err != nil || read.Found {
			t.Fatalf("stale read of lost-write on %s = %+v, %v: want not found on every node", member.ID(), read, err)
		}
	}
}
