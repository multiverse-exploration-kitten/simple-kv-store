package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// quorumLossSlack is the scheduling margin over CommandTimeout under
// parallel -race load.
const quorumLossSlack = time.Second

// Scenario: stop both followers, so 1 of 3 nodes runs. Write at once on the
// old leader, then again after it stepped down. Read in all three modes.
// Expected:
//   - The first write fails within CommandTimeout with RejectedError (503) or
//     OutcomeUnknownError (504, the entry was in flight at the step-down).
//   - The node steps down after LeaderLeaseTimeout and is not Ready.
//   - Then a write fails at once with RejectedError "no_leader" (503).
//   - Linearizable and leader reads fail with RejectedError.
//   - A stale read returns the old value and says that no leader is known.
//
// Raft lesson: without a majority the cluster fails closed. No call waits
// forever: the leader lease ends the leadership, and our own deadline ends
// every wait on a raft future. Stale reads still work, but they can be old.
func TestQuorumLossWritesFailFastAndStaleReadsServeOldData(t *testing.T) {
	const commandTimeout = time.Second
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond), CommandTimeout: commandTimeout})
	survivor := cluster.WaitForLeader()
	mustPut(t, survivor, "config", "old")
	cluster.WaitForValueEverywhere("config", "old")

	for _, follower := range cluster.Followers(survivor.ID()) {
		cluster.Stop(follower.ID())
	}

	start := time.Now()
	_, firstErr := survivor.Put(context.Background(), "config", "new", node.WriteOptions{})
	firstElapsed := time.Since(start)
	var rejected *node.RejectedError
	var unknown *node.OutcomeUnknownError
	if !errors.As(firstErr, &rejected) && !errors.As(firstErr, &unknown) {
		t.Fatalf("first write without quorum: %v, want RejectedError (503) or OutcomeUnknownError (504)", firstErr)
	}
	if firstElapsed > commandTimeout+quorumLossSlack {
		t.Fatalf("first write without quorum took %s, want at most CommandTimeout %s", firstElapsed, commandTimeout)
	}
	t.Logf("first write without quorum failed after %s: %v", firstElapsed.Round(time.Millisecond), firstErr)

	WaitUntil(t, 10*time.Second, "the lone node steps down and is not ready", func() bool {
		return !survivor.IsLeader() && survivor.Ready() != nil
	})

	start = time.Now()
	_, secondErr := survivor.Put(context.Background(), "config", "newer", node.WriteOptions{})
	if !errors.As(secondErr, &rejected) || rejected.Reason != node.ReasonNoLeader {
		t.Fatalf("write after the step-down: %v, want RejectedError %q", secondErr, node.ReasonNoLeader)
	}
	if secondElapsed := time.Since(start); secondElapsed >= commandTimeout {
		t.Fatalf("write after the step-down took %s: it must fail fast, not wait for CommandTimeout %s", secondElapsed, commandTimeout)
	}

	for _, consistency := range []node.ReadConsistency{node.Linearizable, node.LeaderLocal} {
		_, readErr := survivor.Get(context.Background(), "config", node.ReadOptions{Consistency: consistency})
		if !errors.As(readErr, &rejected) {
			t.Fatalf("%s read without quorum: %v, want RejectedError", consistency, readErr)
		}
	}

	staleRead, err := survivor.Get(context.Background(), "config", node.ReadOptions{Consistency: node.Stale})
	if err != nil || !staleRead.Found || staleRead.Entry.Value != "old" {
		t.Fatalf("stale read without quorum = %+v, %v: want the old value", staleRead, err)
	}
	if staleRead.Meta.KnownLeader {
		t.Fatalf("stale read meta = %+v: want KnownLeader=false without quorum", staleRead.Meta)
	}
}
