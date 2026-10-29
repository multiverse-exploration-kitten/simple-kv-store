package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const (
	deadlineCommandTimeout = time.Second
	// deadlineSlack is the scheduling margin under parallel -race load. It
	// is smaller than CommandTimeout, so a call that waits twice fails.
	deadlineSlack = 500 * time.Millisecond
)

// deadlineStall is a cluster whose leader's main loop is stuck in StoreLogs.
type deadlineStall struct {
	cluster *Cluster
	leader  *node.Node
	stall   *diskStallSwitch
}

// startDeadlineStall starts 3 nodes, writes before=ok, stalls the leader's
// disk and sends a first write. That write blocks the leader's main loop in
// StoreLogs, so raft's queues fill up and are never read.
func startDeadlineStall(t *testing.T) deadlineStall {
	t.Helper()
	switches := &diskStallSwitches{}
	// 500 ms: the leader must keep its lease under 230 ms process pauses.
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond), CommandTimeout: deadlineCommandTimeout, WrapLogStore: switches.wrap})
	// Close blocks while a disk hangs, because raft's Shutdown waits for the
	// main loop. Cleanups run last in, first out, so this runs before Close.
	t.Cleanup(switches.releaseAll)

	leader := cluster.WaitForLeader()
	mustPut(t, leader, "before", "ok")
	stall := switches.forNode(leader.ID())
	stall.Stall()
	go func() {
		_, _ = leader.Put(context.Background(), "stuck", "written", node.WriteOptions{})
	}()
	WaitUntil(t, 5*time.Second, "the leader's main loop blocks in StoreLogs", func() bool {
		return stall.blockedWrites.Load() > 0
	})
	return deadlineStall{cluster: cluster, leader: leader, stall: stall}
}

// deadlineCallWithin runs call in a goroutine and fails the test when it
// does not return within limit. A call that ignores its deadline hangs until
// the disk recovers, so the test goroutine must not wait for it directly.
func deadlineCallWithin(t *testing.T, limit time.Duration, description string, call func() error) error {
	t.Helper()
	errs := make(chan error, 1)
	start := time.Now()
	go func() { errs <- call() }()
	select {
	case err := <-errs:
		if elapsed := time.Since(start); elapsed > limit {
			t.Fatalf("%s took %s, want at most %s", description, elapsed, limit)
		}
		return err
	case <-time.After(limit):
		t.Fatalf("%s did not return within %s", description, limit)
		return nil
	}
}

func deadlineWantRejected(t *testing.T, description string, err error, wantReason string) {
	t.Helper()
	var rejected *node.RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != wantReason {
		t.Fatalf("%s: %v, want RejectedError %s (503)", description, err, wantReason)
	}
}

// Scenario: the leader's disk hangs while a write is in StoreLogs, so the
// leader's main loop is stuck. Send 80 linearizable reads to the leader at
// once. raft's verify queue holds 64 requests.
// Expected: every read fails with RejectedError (503) within CommandTimeout
// plus a small margin, also the reads that find the verify queue full.
//
// Raft lesson: some raft calls block before they return a future.
// VerifyLeader blocks when its queue of 64 is full. A deadline on
// future.Error() alone is not enough: the call itself must run under the
// deadline too. Else a stalled leader hangs every read after the 64th until
// the disk recovers.
func TestDeadlineLinearizableReadsDuringLeaderDiskStall(t *testing.T) {
	stalled := startDeadlineStall(t)

	const parallelReads = 80 // more than raft's verify queue of 64
	type readOutcome struct {
		elapsed time.Duration
		err     error
	}
	outcomes := make(chan readOutcome, parallelReads)
	for range parallelReads {
		go func() {
			start := time.Now()
			_, err := stalled.leader.Get(context.Background(), "before", node.ReadOptions{})
			outcomes <- readOutcome{elapsed: time.Since(start), err: err}
		}()
	}
	limit := deadlineCommandTimeout + deadlineSlack
	allReturned := time.After(limit)
	for returned := 0; returned < parallelReads; returned++ {
		select {
		case outcome := <-outcomes:
			var rejected *node.RejectedError
			if !errors.As(outcome.err, &rejected) {
				t.Fatalf("linearizable read during the stall: %v, want RejectedError (503)", outcome.err)
			}
			if outcome.elapsed > limit {
				t.Fatalf("linearizable read during the stall took %s, want at most %s", outcome.elapsed, limit)
			}
		case <-allReturned:
			t.Fatalf("only %d of %d linearizable reads returned within %s, the others hang", returned, parallelReads, limit)
		}
	}
}

// Scenario: the leader's disk hangs while a first write is in StoreLogs, so
// the leader's main loop takes no new entries. Send a second write and a
// membership change to the leader. Then the disk recovers, and the test
// writes once more.
// Expected: the second write and the membership change fail with
// RejectedError enqueue_timeout (503) before CommandTimeout ends. They never
// entered the log: after the disk recovers, the second key does not exist and
// the configuration has no new server.
//
// Raft lesson: raft's enqueue timeout is the only sign that an entry never
// entered the log. It must end before our own deadline. Else the deadline
// fires first, and the client gets "outcome unknown" (504) for a write that
// cannot commit (AGENTS.md Raft rule 6).
func TestDeadlineWriteThatNeverEnteredTheLogIsRejected(t *testing.T) {
	stalled := startDeadlineStall(t)

	// No slack: raft's enqueue timeout must end before our deadline. An
	// answer at the deadline races with it and can be a 504.
	limit := deadlineCommandTimeout
	writeErr := deadlineCallWithin(t, limit, "second write during the stall", func() error {
		_, err := stalled.leader.Put(context.Background(), "second", "x", node.WriteOptions{})
		return err
	})
	deadlineWantRejected(t, "second write during the stall", writeErr, node.ReasonEnqueueTimeout)
	const neverAddedID = "node9"
	membershipErr := deadlineCallWithin(t, limit, "membership change during the stall", func() error {
		return stalled.leader.AddNonvoter(context.Background(), neverAddedID, "127.0.0.1:1")
	})
	deadlineWantRejected(t, "membership change during the stall", membershipErr, node.ReasonEnqueueTimeout)

	stalled.stall.Release()
	stalled.cluster.WaitForValueEverywhere("stuck", "written")
	leader := stalled.cluster.WaitForLeader()
	// A rejected write that entered the log anyway would come before this
	// one, so it would be applied when this Put returns.
	mustPut(t, leader, "after", "ok")
	read, err := leader.Get(context.Background(), "second", node.ReadOptions{Consistency: node.Stale})
	if err != nil || read.Found {
		t.Fatalf("read of the rejected write after the stall = %+v, %v: want not found", read, err)
	}
	servers, err := leader.Configuration()
	if err != nil {
		t.Fatalf("configuration after the stall: %v", err)
	}
	for _, server := range servers {
		if server.ID == neverAddedID {
			t.Fatalf("configuration after the stall has the rejected server %s: %+v", neverAddedID, servers)
		}
	}
}

// Scenario: the leader's disk hangs while a write is in StoreLogs. Ask the
// leader for a snapshot two times. The first snapshot waits for the stuck
// main loop, so raft's snapshot goroutine takes no second request.
// Expected: both calls fail with OutcomeUnknownError timeout (504) within
// CommandTimeout plus a small margin. raft can still take the snapshots after
// the disk recovers.
//
// Raft lesson: raft.Snapshot() blocks until raft's snapshot goroutine takes
// the request, and that goroutine waits for the leader's main loop. Like
// VerifyLeader, the call itself must run under the deadline.
func TestDeadlineSnapshotDuringLeaderDiskStall(t *testing.T) {
	stalled := startDeadlineStall(t)

	limit := deadlineCommandTimeout + deadlineSlack
	for attempt := 1; attempt <= 2; attempt++ {
		err := deadlineCallWithin(t, limit, fmt.Sprintf("snapshot %d during the stall", attempt), func() error {
			return stalled.leader.Snapshot(context.Background())
		})
		var unknown *node.OutcomeUnknownError
		if !errors.As(err, &unknown) || unknown.Reason != node.ReasonTimeout {
			t.Fatalf("snapshot %d during the stall: %v, want OutcomeUnknownError timeout (504)", attempt, err)
		}
	}
}
