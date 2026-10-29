package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const ambiguousWriteCommandTimeout = 5 * time.Second

// ambiguousWriteCluster starts a cluster with a 1 s lease. With a short
// lease the replication backoff after lost responses can delay the entry
// past the lease, and then the write is really lost (storage doc Q10).
func ambiguousWriteCluster(t *testing.T) *Cluster {
	return New(t, Options{Timing: partitionTiming(time.Second), CommandTimeout: ambiguousWriteCommandTimeout})
}

// ambiguousWriteWithLostResponses runs write on the leader while the leader
// loses every response from both followers. The followers get and store the
// entry. It returns the log index of the entry and the error of write.
func ambiguousWriteWithLostResponses(t *testing.T, cluster *Cluster, leader *node.Node, write func() error) (uint64, error) {
	t.Helper()
	followers := cluster.Followers(leader.ID())
	// WaitForLeader waited for the leader's Barrier, so no other entry comes first.
	entryIndex := leader.Raft().LastIndex() + 1
	for _, follower := range followers {
		cluster.DropResponses(leader.ID(), follower.ID())
	}
	writeErrors := make(chan error, 1)
	go func() { writeErrors <- write() }()
	WaitUntil(t, 10*time.Second, fmt.Sprintf("both followers stored entry %d", entryIndex), func() bool {
		for _, follower := range followers {
			if follower.Raft().LastIndex() < entryIndex {
				return false
			}
		}
		return true
	})
	select {
	case err := <-writeErrors:
		return entryIndex, err
	case <-time.After(ambiguousWriteCommandTimeout + 2*time.Second):
		t.Fatalf("the write on %s did not return within %s", leader.ID(), ambiguousWriteCommandTimeout+2*time.Second)
		return 0, nil
	}
}

func ambiguousWriteRequireUnknown(t *testing.T, err error) {
	t.Helper()
	var unknown *node.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("write with lost responses: %v, want OutcomeUnknownError (504)", err)
	}
	t.Logf("write with lost responses: %v", err)
}

// Scenario: the leader's requests reach both followers, and both store the
// new entry, but every response back to the leader is lost. Then heal.
// Expected: the leader cannot count a majority. It steps down when its lease
// ends, and the write fails with OutcomeUnknownError (504). The followers
// elect a new leader, which has the entry and commits it. After the heal
// every node has the value, at the log index of the "failed" write.
// Raft lesson: an error after the enqueue does not mean "not written".
// ErrLeadershipLost is "outcome unknown". The API must say 504, not "failed".
func TestAmbiguousWriteLostResponsesStillCommits(t *testing.T) {
	cluster := ambiguousWriteCluster(t)
	leader := cluster.WaitForLeader()

	entryIndex, err := ambiguousWriteWithLostResponses(t, cluster, leader, func() error {
		_, err := leader.Put(context.Background(), "ghost", "written", node.WriteOptions{})
		return err
	})
	ambiguousWriteRequireUnknown(t, err)

	cluster.Heal()
	cluster.WaitForValueEverywhere("ghost", "written")
	read, err := cluster.WaitForLeader().Get(context.Background(), "ghost", node.ReadOptions{})
	if err != nil || read.Entry.ModifyIndex != entryIndex {
		t.Fatalf("linearizable read of ghost = %+v, %v: want ModifyIndex %d (the entry of the failed write)", read, err, entryIndex)
	}
}

// Scenario: the same lost-responses fault. The client sends a CAS write
// (cas = the current ModifyIndex) without an Idempotency-Key, gets "outcome
// unknown", and after the heal retries the same request.
// Expected: the first attempt committed, so the key's ModifyIndex changed.
// The retry fails its CAS check (Applied=false, a 409 cas_failed). The
// current entry holds the client's own value at the index of the first
// attempt: the "conflict" is with the client itself.
// Raft lesson: CAS makes a blind retry safe (no double write), but not
// exact: the client cannot tell its own committed write from a write of
// another client. Read the key, or use an idempotency key.
func TestAmbiguousWriteCASRetryWithoutIdempotencyKeyReportsFalseConflict(t *testing.T) {
	cluster := ambiguousWriteCluster(t)
	leader := cluster.WaitForLeader()
	created := mustPut(t, leader, "balance", "100")
	casIndex := created.Entry.ModifyIndex
	casWrite := node.WriteOptions{CASIndex: &casIndex}

	entryIndex, err := ambiguousWriteWithLostResponses(t, cluster, leader, func() error {
		_, err := leader.Put(context.Background(), "balance", "150", casWrite)
		return err
	})
	ambiguousWriteRequireUnknown(t, err)

	cluster.Heal()
	newLeader := cluster.WaitForLeader()
	retry, err := newLeader.Put(context.Background(), "balance", "150", casWrite)
	if err != nil {
		t.Fatalf("retry on %s: %v", newLeader.ID(), err)
	}
	if retry.Applied || retry.Duplicate {
		t.Fatalf("retry result = %+v: want a CAS failure (Applied=false), because the first attempt committed", retry)
	}
	if retry.Entry == nil || retry.Entry.Value != "150" || retry.Entry.ModifyIndex != entryIndex {
		t.Fatalf("retry result entry = %+v: want the client's own value 150 at index %d", retry.Entry, entryIndex)
	}
}

// Scenario: the same lost-responses fault. The client sends the CAS write
// with an Idempotency-Key, gets "outcome unknown", and after the heal retries
// the same request with the same key.
// Expected: the retry returns the stored result of the first attempt:
// Applied=true, Duplicate=true, the index of the first attempt. The key was
// written once.
// Raft lesson: an idempotency key in the replicated state turns "outcome
// unknown" into a safe retry with the true result (Raft thesis 6.3). The
// dedupe table must be part of the FSM, so every node gives the same answer.
func TestAmbiguousWriteIdempotentRetryReturnsOriginalResult(t *testing.T) {
	cluster := ambiguousWriteCluster(t)
	leader := cluster.WaitForLeader()
	created := mustPut(t, leader, "balance", "100")
	casIndex := created.Entry.ModifyIndex
	idempotentWrite := node.WriteOptions{CASIndex: &casIndex, IdempotencyKey: "transfer-42"}

	entryIndex, err := ambiguousWriteWithLostResponses(t, cluster, leader, func() error {
		_, err := leader.Put(context.Background(), "balance", "150", idempotentWrite)
		return err
	})
	ambiguousWriteRequireUnknown(t, err)

	cluster.Heal()
	newLeader := cluster.WaitForLeader()
	retry, err := newLeader.Put(context.Background(), "balance", "150", idempotentWrite)
	if err != nil {
		t.Fatalf("retry on %s: %v", newLeader.ID(), err)
	}
	if !retry.Applied || !retry.Duplicate || retry.Index != entryIndex {
		t.Fatalf("retry result = %+v: want Applied=true, Duplicate=true, Index=%d (the first attempt)", retry, entryIndex)
	}
	read, err := newLeader.Get(context.Background(), "balance", node.ReadOptions{})
	if err != nil || read.Entry.Value != "150" || read.Entry.ModifyIndex != entryIndex {
		t.Fatalf("linearizable read of balance = %+v, %v: want 150 written once, at index %d", read, err, entryIndex)
	}
}
