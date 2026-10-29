package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// leaderReadLateCommitTiming: followers learn the commit index only with the
// next AppendEntries. Without new writes the leader sends it after 3-6 s
// (CommitTimeout), so a leader that stops right after a write leaves
// followers that have the entry but did not apply it.
var leaderReadLateCommitTiming = node.Timing{
	HeartbeatTimeout:   500 * time.Millisecond,
	ElectionTimeout:    500 * time.Millisecond,
	LeaderLeaseTimeout: 500 * time.Millisecond,
	CommitTimeout:      3 * time.Second,
}

// Scenario: the leader acknowledges x=v2 and stops before the followers
// learn that v2 committed. One follower's disk hangs, so the new leader
// cannot commit an entry of its term and its Barrier cannot finish. Read x
// on the new leader with consistency=leader, linearizable and stale. Then
// the disk recovers.
// Expected: the stale read returns the old value v1: the new leader has v2
// in its log, but it did not apply it. The "leader" and the linearizable
// reads fail with RejectedError "not_ready" (HTTP 503). After the disk
// recovers, the Barrier finishes and the "leader" read returns v2.
// Raft lesson: a new leader does not know which entries of earlier terms
// are committed until an entry of its own term commits (Raft paper 5.4.2).
// Until then its FSM can miss writes that the old leader acknowledged. So
// even the cheap "leader" read, without a quorum check, must wait for the
// leader's Barrier in the current term.
func TestLeaderReadOnNewLeaderWaitsForBarrier(t *testing.T) {
	stalls := &diskStallSwitches{}
	cluster := New(t, Options{Timing: leaderReadLateCommitTiming, WrapLogStore: stalls.wrap})
	// raft cannot shut down while its StoreLogs blocks. Cleanups run last in,
	// first out, so this runs before the harness stops the nodes.
	t.Cleanup(stalls.releaseAll)
	oldLeader := cluster.WaitForLeader()
	mustPut(t, oldLeader, "x", "v1")
	cluster.WaitForValueEverywhere("x", "v1")

	acknowledged := mustPut(t, oldLeader, "x", "v2")
	followers := cluster.Followers(oldLeader.ID())
	// Both followers must store v2 before the stall: a follower blocked in
	// StoreLogs cannot vote, and then no new leader can win.
	WaitUntil(t, 5*time.Second, "both followers have v2 in their log", func() bool {
		for _, follower := range followers {
			if follower.Status().LastLogIndex < acknowledged.Index {
				return false
			}
		}
		return true
	})
	stalls.forNode(followers[1].ID()).Stall()
	cluster.Stop(oldLeader.ID())

	var newLeader *node.Node
	WaitUntil(t, 10*time.Second, "a new leader among the followers", func() bool {
		for _, follower := range followers {
			if follower.IsLeader() {
				newLeader = follower
				return true
			}
		}
		return false
	})

	stale, err := newLeader.Get(context.Background(), "x", node.ReadOptions{Consistency: node.Stale})
	if err != nil || stale.Entry.Value != "v1" {
		t.Fatalf("stale read on the new leader %s = %+v, %v: want v1, the scenario needs a leader that did not apply v2 yet", newLeader.ID(), stale, err)
	}
	leaderRead, err := newLeader.Get(context.Background(), "x", node.ReadOptions{Consistency: node.LeaderLocal})
	if err == nil {
		t.Fatalf("leader read on the new leader %s before its Barrier returned x=%q, want not_ready: v2 was acknowledged", newLeader.ID(), leaderRead.Entry.Value)
	}
	replicationRequireRejected(t, err, node.ReasonNotReady)
	_, err = newLeader.Get(context.Background(), "x", node.ReadOptions{Consistency: node.Linearizable})
	replicationRequireRejected(t, err, node.ReasonNotReady)

	stalls.releaseAll()
	WaitUntil(t, 20*time.Second, "the new leader finishes its Barrier and its leader read returns v2", func() bool {
		read, err := newLeader.Get(context.Background(), "x", node.ReadOptions{Consistency: node.LeaderLocal})
		return err == nil && read.Entry.Value == "v2"
	})
}
