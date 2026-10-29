package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

func mustPut(t *testing.T, writer *node.Node, key, value string) node.WriteResult {
	t.Helper()
	result, err := writer.Put(context.Background(), key, value, node.WriteOptions{})
	if err != nil {
		t.Fatalf("put %s=%s on %s: %v", key, value, writer.ID(), err)
	}
	if !result.Applied {
		t.Fatalf("put %s=%s on %s was not applied: %+v", key, value, writer.ID(), result)
	}
	return result
}

// Scenario: start 3 nodes that bootstrap with the same configuration.
// Expected: exactly one leader. Every node reports it and the same term.
// Raft lesson: all initial nodes may bootstrap with an identical server list;
// a node that already got the configuration from the leader answers
// ErrCantBootstrap, which is fine.
func TestSmokeThreeNodesElectOneLeader(t *testing.T) {
	cluster := New(t, Options{})
	leader := cluster.WaitForLeader()
	leaderTerm := leader.Status().Term
	for _, member := range cluster.Nodes() {
		status := member.Status()
		if status.LeaderID != leader.ID() || status.Term != leaderTerm {
			t.Fatalf("%s reports leader %q in term %d, want %s in term %d", member.ID(), status.LeaderID, status.Term, leader.ID(), leaderTerm)
		}
	}
}

// Scenario: write one key through the leader.
// Expected: the write returns its Raft index. Every node applies it, and a
// linearizable read on the leader returns it.
// Raft lesson: Apply returns after the entry committed and the leader's FSM
// applied it. Followers apply it after they learn the commit index.
func TestSmokeWriteReplicatesToAllNodes(t *testing.T) {
	cluster := New(t, Options{})
	leader := cluster.WaitForLeader()
	written := mustPut(t, leader, "greeting", "hello")
	if written.Index == 0 || written.Entry.ModifyIndex != written.Index {
		t.Fatalf("write result %+v (entry %+v): want ModifyIndex = Index > 0", written, written.Entry)
	}
	cluster.WaitForValueEverywhere("greeting", "hello")
	read, err := leader.Get(context.Background(), "greeting", node.ReadOptions{})
	if err != nil || !read.Found || read.Entry.Value != "hello" {
		t.Fatalf("linearizable read = %+v, %v", read, err)
	}
	if read.Meta.ServedBy != leader.ID() || read.Meta.Index < written.Index {
		t.Fatalf("read meta = %+v, want served by %s at index >= %d", read.Meta, leader.ID(), written.Index)
	}
}

// Scenario: cut one follower off in both directions, write through the
// leader, then heal.
// Expected: the write commits with 2 of 3 nodes. The cut follower does not
// have it during the partition and catches up after the heal.
// Raft lesson: a majority is enough to commit. A healed follower gets the
// missing entries from the leader's log.
func TestSmokePartitionedFollowerCatchesUpAfterHeal(t *testing.T) {
	cluster := New(t, Options{})
	leader := cluster.WaitForLeader()
	cutFollower := cluster.Followers(leader.ID())[0]
	cluster.Isolate(cutFollower.ID())

	mustPut(t, leader, "during-partition", "committed")
	readOnCut, err := cutFollower.Get(context.Background(), "during-partition", node.ReadOptions{Consistency: node.Stale})
	if err != nil || readOnCut.Found {
		t.Fatalf("cut follower read = %+v, %v: want not found during the partition", readOnCut, err)
	}

	cluster.Heal()
	cluster.WaitForValueEverywhere("during-partition", "committed")
}

// Scenario: block every request from the leader to one follower. That
// follower hears no heartbeats and starts election rounds again and again.
// Its own requests still reach the other nodes. Then heal.
// Expected: every round is a pre-vote that the two others refuse, so no node
// raises its term. After the heal the same leader leads in the same term.
// The log never says "pre-vote is disabled" (the harness checks it).
// Raft lesson: pre-vote stops a cut-off node from forcing a new election.
// raft uses pre-vote only if the transport implements raft.WithPreVote, so a
// fault-injection wrapper must forward it.
//
// The timeouts are 500 ms, not the 100 ms of node.TestTiming: during the cut
// the leader keeps its lease with one follower only, and a test process
// paused for 230 ms under parallel -race load. A pause longer than the lease
// makes the leader step down, which is correct Raft but not this scenario.
func TestSmokePreVoteStaysOnThroughFaultTransport(t *testing.T) {
	cluster := New(t, Options{Timing: node.Timing{
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}})
	leader := cluster.WaitForLeader()
	termBefore := leader.Status().Term
	cutFollower := cluster.Followers(leader.ID())[0]
	cluster.Block(leader.ID(), cutFollower.ID())

	const failedRounds = 3
	WaitUntil(t, 20*time.Second, "the cut follower lost 3 pre-vote rounds", func() bool {
		return strings.Count(cluster.Logs(cutFollower.ID()), "pre-vote campaign failed") >= failedRounds
	})
	for _, member := range cluster.Nodes() {
		if term := member.Status().Term; term != termBefore {
			t.Fatalf("%s is in term %d during the cut, want %d: pre-vote did not stop a real election", member.ID(), term, termBefore)
		}
	}

	cluster.Heal()
	if healedLeader := cluster.WaitForLeader(); healedLeader.ID() != leader.ID() {
		t.Fatalf("leader after heal = %s, want %s", healedLeader.ID(), leader.ID())
	}
	if termAfter := leader.Status().Term; termAfter != termBefore {
		t.Fatalf("term after heal = %d, want %d", termAfter, termBefore)
	}
	if dropped := cluster.Transport(leader.ID()).DroppedRequests.Load(); dropped == 0 {
		t.Fatal("the fault transport dropped no request: the cut did not work")
	}
}
