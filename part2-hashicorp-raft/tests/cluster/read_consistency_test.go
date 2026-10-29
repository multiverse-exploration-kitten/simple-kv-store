package cluster

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// readConsistencyStartElection sends a TimeoutNow RPC to a node, as a
// leadership transfer does. The node starts an election at once, and the
// other nodes vote for it even while they still know a leader. The sender ID
// is not a cluster member, so no fault rule blocks the RPC.
func readConsistencyStartElection(t *testing.T, target *node.Node) {
	t.Helper()
	sender, err := raft.NewTCPTransport("127.0.0.1:0", nil, 1, time.Second, io.Discard)
	if err != nil {
		t.Fatalf("open the TimeoutNow sender: %v", err)
	}
	defer func() { _ = sender.Close() }()
	request := &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersionMax,
		ID:              []byte("read-consistency-test"),
		Addr:            []byte(sender.LocalAddr()),
	}}
	targetAddress := raft.ServerAddress(target.Status().RaftAddress)
	if err := sender.TimeoutNow(raft.ServerID(target.ID()), targetAddress, request, &raft.TimeoutNowResponse{}); err != nil {
		t.Fatalf("TimeoutNow to %s: %v", target.ID(), err)
	}
}

// Scenario: cut the leader off from both followers. Before its lease ends,
// make one follower start an election at once (a TimeoutNow RPC). That is a
// stand-in for followers that decide faster than the old leader notices, for
// example while the old leader's process is paused. The new leader writes a
// new value. Then read on the old leader, which is still in the leader state.
// Expected:
//   - A "leader" read on the old leader returns the old value. The new value
//     is already committed, so this read is stale.
//   - A "linearizable" read on the old leader fails with RejectedError: its
//     VerifyLeader gets no quorum. It fails within CommandTimeout.
//   - After the heal every node has the new value.
//
// Raft lesson: State() == Leader is not proof of leadership. Two nodes can
// be in the leader state at the same time, in different terms. Only a quorum
// check (VerifyLeader) per read makes the read linearizable. The "leader" mode
// is the Consul "default" mode: fast, but it can be stale.
func TestReadConsistencyIsolatedOldLeaderServesStaleLeaderReads(t *testing.T) {
	timing := partitionTiming(time.Second)
	const commandTimeout = 3 * time.Second
	cluster := New(t, Options{Timing: timing, CommandTimeout: commandTimeout})
	oldLeader := cluster.WaitForLeader()
	mustPut(t, oldLeader, "price", "old")
	cluster.WaitForValueEverywhere("price", "old")

	majority := cluster.Isolate(oldLeader.ID())
	readConsistencyStartElection(t, cluster.Node(majority[0]))
	newLeader := cluster.WaitForLeader(majority...)
	mustPut(t, newLeader, "price", "new")

	leaderRead, err := oldLeader.Get(context.Background(), "price", node.ReadOptions{Consistency: node.LeaderLocal})
	if err != nil {
		t.Fatalf("leader read on the old leader %s: %v. Its lease (%s) ended too early for this scenario", oldLeader.ID(), err, timing.LeaderLeaseTimeout)
	}
	if !leaderRead.Found || leaderRead.Entry.Value != "old" || leaderRead.Meta.ServedBy != oldLeader.ID() {
		t.Fatalf("leader read on the old leader = %+v: want the stale value \"old\" served by %s", leaderRead, oldLeader.ID())
	}
	t.Logf("two nodes in the leader state: %s (term %d) and %s (term %d); the old one served price=%q", oldLeader.ID(), oldLeader.Status().Term, newLeader.ID(), newLeader.Status().Term, leaderRead.Entry.Value)

	start := time.Now()
	_, linearizableErr := oldLeader.Get(context.Background(), "price", node.ReadOptions{Consistency: node.Linearizable})
	elapsed := time.Since(start)
	var rejected *node.RejectedError
	if !errors.As(linearizableErr, &rejected) {
		t.Fatalf("linearizable read on the old leader: %v, want RejectedError", linearizableErr)
	}
	if elapsed > commandTimeout+time.Second {
		t.Fatalf("linearizable read on the old leader took %s, want at most CommandTimeout %s", elapsed, commandTimeout)
	}
	t.Logf("linearizable read on the old leader failed after %s: %v", elapsed.Round(time.Millisecond), linearizableErr)

	cluster.Heal()
	cluster.WaitForValueEverywhere("price", "new")
}

// Scenario: cut one follower off in both directions and write a new value
// through the leader. Read on the cut follower with consistency=stale, with
// and without max_stale. Then heal.
// Expected: the stale read on the cut follower returns the old value. When
// the follower has not heard the leader for longer than max_stale, a read
// with max_stale fails with RejectedError "too_stale". The connected follower
// passes the same max_stale check. After the heal the cut follower passes it
// again and returns the new value.
// Raft lesson: a stale read has no freshness bound under a partition.
// max_stale (Consul's max_stale) bounds the time since the last contact with
// the leader. It does not bound how many writes the follower missed.
func TestReadConsistencyStaleReadOnPartitionedFollower(t *testing.T) {
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond)})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "color", "blue")
	cluster.WaitForValueEverywhere("color", "blue")
	followers := cluster.Followers(leader.ID())
	cutFollower, connectedFollower := followers[0], followers[1]

	cluster.Isolate(cutFollower.ID())
	mustPut(t, leader, "color", "green")
	WaitUntil(t, 10*time.Second, "the connected follower applied color=green", func() bool {
		read, err := connectedFollower.Get(context.Background(), "color", node.ReadOptions{Consistency: node.Stale})
		return err == nil && read.Entry.Value == "green"
	})

	staleRead, err := cutFollower.Get(context.Background(), "color", node.ReadOptions{Consistency: node.Stale})
	if err != nil || staleRead.Entry.Value != "blue" {
		t.Fatalf("stale read on the cut follower = %+v, %v: want the old value blue", staleRead, err)
	}

	const maxStale = time.Second
	boundedStale := node.ReadOptions{Consistency: node.Stale, MaxStale: maxStale}
	WaitUntil(t, 10*time.Second, "the cut follower has not heard the leader for more than max_stale", func() bool {
		return cutFollower.Status().LastContactMillis > maxStale.Milliseconds()
	})
	_, err = cutFollower.Get(context.Background(), "color", boundedStale)
	var rejected *node.RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != node.ReasonTooStale {
		t.Fatalf("stale read with max_stale=%s on the cut follower: %v, want RejectedError %q", maxStale, err, node.ReasonTooStale)
	}
	unboundedRead, err := cutFollower.Get(context.Background(), "color", node.ReadOptions{Consistency: node.Stale})
	if err != nil || unboundedRead.Entry.Value != "blue" {
		t.Fatalf("stale read without max_stale on the cut follower = %+v, %v: want blue", unboundedRead, err)
	}
	connectedRead, err := connectedFollower.Get(context.Background(), "color", boundedStale)
	if err != nil || connectedRead.Entry.Value != "green" {
		t.Fatalf("stale read with max_stale on the connected follower = %+v, %v: want green", connectedRead, err)
	}

	cluster.Heal()
	WaitUntil(t, 10*time.Second, "the healed follower passes max_stale and returns green", func() bool {
		read, err := cutFollower.Get(context.Background(), "color", boundedStale)
		return err == nil && read.Entry.Value == "green"
	})
}
