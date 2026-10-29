package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/httpapi"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// Scenario: one follower's disk hangs. Its StoreLogs blocks, so its raft
// main loop is stuck in the last AppendEntries. The leader and the other
// follower still commit. Send a write to the busy follower, first to its
// HTTP API and then to the node itself.
// Expected: the HTTP layer forwards the write to the leader, and the client
// gets 200 with the Raft index. The leader has the value. The node itself
// answers RejectedError "not_leader" with the leader's ID at once, also for a
// membership change and a leadership transfer. When the disk recovers, the
// busy follower applies the value too.
// Raft lesson: raft.Apply on a follower does not fail at once. It waits for
// the main loop to answer ErrNotLeader. A busy main loop does not answer, so
// the call ends with ErrEnqueueTimeout (503 "enqueue_timeout"), which looks
// like a busy leader, and nothing forwards the write. Check raft.State()
// before Apply: it is an atomic read that does not need the main loop.
func TestBusyFollowerWriteIsForwardedToLeader(t *testing.T) {
	stalls := &diskStallSwitches{}
	cluster := New(t, Options{Timing: httpStableTiming(), WrapLogStore: stalls.wrap})
	// raft cannot shut down while its StoreLogs blocks. Cleanups run last in,
	// first out, so this runs before the harness stops the nodes.
	t.Cleanup(stalls.releaseAll)
	leader := cluster.WaitForLeader()
	servers := httpStartServers(t, cluster, httpapi.Options{HoldTimeout: 2 * time.Second, ForwardTimeout: 5 * time.Second})
	busyFollower := cluster.Followers(leader.ID())[0]

	busyDisk := stalls.forNode(busyFollower.ID())
	busyDisk.Stall()
	mustPut(t, leader, "before", "stall")
	WaitUntil(t, 5*time.Second, "the busy follower's main loop blocks in StoreLogs", func() bool {
		return busyDisk.blockedWrites.Load() > 0
	})

	const value = "through-busy-follower"
	written := httpCall(t, "PUT", servers[busyFollower.ID()].URL+"/kv/forwarded", `{"value":"`+value+`"}`)
	if written.status != 200 || written.body["value"] != value || written.number("index") == 0 {
		t.Fatalf("PUT through the busy follower %s: status %d body %v, want 200 from the leader", busyFollower.ID(), written.status, written.body)
	}
	read, err := leader.Get(context.Background(), "forwarded", node.ReadOptions{})
	if err != nil || read.Entry.Value != value {
		t.Fatalf("linearizable read on the leader = %+v, %v: want %q", read, err, value)
	}

	started := time.Now()
	_, err = busyFollower.Put(context.Background(), "direct", "write", node.WriteOptions{})
	rejected := replicationRequireRejected(t, err, node.ReasonNotLeader)
	if rejected.LeaderID != leader.ID() {
		t.Fatalf("not_leader from the busy follower names leader %q, want %s", rejected.LeaderID, leader.ID())
	}
	t.Logf("the busy follower %s answered not_leader after %s", busyFollower.ID(), time.Since(started))

	// Membership changes and leadership transfers also go through the main loop.
	err = busyFollower.AddNonvoter(context.Background(), "extra-node", "127.0.0.1:1")
	replicationRequireRejected(t, err, node.ReasonNotLeader)
	err = busyFollower.TransferLeadership(context.Background(), "")
	replicationRequireRejected(t, err, node.ReasonNotLeader)

	busyDisk.Release()
	cluster.WaitForValueEverywhere("forwarded", value)
}
