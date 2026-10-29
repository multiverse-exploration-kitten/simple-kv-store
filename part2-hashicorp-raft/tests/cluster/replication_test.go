package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// replicationRequireRejected fails the test unless err is a RejectedError
// with this reason.
func replicationRequireRejected(t *testing.T, err error, reason string) *node.RejectedError {
	t.Helper()
	var rejected *node.RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != reason {
		t.Fatalf("error = %v, want a RejectedError with reason %s", err, reason)
	}
	return rejected
}

// replicationLocalEntries returns every entry in the local FSM of a node.
func replicationLocalEntries(t *testing.T, member *node.Node) []fsm.Entry {
	t.Helper()
	listed, err := member.List(context.Background(), "", node.ReadOptions{Consistency: node.Stale})
	if err != nil {
		t.Fatalf("stale list on %s: %v", member.ID(), err)
	}
	return listed.Entries
}

// replicationWaitForAppliedIndex waits until the FSM of every running node
// applied exactly up to index. Use it after the last write of a test.
func replicationWaitForAppliedIndex(t *testing.T, cluster *Cluster, index uint64) {
	t.Helper()
	WaitUntil(t, electionWaitTimeout, fmt.Sprintf("FSM last applied index %d on every running node", index), func() bool {
		for _, member := range cluster.Nodes() {
			if member.Status().LastAppliedIndex != index {
				return false
			}
		}
		return true
	})
}

// replicationRequireSameState fails the test unless every node has the same
// entries (with their indexes) and the same last applied index as reference.
func replicationRequireSameState(t *testing.T, reference *node.Node, members []*node.Node) {
	t.Helper()
	wantEntries := replicationLocalEntries(t, reference)
	wantIndex := reference.Status().LastAppliedIndex
	for _, member := range members {
		if gotEntries := replicationLocalEntries(t, member); !slices.Equal(gotEntries, wantEntries) {
			t.Fatalf("%s has entries %+v, want the entries of %s %+v", member.ID(), gotEntries, reference.ID(), wantEntries)
		}
		if gotIndex := member.Status().LastAppliedIndex; gotIndex != wantIndex {
			t.Fatalf("%s applied up to index %d, %s up to %d", member.ID(), gotIndex, reference.ID(), wantIndex)
		}
	}
}

// Scenario: send a put and a delete to each follower.
// Expected: every call fails with RejectedError{Reason: not_leader} that names
// the leader's ID and Raft address. The key does not exist on the leader.
// Raft lesson: only the leader appends to the log, and hashicorp/raft does not
// forward writes. The node returns the leader it knows, so the HTTP layer can
// forward the request once. not_leader means "known not applied", so a retry
// is safe (HTTP 503).
func TestReplicationWriteOnFollowerIsRejectedWithLeaderID(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	leader := cluster.WaitForLeader()
	leaderAddress := leader.Status().RaftAddress
	const key = "written-on-follower"

	for _, follower := range cluster.Followers(leader.ID()) {
		_, putErr := follower.Put(context.Background(), key, "never", node.WriteOptions{})
		_, deleteErr := follower.Delete(context.Background(), key, node.WriteOptions{})
		for _, call := range []struct {
			operation string
			err       error
		}{{"put", putErr}, {"delete", deleteErr}} {
			rejected := replicationRequireRejected(t, call.err, node.ReasonNotLeader)
			if rejected.LeaderID != leader.ID() || rejected.LeaderAddress != leaderAddress {
				t.Fatalf("%s on %s names leader %q at %q, want %s at %s", call.operation, follower.ID(), rejected.LeaderID, rejected.LeaderAddress, leader.ID(), leaderAddress)
			}
		}
	}

	read, err := leader.Get(context.Background(), key, node.ReadOptions{})
	if err != nil || read.Found {
		t.Fatalf("linearizable read on the leader = %+v, %v: want not found, the rejected writes must not apply", read, err)
	}
}

// Scenario: stop one follower, then write 20 keys through the leader.
// Expected: every write commits and applies. The running follower applies all
// of them, and a linearizable read on the leader still works.
// Raft lesson: an entry commits when a majority (2 of 3) stored it, so one
// failed node of 3 does not stop writes or linearizable reads. This is why
// clusters have an odd size: 4 nodes also survive only one failure.
func TestReplicationOneFollowerDownWritesStillCommit(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	leader := cluster.WaitForLeader()
	followers := cluster.Followers(leader.ID())
	running := followers[1]
	cluster.Stop(followers[0].ID())

	const writeCount = 20
	var last node.WriteResult
	for number := range writeCount {
		last = mustPut(t, leader, fmt.Sprintf("one-down-%02d", number), fmt.Sprintf("value-%02d", number))
	}
	replicationWaitForAppliedIndex(t, cluster, last.Index)
	if count := len(replicationLocalEntries(t, running)); count != writeCount {
		t.Fatalf("running follower %s has %d keys, want %d", running.ID(), count, writeCount)
	}
	read, err := leader.Get(context.Background(), "one-down-00", node.ReadOptions{})
	if err != nil || !read.Found {
		t.Fatalf("linearizable read with one follower down = %+v, %v", read, err)
	}
}

// Scenario: stop a follower, write 30 keys, then start the follower again on
// its old data dir.
// Expected: the follower still has its old log entries on disk. It gets the
// 30 missing entries from the leader's log and ends with the same entries and
// the same last applied index as the leader. No snapshot is sent.
// Raft lesson: the leader keeps a nextIndex per follower and sends the entries
// from there. With the default policy (a snapshot every 8192 entries, 10240
// trailing logs) the leader's log still has every missing entry, so a short
// outage costs only those entries, not a full snapshot.
func TestReplicationRestartedFollowerCatchesUpFromLog(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "before-stop", "on-disk")
	cluster.WaitForValueEverywhere("before-stop", "on-disk")
	followerID := cluster.Followers(leader.ID())[0].ID()
	lastIndexBeforeStop := cluster.Node(followerID).Status().LastLogIndex
	cluster.Stop(followerID)

	var last node.WriteResult
	for number := range 30 {
		last = mustPut(t, leader, fmt.Sprintf("missed-%02d", number), fmt.Sprintf("value-%02d", number))
	}

	restarted := cluster.Start(followerID)
	if lastIndex := restarted.Status().LastLogIndex; lastIndex < lastIndexBeforeStop {
		t.Fatalf("restarted follower has last log index %d, want at least %d from its disk", lastIndex, lastIndexBeforeStop)
	}
	replicationWaitForAppliedIndex(t, cluster, last.Index)
	replicationRequireSameState(t, leader, []*node.Node{restarted})
	if snapshotIndex := restarted.Status().LastSnapshotIndex; snapshotIndex != 0 {
		t.Fatalf("restarted follower has snapshot index %d, want 0: it must catch up from the log", snapshotIndex)
	}
	if strings.Contains(cluster.Logs(followerID), "Installed remote snapshot") {
		t.Fatal("the restarted follower installed a snapshot, want catch-up from the log only")
	}
}

// Scenario: through the leader, one call after the other: put one key 5
// times, put 4 other keys, delete one of them.
// Expected: every write returns a Raft index above the index of the write
// before. A put's ModifyIndex is its index. The key written 5 times keeps the
// CreateIndex of its first write. After the writes, the FSM of every node has
// the index of the last write as its last applied index, and the same entries.
// Raft lesson: the log index is one total order of all writes, so it can be
// the version that CAS compares (Consul's ModifyIndex). Deterministic apply
// gives every node the same state. Use the FSM's own last applied index as
// proof of apply: raft's AppliedIndex() counts entries sent to the FSM.
func TestReplicationWriteIndexesGrowAndAllNodesApplySameIndex(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	leader := cluster.WaitForLeader()
	var previousIndex uint64
	requireGrowingIndex := func(operation string, result node.WriteResult) {
		t.Helper()
		if result.Index <= previousIndex {
			t.Fatalf("%s returned index %d, want an index above %d", operation, result.Index, previousIndex)
		}
		if result.Entry != nil && result.Entry.ModifyIndex != result.Index {
			t.Fatalf("%s: ModifyIndex %d, want the write index %d", operation, result.Entry.ModifyIndex, result.Index)
		}
		previousIndex = result.Index
	}

	first := mustPut(t, leader, "versioned", "v0")
	requireGrowingIndex("first put of versioned", first)
	if first.Entry.CreateIndex != first.Index {
		t.Fatalf("first put: CreateIndex %d, want the write index %d", first.Entry.CreateIndex, first.Index)
	}
	for version := 1; version < 5; version++ {
		updated := mustPut(t, leader, "versioned", fmt.Sprintf("v%d", version))
		requireGrowingIndex(fmt.Sprintf("put %d of versioned", version), updated)
		if updated.Entry.CreateIndex != first.Index {
			t.Fatalf("put %d of versioned: CreateIndex %d, want %d from the first put", version, updated.Entry.CreateIndex, first.Index)
		}
	}
	for number := range 4 {
		requireGrowingIndex(fmt.Sprintf("put other-%d", number), mustPut(t, leader, fmt.Sprintf("other-%d", number), "value"))
	}
	deleted, err := leader.Delete(context.Background(), "other-0", node.WriteOptions{})
	if err != nil || !deleted.Applied || !deleted.Existed {
		t.Fatalf("delete other-0 = %+v, %v: want applied, existed", deleted, err)
	}
	requireGrowingIndex("delete other-0", deleted)

	replicationWaitForAppliedIndex(t, cluster, deleted.Index)
	replicationRequireSameState(t, leader, cluster.Followers(leader.ID()))
	if keyCount := len(replicationLocalEntries(t, leader)); keyCount != 4 {
		t.Fatalf("leader has %d keys, want 4 (versioned, other-1 to other-3)", keyCount)
	}
}
