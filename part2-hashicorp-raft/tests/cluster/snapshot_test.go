package cluster

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// snapshotLogStores records the BoltDB log store of each node, so a test can
// read the first index that is still in the log of a node. It does not wrap
// the stores.
type snapshotLogStores struct {
	mutex  sync.Mutex
	byNode map[string]raft.LogStore
}

func (stores *snapshotLogStores) record(nodeID string, store raft.LogStore) raft.LogStore {
	stores.mutex.Lock()
	defer stores.mutex.Unlock()
	stores.byNode[nodeID] = store
	return store
}

func (stores *snapshotLogStores) firstIndex(t *testing.T, nodeID string) uint64 {
	t.Helper()
	stores.mutex.Lock()
	store := stores.byNode[nodeID]
	stores.mutex.Unlock()
	index, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("first log index of %s: %v", nodeID, err)
	}
	return index
}

// Scenario: every node takes a snapshot after 20 new entries and keeps only 10
// entries after it (TrailingLogs). Stop a follower, write 100 keys, wait until
// the leader's log no longer has the entries that the follower needs, then
// start the follower.
// Expected: the leader cannot send the missing entries, so it sends its
// snapshot (InstallSnapshot). The follower logs "Installed remote snapshot".
// Its last snapshot index is at least the last index that the leader deleted
// from its log. Then the follower has the same entries and the same last
// applied index as the leader.
// Raft lesson: log compaction deletes entries that a slow follower still
// needs. Then the leader sends its whole state as one snapshot stream, which
// costs much more than the missing entries. TrailingLogs is the margin for
// slow followers: make it big enough to cover a normal restart.
func TestSnapshotLaggingFollowerCatchesUpThroughInstallSnapshot(t *testing.T) {
	stores := &snapshotLogStores{byNode: map[string]raft.LogStore{}}
	cluster := New(t, Options{
		Timing:       electionStableTiming(),
		Snapshots:    node.SnapshotPolicy{Threshold: 20, Interval: 50 * time.Millisecond, TrailingLogs: 10, Retain: 2},
		WrapLogStore: stores.record,
	})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "before-stop", "replicated")
	cluster.WaitForValueEverywhere("before-stop", "replicated")
	laggingID := cluster.Followers(leader.ID())[0].ID()
	laggingLastIndex := cluster.Node(laggingID).Status().LastLogIndex
	cluster.Stop(laggingID)

	var last node.WriteResult
	for number := range 100 {
		last = mustPut(t, leader, fmt.Sprintf("bulk-%03d", number), fmt.Sprintf("value-%03d", number))
	}
	var leaderFirstIndex uint64
	description := fmt.Sprintf("the leader compacted its log past index %d, the first entry that the stopped follower needs", laggingLastIndex+1)
	WaitUntil(t, electionWaitTimeout, description, func() bool {
		leaderFirstIndex = stores.firstIndex(t, leader.ID())
		return leaderFirstIndex > laggingLastIndex+1
	})

	restarted := cluster.Start(laggingID)
	replicationWaitForAppliedIndex(t, cluster, last.Index)
	if !strings.Contains(cluster.Logs(laggingID), "Installed remote snapshot") {
		t.Fatal("the lagging follower caught up without logging \"Installed remote snapshot\"")
	}
	snapshotIndex := restarted.Status().LastSnapshotIndex
	if snapshotIndex+1 < leaderFirstIndex {
		t.Fatalf("lagging follower has snapshot index %d, want at least %d, the last index that the leader deleted from its log", snapshotIndex, leaderFirstIndex-1)
	}
	t.Logf("follower stopped at index %d, leader log started at %d, follower snapshot index %d, leader snapshot index %d",
		laggingLastIndex, leaderFirstIndex, snapshotIndex, leader.Status().LastSnapshotIndex)
	replicationRequireSameState(t, leader, []*node.Node{restarted})
}
