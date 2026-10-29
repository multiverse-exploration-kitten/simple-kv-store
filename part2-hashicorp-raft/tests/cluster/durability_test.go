package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// durabilityStoreGate blocks StoreLogs on every node while it is shut. Votes
// and the current term go to the stable store, which the gate does not touch.
type durabilityStoreGate struct {
	mutex  sync.Mutex
	opened chan struct{} // closed while the gate is open
	// waiting counts the StoreLogs calls that wait at the gate now.
	waiting atomic.Int64
}

func newDurabilityStoreGate() *durabilityStoreGate {
	opened := make(chan struct{})
	close(opened)
	return &durabilityStoreGate{opened: opened}
}

func (gate *durabilityStoreGate) shut() {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	select {
	case <-gate.opened:
		gate.opened = make(chan struct{})
	default:
	}
}

func (gate *durabilityStoreGate) open() {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	select {
	case <-gate.opened:
	default:
		close(gate.opened)
	}
}

func (gate *durabilityStoreGate) pass() {
	gate.mutex.Lock()
	opened := gate.opened
	gate.mutex.Unlock()
	select {
	case <-opened:
		return
	default:
	}
	gate.waiting.Add(1)
	defer gate.waiting.Add(-1)
	<-opened
}

func (gate *durabilityStoreGate) wrap(_ string, store raft.LogStore) raft.LogStore {
	return &durabilityGatedLogStore{LogStore: store, gate: gate}
}

// durabilityGatedLogStore passes every call to the BoltDB store. StoreLogs
// waits at the gate first.
type durabilityGatedLogStore struct {
	raft.LogStore
	gate *durabilityStoreGate
}

func (store *durabilityGatedLogStore) StoreLog(entry *raft.Log) error {
	return store.StoreLogs([]*raft.Log{entry})
}

func (store *durabilityGatedLogStore) StoreLogs(entries []*raft.Log) error {
	store.gate.pass()
	return store.LogStore.StoreLogs(entries)
}

// Scenario: write 10 keys, stop all 3 nodes, and start all 3 again on their
// data dirs. While they start, StoreLogs blocks on every node: the new leader
// cannot store, so it cannot commit, the first entries of its term (its no-op
// and its Barrier). Then StoreLogs works again.
// Expected: the nodes elect a leader. Until an entry of the new term commits,
// every FSM is empty although the 10 writes are on disk, and a linearizable
// read on the leader fails with not_ready. After StoreLogs works again, the
// leader becomes ready and serves all 10 keys with their old indexes. Every
// node applies the same log again and ends at the same last applied index.
// Raft lesson: raft stores (with fsync) an entry before it acknowledges it,
// so acknowledged writes survive a full restart. But a restarted node does not
// apply its log until it learns the commit index, and a new leader learns it
// only when an entry of its own term commits. So a leader must finish a
// Barrier in its term before it serves linearizable reads. Without that check
// it would read an empty or old FSM.
func TestDurabilityFullClusterRestartServesLinearizableReadsOnlyAfterBarrier(t *testing.T) {
	gate := newDurabilityStoreGate()
	cluster := New(t, Options{Timing: electionStableTiming(), WrapLogStore: gate.wrap})
	// Cleanups run in reverse order, so this opens the gate before the harness
	// stops the nodes. raft cannot shut down while its StoreLogs blocks.
	t.Cleanup(gate.open)

	leader := cluster.WaitForLeader()
	written := map[string]fsm.Entry{}
	var lastIndex uint64
	for number := range 10 {
		key := fmt.Sprintf("durable-%02d", number)
		result := mustPut(t, leader, key, fmt.Sprintf("value-%02d", number))
		written[key] = *result.Entry
		lastIndex = result.Index
	}
	for _, id := range cluster.IDs() {
		cluster.Stop(id)
	}

	gate.shut()
	for _, id := range cluster.IDs() {
		cluster.Start(id)
	}
	var newLeader *node.Node
	WaitUntil(t, electionWaitTimeout, "a leader after the full restart, with its first StoreLogs blocked", func() bool {
		newLeader = nil
		for _, member := range cluster.Nodes() {
			if member.IsLeader() {
				newLeader = member
			}
		}
		return newLeader != nil && gate.waiting.Load() > 0
	})
	_, err := newLeader.Get(context.Background(), "durable-00", node.ReadOptions{})
	replicationRequireRejected(t, err, node.ReasonNotReady)
	for _, member := range cluster.Nodes() {
		if status := member.Status(); status.LastAppliedIndex != 0 || status.KeyCount != 0 {
			t.Fatalf("%s applied up to index %d with %d keys before an entry of the new term committed, want an empty FSM", member.ID(), status.LastAppliedIndex, status.KeyCount)
		}
	}

	gate.open()
	readyLeader := cluster.WaitForLeader()
	for key, entry := range written {
		read, err := readyLeader.Get(context.Background(), key, node.ReadOptions{})
		if err != nil || !read.Found || read.Entry != entry {
			t.Fatalf("linearizable read of %s on %s after the restart = %+v, %v: want %+v", key, readyLeader.ID(), read, err, entry)
		}
	}
	replicationWaitForAppliedIndex(t, cluster, lastIndex)
	replicationRequireSameState(t, readyLeader, cluster.Followers(readyLeader.ID()))
}
