package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// diskStallSwitch stalls the log writes of one node until Release.
type diskStallSwitch struct {
	mutex         sync.Mutex
	released      chan struct{} // nil while the disk works
	blockedWrites atomic.Int64
}

// Stall makes every later StoreLog and StoreLogs call block.
func (stall *diskStallSwitch) Stall() {
	stall.mutex.Lock()
	defer stall.mutex.Unlock()
	if stall.released == nil {
		stall.released = make(chan struct{})
	}
}

// Release unblocks the waiting writes. It is safe to call twice.
func (stall *diskStallSwitch) Release() {
	stall.mutex.Lock()
	defer stall.mutex.Unlock()
	if stall.released != nil {
		close(stall.released)
		stall.released = nil
	}
}

func (stall *diskStallSwitch) wait() {
	stall.mutex.Lock()
	released := stall.released
	stall.mutex.Unlock()
	if released == nil {
		return
	}
	stall.blockedWrites.Add(1)
	defer stall.blockedWrites.Add(-1)
	<-released
}

// diskStallLogStore blocks log writes while its switch is stalled. Reads go
// to the inner store.
type diskStallLogStore struct {
	raft.LogStore
	stall *diskStallSwitch
}

func (store *diskStallLogStore) StoreLog(entry *raft.Log) error {
	store.stall.wait()
	return store.LogStore.StoreLog(entry)
}

func (store *diskStallLogStore) StoreLogs(entries []*raft.Log) error {
	store.stall.wait()
	return store.LogStore.StoreLogs(entries)
}

// diskStallSwitches holds one switch per node ID.
type diskStallSwitches struct {
	mutex    sync.Mutex
	switches map[string]*diskStallSwitch
}

func (all *diskStallSwitches) forNode(nodeID string) *diskStallSwitch {
	all.mutex.Lock()
	defer all.mutex.Unlock()
	if all.switches == nil {
		all.switches = map[string]*diskStallSwitch{}
	}
	if all.switches[nodeID] == nil {
		all.switches[nodeID] = &diskStallSwitch{}
	}
	return all.switches[nodeID]
}

func (all *diskStallSwitches) releaseAll() {
	all.mutex.Lock()
	defer all.mutex.Unlock()
	for _, stall := range all.switches {
		stall.Release()
	}
}

func (all *diskStallSwitches) wrap(nodeID string, store raft.LogStore) raft.LogStore {
	return &diskStallLogStore{LogStore: store, stall: all.forNode(nodeID)}
}

// diskStallHoldsFor checks invariant again and again for the whole window
// and fails at the first violation. The test must prove that something does
// NOT happen during the window, so it cannot poll for an end condition.
func diskStallHoldsFor(t *testing.T, window time.Duration, invariant func() error) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if err := invariant(); err != nil {
			t.Fatalf("during the disk stall: %v", err)
		}
		time.Sleep(pollInterval)
	}
}

// Scenario: the leader's disk hangs: every StoreLogs call on the leader
// blocks (a wrapped LogStore). Write, read, and watch the cluster for 6
// election timeouts. Then the disk recovers.
// Expected (observed with hashicorp/raft v1.8.0):
//   - The first write is enqueued and fails at CommandTimeout with
//     OutcomeUnknownError (504). The next write fails at CommandTimeout too.
//   - A linearizable read fails at CommandTimeout with RejectedError (503).
//   - No election starts. The stalled node stays leader in the same term,
//     both followers still name it, and every node reports Ready.
//   - Stale reads on the followers pass even with max_stale.
//   - When the disk recovers, the "outcome unknown" write commits, and the
//     same leader goes on in the same term.
//
// Raft lesson: a hung leader disk stops the whole cluster, and no failover
// happens (issue #503, open; fix PR #683). The leader's main loop is blocked
// inside StoreLogs, so its lease check never runs (issue #666). The heartbeat
// goroutines do not touch the disk, so the followers keep the leader. Health
// checks based on Ready and on last contact cannot see the problem. Only our
// own deadline on every raft future keeps the API from hanging. Watch the
// latency of raft.leader.dispatchLog or of the writes instead.
func TestDiskStallOnLeaderFreezesClusterWithoutElection(t *testing.T) {
	switches := &diskStallSwitches{}
	timing := partitionTiming(500 * time.Millisecond)
	const commandTimeout = time.Second
	const slack = time.Second // scheduling margin under parallel -race load
	cluster := New(t, Options{Timing: timing, CommandTimeout: commandTimeout, WrapLogStore: switches.wrap})
	// Close blocks while a disk hangs, because raft's Shutdown waits for the
	// main loop. Cleanups run last in, first out, so this runs before Close.
	t.Cleanup(switches.releaseAll)

	leader := cluster.WaitForLeader()
	termBefore := leader.Status().Term
	mustPut(t, leader, "before", "ok")
	cluster.WaitForValueEverywhere("before", "ok")

	stall := switches.forNode(leader.ID())
	stall.Stall()
	stuckWriteErrors := make(chan error, 1)
	go func() {
		_, err := leader.Put(context.Background(), "stuck", "written", node.WriteOptions{})
		stuckWriteErrors <- err
	}()
	WaitUntil(t, 5*time.Second, "the leader's main loop blocks in StoreLogs", func() bool {
		return stall.blockedWrites.Load() > 0
	})

	start := time.Now()
	_, nextWriteErr := leader.Put(context.Background(), "next", "x", node.WriteOptions{})
	var rejected *node.RejectedError
	var unknown *node.OutcomeUnknownError
	if !errors.As(nextWriteErr, &rejected) && !errors.As(nextWriteErr, &unknown) {
		t.Fatalf("write during the stall: %v, want RejectedError or OutcomeUnknownError", nextWriteErr)
	}
	if elapsed := time.Since(start); elapsed > commandTimeout+slack {
		t.Fatalf("write during the stall took %s, want at most CommandTimeout %s", elapsed, commandTimeout)
	}
	select {
	case err := <-stuckWriteErrors:
		if !errors.As(err, &unknown) {
			t.Fatalf("the enqueued write during the stall: %v, want OutcomeUnknownError", err)
		}
	case <-time.After(commandTimeout + slack):
		t.Fatalf("the enqueued write did not return within CommandTimeout %s", commandTimeout)
	}

	start = time.Now()
	_, readErr := leader.Get(context.Background(), "before", node.ReadOptions{})
	if !errors.As(readErr, &rejected) {
		t.Fatalf("linearizable read during the stall: %v, want RejectedError", readErr)
	}
	if elapsed := time.Since(start); elapsed > commandTimeout+slack {
		t.Fatalf("linearizable read during the stall took %s, want at most CommandTimeout %s", elapsed, commandTimeout)
	}

	diskStallHoldsFor(t, 6*timing.ElectionTimeout, func() error {
		for _, member := range cluster.Nodes() {
			status := member.Status()
			if status.Term != termBefore || status.LeaderID != leader.ID() || !status.Ready {
				return fmt.Errorf("%s: term %d, leader %q, ready %v (%s): want term %d, leader %s, ready", member.ID(), status.Term, status.LeaderID, status.Ready, status.NotReadyReason, termBefore, leader.ID())
			}
		}
		if !leader.IsLeader() {
			return fmt.Errorf("%s left the leader state", leader.ID())
		}
		return nil
	})
	for _, follower := range cluster.Followers(leader.ID()) {
		read, err := follower.Get(context.Background(), "before", node.ReadOptions{Consistency: node.Stale, MaxStale: timing.HeartbeatTimeout})
		if err != nil || read.Entry.Value != "ok" {
			t.Fatalf("stale read with max_stale on %s during the stall = %+v, %v: want ok", follower.ID(), read, err)
		}
	}

	stall.Release()
	cluster.WaitForValueEverywhere("stuck", "written")
	recovered := cluster.WaitForLeader()
	if recovered.ID() != leader.ID() || recovered.Status().Term != termBefore {
		t.Fatalf("leader after the stall = %s in term %d, want %s in term %d", recovered.ID(), recovered.Status().Term, leader.ID(), termBefore)
	}
	mustPut(t, recovered, "after", "ok")
}
