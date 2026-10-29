package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const concurrencyCounterKey = "counter"

// concurrencyCASCounts are the totals of all workers.
type concurrencyCASCounts struct {
	successes atomic.Int64
	conflicts atomic.Int64
}

// concurrencyPutThroughNode sends a write to entry, as a client does. On
// not_leader it sends the write once to the leader that entry names, as the
// HTTP layer forwards it. nodesByID is read only, so goroutines may share it.
func concurrencyPutThroughNode(nodesByID map[string]*node.Node, entry *node.Node, value string, opts node.WriteOptions) (node.WriteResult, error) {
	result, err := entry.Put(context.Background(), concurrencyCounterKey, value, opts)
	var rejected *node.RejectedError
	if !errors.As(err, &rejected) || rejected.Reason != node.ReasonNotLeader {
		return result, err
	}
	leader, found := nodesByID[rejected.LeaderID]
	if !found {
		return result, fmt.Errorf("leader %q named by %s is not in the cluster: %w", rejected.LeaderID, entry.ID(), err)
	}
	return leader.Put(context.Background(), concurrencyCounterKey, value, opts)
}

// concurrencyIncrement adds 1 to the counter increments times with
// read-modify-write and CAS. It reads the counter on entry (a stale read, so
// a follower can return an old value) and retries after a CAS failure.
func concurrencyIncrement(nodesByID map[string]*node.Node, entry *node.Node, increments int, counts *concurrencyCASCounts) error {
	const maxAttemptsPerIncrement = 1000
	for done := 0; done < increments; done++ {
		for attempt := 0; ; attempt++ {
			if attempt == maxAttemptsPerIncrement {
				return fmt.Errorf("via %s: no CAS success in %d attempts", entry.ID(), maxAttemptsPerIncrement)
			}
			current, err := entry.Get(context.Background(), concurrencyCounterKey, node.ReadOptions{Consistency: node.Stale})
			if err != nil || !current.Found {
				return fmt.Errorf("read the counter on %s: found=%v, %v", entry.ID(), current.Found, err)
			}
			value, err := strconv.Atoi(current.Entry.Value)
			if err != nil {
				return fmt.Errorf("counter value %q on %s: %w", current.Entry.Value, entry.ID(), err)
			}
			casIndex := current.Entry.ModifyIndex
			result, err := concurrencyPutThroughNode(nodesByID, entry, strconv.Itoa(value+1), node.WriteOptions{CASIndex: &casIndex})
			if err != nil {
				return fmt.Errorf("CAS write via %s: %w", entry.ID(), err)
			}
			if result.Applied {
				counts.successes.Add(1)
				break
			}
			counts.conflicts.Add(1)
		}
	}
	return nil
}

// Scenario: 6 goroutines increment one counter at the same time, 2 through
// each node. Each one reads the counter on its node (stale read), then
// writes value+1 with cas = the ModifyIndex it read. A write that reaches a
// follower goes on to the leader. After a CAS failure the goroutine reads
// again and retries.
// Expected: the final value equals the number of successful CAS writes, on
// every node. No increment is lost, though many reads were stale.
// Raft lesson: Raft orders the writes, but only CAS makes a read-modify-write
// safe. Without CAS two clients that read the same value both write value+1,
// and one increment is lost (rule 5).
func TestConcurrencyCASIncrementsThroughDifferentNodes(t *testing.T) {
	cluster := New(t, Options{Timing: partitionTiming(500 * time.Millisecond)})
	leader := cluster.WaitForLeader()
	createOnly := uint64(0)
	if _, err := leader.Put(context.Background(), concurrencyCounterKey, "0", node.WriteOptions{CASIndex: &createOnly}); err != nil {
		t.Fatalf("create the counter: %v", err)
	}
	cluster.WaitForValueEverywhere(concurrencyCounterKey, "0")

	const workers = 6
	const incrementsPerWorker = 5
	nodes := cluster.Nodes()
	nodesByID := map[string]*node.Node{}
	for _, member := range nodes {
		nodesByID[member.ID()] = member
	}
	var counts concurrencyCASCounts
	workerErrors := make(chan error, workers)
	var running sync.WaitGroup
	for workerNumber := range workers {
		entry := nodes[workerNumber%len(nodes)]
		running.Add(1)
		go func() {
			defer running.Done()
			workerErrors <- concurrencyIncrement(nodesByID, entry, incrementsPerWorker, &counts)
		}()
	}
	running.Wait()
	close(workerErrors)
	for err := range workerErrors {
		if err != nil {
			t.Fatal(err)
		}
	}

	successes := counts.successes.Load()
	if successes != workers*incrementsPerWorker {
		t.Fatalf("%d successful CAS writes, want %d", successes, workers*incrementsPerWorker)
	}
	final, err := cluster.WaitForLeader().Get(context.Background(), concurrencyCounterKey, node.ReadOptions{})
	if err != nil {
		t.Fatalf("linearizable read of the counter: %v", err)
	}
	if final.Entry.Value != strconv.FormatInt(successes, 10) {
		t.Fatalf("counter = %s after %d successful CAS writes: an increment was lost", final.Entry.Value, successes)
	}
	cluster.WaitForValueEverywhere(concurrencyCounterKey, final.Entry.Value)
	t.Logf("counter = %s: %d CAS successes, %d CAS conflicts", final.Entry.Value, successes, counts.conflicts.Load())
}
