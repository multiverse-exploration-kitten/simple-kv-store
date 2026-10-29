package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// electionWaitTimeout bounds the own waits of the election, replication,
// durability, snapshot and transfer tests. It equals the harness wait for
// electionStableTiming (10 s + 20 x 500 ms).
const electionWaitTimeout = 20 * time.Second

// electionStableTiming makes accidental elections unlikely. Under parallel
// -race load the test process can pause for 230 ms. That is longer than the
// 100 ms lease of node.TestTiming, and a leader that misses its lease steps
// down. Tests that need the same leader for more than a few milliseconds use
// this timing.
func electionStableTiming() node.Timing {
	return node.Timing{
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}
}

// electionLeadersByTerm records which nodes were leader in which term.
type electionLeadersByTerm map[uint64]map[string]bool

// observe records the nodes that are leader now. It reads the term before and
// after the state: when both reads are equal, the node was leader in exactly
// that term.
func (leadersByTerm electionLeadersByTerm) observe(members []*node.Node) {
	for _, member := range members {
		termBefore := member.Raft().CurrentTerm()
		isLeader := member.IsLeader()
		if !isLeader || member.Raft().CurrentTerm() != termBefore {
			continue
		}
		if leadersByTerm[termBefore] == nil {
			leadersByTerm[termBefore] = map[string]bool{}
		}
		leadersByTerm[termBefore][member.ID()] = true
	}
}

// electionAgreedLeader returns the leader ID and term when exactly one node is
// leader, every other node is a follower, and every node reports the same
// leader and term. Otherwise it returns "".
func electionAgreedLeader(members []*node.Node) (leaderID string, term uint64) {
	statuses := make([]node.Status, 0, len(members))
	leaderCount := 0
	for _, member := range members {
		status := member.Status()
		statuses = append(statuses, status)
		if status.State == "Leader" {
			leaderCount++
			leaderID, term = status.ID, status.Term
		}
	}
	if leaderCount != 1 {
		return "", 0
	}
	for _, status := range statuses {
		if status.LeaderID != leaderID || status.Term != term {
			return "", 0
		}
		if status.ID != leaderID && status.State != "Follower" {
			return "", 0
		}
	}
	return leaderID, term
}

// Scenario: start 3 or 5 nodes that bootstrap with the same configuration.
// Watch every node while they elect a leader.
// Expected: no term ever has two leaders. Then exactly one node is leader,
// every other node is a follower, and all nodes report the same leader ID and
// the same term.
// Raft lesson: election safety. A candidate needs the votes of a majority,
// and a node votes once per term, so a term has at most one leader. Every
// node learns the leader and the term from the leader's heartbeats.
func TestElectionExactlyOneLeaderThatAllNodesAgreeOn(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d_nodes", size), func(t *testing.T) {
			cluster := New(t, Options{Size: size})
			leadersByTerm := electionLeadersByTerm{}
			var leaderID string
			var leaderTerm uint64
			description := fmt.Sprintf("exactly one leader of %d nodes, the others followers, all with the same leader ID and term", size)
			WaitUntil(t, electionWaitTimeout, description, func() bool {
				members := cluster.Nodes()
				leadersByTerm.observe(members)
				leaderID, leaderTerm = electionAgreedLeader(members)
				return leaderID != ""
			})
			for term, leaders := range leadersByTerm {
				if len(leaders) > 1 {
					t.Fatalf("term %d had %d leaders %v: election safety is broken", term, len(leaders), leaders)
				}
			}
			t.Logf("%s leads term %d; leaders seen per term: %v", leaderID, leaderTerm, leadersByTerm)
		})
	}
}

// Scenario: write a key, then stop the leader like a crash. Later start the
// old leader again.
// Expected: the 2 survivors elect a new leader in a higher term. The new
// leader serves the committed key, with its old index, in a linearizable read
// and accepts new writes. The old leader comes back as a follower and gets the
// new write.
// Raft lesson: 2 of 3 nodes are a majority, so the cluster survives one crash.
// A committed entry is on a majority, and a node wins a vote only if its log
// is at least as new as the voter's log, so the new leader has every
// committed entry.
func TestElectionLeaderStopSurvivorsElectNewLeaderInHigherTerm(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	oldLeader := cluster.WaitForLeader()
	oldLeaderID := oldLeader.ID()
	oldTerm := oldLeader.Status().Term
	committed := mustPut(t, oldLeader, "before-crash", "committed")

	cluster.Stop(oldLeaderID)
	var survivors []string
	for _, id := range cluster.IDs() {
		if id != oldLeaderID {
			survivors = append(survivors, id)
		}
	}
	newLeader := cluster.WaitForLeader(survivors...)
	newTerm := newLeader.Status().Term
	if newTerm <= oldTerm {
		t.Fatalf("new leader %s is in term %d, want a term above %d", newLeader.ID(), newTerm, oldTerm)
	}

	read, err := newLeader.Get(context.Background(), "before-crash", node.ReadOptions{})
	if err != nil || !read.Found || read.Entry != *committed.Entry {
		t.Fatalf("linearizable read on the new leader = %+v, %v: want the committed entry %+v", read, err, *committed.Entry)
	}
	mustPut(t, newLeader, "after-crash", "accepted")
	cluster.WaitForValueEverywhere("after-crash", "accepted")

	rejoined := cluster.Start(oldLeaderID)
	cluster.WaitForValueEverywhere("after-crash", "accepted")
	if term := rejoined.Status().Term; term < newTerm {
		t.Fatalf("rejoined old leader is in term %d, want at least %d", term, newTerm)
	}
}
