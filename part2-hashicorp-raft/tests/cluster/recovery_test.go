package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// recoveryRecoverAlone runs node.RecoverCluster on the stopped node id with
// a configuration that has only this node, as an operator does with
// peers.json.
func recoveryRecoverAlone(t *testing.T, cluster *Cluster, id string) {
	t.Helper()
	config := cluster.NodeConfig(id)
	// raft uses the transport only to encode the peers of the new snapshot.
	// An in-memory transport leaves the node's port free for Start.
	_, transport := raft.NewInmemTransport(raft.ServerAddress(config.RaftAddress))
	defer func() { _ = transport.Close() }()
	err := node.RecoverCluster(config, node.Dependencies{Transport: transport}, []node.Server{{ID: id, Address: config.RaftAddress}})
	if err != nil {
		t.Fatalf("RecoverCluster on %s: %v", id, err)
	}
}

func recoveryCheckOnlyVoter(t *testing.T, leader *node.Node) {
	t.Helper()
	servers, err := leader.Configuration()
	if err != nil {
		t.Fatalf("configuration on %s: %v", leader.ID(), err)
	}
	if len(servers) != 1 || servers[0].ID != leader.ID() || servers[0].Suffrage != node.SuffrageVoter {
		t.Fatalf("configuration after recovery = %+v, want only %s as a voter", servers, leader.ID())
	}
}

func recoveryRead(t *testing.T, reader *node.Node, key string) node.ReadResult {
	t.Helper()
	read, err := reader.Get(context.Background(), key, node.ReadOptions{Consistency: node.Linearizable})
	if err != nil {
		t.Fatalf("linearizable read of %s on %s: %v", key, reader.ID(), err)
	}
	return read
}

// Scenario: 2 of 3 nodes die for good (the leader and one follower). The
// survivor cannot elect a leader. The operator stops it, runs RecoverCluster
// with a configuration that has only the survivor, and starts it again.
// Expected: before the recovery a write on the survivor is rejected (503).
// After it the survivor is the only voter, elects itself, has all old data,
// and accepts writes.
// Raft lesson: Raft cannot leave a quorum loss by itself. RecoverCluster is
// the manual way out: it writes a new configuration into a snapshot and
// deletes the log. Run it only on stopped nodes, on every node that stays,
// with the same server list.
func TestRecoveryLoseTwoOfThreeRecoverSurvivorAlone(t *testing.T) {
	cluster := New(t, Options{})
	leader := cluster.WaitForLeader()
	for _, key := range []string{"first", "second", "third"} {
		mustPut(t, leader, key, "old")
	}
	cluster.WaitForValueEverywhere("third", "old")
	followers := cluster.Followers(leader.ID())
	survivor, lostFollower := followers[0], followers[1]

	cluster.Stop(leader.ID())
	cluster.Stop(lostFollower.ID())
	var rejected *node.RejectedError
	if _, err := survivor.Put(context.Background(), "without-quorum", "value", node.WriteOptions{}); !errors.As(err, &rejected) {
		t.Fatalf("write on the survivor without quorum = %v, want RejectedError (503)", err)
	}

	cluster.Stop(survivor.ID())
	recoveryRecoverAlone(t, cluster, survivor.ID())
	cluster.Start(survivor.ID())
	recovered := cluster.WaitForLeader(survivor.ID())

	recoveryCheckOnlyVoter(t, recovered)
	for _, key := range []string{"first", "second", "third"} {
		if read := recoveryRead(t, recovered, key); !read.Found || read.Entry.Value != "old" {
			t.Fatalf("%s after recovery = %+v, want the old value", key, read)
		}
	}
	mustPut(t, recovered, "after-recovery", "new")
}

// Scenario (reference Q10): isolate the leader. A write on it gets
// "outcome unknown" (504): its entry stays only in the old leader's log. The
// other 2 nodes elect a new leader and commit a write (200 to the client).
// Then those 2 nodes are lost. The operator recovers the old leader alone.
// Expected: after the recovery the never-committed write is there, and the
// acknowledged write is gone.
// Raft lesson: RecoverCluster commits every entry of the local log, because
// the commit information of the lost majority is gone. So recovery can bring
// back a write that failed and lose a write that succeeded. Recover the
// survivor with the highest last log term and index, and tell clients that
// recent writes can change.
//
// The timeouts are 500 ms, not 100 ms: the write must reach the old leader's
// log before its lease ends, and a 230 ms process pause ends a 100 ms lease.
func TestRecoveryCommitsUncommittedEntryAndLosesAcknowledgedWrite(t *testing.T) {
	cluster := New(t, Options{Timing: node.Timing{
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}})
	oldLeader := cluster.WaitForLeader()
	mustPut(t, oldLeader, "base", "committed")
	cluster.WaitForValueEverywhere("base", "committed")

	majority := cluster.Isolate(oldLeader.ID())
	var unknown *node.OutcomeUnknownError
	if _, err := oldLeader.Put(context.Background(), "never-committed", "client got 504", node.WriteOptions{}); !errors.As(err, &unknown) {
		t.Fatalf("write on the isolated leader = %v, want OutcomeUnknownError (504) with the entry in its log", err)
	}
	newLeader := cluster.WaitForLeader(majority...)
	mustPut(t, newLeader, "acknowledged", "client got 200")

	// Keep the partition: a heal would let the new leader replace the old
	// leader's uncommitted entry.
	for _, lostID := range majority {
		cluster.Stop(lostID)
	}
	cluster.Stop(oldLeader.ID())
	recoveryRecoverAlone(t, cluster, oldLeader.ID())
	cluster.Start(oldLeader.ID())
	recovered := cluster.WaitForLeader(oldLeader.ID())

	recoveryCheckOnlyVoter(t, recovered)
	if read := recoveryRead(t, recovered, "base"); !read.Found {
		t.Fatalf("base after recovery = %+v, want found", read)
	}
	if read := recoveryRead(t, recovered, "never-committed"); !read.Found {
		t.Fatalf("never-committed after recovery = %+v: want found, RecoverCluster commits the whole local log", read)
	}
	if read := recoveryRead(t, recovered, "acknowledged"); read.Found {
		t.Fatalf("acknowledged after recovery = %+v: want not found, only the lost nodes stored it", read)
	}
}
