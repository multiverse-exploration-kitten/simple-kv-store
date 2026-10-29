package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const membershipWaitTimeout = 20 * time.Second

// membershipTiming: these tests run for a while with exactly a quorum. With
// the 100 ms lease of node.TestTiming, a 230 ms process pause under parallel
// -race load makes the leader step down (phase 1 notes).
var membershipTiming = node.Timing{
	HeartbeatTimeout:   500 * time.Millisecond,
	ElectionTimeout:    500 * time.Millisecond,
	LeaderLeaseTimeout: 500 * time.Millisecond,
	CommitTimeout:      10 * time.Millisecond,
}

// membershipSuffrages returns server ID -> suffrage of the leader's latest
// configuration.
func membershipSuffrages(t *testing.T, leader *node.Node) map[string]string {
	t.Helper()
	servers, err := leader.Configuration()
	if err != nil {
		t.Fatalf("configuration on %s: %v", leader.ID(), err)
	}
	suffrages := map[string]string{}
	for _, server := range servers {
		suffrages[server.ID] = server.Suffrage
	}
	return suffrages
}

func membershipCheckSuffrages(t *testing.T, leader *node.Node, want map[string]string) {
	t.Helper()
	got := membershipSuffrages(t, leader)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("configuration on %s = %v, want %v", leader.ID(), got, want)
	}
}

// membershipWaitForCatchUp waits until follower applied every command that
// leader applied. Call it when no write is in flight.
func membershipWaitForCatchUp(t *testing.T, leader, follower *node.Node) {
	t.Helper()
	WaitUntil(t, membershipWaitTimeout, fmt.Sprintf("%s applied every command that %s applied", follower.ID(), leader.ID()), func() bool {
		return follower.Status().LastAppliedIndex == leader.Status().LastAppliedIndex
	})
}

// membershipAddCaughtUpVoter adds a new node as a non-voter, waits until it
// caught up, and then promotes it to a voter (reference Q9).
func membershipAddCaughtUpVoter(t *testing.T, cluster *Cluster, leader *node.Node, id string) *node.Node {
	t.Helper()
	added := cluster.AddNode(id)
	address := added.Status().RaftAddress
	if err := leader.AddNonvoter(context.Background(), id, address); err != nil {
		t.Fatalf("AddNonvoter %s: %v", id, err)
	}
	if suffrage := membershipSuffrages(t, leader)[id]; suffrage != node.SuffrageNonvoter {
		t.Fatalf("suffrage of %s after AddNonvoter = %q, want %q", id, suffrage, node.SuffrageNonvoter)
	}
	membershipWaitForCatchUp(t, leader, added)
	if err := leader.AddVoter(context.Background(), id, address); err != nil {
		t.Fatalf("AddVoter %s: %v", id, err)
	}
	return added
}

// membershipWaitForValueOn waits until each of the nodes applied key=value.
func membershipWaitForValueOn(t *testing.T, nodes []*node.Node, key, value string) {
	t.Helper()
	WaitUntil(t, membershipWaitTimeout, fmt.Sprintf("%s=%q on %d nodes", key, value, len(nodes)), func() bool {
		for _, member := range nodes {
			read, err := member.Get(context.Background(), key, node.ReadOptions{Consistency: node.Stale})
			if err != nil || !read.Found || read.Entry.Value != value {
				return false
			}
		}
		return true
	})
}

// Scenario: add node4 to a 3-node cluster as a non-voter, wait until it
// caught up, then promote it with AddVoter. Stop node4 and one old follower.
// Then start the old follower again.
// Expected: node4 has all old data before it votes. With 4 voters the quorum
// is 3: with 2 of 4 stopped, a write fails fast (503 or 504, within
// CommandTimeout). With 3 of 4 running, writes work again.
// Raft lesson: AddVoter makes a server a full voter at once, also when it is
// far behind, so add it as a non-voter first. A 4th voter raises the quorum
// to 3 and still tolerates only 1 failure, the same as 3 voters.
func TestMembershipAddFourthNodeAsNonvoterThenPromote(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	for number := range 20 {
		mustPut(t, leader, fmt.Sprintf("before-join-%d", number), "old")
	}

	oldFollowers := cluster.Followers(leader.ID())
	added := membershipAddCaughtUpVoter(t, cluster, leader, "node4")
	membershipCheckSuffrages(t, leader, map[string]string{
		leader.ID():          node.SuffrageVoter,
		oldFollowers[0].ID(): node.SuffrageVoter,
		oldFollowers[1].ID(): node.SuffrageVoter,
		added.ID():           node.SuffrageVoter,
	})
	if read, err := added.Get(context.Background(), "before-join-19", node.ReadOptions{Consistency: node.Stale}); err != nil || !read.Found {
		t.Fatalf("stale read of old data on node4 = %+v, %v", read, err)
	}

	// Stop node4 and one old follower. If node4 were still a non-voter, 2 of
	// 3 voters would remain and the write would commit.
	stoppedFollower := oldFollowers[0].ID()
	cluster.Stop(added.ID())
	cluster.Stop(stoppedFollower)
	started := time.Now()
	_, err := leader.Put(context.Background(), "with-two-of-four", "value", node.WriteOptions{})
	elapsed := time.Since(started)
	var rejected *node.RejectedError
	var unknown *node.OutcomeUnknownError
	if !errors.As(err, &rejected) && !errors.As(err, &unknown) {
		t.Fatalf("write with 2 of 4 voters = %v, want RejectedError (503) or OutcomeUnknownError (504)", err)
	}
	if limit := defaultCommandTimeout + time.Second; elapsed > limit {
		t.Fatalf("write with 2 of 4 voters took %s, want at most %s: it must fail fast", elapsed, limit)
	}
	t.Logf("write with 2 of 4 voters failed after %s: %v", elapsed, err)

	cluster.Start(stoppedFollower)
	newLeader := cluster.WaitForLeader()
	mustPut(t, newLeader, "with-three-of-four", "value")
}

// Scenario: stop a follower for good (a dead disk). Remove it with
// RemoveServer. Add a replacement node4 with a new ID and an empty data dir:
// non-voter first, then voter. Then stop the other old follower.
// Expected: the 2 remaining voters commit the removal. node4 gets all data.
// After the promotion the leader and node4 are 2 of 3 voters, so writes still
// commit without the old follower.
// Raft lesson: replace a dead server with RemoveServer plus a new server with
// a new ID and empty data. Do not bring the dead server back under its old
// ID with an empty disk: it forgot its term and its vote. Until the
// replacement is a voter, the cluster cannot lose one more node.
func TestMembershipReplaceFailedNode(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "before-failure", "old")
	followers := cluster.Followers(leader.ID())
	deadID, survivorID := followers[0].ID(), followers[1].ID()

	cluster.Stop(deadID)
	mustPut(t, leader, "while-dead", "old")
	if err := leader.RemoveServer(context.Background(), deadID); err != nil {
		t.Fatalf("RemoveServer %s: %v", deadID, err)
	}
	membershipCheckSuffrages(t, leader, map[string]string{leader.ID(): node.SuffrageVoter, survivorID: node.SuffrageVoter})

	replacement := membershipAddCaughtUpVoter(t, cluster, leader, "node4")
	membershipCheckSuffrages(t, leader, map[string]string{
		leader.ID():      node.SuffrageVoter,
		survivorID:       node.SuffrageVoter,
		replacement.ID(): node.SuffrageVoter,
	})
	for _, key := range []string{"before-failure", "while-dead"} {
		if read, err := replacement.Get(context.Background(), key, node.ReadOptions{Consistency: node.Stale}); err != nil || !read.Found {
			t.Fatalf("stale read of %s on the replacement = %+v, %v", key, read, err)
		}
	}

	cluster.Stop(survivorID)
	mustPut(t, leader, "after-replacement", "new")
	membershipWaitForValueOn(t, []*node.Node{replacement}, "after-replacement", "new")
}

// Scenario: remove a running follower with RemoveServer. Write through the
// leader.
// Expected: the 2-node cluster keeps writing. The removed node keeps running
// as a follower (it is not shut down). It logs that it is not in the
// configuration and starts no election, so its term stays the same. It does
// not get the new write, and it reports no_leader.
// Raft lesson: hashicorp/raft does not stop a removed follower (issue #174).
// Stop it yourself. Until then a stale read on it returns old data.
func TestMembershipRemovedNodeGetsNoNewWrites(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "before-removal", "old")
	cluster.WaitForValueEverywhere("before-removal", "old")
	followers := cluster.Followers(leader.ID())
	removed, remaining := followers[0], followers[1]
	termAtRemoval := removed.Status().Term

	if err := leader.RemoveServer(context.Background(), removed.ID()); err != nil {
		t.Fatalf("RemoveServer %s: %v", removed.ID(), err)
	}
	membershipCheckSuffrages(t, leader, map[string]string{leader.ID(): node.SuffrageVoter, remaining.ID(): node.SuffrageVoter})
	// raft logs one of these on the first heartbeat timeout of a node that is
	// not a voter in its latest configuration (raft.go:233-250).
	WaitUntil(t, membershipWaitTimeout, removed.ID()+" logs that it is not in the configuration", func() bool {
		logs := cluster.Logs(removed.ID())
		return strings.Contains(logs, "not part of stable configuration, aborting election") ||
			strings.Contains(logs, "not part of a stable configuration or a non-voter, not triggering a leader election")
	})

	mustPut(t, leader, "after-removal", "new")
	membershipWaitForValueOn(t, []*node.Node{leader, remaining}, "after-removal", "new")
	if read, err := removed.Get(context.Background(), "after-removal", node.ReadOptions{Consistency: node.Stale}); err != nil || read.Found {
		t.Fatalf("stale read of the new key on the removed node = %+v, %v; want not found", read, err)
	}
	if read, err := removed.Get(context.Background(), "before-removal", node.ReadOptions{Consistency: node.Stale}); err != nil || !read.Found {
		t.Fatalf("stale read of the old key on the removed node = %+v, %v; want the old value", read, err)
	}
	status := removed.Status()
	if status.State != "Follower" || status.Term != termAtRemoval {
		t.Fatalf("removed node: state %s term %d, want Follower in term %d (running, no election)", status.State, status.Term, termAtRemoval)
	}
	var rejected *node.RejectedError
	if err := removed.Ready(); !errors.As(err, &rejected) || rejected.Reason != node.ReasonNoLeader {
		t.Fatalf("Ready on the removed node = %v, want RejectedError no_leader", err)
	}
}

// Scenario: 3 voters and the non-voter node4. Stop both other voters, so the
// leader reaches only node4. Then: (1) a linearizable read on the leader,
// (2) a write on the leader, (3) the same read with node4 cut off too, and
// (4) heal node4 and wait.
// Expected (issue #725, open in v1.8.0): read 1 succeeds, although no other
// voter answered. VerifyLeader counted the heartbeat reply of node4. The
// write cannot commit (outcome unknown): commit counts voters only. Read 3
// fails with verify_failed. In step 4 the leader steps down at the end of
// its lease, although node4 answers: the lease counts voters only.
// Raft lesson: in hashicorp/raft a non-voter can confirm leadership. A
// linearizable read then rests on the leader lease (a clock), not on a
// quorum round. This test cannot show a stale value: the lease
// (LeaderLeaseTimeout <= HeartbeatTimeout) steps the old leader down before
// the other voters can elect a new one. Keep non-voters only for a short
// catch-up, as autopilot does.
//
// The timeouts are 1 s: steps 1 to 3 must run inside the lease.
func TestMembershipVerifyLeaderCountsNonvoterIssue725(t *testing.T) {
	cluster := New(t, Options{Timing: node.Timing{
		HeartbeatTimeout:   time.Second,
		ElectionTimeout:    time.Second,
		LeaderLeaseTimeout: time.Second,
		CommitTimeout:      10 * time.Millisecond,
	}})
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "key", "value")
	nonvoter := cluster.AddNode("node4")
	if err := leader.AddNonvoter(context.Background(), nonvoter.ID(), nonvoter.Status().RaftAddress); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}
	membershipWaitForCatchUp(t, leader, nonvoter)

	for _, follower := range cluster.Followers(leader.ID()) {
		if follower.ID() != nonvoter.ID() {
			cluster.Stop(follower.ID())
		}
	}

	readStarted := time.Now()
	read, err := leader.Get(context.Background(), "key", node.ReadOptions{Consistency: node.Linearizable})
	if err != nil || !read.Found || read.Entry.Value != "value" {
		t.Fatalf("linearizable read with only the non-voter reachable = %+v, %v; want success (issue #725). Leader state: %s", read, err, leader.Status().State)
	}
	t.Logf("linearizable read with only the non-voter reachable succeeded after %s", time.Since(readStarted))

	writeContext, cancelWrite := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelWrite()
	var unknown *node.OutcomeUnknownError
	if _, err := leader.Put(writeContext, "not-committed", "value", node.WriteOptions{}); !errors.As(err, &unknown) {
		t.Fatalf("write with 1 of 3 voters = %v, want OutcomeUnknownError: commit counts voters only", err)
	}

	cluster.Partition([]string{leader.ID()}, []string{nonvoter.ID()})
	readContext, cancelRead := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelRead()
	var rejected *node.RejectedError
	if _, err := leader.Get(readContext, "key", node.ReadOptions{Consistency: node.Linearizable}); !errors.As(err, &rejected) || rejected.Reason != node.ReasonVerifyFailed {
		t.Fatalf("linearizable read with no other node reachable = %v, want RejectedError verify_failed", err)
	}
	if !leader.IsLeader() {
		t.Fatal("the leader stepped down before steps 1 to 3 ended: a process pause longer than the 1 s lease")
	}

	cluster.Heal()
	WaitUntil(t, membershipWaitTimeout, leader.ID()+" steps down at the end of its lease although node4 answers", func() bool {
		return !leader.IsLeader()
	})
}

// Scenario: the leader gets AddNonvoter for a server that is already a
// voter, first with its real address and then with a wrong address.
// Expected: both calls return a ValidationError (HTTP 400). The
// configuration does not change: same suffrage, same address.
// Raft lesson: raft.AddNonvoter on an existing voter keeps it a voter but
// rewrites its address. A typo can then break the quorum. The application
// must refuse the call.
func TestMembershipAddNonvoterOnExistingVoterIsRejected(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	follower := cluster.Followers(leader.ID())[0]
	configurationBefore, err := leader.Configuration()
	if err != nil {
		t.Fatal(err)
	}

	for _, address := range []string{follower.Status().RaftAddress, "127.0.0.1:1"} {
		err := leader.AddNonvoter(context.Background(), follower.ID(), address)
		var validationError *node.ValidationError
		if !errors.As(err, &validationError) {
			t.Fatalf("AddNonvoter(%s, %s) = %v, want ValidationError", follower.ID(), address, err)
		}
	}

	configurationAfter, err := leader.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(configurationAfter) != fmt.Sprint(configurationBefore) {
		t.Fatalf("configuration changed: %+v, want %+v", configurationAfter, configurationBefore)
	}
}

// Scenario: AddNonvoter adds a new server, then the same server again with
// the same address.
// Expected: both calls succeed and the server is a non-voter.
// Raft lesson: the voter check must not break the normal path. A repeated
// AddNonvoter on a non-voter is harmless.
func TestMembershipAddNonvoterTwiceIsAccepted(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	added := cluster.AddNode("node4")
	address := added.Status().RaftAddress

	for attempt := 0; attempt < 2; attempt++ {
		if err := leader.AddNonvoter(context.Background(), "node4", address); err != nil {
			t.Fatalf("AddNonvoter attempt %d: %v", attempt, err)
		}
	}
	if got := membershipSuffrages(t, leader)["node4"]; got != node.SuffrageNonvoter {
		t.Fatalf("node4 suffrage = %q, want nonvoter", got)
	}
}
