//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"
)

// failFastDeadline: without a majority, a request must end within this time.
// The worst case is the API's hold for a leader (5 s) after a write that
// waited for the leader's step-down (LeaderLeaseTimeout 500 ms). The rest is
// margin for Docker's port proxy.
const failFastDeadline = 10 * time.Second

// TestDockerCluster builds the image, starts one cluster and runs the
// scenarios on it. After each scenario it undoes the faults and waits for a
// leader. At the end it deletes the cluster and its volumes.
func TestDockerCluster(t *testing.T) {
	cluster := startDockerCluster(t)
	for _, scenario := range []struct {
		name string
		run  func(*testing.T, *dockerCluster)
	}{
		{"ReplicatesToEveryNode", testReplicatesToEveryNode},
		{"WriteThroughFollowerIsForwarded", testWriteThroughFollowerIsForwarded},
		{"KilledLeaderIsReplaced", testKilledLeaderIsReplaced},
		{"PausedFollowerCatchesUp", testPausedFollowerCatchesUp},
		{"DisconnectedLeaderRejoinsAsFollower", testDisconnectedLeaderRejoinsAsFollower},
		{"WithoutMajorityRequestsFailFast", testWithoutMajorityRequestsFailFast},
		{"AdminCallNeedsTheToken", testAdminCallNeedsTheToken},
		{"EachNodeMountsOnlyItsOwnCertificates", testEachNodeMountsOnlyItsOwnCertificates},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Cleanup(func() { cluster.restore(t) })
			scenario.run(t, cluster)
		})
	}
}

// testReplicatesToEveryNode:
//
// Scenario: write a key on the leader. Read it on every node.
//
// Expected: a stale read on every node returns the value soon. A
// linearizable read on every node returns it at once (a follower forwards
// the read to the leader).
//
// Raft lesson: the leader acknowledges a write when a majority stored it. The
// last node gets it a little later, so its local read can be behind. That is
// why the default read is linearizable.
func testReplicatesToEveryNode(t *testing.T, cluster *dockerCluster) {
	leader, _ := cluster.waitForLeader(t, services...)
	key := keyFor(t)
	cluster.mustPut(t, leader, key, "replicated")
	for _, service := range services {
		cluster.waitForValue(t, service, key, "replicated")
		requireValue(t, cluster.get(t, service, key, "linearizable"), "replicated")
	}
}

// testWriteThroughFollowerIsForwarded:
//
// Scenario: write a key on a follower. Read it on the same follower, once
// linearizable and once stale.
//
// Expected: the write returns 200. The linearizable read is served by the
// leader (X-KV-Served-By). The stale read is served by the follower itself.
//
// Raft lesson: hashicorp/raft accepts Apply only on the leader and does not
// forward. The application forwards the request to the leader, one hop
// (Consul ForwardRPC). A stale read never leaves the node.
func testWriteThroughFollowerIsForwarded(t *testing.T, cluster *dockerCluster) {
	leader, _ := cluster.waitForLeader(t, services...)
	follower := others(services, leader)[0]
	key := keyFor(t)

	cluster.mustPut(t, follower, key, "forwarded")

	linearizable := cluster.get(t, follower, key, "linearizable")
	requireValue(t, linearizable, "forwarded")
	if servedBy := linearizable.header.Get("X-KV-Served-By"); servedBy != leader {
		t.Errorf("linearizable read on %s: X-KV-Served-By = %q, want the leader %s", follower, servedBy, leader)
	}
	cluster.waitForValue(t, follower, key, "forwarded")
	stale := cluster.get(t, follower, key, "stale")
	if servedBy := stale.header.Get("X-KV-Served-By"); servedBy != follower {
		t.Errorf("stale read on %s: X-KV-Served-By = %q, want %s itself", follower, servedBy, follower)
	}
}

// testKilledLeaderIsReplaced:
//
// Scenario: kill the leader container with SIGKILL. At once, before a new
// leader exists, send one linearizable read to a survivor. Then read and
// write through the 2 survivors. Start the old leader again.
//
// Expected: the first read returns 200 with the old value within the API's
// hold time plus a margin: the survivor holds it during the election. The
// survivors elect a new leader in a higher term. The old value is still
// there. A write through the surviving follower works. The old leader comes
// back and gets the new value.
//
// Raft lesson: 2 of 3 is a majority, so the cluster survives one crash. A
// crash gives no leadership transfer: the followers wait an election timeout
// before they elect. The forwarding layer must ride out that gap: the IP of
// a dead leader does not answer, so a dial needs a short connect timeout,
// or it uses up the hold. The restarted node reads its log from its volume
// and gets only the missing entries from the leader.
func testKilledLeaderIsReplaced(t *testing.T, cluster *dockerCluster) {
	oldLeader, oldTerm := cluster.waitForLeader(t, services...)
	survivors := others(services, oldLeader)
	key := keyFor(t)
	cluster.mustPut(t, oldLeader, key+"-before", "old")

	cluster.kill(t, oldLeader)
	// cmd/kvstore holds a request for up to 5 s. The margin covers the last
	// forward (connect timeout 1 s) and Docker's port proxy.
	const apiHoldTimeout, holdMargin = 5 * time.Second, 3 * time.Second
	started := time.Now()
	readDuringElection := cluster.get(t, survivors[0], key+"-before", "linearizable")
	if elapsed := time.Since(started); elapsed > apiHoldTimeout+holdMargin {
		t.Errorf("linearizable read on %s right after the kill took %s, want at most %s", survivors[0], elapsed, apiHoldTimeout+holdMargin)
	}
	requireValue(t, readDuringElection, "old")
	newLeader, newTerm := cluster.waitForLeader(t, survivors...)
	if newTerm <= oldTerm {
		t.Errorf("new leader %s has term %d, want a term above %d", newLeader, newTerm, oldTerm)
	}
	for _, survivor := range survivors {
		requireValue(t, cluster.get(t, survivor, key+"-before", "linearizable"), "old")
	}
	survivingFollower := others(survivors, newLeader)[0]
	cluster.mustPut(t, survivingFollower, key+"-after", "new")

	cluster.start(t, oldLeader)
	cluster.waitForLeader(t, services...)
	cluster.waitForValue(t, oldLeader, key+"-after", "new")
	cluster.waitForValue(t, oldLeader, key+"-before", "old")
}

// testPausedFollowerCatchesUp:
//
// Scenario: freeze one follower with docker pause. Write through the leader.
// Unpause the follower.
//
// Expected: the write returns 200 while the follower is frozen, and the
// other follower has it. After the unpause the frozen follower has it too.
//
// Raft lesson: the leader needs a majority, not every node. A frozen node
// (a long GC pause, a stalled VM) is only a slow follower. It catches up
// from the leader when it runs again.
func testPausedFollowerCatchesUp(t *testing.T, cluster *dockerCluster) {
	leader, _ := cluster.waitForLeader(t, services...)
	followers := others(services, leader)
	pausedFollower, activeFollower := followers[0], followers[1]
	key := keyFor(t)

	cluster.pause(t, pausedFollower)
	cluster.mustPut(t, leader, key, "written-while-paused")
	requireValue(t, cluster.get(t, activeFollower, key, "linearizable"), "written-while-paused")

	cluster.unpause(t, pausedFollower)
	cluster.waitForValue(t, pausedFollower, key, "written-while-paused")
}

// testDisconnectedLeaderRejoinsAsFollower:
//
// Scenario: disconnect the leader container from the Raft network. Write
// through the other two. Connect the old leader again.
//
// Expected: the other two elect a new leader and accept the write. After the
// reconnect the old leader is a follower of the new leader and has the new
// value.
//
// Raft lesson: the majority side keeps working without the old leader. The
// old leader steps down when its lease ends (it cannot reach a majority).
// When it comes back it sees the higher term and takes the log of the
// majority. Its log lacks the new entries, so it cannot win an election.
func testDisconnectedLeaderRejoinsAsFollower(t *testing.T, cluster *dockerCluster) {
	oldLeader, _ := cluster.waitForLeader(t, services...)
	majority := others(services, oldLeader)
	key := keyFor(t)

	// A disconnected container is not reachable on its host port either. So
	// during the partition, talk only to the majority.
	cluster.disconnect(t, oldLeader)
	newLeader, _ := cluster.waitForLeader(t, majority...)
	cluster.mustPut(t, newLeader, key, "written-during-partition")

	cluster.connect(t, oldLeader)
	if leader, _ := cluster.waitForLeader(t, services...); leader == oldLeader {
		t.Errorf("the old leader %s won again, but its log lacks the write of the majority", oldLeader)
	}
	cluster.waitForValue(t, oldLeader, key, "written-during-partition")
}

// testWithoutMajorityRequestsFailFast:
//
// Scenario: write a key. Stop the 2 followers (SIGTERM). Write and read
// through the last node. Start the 2 nodes again.
//
// Expected: the write ends with 503 (not applied) or 504 (outcome unknown)
// within failFastDeadline, and so does a linearizable read (503). A stale
// read returns the old value. After the restart, writes work again.
//
// Raft lesson: without a majority nothing can commit. The leader steps down
// when its lease ends. The API must fail closed and fast, not hang. A 504
// write may still commit later, so the client must not treat it as failed.
// A stale read is the only read left, and it can be old.
func testWithoutMajorityRequestsFailFast(t *testing.T, cluster *dockerCluster) {
	lastNode, _ := cluster.waitForLeader(t, services...)
	stoppedNodes := others(services, lastNode)
	key := keyFor(t)
	cluster.mustPut(t, lastNode, key, "before")

	cluster.stop(t, stoppedNodes...)

	writeStarted := time.Now()
	write := cluster.put(t, lastNode, key, "during-outage")
	writeDuration := time.Since(writeStarted)
	if write.status != http.StatusServiceUnavailable && write.status != http.StatusGatewayTimeout {
		t.Errorf("write without a majority = %s, want 503 or 504", write)
	}
	if writeDuration > failFastDeadline {
		t.Errorf("write without a majority took %s, want at most %s", writeDuration, failFastDeadline)
	}

	readStarted := time.Now()
	read := cluster.get(t, lastNode, key, "linearizable")
	readDuration := time.Since(readStarted)
	if read.status != http.StatusServiceUnavailable {
		t.Errorf("linearizable read without a majority = %s, want 503", read)
	}
	if readDuration > failFastDeadline {
		t.Errorf("linearizable read without a majority took %s, want at most %s", readDuration, failFastDeadline)
	}

	requireValue(t, cluster.get(t, lastNode, key, "stale"), "before")
	t.Logf("without a majority: write %d after %s, linearizable read %d after %s", write.status, writeDuration.Round(time.Millisecond), read.status, readDuration.Round(time.Millisecond))

	cluster.start(t, stoppedNodes...)
	cluster.waitForLeader(t, services...)
	cluster.mustPut(t, lastNode, key, "after")
	requireValue(t, cluster.get(t, lastNode, key, "linearizable"), "after")
}

// testAdminCallNeedsTheToken:
//
// Scenario: send POST /admin/snapshot to a follower: without a token, with a
// wrong token, and with the cluster's KV_ADMIN_TOKEN.
//
// Expected: 401, 401, then 200. The follower checks the token before it
// forwards, and the leader checks the forwarded header again.
//
// Raft lesson: hashicorp/raft has no access control. Whoever can call
// RemoveServer or TransferLeadership through the API can break the quorum
// or move the leader. The application must guard these calls, and it must
// fail closed when no token is set.
func testAdminCallNeedsTheToken(t *testing.T, cluster *dockerCluster) {
	leader, _ := cluster.waitForLeader(t, services...)
	follower := others(services, leader)[0]
	cluster.mustPut(t, leader, keyFor(t), "something to snapshot")

	for _, attempt := range []struct {
		authorization string
		wantStatus    int
	}{
		{authorization: "", wantStatus: http.StatusUnauthorized},
		{authorization: "Bearer wrong-" + cluster.adminToken, wantStatus: http.StatusUnauthorized},
		{authorization: "Bearer " + cluster.adminToken, wantStatus: http.StatusOK},
	} {
		response, err := cluster.doWithAuthorization(http.MethodPost, follower, "/admin/snapshot", "", attempt.authorization)
		if err != nil {
			t.Fatalf("POST /admin/snapshot on %s: %v", follower, err)
		}
		if response.status != attempt.wantStatus {
			t.Errorf("POST /admin/snapshot on %s with a token %t = %s, want %d", follower, attempt.authorization != "", response, attempt.wantStatus)
		}
	}
}

// testEachNodeMountsOnlyItsOwnCertificates:
//
// Scenario: list the volumes that each node container mounts.
//
// Expected: nodeN mounts only nodeN-data at /data and nodeN-certs at /certs.
// No node mounts the CA volume (certs-ca) or the certificates of another
// node.
//
// Raft lesson: mutual TLS is only as strong as the CA key. A node that can
// read ca.key, or the key of another node, can show the certificate of any
// peer and send that peer's Raft RPCs.
func testEachNodeMountsOnlyItsOwnCertificates(t *testing.T, cluster *dockerCluster) {
	for _, service := range services {
		want := []string{
			fmt.Sprintf("%s /data", cluster.volumeName(service+"-data")),
			fmt.Sprintf("%s /certs", cluster.volumeName(service+"-certs")),
		}
		slices.Sort(want)
		if got := cluster.mounts(t, service); !slices.Equal(got, want) {
			t.Errorf("%s mounts %q, want exactly %q", service, got, want)
		}
	}
}
