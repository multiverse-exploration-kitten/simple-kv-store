package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const transferKeyPrefix = "during-transfer-"

// transferWriteOutcome is the result of one put during the transfer.
type transferWriteOutcome struct {
	key string
	err error
}

// transferWriteUntilCanceled puts new keys through writer, one after the
// other, until ctx ends. The puts themselves do not use ctx, so the end of
// ctx does not fail a put.
func transferWriteUntilCanceled(ctx context.Context, writer *node.Node) []transferWriteOutcome {
	var outcomes []transferWriteOutcome
	for number := 0; ctx.Err() == nil; number++ {
		key := fmt.Sprintf("%s%05d", transferKeyPrefix, number)
		_, err := writer.Put(context.Background(), key, "written", node.WriteOptions{})
		outcomes = append(outcomes, transferWriteOutcome{key: key, err: err})
	}
	return outcomes
}

// Scenario: a writer puts new keys through the leader in a loop. While it
// runs, the leader transfers leadership to a chosen follower.
// Expected: TransferLeadership returns nil, and the target becomes the leader
// in a higher term. Each put to the old leader succeeds, or is rejected:
// leadership_transfer_in_progress while the transfer runs, not_leader or
// no_leader after it. Every successful put is on the new leader, and no
// rejected put is. Writes through the new leader work. A put that was in
// flight when the old leader stepped down can end as outcome unknown
// (leadership_lost). The test accepts it and does not check its key.
// Raft lesson: the leader stops new writes, brings the target up to date and
// sends it TimeoutNow, so the target starts an election at once. The cluster
// is without a leader for a much shorter time than after a crash, so transfer
// leadership before you stop a leader on purpose. A rejected write is "known
// not applied" (HTTP 503): a client can retry it.
func TestTransferLeadershipToChosenFollower(t *testing.T) {
	cluster := New(t, Options{Timing: electionStableTiming()})
	oldLeader := cluster.WaitForLeader()
	oldTerm := oldLeader.Status().Term
	target := cluster.Followers(oldLeader.ID())[0]

	writerContext, stopWriter := context.WithCancel(context.Background())
	defer stopWriter()
	outcomesChannel := make(chan []transferWriteOutcome, 1)
	go func() { outcomesChannel <- transferWriteUntilCanceled(writerContext, oldLeader) }()
	WaitUntil(t, electionWaitTimeout, "the writer applied a first put on the old leader", func() bool {
		return oldLeader.Status().KeyCount > 0
	})

	transferErr := oldLeader.TransferLeadership(context.Background(), target.ID())
	stopWriter()
	outcomes := <-outcomesChannel
	if transferErr != nil {
		t.Fatalf("transfer leadership from %s to %s: %v", oldLeader.ID(), target.ID(), transferErr)
	}
	newLeader := cluster.WaitForLeader()
	if newLeader.ID() != target.ID() {
		t.Fatalf("leader after the transfer = %s, want the target %s", newLeader.ID(), target.ID())
	}
	if newTerm := newLeader.Status().Term; newTerm <= oldTerm {
		t.Fatalf("new leader is in term %d, want a term above %d", newTerm, oldTerm)
	}
	mustPut(t, newLeader, "after-transfer", "accepted")
	cluster.WaitForValueEverywhere("after-transfer", "accepted")

	listed, err := newLeader.List(context.Background(), transferKeyPrefix, node.ReadOptions{})
	if err != nil {
		t.Fatalf("linearizable list on the new leader: %v", err)
	}
	present := map[string]bool{}
	for _, entry := range listed.Entries {
		present[entry.Key] = true
	}
	counts := map[string]int{}
	for _, outcome := range outcomes {
		var rejected *node.RejectedError
		var unknown *node.OutcomeUnknownError
		switch {
		case outcome.err == nil:
			counts["applied"]++
			if !present[outcome.key] {
				t.Fatalf("put %s succeeded on the old leader but is missing on the new leader", outcome.key)
			}
		case errors.As(outcome.err, &rejected):
			counts[rejected.Reason]++
			switch rejected.Reason {
			case node.ReasonLeadershipTransferInProgress, node.ReasonNotLeader, node.ReasonNoLeader:
			default:
				t.Fatalf("put %s: unexpected rejection %v", outcome.key, outcome.err)
			}
			if present[outcome.key] {
				t.Fatalf("put %s was rejected (%s) but is on the new leader: a rejection must mean not applied", outcome.key, rejected.Reason)
			}
		case errors.As(outcome.err, &unknown) && unknown.Reason == node.ReasonLeadershipLost:
			counts["outcome_unknown_"+unknown.Reason]++
		default:
			t.Fatalf("put %s: unexpected error %v", outcome.key, outcome.err)
		}
	}
	t.Logf("puts to the old leader while the transfer ran: %v", counts)
}

// Scenario: the cluster has 3 voters and a caught-up non-voter. The leader is
// asked to transfer leadership to the non-voter.
// Expected: TransferLeadership returns a ValidationError (HTTP 400). The
// leader stays leader in the same term and the non-voter stays a non-voter.
// Raft lesson: hashicorp/raft (issue #727) accepts a transfer to a non-voter,
// forces an election and reports success. The application must check the
// suffrage itself before it calls raft.
func TestTransferLeadershipToNonvoterIsRejected(t *testing.T) {
	cluster := New(t, Options{Timing: membershipTiming})
	leader := cluster.WaitForLeader()
	added := cluster.AddNode("node4")
	if err := leader.AddNonvoter(context.Background(), "node4", added.Status().RaftAddress); err != nil {
		t.Fatal(err)
	}
	membershipWaitForCatchUp(t, leader, added)
	termBefore := leader.Status().Term

	err := leader.TransferLeadership(context.Background(), "node4")

	var validationError *node.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("TransferLeadership to a non-voter = %v, want ValidationError", err)
	}
	if got := cluster.WaitForLeader(); got.ID() != leader.ID() || got.Status().Term != termBefore {
		t.Fatalf("leadership changed: leader %s term %d, want %s term %d", got.ID(), got.Status().Term, leader.ID(), termBefore)
	}
	membershipCheckSuffrages(t, leader, map[string]string{
		"node1": node.SuffrageVoter, "node2": node.SuffrageVoter, "node3": node.SuffrageVoter, "node4": node.SuffrageNonvoter,
	})
}
