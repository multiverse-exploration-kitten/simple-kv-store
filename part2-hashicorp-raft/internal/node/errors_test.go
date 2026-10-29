package node

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
)

var leaderKnown = leaderInfo{localID: "node1", leaderID: "node2", leaderAddress: "127.0.0.1:7002"}

func rejected(reason string) error { return &RejectedError{Reason: reason} }

func outcomeUnknown(reason string) error { return &OutcomeUnknownError{Reason: reason} }

func checkTranslation(t *testing.T, got, want error) {
	t.Helper()
	var wantRejected *RejectedError
	var wantUnknown *OutcomeUnknownError
	var gotRejected *RejectedError
	var gotUnknown *OutcomeUnknownError
	switch {
	case errors.As(want, &wantRejected):
		if !errors.As(got, &gotRejected) || errors.As(got, &gotUnknown) {
			t.Fatalf("got %T (%v), want RejectedError %q", got, got, wantRejected.Reason)
		}
		if gotRejected.Reason != wantRejected.Reason {
			t.Fatalf("reason = %q, want %q", gotRejected.Reason, wantRejected.Reason)
		}
	case errors.As(want, &wantUnknown):
		if !errors.As(got, &gotUnknown) || errors.As(got, &gotRejected) {
			t.Fatalf("got %T (%v), want OutcomeUnknownError %q", got, got, wantUnknown.Reason)
		}
		if gotUnknown.Reason != wantUnknown.Reason {
			t.Fatalf("reason = %q, want %q", gotUnknown.Reason, wantUnknown.Reason)
		}
	}
}

func TestTranslateWriteError(t *testing.T) {
	testCases := []struct {
		raftError error
		want      error
	}{
		{raft.ErrNotLeader, rejected(ReasonNotLeader)},
		{raft.ErrEnqueueTimeout, rejected(ReasonEnqueueTimeout)},
		{raft.ErrLeadershipTransferInProgress, rejected(ReasonLeadershipTransferInProgress)},
		{raft.ErrLeadershipLost, outcomeUnknown(ReasonLeadershipLost)},
		{raft.ErrRaftShutdown, outcomeUnknown(ReasonShutdown)},
		{errWaitDeadline, outcomeUnknown(ReasonTimeout)},
		{errors.New("failed to write the log store"), outcomeUnknown(ReasonUnknownError)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.raftError.Error(), func(t *testing.T) {
			got := translateWriteError(fmt.Errorf("wrapped: %w", testCase.raftError), leaderKnown)
			checkTranslation(t, got, testCase.want)
			if !errors.Is(got, testCase.raftError) {
				t.Fatalf("%v does not wrap the raft error %v", got, testCase.raftError)
			}
		})
	}
}

func TestTranslateReadErrorIsAlwaysSafeToRetry(t *testing.T) {
	testCases := []struct {
		raftError error
		want      error
	}{
		{raft.ErrNotLeader, rejected(ReasonNotLeader)},
		{raft.ErrLeadershipLost, rejected(ReasonVerifyFailed)},
		{raft.ErrRaftShutdown, rejected(ReasonShutdown)},
		{errWaitDeadline, rejected(ReasonVerifyFailed)},
		{errors.New("something else"), rejected(ReasonVerifyFailed)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.raftError.Error(), func(t *testing.T) {
			checkTranslation(t, translateReadError(testCase.raftError, leaderKnown), testCase.want)
		})
	}
}

func TestNotLeaderErrorCarriesTheKnownLeader(t *testing.T) {
	var withLeader *RejectedError
	if !errors.As(translateWriteError(raft.ErrNotLeader, leaderKnown), &withLeader) {
		t.Fatal("not a RejectedError")
	}
	if withLeader.Reason != ReasonNotLeader || withLeader.LeaderID != "node2" || withLeader.LeaderAddress != "127.0.0.1:7002" {
		t.Fatalf("got %+v, want not_leader with leader node2", withLeader)
	}
	for name, leader := range map[string]leaderInfo{
		"no leader known":         {localID: "node1"},
		"stale self as leader id": {localID: "node1", leaderID: "node1", leaderAddress: "127.0.0.1:7001"},
	} {
		t.Run(name, func(t *testing.T) {
			var withoutLeader *RejectedError
			if !errors.As(translateWriteError(raft.ErrNotLeader, leader), &withoutLeader) || withoutLeader.Reason != ReasonNoLeader {
				t.Fatalf("got %+v, want no_leader", withoutLeader)
			}
		})
	}
}
