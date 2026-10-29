package node

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
)

// Reasons in RejectedError and OutcomeUnknownError.
const (
	ReasonNotLeader                    = "not_leader"
	ReasonNoLeader                     = "no_leader"
	ReasonEnqueueTimeout               = "enqueue_timeout"
	ReasonLeadershipTransferInProgress = "leadership_transfer_in_progress"
	ReasonNotReady                     = "not_ready"
	ReasonTooStale                     = "too_stale"
	ReasonVerifyFailed                 = "verify_failed"
	ReasonShutdown                     = "shutdown"
	ReasonTimeout                      = "timeout"
	ReasonLeadershipLost               = "leadership_lost"
	// ReasonUnknownError is a raft error that this package does not know.
	// For a write it is "outcome unknown", because that is the safe answer.
	ReasonUnknownError = "unknown_error"
)

// ErrIdempotencyKeyReused: the idempotency key was used before for a
// different request. Nothing was written (HTTP 422).
var ErrIdempotencyKeyReused = fsm.ErrIdempotencyKeyReused

// errWaitDeadline: our own deadline for a raft future passed. raft futures
// have no deadline: Error() can block forever (reference Q3).
var errWaitDeadline = errors.New("deadline passed before raft answered")

// RejectedError: the operation was not applied. A retry is safe (HTTP 503).
// LeaderID and LeaderAddress are set for not_leader when a leader is known.
type RejectedError struct {
	Reason        string
	LeaderID      string
	LeaderAddress string
	cause         error
}

func (rejected *RejectedError) Error() string {
	message := "rejected: " + rejected.Reason
	if rejected.LeaderID != "" {
		message += fmt.Sprintf(" (leader %s at %s)", rejected.LeaderID, rejected.LeaderAddress)
	}
	if rejected.cause != nil {
		message += ": " + rejected.cause.Error()
	}
	return message
}

func (rejected *RejectedError) Unwrap() error { return rejected.cause }

// OutcomeUnknownError: the write may or may not commit (HTTP 504). Retry only
// with the same idempotency key, or read first.
type OutcomeUnknownError struct {
	Reason string
	cause  error
}

func (unknown *OutcomeUnknownError) Error() string {
	message := "outcome unknown: " + unknown.Reason
	if unknown.cause != nil {
		message += ": " + unknown.cause.Error()
	}
	return message
}

func (unknown *OutcomeUnknownError) Unwrap() error { return unknown.cause }

// ValidationError: the input is invalid (HTTP 400, or 413 when TooLarge).
type ValidationError struct {
	Field    string
	Message  string
	TooLarge bool
}

func (invalid *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", invalid.Field, invalid.Message)
}

// leaderInfo is what this node knows about the leader when an error happens.
type leaderInfo struct {
	localID       string
	leaderID      string
	leaderAddress string
}

func (leader leaderInfo) notLeader(cause error) *RejectedError {
	if leader.leaderID == "" || leader.leaderID == leader.localID {
		return &RejectedError{Reason: ReasonNoLeader, cause: cause}
	}
	return &RejectedError{Reason: ReasonNotLeader, LeaderID: leader.leaderID, LeaderAddress: leader.leaderAddress, cause: cause}
}

// translateWriteError maps the error of a write future (Apply, membership
// change, leadership transfer). See reference Q2 for the table.
func translateWriteError(err error, leader leaderInfo) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return leader.notLeader(err)
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return &RejectedError{Reason: ReasonEnqueueTimeout, cause: err}
	case errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return &RejectedError{Reason: ReasonLeadershipTransferInProgress, cause: err}
	case errors.Is(err, raft.ErrLeadershipLost):
		return &OutcomeUnknownError{Reason: ReasonLeadershipLost, cause: err}
	case errors.Is(err, raft.ErrRaftShutdown):
		// Can come before the enqueue or for an entry that already committed.
		return &OutcomeUnknownError{Reason: ReasonShutdown, cause: err}
	case errors.Is(err, errWaitDeadline):
		return &OutcomeUnknownError{Reason: ReasonTimeout, cause: err}
	default:
		return &OutcomeUnknownError{Reason: ReasonUnknownError, cause: err}
	}
}

// translateReadError maps the error of a read check (VerifyLeader,
// GetConfiguration). A read changes nothing, so it is always safe to retry.
func translateReadError(err error, leader leaderInfo) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return leader.notLeader(err)
	case errors.Is(err, raft.ErrRaftShutdown):
		return &RejectedError{Reason: ReasonShutdown, cause: err}
	default:
		return &RejectedError{Reason: ReasonVerifyFailed, cause: err}
	}
}
