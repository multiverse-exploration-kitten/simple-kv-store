package node

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
)

// ReadConsistency selects how fresh a read must be.
type ReadConsistency string

const (
	// Linearizable is the default: the leader checks that it is still the
	// leader (VerifyLeader) and that its FSM caught up in this term.
	Linearizable ReadConsistency = "linearizable"
	// LeaderLocal reads the leader's FSM without checks (Consul "default").
	// An old leader that did not step down yet can return stale data.
	LeaderLocal ReadConsistency = "leader"
	// Stale reads the local FSM on any node. It works without a quorum and
	// can be stale without a bound, unless MaxStale is set.
	Stale ReadConsistency = "stale"
)

// ParseReadConsistency parses a query value. "" means Linearizable.
func ParseReadConsistency(text string) (ReadConsistency, error) {
	switch consistency := ReadConsistency(text); consistency {
	case "":
		return Linearizable, nil
	case Linearizable, LeaderLocal, Stale:
		return consistency, nil
	}
	return "", &ValidationError{Field: "consistency", Message: fmt.Sprintf("must be %s, %s or %s, got %q", Linearizable, LeaderLocal, Stale, text)}
}

// ReadOptions are the options of Get and List.
type ReadOptions struct {
	Consistency ReadConsistency
	// MaxStale (Stale only): reject the read with too_stale when this
	// follower heard from the leader longer ago. 0 = no limit.
	MaxStale time.Duration
}

// ReadMeta describes the state that served a read.
type ReadMeta struct {
	// Index is the FSM's last applied index when the read started.
	Index       uint64
	KnownLeader bool
	// LastContact is the time since this node heard from the leader: 0 on
	// the leader, -1 when no leader was ever heard.
	LastContact time.Duration
	ServedBy    string
}

// ReadResult is the result of Get.
type ReadResult struct {
	Entry fsm.Entry
	Found bool
	Meta  ReadMeta
}

// ListResult is the result of List.
type ListResult struct {
	Entries []fsm.Entry
	Meta    ReadMeta
}

// neverContacted is the LastContact of a node that never heard a leader.
const neverContacted time.Duration = -1

// Get reads one key. The error is a ValidationError or a RejectedError. A
// read never returns OutcomeUnknownError: it is always safe to retry.
func (node *Node) Get(ctx context.Context, key string, opts ReadOptions) (ReadResult, error) {
	if err := validateKey("key", key); err != nil {
		return ReadResult{}, err
	}
	meta, err := node.checkRead(ctx, opts)
	if err != nil {
		return ReadResult{}, err
	}
	entry, found := node.fsm.Get(key)
	return ReadResult{Entry: entry, Found: found, Meta: meta}, nil
}

// List reads all keys that start with prefix, sorted by key.
func (node *Node) List(ctx context.Context, prefix string, opts ReadOptions) (ListResult, error) {
	if len(prefix) > MaxKeyBytes {
		return ListResult{}, &ValidationError{Field: "prefix", Message: fmt.Sprintf("must be at most %d bytes", MaxKeyBytes)}
	}
	meta, err := node.checkRead(ctx, opts)
	if err != nil {
		return ListResult{}, err
	}
	return ListResult{Entries: node.fsm.List(prefix), Meta: meta}, nil
}

// checkRead runs the checks of the consistency mode before the local read.
func (node *Node) checkRead(ctx context.Context, opts ReadOptions) (ReadMeta, error) {
	if opts.MaxStale < 0 || (opts.MaxStale > 0 && opts.Consistency != Stale) {
		return ReadMeta{}, &ValidationError{Field: "max_stale", Message: "must be positive and needs consistency=stale"}
	}
	switch opts.Consistency {
	case "", Linearizable:
		if err := node.verifyLinearizable(ctx); err != nil {
			return ReadMeta{}, err
		}
	case LeaderLocal:
		if !node.IsLeader() {
			return ReadMeta{}, node.leaderInfo().notLeader(nil)
		}
	case Stale:
		if lastContact := node.lastContact(); opts.MaxStale > 0 && (lastContact == neverContacted || lastContact > opts.MaxStale) {
			return ReadMeta{}, &RejectedError{Reason: ReasonTooStale}
		}
	default:
		_, err := ParseReadConsistency(string(opts.Consistency))
		return ReadMeta{}, err
	}
	leaderID, _ := node.Leader()
	return ReadMeta{
		Index:       node.fsm.LastAppliedIndex(),
		KnownLeader: leaderID != "",
		LastContact: node.lastContact(),
		ServedBy:    node.config.NodeID,
	}, nil
}

// verifyLinearizable: this node is the leader, its Barrier of the current
// term is done, and a quorum still accepts it (VerifyLeader). VerifyLeader
// counts non-voters too (issue #725). The VerifyLeader call itself blocks
// when raft's verify queue of 64 is full, so it runs under the deadline.
func (node *Node) verifyLinearizable(ctx context.Context) error {
	if !node.IsLeader() {
		return node.leaderInfo().notLeader(nil)
	}
	if err := node.leaderReady(); err != nil {
		return err
	}
	if _, err := waitForCall(ctx, node.deadline(ctx), node.raft.VerifyLeader); err != nil {
		return translateReadError(err, node.leaderInfo())
	}
	return nil
}

// lastContact is the time since the leader was last heard: 0 on the leader,
// neverContacted when no leader was ever heard.
func (node *Node) lastContact() time.Duration {
	if node.raft.State() == raft.Leader {
		return 0
	}
	lastContact := node.raft.LastContact()
	if lastContact.IsZero() {
		return neverContacted
	}
	return time.Since(lastContact)
}
