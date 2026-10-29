package node

import (
	"time"

	"github.com/hashicorp/raft"
)

const barrierRetryDelay = 100 * time.Millisecond

// runLeaderLoop is the only reader of raft's NotifyCh (Consul's
// monitorLeadership). raft sends the changes in order and blocks when the
// buffer is full, so this loop must keep reading.
func (node *Node) runLeaderLoop() {
	for {
		select {
		case isLeader := <-node.leaderChanges:
			node.readyTerm.Store(0)
			if isLeader {
				node.establishLeadership()
			}
		case <-node.lifetime.Done():
			return
		}
	}
}

// establishLeadership runs a Barrier and then marks this term as ready. A new
// leader's FSM can be behind the committed log until its first entries of the
// term commit and apply. The Barrier waits for exactly that (reference Q4).
func (node *Node) establishLeadership() {
	for node.raft.State() == raft.Leader {
		// Read the term before the Barrier. If this node lost and won
		// leadership in between, the stored term is old and stays not ready.
		term := node.raft.CurrentTerm()
		deadline := time.Now().Add(node.config.CommandTimeout)
		_, err := waitForCall(node.lifetime, deadline, func() raft.Future { return node.raft.Barrier(enqueueTimeoutFor(deadline)) })
		if err == nil {
			node.readyTerm.Store(term)
			node.logger.Info("leader is ready for linearizable reads", "term", term)
			return
		}
		node.logger.Warn("leader barrier failed, will retry", "term", term, "error", err)
		select {
		case <-node.lifetime.Done():
			return
		case <-time.After(barrierRetryDelay):
		}
	}
}

// Ready reports whether this node can serve. A leader is ready after its
// Barrier in the current term. A follower is ready when it knows a leader and
// heard from it within ReadyMaxLastContact. The error is a RejectedError.
func (node *Node) Ready() error {
	switch node.raft.State() {
	case raft.Leader:
		return node.leaderReady()
	case raft.Shutdown:
		return &RejectedError{Reason: ReasonShutdown}
	}
	leader := node.leaderInfo()
	if leader.leaderID == "" {
		return &RejectedError{Reason: ReasonNoLeader}
	}
	if lastContact := node.lastContact(); lastContact == neverContacted || lastContact > node.config.ReadyMaxLastContact {
		return &RejectedError{Reason: ReasonNotReady, LeaderID: leader.leaderID, LeaderAddress: leader.leaderAddress}
	}
	return nil
}

func (node *Node) leaderReady() error {
	if node.readyTerm.Load() != node.raft.CurrentTerm() {
		return &RejectedError{Reason: ReasonNotReady}
	}
	return nil
}
