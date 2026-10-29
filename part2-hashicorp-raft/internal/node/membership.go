package node

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
)

// Configuration returns the latest Raft configuration. It may not be
// committed yet (reference Q9).
func (node *Node) Configuration() ([]ServerInfo, error) {
	future, err := waitForCall(node.lifetime, time.Now().Add(node.config.CommandTimeout), node.raft.GetConfiguration)
	if err != nil {
		return nil, translateReadError(err, node.leaderInfo())
	}
	servers := future.Configuration().Servers
	infos := make([]ServerInfo, 0, len(servers))
	for _, server := range servers {
		infos = append(infos, ServerInfo{ID: string(server.ID), Address: string(server.Address), Suffrage: suffrageName(server.Suffrage)})
	}
	return infos, nil
}

func suffrageName(suffrage raft.ServerSuffrage) string {
	switch suffrage {
	case raft.Voter:
		return SuffrageVoter
	case raft.Nonvoter:
		return SuffrageNonvoter
	default:
		return SuffrageStaging
	}
}

// AddVoter adds a voter at once, also when it is far behind: the quorum grows
// at once (reference Q9). Add a non-voter first and promote it after it
// caught up. AddVoter on an existing non-voter promotes it.
func (node *Node) AddVoter(ctx context.Context, id, address string) error {
	if err := validateServer(id, address); err != nil {
		return err
	}
	return node.changeMembership(ctx, func(enqueueTimeout time.Duration) raft.Future {
		return node.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, enqueueTimeout)
	})
}

// AddNonvoter adds a server that gets the log but does not vote.
func (node *Node) AddNonvoter(ctx context.Context, id, address string) error {
	if err := validateServer(id, address); err != nil {
		return err
	}
	return node.changeMembership(ctx, func(enqueueTimeout time.Duration) raft.Future {
		return node.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, enqueueTimeout)
	})
}

// RemoveServer removes a server. A removed follower keeps running: stop it
// yourself (reference Q9).
func (node *Node) RemoveServer(ctx context.Context, id string) error {
	if id == "" {
		return &ValidationError{Field: "id", Message: "must not be empty"}
	}
	return node.changeMembership(ctx, func(enqueueTimeout time.Duration) raft.Future {
		return node.raft.RemoveServer(raft.ServerID(id), 0, enqueueTimeout)
	})
}

// changeMembership passes prevIndex 0: GetConfiguration().Index() is always
// 0 in v1.8.0 (#722), so a real guard index is not available.
func (node *Node) changeMembership(ctx context.Context, change func(enqueueTimeout time.Duration) raft.Future) error {
	deadline := node.deadline(ctx)
	enqueueTimeout := enqueueTimeoutFor(deadline)
	if enqueueTimeout <= 0 || ctx.Err() != nil {
		return &RejectedError{Reason: ReasonEnqueueTimeout, cause: context.Cause(ctx)}
	}
	if _, err := waitForCall(ctx, deadline, func() raft.Future { return change(enqueueTimeout) }); err != nil {
		return translateWriteError(err, node.leaderInfo())
	}
	return nil
}

// TransferLeadership asks raft to move leadership to targetID, or to the most
// up-to-date voter when targetID is "". After a timeout, leadership can still
// move (reference Q7), so a timeout is OutcomeUnknownError.
func (node *Node) TransferLeadership(ctx context.Context, targetID string) error {
	if targetID == node.config.NodeID {
		return &ValidationError{Field: "id", Message: "cannot transfer leadership to this node itself"}
	}
	deadline := node.deadline(ctx)
	transfer := node.raft.LeadershipTransfer
	if targetID != "" {
		targetAddress, found, err := node.serverAddress(targetID)
		if err != nil {
			return err
		}
		if !found {
			return &ValidationError{Field: "id", Message: "server " + targetID + " is not in the configuration"}
		}
		transfer = func() raft.Future {
			return node.raft.LeadershipTransferToServer(raft.ServerID(targetID), raft.ServerAddress(targetAddress))
		}
	}
	if _, err := waitForCall(ctx, deadline, transfer); err != nil {
		return translateWriteError(err, node.leaderInfo())
	}
	return nil
}

func (node *Node) serverAddress(id string) (address string, found bool, err error) {
	servers, err := node.Configuration()
	if err != nil {
		return "", false, err
	}
	for _, server := range servers {
		if server.ID == id {
			return server.Address, true, nil
		}
	}
	return "", false, nil
}

func validateServer(id, address string) error {
	switch {
	case id == "":
		return &ValidationError{Field: "id", Message: "must not be empty"}
	case address == "":
		return &ValidationError{Field: "address", Message: "must not be empty"}
	}
	return nil
}
