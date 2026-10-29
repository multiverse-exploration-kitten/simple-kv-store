package cluster

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// ErrLinkBlocked is the error of every RPC that Faults drops. The text must
// not contain "unexpected command": raft counts a pre-vote error with that
// text as a granted vote (raft.go:2107).
var ErrLinkBlocked = errors.New("fault injection: link blocked")

type link struct {
	from raft.ServerID
	to   raft.ServerID
}

// Faults is the rule table that every FaultTransport of one cluster shares.
// The rules are about RPCs: "from" is the node that sends the request.
type Faults struct {
	mutex            sync.RWMutex
	blockedRequests  map[link]bool
	droppedResponses map[link]bool
}

// NewFaults returns a table without rules.
func NewFaults() *Faults {
	return &Faults{blockedRequests: map[link]bool{}, droppedResponses: map[link]bool{}}
}

// Block makes every request from "from" to "to" fail before it is sent.
// Requests from "to" to "from" still work.
func (faults *Faults) Block(from, to string) {
	faults.mutex.Lock()
	defer faults.mutex.Unlock()
	faults.blockedRequests[link{from: raft.ServerID(from), to: raft.ServerID(to)}] = true
}

// DropResponses delivers every request from "from" to "to", and "to"
// handles it. Then the response is lost: "from" gets an error.
func (faults *Faults) DropResponses(from, to string) {
	faults.mutex.Lock()
	defer faults.mutex.Unlock()
	faults.droppedResponses[link{from: raft.ServerID(from), to: raft.ServerID(to)}] = true
}

// Partition blocks requests in both directions between the two groups.
func (faults *Faults) Partition(groupA, groupB []string) {
	for _, nodeA := range groupA {
		for _, nodeB := range groupB {
			faults.Block(nodeA, nodeB)
			faults.Block(nodeB, nodeA)
		}
	}
}

// Heal removes every rule.
func (faults *Faults) Heal() {
	faults.mutex.Lock()
	defer faults.mutex.Unlock()
	faults.blockedRequests = map[link]bool{}
	faults.droppedResponses = map[link]bool{}
}

func (faults *Faults) requestBlocked(from, to raft.ServerID) bool {
	faults.mutex.RLock()
	defer faults.mutex.RUnlock()
	return faults.blockedRequests[link{from: from, to: to}]
}

func (faults *Faults) responseDropped(from, to raft.ServerID) bool {
	faults.mutex.RLock()
	defer faults.mutex.RUnlock()
	return faults.droppedResponses[link{from: from, to: to}]
}

// FaultTransport wraps a NetworkTransport and applies the Faults rules.
//
// It must keep three behaviors of the inner transport (storage doc Q10):
//   - raft.WithPreVote: without it raft turns pre-vote off with only a warning.
//   - raft.WithClose: without it Shutdown never closes the listener.
//   - pipelining: ErrPipelineReplicationNotSupported goes back unwrapped.
type FaultTransport struct {
	inner           *raft.NetworkTransport
	localID         raft.ServerID
	faults          *Faults
	consumerChannel chan raft.RPC
	stopChannel     chan struct{}
	closeOnce       sync.Once

	DroppedRequests  atomic.Int64
	DroppedResponses atomic.Int64
	RejectedIncoming atomic.Int64
}

var (
	_ raft.Transport   = (*FaultTransport)(nil)
	_ raft.WithClose   = (*FaultTransport)(nil)
	_ raft.WithPreVote = (*FaultTransport)(nil)
)

// NewFaultTransport wraps inner. Call it before raft.NewRaft: NewRaft reads
// Consumer() once.
func NewFaultTransport(inner *raft.NetworkTransport, localID string, faults *Faults) *FaultTransport {
	transport := &FaultTransport{
		inner:           inner,
		localID:         raft.ServerID(localID),
		faults:          faults,
		consumerChannel: make(chan raft.RPC),
		stopChannel:     make(chan struct{}),
	}
	go transport.forwardIncoming()
	return transport
}

func (transport *FaultTransport) forwardIncoming() {
	innerConsumer := transport.inner.Consumer()
	for {
		select {
		case rpc := <-innerConsumer:
			if transport.rejectIncoming(rpc) {
				continue
			}
			select {
			case transport.consumerChannel <- rpc:
			case <-transport.stopChannel:
				return
			}
		case <-transport.stopChannel:
			return
		}
	}
}

// rejectIncoming answers an RPC from a blocked sender with an error. It must
// answer: without a response the sender waits until its I/O deadline.
func (transport *FaultTransport) rejectIncoming(rpc raft.RPC) bool {
	headerCarrier, hasHeader := rpc.Command.(raft.WithRPCHeader)
	if !hasHeader {
		return false
	}
	senderID := raft.ServerID(headerCarrier.GetRPCHeader().ID)
	if !transport.faults.requestBlocked(senderID, transport.localID) {
		return false
	}
	transport.RejectedIncoming.Add(1)
	if rpc.Reader != nil {
		_, _ = io.Copy(io.Discard, rpc.Reader)
	}
	rpc.Respond(nil, fmt.Errorf("%w: request %s -> %s (rejected on arrival)", ErrLinkBlocked, senderID, transport.localID))
	return true
}

// send runs one outgoing RPC with the checks before and after it.
func (transport *FaultTransport) send(peerID raft.ServerID, resetResponse func(), call func() error) error {
	if transport.faults.requestBlocked(transport.localID, peerID) {
		transport.DroppedRequests.Add(1)
		return fmt.Errorf("%w: request %s -> %s", ErrLinkBlocked, transport.localID, peerID)
	}
	err := call()
	if transport.faults.responseDropped(transport.localID, peerID) {
		transport.DroppedResponses.Add(1)
		resetResponse()
		return fmt.Errorf("%w: response of %s to %s lost, the request was delivered", ErrLinkBlocked, peerID, transport.localID)
	}
	return err
}

// Consumer implements raft.Transport.
func (transport *FaultTransport) Consumer() <-chan raft.RPC { return transport.consumerChannel }

// LocalAddr implements raft.Transport.
func (transport *FaultTransport) LocalAddr() raft.ServerAddress { return transport.inner.LocalAddr() }

// EncodePeer implements raft.Transport.
func (transport *FaultTransport) EncodePeer(id raft.ServerID, address raft.ServerAddress) []byte {
	return transport.inner.EncodePeer(id, address)
}

// DecodePeer implements raft.Transport.
func (transport *FaultTransport) DecodePeer(encoded []byte) raft.ServerAddress {
	return transport.inner.DecodePeer(encoded)
}

// SetHeartbeatHandler implements raft.Transport. NetworkTransport calls the
// handler directly for heartbeats and skips Consumer(), so the filter must
// wrap the handler too.
func (transport *FaultTransport) SetHeartbeatHandler(handler func(rpc raft.RPC)) {
	if handler == nil {
		transport.inner.SetHeartbeatHandler(nil)
		return
	}
	transport.inner.SetHeartbeatHandler(func(rpc raft.RPC) {
		if transport.rejectIncoming(rpc) {
			return
		}
		handler(rpc)
	})
}

// AppendEntries implements raft.Transport.
func (transport *FaultTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return transport.send(id, func() { *resp = raft.AppendEntriesResponse{} }, func() error {
		return transport.inner.AppendEntries(id, target, args, resp)
	})
}

// RequestVote implements raft.Transport.
func (transport *FaultTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return transport.send(id, func() { *resp = raft.RequestVoteResponse{} }, func() error {
		return transport.inner.RequestVote(id, target, args, resp)
	})
}

// RequestPreVote implements raft.WithPreVote.
func (transport *FaultTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return transport.send(id, func() { *resp = raft.RequestPreVoteResponse{} }, func() error {
		return transport.inner.RequestPreVote(id, target, args, resp)
	})
}

// InstallSnapshot implements raft.Transport.
func (transport *FaultTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return transport.send(id, func() { *resp = raft.InstallSnapshotResponse{} }, func() error {
		return transport.inner.InstallSnapshot(id, target, args, resp, data)
	})
}

// TimeoutNow implements raft.Transport.
func (transport *FaultTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return transport.send(id, func() { *resp = raft.TimeoutNowResponse{} }, func() error {
		return transport.inner.TimeoutNow(id, target, args, resp)
	})
}

// AppendEntriesPipeline implements raft.Transport. The inner error goes back
// unchanged: raft compares it with ErrPipelineReplicationNotSupported by ==
// (replication.go:189).
func (transport *FaultTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if transport.faults.requestBlocked(transport.localID, id) {
		transport.DroppedRequests.Add(1)
		return nil, fmt.Errorf("%w: pipeline %s -> %s", ErrLinkBlocked, transport.localID, id)
	}
	innerPipeline, err := transport.inner.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return newFaultPipeline(transport, id, innerPipeline), nil
}

// Close implements raft.WithClose. It is safe to call twice.
func (transport *FaultTransport) Close() error {
	var closeErr error
	transport.closeOnce.Do(func() {
		close(transport.stopChannel)
		closeErr = transport.inner.Close()
	})
	return closeErr
}

type faultPipeline struct {
	transport   *FaultTransport
	peerID      raft.ServerID
	inner       raft.AppendPipeline
	doneChannel chan raft.AppendFuture
	stopChannel chan struct{}
	closeOnce   sync.Once
}

func newFaultPipeline(transport *FaultTransport, peerID raft.ServerID, inner raft.AppendPipeline) *faultPipeline {
	pipeline := &faultPipeline{
		transport:   transport,
		peerID:      peerID,
		inner:       inner,
		doneChannel: make(chan raft.AppendFuture),
		stopChannel: make(chan struct{}),
	}
	go pipeline.forwardResponses()
	return pipeline
}

func (pipeline *faultPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	if pipeline.transport.faults.requestBlocked(pipeline.transport.localID, pipeline.peerID) {
		pipeline.transport.DroppedRequests.Add(1)
		return nil, fmt.Errorf("%w: pipeline request %s -> %s", ErrLinkBlocked, pipeline.transport.localID, pipeline.peerID)
	}
	return pipeline.inner.AppendEntries(args, resp)
}

// forwardResponses replaces a future with a failed one when the response is
// dropped. raft reads only Response() of a pipelined future
// (replication.go:544), so the failed future also has a zero response
// (Success=false).
func (pipeline *faultPipeline) forwardResponses() {
	innerConsumer := pipeline.inner.Consumer()
	for {
		select {
		case future := <-innerConsumer:
			if pipeline.transport.faults.responseDropped(pipeline.transport.localID, pipeline.peerID) {
				pipeline.transport.DroppedResponses.Add(1)
				future = lostResponseFuture{AppendFuture: future}
			}
			select {
			case pipeline.doneChannel <- future:
			case <-pipeline.stopChannel:
				return
			}
		case <-pipeline.stopChannel:
			return
		}
	}
}

func (pipeline *faultPipeline) Consumer() <-chan raft.AppendFuture { return pipeline.doneChannel }

func (pipeline *faultPipeline) Close() error {
	var closeErr error
	pipeline.closeOnce.Do(func() {
		close(pipeline.stopChannel)
		closeErr = pipeline.inner.Close()
	})
	return closeErr
}

type lostResponseFuture struct {
	raft.AppendFuture
}

func (lostResponseFuture) Error() error { return ErrLinkBlocked }

func (lostResponseFuture) Response() *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{}
}
