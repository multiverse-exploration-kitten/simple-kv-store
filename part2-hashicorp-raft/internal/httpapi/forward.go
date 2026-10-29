package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// holdBackoffDivisor: the first wait is HoldTimeout/16, and it doubles
// (Consul's RPCHoldTimeout backoff).
const holdBackoffDivisor = 16

// operation is one API call that serve runs on this node or forwards.
type operation struct {
	// write: a forward that fails after the connection is made is 504
	// (the leader may have applied it). Else 503.
	write          bool
	idempotencyKey string
	// body is the request body, sent again on a forward.
	body []byte
	// run calls the node and writes the answer. On an error it writes
	// nothing and returns the error.
	run func(w http.ResponseWriter) error
}

// serve runs the operation. On not_leader it forwards the request to the
// leader, one hop. It waits and runs the operation again until HoldTimeout
// when nothing changed, so a retry is safe (Consul canRetry):
//   - no_leader, leadership_transfer_in_progress or not_ready, from this
//     node or in the leader's 503 answer;
//   - a dial error to the leader: the leader got nothing.
//
// Each try calls the node again, so it uses the leader that the node knows
// now. Errors after the request reached the leader (forward_failed) are never
// retried. A request that was forwarded to this node is run once, without
// forward and without hold: the node that got it from the client owns the
// hold.
func (server *api) serve(w http.ResponseWriter, r *http.Request, current operation) {
	forwarded := r.Header.Get(HeaderForwardedFrom) != ""
	holdDeadline := time.Now().Add(server.options.HoldTimeout)
	backoff := server.options.HoldTimeout / holdBackoffDivisor
	for {
		err := current.run(w)
		if err == nil {
			return
		}
		var rejected *node.RejectedError
		if forwarded || !errors.As(err, &rejected) {
			server.writeError(w, err, current)
			return
		}
		if rejected.Reason == node.ReasonNotLeader {
			answer, forwardErr := server.forward(r, current, rejected.LeaderID)
			var notSent *dialError
			switch {
			case errors.As(forwardErr, &notSent) && hold(r.Context(), holdDeadline, &backoff):
				continue
			case forwardErr != nil:
				server.writeError(w, forwardErr, current)
				return
			case holdable(answer.rejectedReason()) && hold(r.Context(), holdDeadline, &backoff):
				continue
			}
			answer.writeTo(w)
			return
		}
		if holdable(rejected.Reason) && hold(r.Context(), holdDeadline, &backoff) {
			continue
		}
		server.writeError(w, err, current)
		return
	}
}

// holdable: the rejection reasons that a short wait can fix. A new leader
// is not_ready only until its Barrier commits.
func holdable(reason string) bool {
	switch reason {
	case node.ReasonNoLeader, node.ReasonLeadershipTransferInProgress, node.ReasonNotReady:
		return true
	}
	return false
}

// dialError: no connection to the leader was made, so the leader got
// nothing and a retry is safe. It answers as 503 leader_unreachable.
type dialError struct {
	rejected *node.RejectedError
}

func (failed *dialError) Error() string { return failed.rejected.Error() }

func (failed *dialError) Unwrap() error { return failed.rejected }

// hold waits for the backoff, at most until the deadline, and doubles the
// backoff. It reports false when the deadline passed or the client left.
func hold(ctx context.Context, deadline time.Time, backoff *time.Duration) bool {
	wait := min(*backoff, time.Until(deadline))
	if wait <= 0 {
		return false
	}
	*backoff *= 2
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// leaderAnswer is the leader's complete answer. It is read fully before it is
// relayed, so that a broken answer is still an error that serve can map.
type leaderAnswer struct {
	status int
	header http.Header
	body   []byte
}

// rejectedReason is the reason of a 503 answer, else "".
func (answer *leaderAnswer) rejectedReason() string {
	if answer.status != http.StatusServiceUnavailable {
		return ""
	}
	var rejected errorResponse
	if json.Unmarshal(answer.body, &rejected) != nil {
		return ""
	}
	return rejected.Reason
}

func (answer *leaderAnswer) writeTo(w http.ResponseWriter) {
	for name, values := range answer.header {
		if name != "Connection" {
			w.Header()[name] = values
		}
	}
	w.WriteHeader(answer.status)
	_, _ = w.Write(answer.body)
}

// forward sends the request to the leader once. The error is a node error:
// RejectedError leader_unreachable when the leader has no usable URL, a
// dialError (leader_unreachable) when no connection was made, else
// forward_failed (OutcomeUnknownError for a write).
func (server *api) forward(r *http.Request, current operation, leaderID string) (*leaderAnswer, error) {
	leaderURL, known := server.peers[leaderID]
	if !known {
		server.logger.Warn("cannot forward: the leader has no HTTP URL in the peer directory", "leader_id", leaderID)
		return nil, &node.RejectedError{Reason: ReasonLeaderUnreachable, LeaderID: leaderID}
	}
	ctx, cancel := context.WithTimeout(r.Context(), server.options.ForwardTimeout)
	defer cancel()
	// Before GotConn no byte of the request can reach the leader. After it,
	// the leader may get the request.
	var connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }})

	target := strings.TrimSuffix(leaderURL, "/") + r.URL.RequestURI()
	request, err := http.NewRequestWithContext(ctx, r.Method, target, bytes.NewReader(current.body))
	if err != nil {
		server.logger.Warn("cannot forward: bad leader URL", "leader_id", leaderID, "url", leaderURL, "error", err)
		return nil, &node.RejectedError{Reason: ReasonLeaderUnreachable, LeaderID: leaderID}
	}
	request.Header = r.Header.Clone()
	request.Header.Del("Connection")
	request.Header.Set(HeaderForwardedFrom, server.node.ID())

	server.logger.Debug("forwarding to the leader", "leader_id", leaderID, "method", r.Method, "uri", r.URL.RequestURI())
	answer, err := server.send(request)
	if err == nil {
		return answer, nil
	}
	server.logger.Warn("forward to the leader failed", "leader_id", leaderID, "method", r.Method, "uri", r.URL.RequestURI(), "connected", connected.Load(), "error", err)
	switch {
	case !connected.Load():
		return nil, &dialError{rejected: &node.RejectedError{Reason: ReasonLeaderUnreachable, LeaderID: leaderID}}
	case current.write:
		return nil, &node.OutcomeUnknownError{Reason: ReasonForwardFailed}
	default:
		return nil, &node.RejectedError{Reason: ReasonForwardFailed, LeaderID: leaderID}
	}
}

func (server *api) send(request *http.Request) (*leaderAnswer, error) {
	response, err := server.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &leaderAnswer{status: response.StatusCode, header: response.Header, body: body}, nil
}
