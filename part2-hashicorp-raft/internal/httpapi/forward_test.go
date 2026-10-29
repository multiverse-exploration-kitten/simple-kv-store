package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// receivedRequest is what a fake leader server got.
type receivedRequest struct {
	method, requestURI, body string
	header                   http.Header
}

// fakeLeaderServer is an httptest.Server that records each request and
// answers with answer.
type fakeLeaderServer struct {
	server   *httptest.Server
	mutex    sync.Mutex
	received []receivedRequest
}

func newFakeLeaderServer(t *testing.T, answer http.HandlerFunc) *fakeLeaderServer {
	t.Helper()
	leader := &fakeLeaderServer{}
	leader.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		leader.mutex.Lock()
		leader.received = append(leader.received, receivedRequest{method: r.Method, requestURI: r.RequestURI, body: string(body), header: r.Header.Clone()})
		leader.mutex.Unlock()
		answer(w, r)
	}))
	t.Cleanup(leader.server.Close)
	return leader
}

func (leader *fakeLeaderServer) requests() []receivedRequest {
	leader.mutex.Lock()
	defer leader.mutex.Unlock()
	return append([]receivedRequest(nil), leader.received...)
}

func answerJSON(status int, body string, header ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for index := 0; index+1 < len(header); index += 2 {
			w.Header().Set(header[index], header[index+1])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// answerBrokenConnection closes the connection after the request arrived:
// the leader may have applied the write, but the follower gets no answer.
func answerBrokenConnection(w http.ResponseWriter, _ *http.Request) {
	connection, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		_ = connection.Close()
	}
}

func notLeaderOf(leaderID string) error {
	return &node.RejectedError{Reason: node.ReasonNotLeader, LeaderID: leaderID, LeaderAddress: "127.0.0.1:7001"}
}

// newForwardingFollower is a follower fake whose writes and reads all
// answer not_leader with leader node1.
func newForwardingFollower() *fakeNode {
	follower := newFollowerFake()
	follower.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
		return node.WriteResult{}, notLeaderOf("node1")
	}
	follower.delete = func(context.Context, string, node.WriteOptions) (node.WriteResult, error) {
		return node.WriteResult{}, notLeaderOf("node1")
	}
	follower.get = func(context.Context, string, node.ReadOptions) (node.ReadResult, error) {
		return node.ReadResult{}, notLeaderOf("node1")
	}
	follower.list = func(context.Context, string, node.ReadOptions) (node.ListResult, error) {
		return node.ListResult{}, notLeaderOf("node1")
	}
	return follower
}

func TestForwardSendsWriteToLeaderOnceAndRelaysAnswer(t *testing.T) {
	leader := newFakeLeaderServer(t, answerJSON(200, `{"key":"config/db","value":"v1","create_index":3,"modify_index":3,"index":3}`, HeaderDuplicate, "true"))
	follower := newForwardingFollower()
	handler := New(follower, PeerDirectory{"node1": leader.server.URL + "/"}, Options{})

	response := serveTest(t, handler, "PUT", "/kv/config/db?cas=0", `{"value":"v1"}`, headerWith(HeaderIdempotencyKey, "request-1"))

	if response.status != 200 || response.field("index") != "3" || response.header.Get(HeaderDuplicate) != "true" {
		t.Fatalf("relayed answer: status %d header %v body %s", response.status, response.header, response.text)
	}
	received := leader.requests()
	if len(received) != 1 {
		t.Fatalf("leader got %d requests, want 1", len(received))
	}
	got := received[0]
	if got.method != "PUT" || got.requestURI != "/kv/config/db?cas=0" || got.body != `{"value":"v1"}` {
		t.Fatalf("leader got %s %s body %q", got.method, got.requestURI, got.body)
	}
	if got.header.Get(HeaderForwardedFrom) != "node2" || got.header.Get(HeaderIdempotencyKey) != "request-1" {
		t.Fatalf("leader got headers %v", got.header)
	}
	if follower.calls.Load() != 1 {
		t.Fatalf("follower node was called %d times, want 1", follower.calls.Load())
	}
}

func TestForwardSendsLinearizableAndLeaderReads(t *testing.T) {
	for _, target := range []string{"/kv/a", "/kv/a?consistency=leader", "/kv?prefix=a"} {
		t.Run(target, func(t *testing.T) {
			leader := newFakeLeaderServer(t, answerJSON(200, `{"key":"a","value":"1"}`, HeaderServedBy, "node1", HeaderIndex, "8"))
			handler := New(newForwardingFollower(), PeerDirectory{"node1": leader.server.URL}, Options{})
			response := serveTest(t, handler, "GET", target, "", nil)
			if response.status != 200 || response.header.Get(HeaderServedBy) != "node1" || response.header.Get(HeaderIndex) != "8" {
				t.Fatalf("status %d header %v body %s", response.status, response.header, response.text)
			}
			if received := leader.requests(); len(received) != 1 || received[0].requestURI != target {
				t.Fatalf("leader got %+v", received)
			}
		})
	}
}

// The follower does not call its node for a cluster call: the node of a
// follower would reject "transfer leadership to me", but the leader can do it.
func TestForwardSendsClusterAndAdminCallsWithoutLocalCall(t *testing.T) {
	calls := []struct{ method, path, body string }{
		{method: "GET", path: "/cluster/servers"},
		{method: "POST", path: "/cluster/servers", body: `{"id":"node4","address":"127.0.0.1:7004"}`},
		{method: "DELETE", path: "/cluster/servers/node3"},
		{method: "POST", path: "/cluster/transfer-leadership", body: `{"id":"node2"}`},
		{method: "POST", path: "/admin/snapshot"},
	}
	for _, clusterCall := range calls {
		t.Run(clusterCall.method+" "+clusterCall.path, func(t *testing.T) {
			leader := newFakeLeaderServer(t, answerJSON(200, `{"status":"ok"}`))
			follower := newFollowerFake()
			handler := New(follower, PeerDirectory{"node1": leader.server.URL}, Options{AdminToken: testAdminToken})
			response := serveTest(t, handler, clusterCall.method, clusterCall.path, clusterCall.body, adminHeader())
			if response.status != 200 {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
			received := leader.requests()
			if len(received) != 1 || received[0].method != clusterCall.method || received[0].requestURI != clusterCall.path || received[0].body != clusterCall.body {
				t.Fatalf("leader got %+v", received)
			}
			// The leader checks the admin token again.
			if got := received[0].header.Get("Authorization"); got != "Bearer "+testAdminToken {
				t.Fatalf("leader got Authorization %q, want the client's bearer token", got)
			}
			if follower.calls.Load() != 0 {
				t.Fatalf("the follower called its node %d times, want 0", follower.calls.Load())
			}
		})
	}
}

// Loop guard: node1 got a forwarded write but is not the leader any more.
// It answers 503 not_leader at once, and does not forward to node3.
func TestForwardedRequestIsNotForwardedAgain(t *testing.T) {
	node3 := newFakeLeaderServer(t, answerJSON(200, `{}`))
	oldLeader := newLeaderFake()
	oldLeader.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
		return node.WriteResult{}, notLeaderOf("node3")
	}
	oldLeaderServer := httptest.NewServer(New(oldLeader, PeerDirectory{"node3": node3.server.URL}, Options{}))
	t.Cleanup(oldLeaderServer.Close)
	follower := newForwardingFollower()
	handler := New(follower, PeerDirectory{"node1": oldLeaderServer.URL, "node3": node3.server.URL}, Options{})

	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)

	if response.status != 503 || response.field("reason") != "not_leader" || response.field("retry_safe") != "true" {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
	if count := len(node3.requests()); count != 0 {
		t.Fatalf("node3 got %d requests: the forwarded request was forwarded again", count)
	}
	if oldLeader.calls.Load() != 1 || follower.calls.Load() != 1 {
		t.Fatalf("node calls: old leader %d, follower %d, want 1 and 1", oldLeader.calls.Load(), follower.calls.Load())
	}
}

func TestForwardedRequestIsNeitherForwardedNorHeld(t *testing.T) {
	for _, reason := range []string{node.ReasonNotLeader, node.ReasonNoLeader} {
		t.Run(reason, func(t *testing.T) {
			leader := newFakeLeaderServer(t, answerJSON(200, `{}`))
			follower := newFollowerFake()
			follower.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
				return node.WriteResult{}, &node.RejectedError{Reason: reason, LeaderID: "node1"}
			}
			handler := New(follower, PeerDirectory{"node1": leader.server.URL}, Options{HoldTimeout: time.Minute})
			response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, headerWith(HeaderForwardedFrom, "node3"))
			if response.status != 503 || response.field("reason") != reason {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
			if len(leader.requests()) != 0 || follower.calls.Load() != 1 {
				t.Fatalf("leader got %d requests, node called %d times, want 0 and 1", len(leader.requests()), follower.calls.Load())
			}
		})
	}
}

// closedServerURL is the URL of a server that is closed: a dial fails.
func closedServerURL() string {
	closedServer := httptest.NewServer(http.NotFoundHandler())
	closedServer.Close()
	return closedServer.URL
}

// A leader without a URL in the directory is a configuration error. A wait
// cannot fix it, so the answer is 503 at once.
func TestForwardLeaderNotInDirectoryIs503WithoutHold(t *testing.T) {
	follower := newForwardingFollower()
	handler := New(follower, PeerDirectory{}, Options{HoldTimeout: time.Minute})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 503 || response.field("reason") != ReasonLeaderUnreachable || follower.calls.Load() != 1 {
		t.Fatalf("status %d after %d tries, body %s", response.status, follower.calls.Load(), response.text)
	}
}

// The node names a leader that cannot be dialed, and later a leader that
// works. The leader got nothing from the failed dial, so the follower holds,
// asks its node again and forwards to the new leader.
func TestHoldRetriesWhenLeaderIsUnreachable(t *testing.T) {
	newLeader := newFakeLeaderServer(t, answerJSON(200, `{"key":"a","value":"x","index":7}`))
	follower := newFollowerFake()
	follower.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
		if follower.calls.Load() == 1 {
			return node.WriteResult{}, notLeaderOf("node1")
		}
		return node.WriteResult{}, notLeaderOf("node3")
	}
	handler := New(follower, PeerDirectory{"node1": closedServerURL(), "node3": newLeader.server.URL}, Options{HoldTimeout: 2 * time.Second})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 200 || follower.calls.Load() != 2 || len(newLeader.requests()) != 1 {
		t.Fatalf("status %d after %d tries, new leader got %d requests (body %s)", response.status, follower.calls.Load(), len(newLeader.requests()), response.text)
	}
}

// The leader stays impossible to dial. After HoldTimeout the answer is 503
// for reads and writes: the leader never got the request.
func TestHoldEndsWith503WhenLeaderStaysUnreachable(t *testing.T) {
	peers := PeerDirectory{"node1": closedServerURL()}
	for _, request := range []struct{ method, target, body string }{{"PUT", "/kv/a", `{"value":"x"}`}, {"GET", "/kv/a", ""}} {
		t.Run(request.method, func(t *testing.T) {
			follower := newForwardingFollower()
			const holdTimeout = 300 * time.Millisecond
			handler := New(follower, peers, Options{HoldTimeout: holdTimeout})
			started := time.Now()
			response := serveTest(t, handler, request.method, request.target, request.body, headerWith(HeaderIdempotencyKey, "request-1"))
			elapsed := time.Since(started)
			if response.status != 503 || response.field("reason") != ReasonLeaderUnreachable || response.field("retry_safe") != "true" {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
			if elapsed < holdTimeout || elapsed > holdTimeout+5*time.Second || follower.calls.Load() < 2 {
				t.Fatalf("answered after %s and %d tries, want at least %s and 2 tries", elapsed, follower.calls.Load(), holdTimeout)
			}
		})
	}
}

// Scenario: the node names a killed leader. Its IP does not answer, so the
// dial blocks (the seam blocks until the dial context ends). Then the node
// names the new leader.
// Expected: the dial gives up after DialTimeout, long before ForwardTimeout.
// The hold retries, asks the node again and gets the answer of the new
// leader, all within HoldTimeout.
// Without DialTimeout the dial would hang until ForwardTimeout (3 s), after
// the hold deadline (2 s): 503 leader_unreachable.
func TestHoldRetriesWhenDialToLeaderTimesOut(t *testing.T) {
	newLeader := newFakeLeaderServer(t, answerJSON(200, `{"key":"a","value":"x"}`, HeaderServedBy, "node3"))
	deadLeaderURL := closedServerURL()
	deadLeaderAddress := strings.TrimPrefix(deadLeaderURL, "http://")
	var blockedDials atomic.Int64
	follower := newFollowerFake()
	follower.get = func(context.Context, string, node.ReadOptions) (node.ReadResult, error) {
		if follower.calls.Load() == 1 {
			return node.ReadResult{}, notLeaderOf("node1")
		}
		return node.ReadResult{}, notLeaderOf("node3")
	}
	handler := New(follower, PeerDirectory{"node1": deadLeaderURL, "node3": newLeader.server.URL}, Options{
		HoldTimeout:    2 * time.Second,
		ForwardTimeout: 3 * time.Second,
		DialTimeout:    100 * time.Millisecond,
		dialControl: func(ctx context.Context, _, address string, _ syscall.RawConn) error {
			if address != deadLeaderAddress {
				return nil
			}
			blockedDials.Add(1)
			<-ctx.Done()
			return ctx.Err()
		},
	})

	response := serveTest(t, handler, "GET", "/kv/a", "", nil)

	if response.status != 200 || response.header.Get(HeaderServedBy) != "node3" {
		t.Fatalf("status %d header %v body %s", response.status, response.header, response.text)
	}
	if blockedDials.Load() != 1 || follower.calls.Load() != 2 || len(newLeader.requests()) != 1 {
		t.Fatalf("blocked dials %d, node calls %d, new leader requests %d, want 1, 2 and 1", blockedDials.Load(), follower.calls.Load(), len(newLeader.requests()))
	}
}

// The connection breaks after the leader got the request. A write may be
// applied (504). A read changed nothing (503). Neither is retried: the
// request reached the leader.
func TestForwardBrokenAfterSendIs504ForWrites503ForReads(t *testing.T) {
	tests := []struct {
		method, target, body string
		header               map[string]string
		wantStatus           int
		wantBody             map[string]string
	}{
		{
			method: "PUT", target: "/kv/a", body: `{"value":"x"}`, wantStatus: 504,
			wantBody: map[string]string{"error": "outcome_unknown", "reason": ReasonForwardFailed, "retry_safe": "false", "retry_with_same_idempotency_key": ""},
		},
		{
			method: "DELETE", target: "/kv/a", header: map[string]string{HeaderIdempotencyKey: "request-1"}, wantStatus: 504,
			wantBody: map[string]string{"error": "outcome_unknown", "reason": ReasonForwardFailed, "retry_with_same_idempotency_key": "true"},
		},
		{
			method: "GET", target: "/kv/a", wantStatus: 503,
			wantBody: map[string]string{"error": "command_rejected", "reason": ReasonForwardFailed, "retry_safe": "true"},
		},
	}
	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			leader := newFakeLeaderServer(t, answerBrokenConnection)
			follower := newForwardingFollower()
			handler := New(follower, PeerDirectory{"node1": leader.server.URL}, Options{HoldTimeout: time.Minute})
			header := headerWith()
			for name, value := range test.header {
				header.Set(name, value)
			}
			response := serveTest(t, handler, test.method, test.target, test.body, header)
			if response.status != test.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", response.status, test.wantStatus, response.text)
			}
			for name, value := range test.wantBody {
				if got := response.field(name); got != value {
					t.Errorf("body %s = %q, want %q", name, got, value)
				}
			}
			if len(leader.requests()) != 1 || follower.calls.Load() != 1 {
				t.Fatalf("leader got %d requests, follower node called %d times, want 1 and 1", len(leader.requests()), follower.calls.Load())
			}
		})
	}
}

// The leader does not answer within ForwardTimeout. The write may still
// commit on the leader, so the answer is 504, without a retry.
func TestForwardTimeoutIs504ForWrites(t *testing.T) {
	leader := newFakeLeaderServer(t, func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	handler := New(newForwardingFollower(), PeerDirectory{"node1": leader.server.URL}, Options{ForwardTimeout: 100 * time.Millisecond, HoldTimeout: time.Minute})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 504 || response.field("reason") != ReasonForwardFailed || len(leader.requests()) != 1 {
		t.Fatalf("status %d body %s, leader got %d requests", response.status, response.text, len(leader.requests()))
	}
}

// The node rejects the first two tries with a reason that a wait fixes (no
// leader, transfer, a new leader before its Barrier). The API holds the
// request and then succeeds, so the client sees a slow answer, not an error.
func TestHoldRetriesWhileNoLeader(t *testing.T) {
	for _, reason := range []string{node.ReasonNoLeader, node.ReasonLeadershipTransferInProgress, node.ReasonNotReady} {
		t.Run(reason, func(t *testing.T) {
			fake := newLeaderFake()
			var tries atomic.Int64
			fake.put = func(_ context.Context, key, value string, _ node.WriteOptions) (node.WriteResult, error) {
				if tries.Add(1) <= 2 {
					return node.WriteResult{}, &node.RejectedError{Reason: reason}
				}
				return node.WriteResult{Index: 4, Applied: true, Entry: &fsm.Entry{Key: key, Value: value, CreateIndex: 4, ModifyIndex: 4}}, nil
			}
			// Waits of 125 ms and 250 ms; the rest of the 2 s is margin for a busy machine.
			handler := New(fake, PeerDirectory{}, Options{HoldTimeout: 2 * time.Second})
			response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
			if response.status != 200 || tries.Load() != 3 {
				t.Fatalf("status %d after %d tries, want 200 after 3 (body %s)", response.status, tries.Load(), response.text)
			}
		})
	}
}

func TestHoldEndsWith503AfterHoldTimeout(t *testing.T) {
	for _, reason := range []string{node.ReasonNoLeader, node.ReasonNotReady} {
		t.Run(reason, func(t *testing.T) {
			fake := newLeaderFake()
			fake.get = func(context.Context, string, node.ReadOptions) (node.ReadResult, error) {
				return node.ReadResult{}, &node.RejectedError{Reason: reason}
			}
			const holdTimeout = 300 * time.Millisecond
			handler := New(fake, PeerDirectory{}, Options{HoldTimeout: holdTimeout})
			started := time.Now()
			response := serveTest(t, handler, "GET", "/kv/a", "", nil)
			elapsed := time.Since(started)
			if response.status != 503 || response.field("reason") != reason {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
			if elapsed < holdTimeout || elapsed > holdTimeout+5*time.Second || fake.calls.Load() < 2 {
				t.Fatalf("answered after %s and %d tries, want at least %s and 2 tries", elapsed, fake.calls.Load(), holdTimeout)
			}
		})
	}
}

// Rejections that a short wait does not fix answer at once.
func TestHoldDoesNotRetryOtherRejections(t *testing.T) {
	fake := newLeaderFake()
	fake.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
		return node.WriteResult{}, &node.RejectedError{Reason: node.ReasonEnqueueTimeout}
	}
	handler := New(fake, PeerDirectory{}, Options{HoldTimeout: time.Minute})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 503 || fake.calls.Load() != 1 {
		t.Fatalf("status %d after %d tries, want 503 after 1", response.status, fake.calls.Load())
	}
}

// The leader answers "no leader" (it lost leadership before it appended
// the write). Nothing changed, so the follower holds and tries again.
func TestHoldRetriesWhenLeaderAnswersNoLeader(t *testing.T) {
	var leaderAnswers atomic.Int64
	leader := newFakeLeaderServer(t, func(w http.ResponseWriter, r *http.Request) {
		if leaderAnswers.Add(1) == 1 {
			answerJSON(503, `{"error":"command_rejected","reason":"no_leader","retry_safe":true}`, "Retry-After", "1")(w, r)
			return
		}
		answerJSON(200, `{"key":"a","value":"x","index":5}`)(w, r)
	})
	follower := newForwardingFollower()
	handler := New(follower, PeerDirectory{"node1": leader.server.URL}, Options{HoldTimeout: 5 * time.Second})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 200 || len(leader.requests()) != 2 || follower.calls.Load() != 2 {
		t.Fatalf("status %d, leader got %d requests, follower node called %d times (body %s)", response.status, len(leader.requests()), follower.calls.Load(), response.text)
	}
}

// The leader's other 503 answers are relayed as they are, without a retry.
func TestForwardRelaysOtherLeaderRejections(t *testing.T) {
	leader := newFakeLeaderServer(t, answerJSON(503, `{"error":"command_rejected","reason":"not_leader","retry_safe":true}`, "Retry-After", "1"))
	follower := newForwardingFollower()
	handler := New(follower, PeerDirectory{"node1": leader.server.URL}, Options{HoldTimeout: time.Minute})
	response := serveTest(t, handler, "PUT", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 503 || response.field("reason") != "not_leader" || response.header.Get("Retry-After") != "1" || len(leader.requests()) != 1 {
		t.Fatalf("status %d header %v body %s, leader got %d requests", response.status, response.header, response.text, len(leader.requests()))
	}
}
