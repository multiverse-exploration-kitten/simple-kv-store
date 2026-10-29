package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// fakeNode is a NodeAPI for unit tests. A test sets the functions it
// expects. A call to an unset function panics, so an unexpected call fails
// the test.
type fakeNode struct {
	id            string
	isLeader      bool
	leaderID      string
	leaderAddress string
	readyErr      error
	status        node.Status

	put                func(ctx context.Context, key, value string, opts node.WriteOptions) (node.WriteResult, error)
	delete             func(ctx context.Context, key string, opts node.WriteOptions) (node.WriteResult, error)
	get                func(ctx context.Context, key string, opts node.ReadOptions) (node.ReadResult, error)
	list               func(ctx context.Context, prefix string, opts node.ReadOptions) (node.ListResult, error)
	configuration      func() ([]node.ServerInfo, error)
	addVoter           func(ctx context.Context, id, address string) error
	addNonvoter        func(ctx context.Context, id, address string) error
	removeServer       func(ctx context.Context, id string) error
	transferLeadership func(ctx context.Context, targetID string) error
	snapshot           func(ctx context.Context) error

	calls atomic.Int64 // calls of the functions above
}

func (fake *fakeNode) ID() string                   { return fake.id }
func (fake *fakeNode) IsLeader() bool               { return fake.isLeader }
func (fake *fakeNode) Leader() (id, address string) { return fake.leaderID, fake.leaderAddress }
func (fake *fakeNode) Ready() error                 { return fake.readyErr }
func (fake *fakeNode) Status() node.Status          { return fake.status }

func (fake *fakeNode) Put(ctx context.Context, key, value string, opts node.WriteOptions) (node.WriteResult, error) {
	fake.calls.Add(1)
	return fake.put(ctx, key, value, opts)
}

func (fake *fakeNode) Delete(ctx context.Context, key string, opts node.WriteOptions) (node.WriteResult, error) {
	fake.calls.Add(1)
	return fake.delete(ctx, key, opts)
}

func (fake *fakeNode) Get(ctx context.Context, key string, opts node.ReadOptions) (node.ReadResult, error) {
	fake.calls.Add(1)
	return fake.get(ctx, key, opts)
}

func (fake *fakeNode) List(ctx context.Context, prefix string, opts node.ReadOptions) (node.ListResult, error) {
	fake.calls.Add(1)
	return fake.list(ctx, prefix, opts)
}

func (fake *fakeNode) Configuration() ([]node.ServerInfo, error) {
	fake.calls.Add(1)
	return fake.configuration()
}

func (fake *fakeNode) AddVoter(ctx context.Context, id, address string) error {
	fake.calls.Add(1)
	return fake.addVoter(ctx, id, address)
}

func (fake *fakeNode) AddNonvoter(ctx context.Context, id, address string) error {
	fake.calls.Add(1)
	return fake.addNonvoter(ctx, id, address)
}

func (fake *fakeNode) RemoveServer(ctx context.Context, id string) error {
	fake.calls.Add(1)
	return fake.removeServer(ctx, id)
}

func (fake *fakeNode) TransferLeadership(ctx context.Context, targetID string) error {
	fake.calls.Add(1)
	return fake.transferLeadership(ctx, targetID)
}

func (fake *fakeNode) Snapshot(ctx context.Context) error {
	fake.calls.Add(1)
	return fake.snapshot(ctx)
}

// newLeaderFake is a fake that is the ready leader.
func newLeaderFake() *fakeNode {
	return &fakeNode{id: "node1", isLeader: true, leaderID: "node1", leaderAddress: "127.0.0.1:7001"}
}

// newFollowerFake is a fake follower of leader node1.
func newFollowerFake() *fakeNode {
	return &fakeNode{id: "node2", leaderID: "node1", leaderAddress: "127.0.0.1:7001"}
}

// testResponse is a finished HTTP answer with a decoded JSON body.
type testResponse struct {
	status int
	header http.Header
	body   map[string]any
	text   string
}

// field returns a JSON body field as a string ("" when it is missing).
func (response testResponse) field(name string) string {
	value, found := response.body[name]
	if !found {
		return ""
	}
	if text, isText := value.(string); isText {
		return text
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// serveTest runs one request through handler on the test goroutine.
func serveTest(t *testing.T, handler http.Handler, method, target, body string, header http.Header) testResponse {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, values := range header {
		request.Header[name] = values
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return decodeTestResponse(t, recorder.Code, recorder.Header(), recorder.Body)
}

func decodeTestResponse(t *testing.T, status int, header http.Header, body io.Reader) testResponse {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	response := testResponse{status: status, header: header, text: string(data)}
	if strings.HasPrefix(header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(data, &response.body); err != nil {
			t.Fatalf("decode JSON body %q: %v", data, err)
		}
	}
	return response
}

func headerWith(pairs ...string) http.Header {
	header := http.Header{}
	for index := 0; index+1 < len(pairs); index += 2 {
		header.Set(pairs[index], pairs[index+1])
	}
	return header
}
