package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

func newTestAPI(fake *fakeNode) http.Handler {
	return New(fake, PeerDirectory{}, Options{AdminToken: testAdminToken})
}

func casIndex(value uint64) *uint64 { return &value }

func TestPutPassesInputToNodeAndReturnsEntry(t *testing.T) {
	fake := newLeaderFake()
	var gotKey, gotValue string
	var gotOptions node.WriteOptions
	fake.put = func(_ context.Context, key, value string, opts node.WriteOptions) (node.WriteResult, error) {
		gotKey, gotValue, gotOptions = key, value, opts
		return node.WriteResult{Index: 12, Applied: true, Entry: &fsm.Entry{Key: key, Value: value, CreateIndex: 5, ModifyIndex: 12}}, nil
	}
	response := serveTest(t, newTestAPI(fake), "PUT", "/kv/config/db?cas=5", `{"value":"v1"}`, headerWith(HeaderIdempotencyKey, "request-1"))

	if response.status != 200 {
		t.Fatalf("status = %d, body %s", response.status, response.text)
	}
	if gotKey != "config/db" || gotValue != "v1" || gotOptions.IdempotencyKey != "request-1" || gotOptions.CASIndex == nil || *gotOptions.CASIndex != 5 {
		t.Fatalf("node got key %q value %q options %+v", gotKey, gotValue, gotOptions)
	}
	want := map[string]string{"key": "config/db", "value": "v1", "create_index": "5", "modify_index": "12", "index": "12"}
	for name, value := range want {
		if got := response.field(name); got != value {
			t.Errorf("body %s = %q, want %q (body %s)", name, got, value, response.text)
		}
	}
	if response.header.Get(HeaderDuplicate) != "" {
		t.Errorf("%s set on a first write", HeaderDuplicate)
	}
}

func TestPutCASQuery(t *testing.T) {
	tests := []struct {
		query string
		want  *uint64
	}{
		{query: "", want: nil},
		{query: "?cas=0", want: casIndex(0)},
		{query: "?cas=42", want: casIndex(42)},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			fake := newLeaderFake()
			var got *uint64
			fake.put = func(_ context.Context, key, value string, opts node.WriteOptions) (node.WriteResult, error) {
				got = opts.CASIndex
				return node.WriteResult{Index: 1, Applied: true, Entry: &fsm.Entry{Key: key, Value: value}}, nil
			}
			serveTest(t, newTestAPI(fake), "PUT", "/kv/a"+test.query, `{"value":"x"}`, nil)
			if (got == nil) != (test.want == nil) || (got != nil && *got != *test.want) {
				t.Fatalf("CASIndex = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPutDuplicateSetsHeader(t *testing.T) {
	fake := newLeaderFake()
	fake.put = func(_ context.Context, key, value string, _ node.WriteOptions) (node.WriteResult, error) {
		return node.WriteResult{Index: 3, Applied: true, Duplicate: true, Entry: &fsm.Entry{Key: key, Value: value, CreateIndex: 3, ModifyIndex: 3}}, nil
	}
	response := serveTest(t, newTestAPI(fake), "PUT", "/kv/a", `{"value":"x"}`, headerWith(HeaderIdempotencyKey, "request-1"))
	if response.status != 200 || response.header.Get(HeaderDuplicate) != "true" || response.field("index") != "3" {
		t.Fatalf("status %d, %s = %q, body %s", response.status, HeaderDuplicate, response.header.Get(HeaderDuplicate), response.text)
	}
}

// Bad input is rejected before the node is called: the fake has no put.
func TestPutRejectsBadInputLocally(t *testing.T) {
	tests := []struct {
		name, target, body string
		wantStatus         int
		wantField          string
	}{
		{name: "invalid JSON", target: "/kv/a", body: `{"value":`, wantStatus: 400, wantField: "body"},
		{name: "empty body", target: "/kv/a", body: ``, wantStatus: 400, wantField: "body"},
		{name: "missing value", target: "/kv/a", body: `{"other":"x"}`, wantStatus: 400, wantField: "value"},
		{name: "null value", target: "/kv/a", body: `{"value":null}`, wantStatus: 400, wantField: "value"},
		{name: "cas not a number", target: "/kv/a?cas=abc", body: `{"value":"x"}`, wantStatus: 400, wantField: "cas"},
		{name: "cas negative", target: "/kv/a?cas=-1", body: `{"value":"x"}`, wantStatus: 400, wantField: "cas"},
		{name: "body over the limit", target: "/kv/a", body: `{"value":"` + strings.Repeat("a", maxRequestBodyBytes) + `"}`, wantStatus: 413, wantField: "body"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveTest(t, newTestAPI(newLeaderFake()), "PUT", test.target, test.body, nil)
			if response.status != test.wantStatus || response.field("field") != test.wantField {
				t.Fatalf("status %d field %q, want %d %q (body %.200s)", response.status, response.field("field"), test.wantStatus, test.wantField, response.text)
			}
		})
	}
}

// errorCase is one node answer and the HTTP answer that it must give.
type errorCase struct {
	name       string
	result     node.WriteResult
	err        error
	header     map[string]string
	wantStatus int
	wantBody   map[string]string // "" = the field must be missing
	wantHeader map[string]string
}

func writeErrorCases() []errorCase {
	return []errorCase{
		{
			name: "validation", err: &node.ValidationError{Field: "key", Message: "must not be empty"},
			wantStatus: 400, wantBody: map[string]string{"error": "invalid_request", "field": "key", "message": "must not be empty"},
		},
		{
			name: "value too large", err: &node.ValidationError{Field: "value", Message: "too big", TooLarge: true},
			wantStatus: 413, wantBody: map[string]string{"error": "too_large", "field": "value"},
		},
		{
			name: "cas failed on an existing key", result: node.WriteResult{Index: 9, Entry: &fsm.Entry{Key: "a", ModifyIndex: 7}},
			wantStatus: 409, wantBody: map[string]string{"error": "cas_failed", "current_modify_index": "7"},
		},
		{
			name: "cas failed on a missing key", result: node.WriteResult{Index: 9},
			wantStatus: 409, wantBody: map[string]string{"error": "cas_failed", "current_modify_index": "0"},
		},
		{
			name: "idempotency key reused", result: node.WriteResult{Index: 9, Err: node.ErrIdempotencyKeyReused}, err: node.ErrIdempotencyKeyReused,
			header:     map[string]string{HeaderIdempotencyKey: "request-1"},
			wantStatus: 422, wantBody: map[string]string{"error": "idempotency_key_reused"},
		},
		{
			name: "rejected", err: &node.RejectedError{Reason: node.ReasonEnqueueTimeout},
			wantStatus: 503, wantBody: map[string]string{"error": "command_rejected", "reason": "enqueue_timeout", "retry_safe": "true"},
			wantHeader: map[string]string{"Retry-After": "1"},
		},
		{
			name: "outcome unknown without idempotency key", err: &node.OutcomeUnknownError{Reason: node.ReasonLeadershipLost},
			wantStatus: 504, wantBody: map[string]string{"error": "outcome_unknown", "reason": "leadership_lost", "retry_safe": "false", "retry_with_same_idempotency_key": ""},
		},
		{
			name: "outcome unknown with idempotency key", err: &node.OutcomeUnknownError{Reason: node.ReasonTimeout},
			header:     map[string]string{HeaderIdempotencyKey: "request-1"},
			wantStatus: 504, wantBody: map[string]string{"error": "outcome_unknown", "reason": "timeout", "retry_safe": "false", "retry_with_same_idempotency_key": "true"},
		},
		{
			name: "unknown error", err: errors.New("disk on fire"),
			wantStatus: 500, wantBody: map[string]string{"error": "internal_error", "message": "disk on fire"},
		},
	}
}

func checkErrorCase(t *testing.T, test errorCase, response testResponse) {
	t.Helper()
	if response.status != test.wantStatus {
		t.Fatalf("status = %d, want %d (body %s)", response.status, test.wantStatus, response.text)
	}
	for name, value := range test.wantBody {
		if got := response.field(name); got != value {
			t.Errorf("body %s = %q, want %q (body %s)", name, got, value, response.text)
		}
	}
	for name, value := range test.wantHeader {
		if got := response.header.Get(name); got != value {
			t.Errorf("header %s = %q, want %q", name, got, value)
		}
	}
}

func TestPutErrorMapping(t *testing.T) {
	for _, test := range writeErrorCases() {
		t.Run(test.name, func(t *testing.T) {
			fake := newLeaderFake()
			fake.put = func(context.Context, string, string, node.WriteOptions) (node.WriteResult, error) {
				return test.result, test.err
			}
			header := headerWith()
			for name, value := range test.header {
				header.Set(name, value)
			}
			checkErrorCase(t, test, serveTest(t, newTestAPI(fake), "PUT", "/kv/a?cas=1", `{"value":"x"}`, header))
		})
	}
}

func TestDeleteErrorMapping(t *testing.T) {
	for _, test := range writeErrorCases() {
		t.Run(test.name, func(t *testing.T) {
			fake := newLeaderFake()
			fake.delete = func(context.Context, string, node.WriteOptions) (node.WriteResult, error) {
				return test.result, test.err
			}
			header := headerWith()
			for name, value := range test.header {
				header.Set(name, value)
			}
			checkErrorCase(t, test, serveTest(t, newTestAPI(fake), "DELETE", "/kv/a?cas=1", "", header))
		})
	}
}

func TestDeleteReturnsWhetherTheKeyExisted(t *testing.T) {
	for _, existed := range []bool{true, false} {
		t.Run(fmt.Sprint(existed), func(t *testing.T) {
			fake := newLeaderFake()
			var gotKey string
			var gotOptions node.WriteOptions
			fake.delete = func(_ context.Context, key string, opts node.WriteOptions) (node.WriteResult, error) {
				gotKey, gotOptions = key, opts
				return node.WriteResult{Index: 21, Applied: true, Existed: existed}, nil
			}
			response := serveTest(t, newTestAPI(fake), "DELETE", "/kv/dir/a?cas=0", "", headerWith(HeaderIdempotencyKey, "request-2"))
			if response.status != 200 || response.field("key") != "dir/a" || response.field("deleted") != fmt.Sprint(existed) || response.field("index") != "21" {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
			if gotKey != "dir/a" || gotOptions.CASIndex == nil || *gotOptions.CASIndex != 0 || gotOptions.IdempotencyKey != "request-2" {
				t.Fatalf("node got key %q options %+v", gotKey, gotOptions)
			}
		})
	}
}

func TestGetReturnsEntryAndReadHeaders(t *testing.T) {
	fake := newFollowerFake()
	var gotKey string
	var gotOptions node.ReadOptions
	fake.get = func(_ context.Context, key string, opts node.ReadOptions) (node.ReadResult, error) {
		gotKey, gotOptions = key, opts
		return node.ReadResult{
			Found: true,
			Entry: fsm.Entry{Key: key, Value: "v", CreateIndex: 4, ModifyIndex: 8},
			Meta:  node.ReadMeta{Index: 30, KnownLeader: true, LastContact: 1500 * time.Microsecond, ServedBy: "node2"},
		}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/kv/dir/a?consistency=stale&max_stale=250ms", "", nil)

	if response.status != 200 {
		t.Fatalf("status = %d, body %s", response.status, response.text)
	}
	if gotKey != "dir/a" || gotOptions.Consistency != node.Stale || gotOptions.MaxStale != 250*time.Millisecond {
		t.Fatalf("node got key %q options %+v", gotKey, gotOptions)
	}
	for name, value := range map[string]string{"key": "dir/a", "value": "v", "create_index": "4", "modify_index": "8"} {
		if got := response.field(name); got != value {
			t.Errorf("body %s = %q, want %q", name, got, value)
		}
	}
	for name, value := range map[string]string{HeaderIndex: "30", HeaderKnownLeader: "true", HeaderLastContact: "1", HeaderServedBy: "node2"} {
		if got := response.header.Get(name); got != value {
			t.Errorf("header %s = %q, want %q", name, got, value)
		}
	}
}

func TestGetConsistencyQuery(t *testing.T) {
	tests := []struct {
		query string
		want  node.ReadConsistency
	}{
		{query: "", want: node.Linearizable},
		{query: "?consistency=linearizable", want: node.Linearizable},
		{query: "?consistency=leader", want: node.LeaderLocal},
		{query: "?consistency=stale", want: node.Stale},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			fake := newLeaderFake()
			var got node.ReadOptions
			fake.get = func(_ context.Context, key string, opts node.ReadOptions) (node.ReadResult, error) {
				got = opts
				return node.ReadResult{Found: true, Entry: fsm.Entry{Key: key}}, nil
			}
			serveTest(t, newTestAPI(fake), "GET", "/kv/a"+test.query, "", nil)
			if got.Consistency != test.want || got.MaxStale != 0 {
				t.Fatalf("options = %+v, want consistency %s", got, test.want)
			}
		})
	}
}

// A missing key is 404, and the headers still say which state served it.
func TestGetMissingKeyIs404WithReadHeaders(t *testing.T) {
	fake := newLeaderFake()
	fake.get = func(context.Context, string, node.ReadOptions) (node.ReadResult, error) {
		return node.ReadResult{Meta: node.ReadMeta{Index: 6, KnownLeader: true, ServedBy: "node1"}}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/kv/missing", "", nil)
	if response.status != 404 || response.field("error") != "key_not_found" || response.field("key") != "missing" {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
	if response.header.Get(HeaderIndex) != "6" || response.header.Get(HeaderServedBy) != "node1" || response.header.Get(HeaderLastContact) != "0" {
		t.Fatalf("headers %v", response.header)
	}
}

// LastContact -1 ns means "never heard a leader". Milliseconds() would round it to 0.
func TestGetNeverContactedLeaderIsMinusOne(t *testing.T) {
	fake := newFollowerFake()
	fake.get = func(_ context.Context, key string, _ node.ReadOptions) (node.ReadResult, error) {
		return node.ReadResult{Found: true, Entry: fsm.Entry{Key: key}, Meta: node.ReadMeta{LastContact: -1, ServedBy: "node2"}}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/kv/a?consistency=stale", "", nil)
	if got := response.header.Get(HeaderLastContact); got != "-1" || response.header.Get(HeaderKnownLeader) != "false" {
		t.Fatalf("%s = %q, %s = %q, want -1 and false", HeaderLastContact, got, HeaderKnownLeader, response.header.Get(HeaderKnownLeader))
	}
}

func TestReadRejectsBadQueryLocally(t *testing.T) {
	for _, target := range []string{"/kv/a?consistency=strong", "/kv/a?consistency=stale&max_stale=soon", "/kv?consistency=strong", "/kv?max_stale=-"} {
		t.Run(target, func(t *testing.T) {
			response := serveTest(t, newTestAPI(newLeaderFake()), "GET", target, "", nil)
			if response.status != 400 || response.field("error") != "invalid_request" {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
		})
	}
}

func TestReadErrorMapping(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantError  string
	}{
		{err: &node.ValidationError{Field: "max_stale", Message: "needs consistency=stale"}, wantStatus: 400, wantError: "invalid_request"},
		{err: &node.RejectedError{Reason: node.ReasonVerifyFailed}, wantStatus: 503, wantError: "command_rejected"},
		{err: &node.RejectedError{Reason: node.ReasonTooStale}, wantStatus: 503, wantError: "command_rejected"},
	}
	for _, test := range tests {
		t.Run(test.err.Error(), func(t *testing.T) {
			fake := newLeaderFake()
			fake.get = func(context.Context, string, node.ReadOptions) (node.ReadResult, error) {
				return node.ReadResult{}, test.err
			}
			fake.list = func(context.Context, string, node.ReadOptions) (node.ListResult, error) {
				return node.ListResult{}, test.err
			}
			for _, target := range []string{"/kv/a", "/kv"} {
				response := serveTest(t, newTestAPI(fake), "GET", target, "", nil)
				if response.status != test.wantStatus || response.field("error") != test.wantError || response.header.Get(HeaderServedBy) != "" {
					t.Fatalf("GET %s: status %d body %s headers %v", target, response.status, response.text, response.header)
				}
			}
		})
	}
}

func TestListReturnsEntriesAndReadHeaders(t *testing.T) {
	fake := newLeaderFake()
	var gotPrefix string
	var gotOptions node.ReadOptions
	fake.list = func(_ context.Context, prefix string, opts node.ReadOptions) (node.ListResult, error) {
		gotPrefix, gotOptions = prefix, opts
		return node.ListResult{
			Entries: []fsm.Entry{{Key: "dir/a", Value: "1", CreateIndex: 2, ModifyIndex: 2}, {Key: "dir/b", Value: "2", CreateIndex: 3, ModifyIndex: 5}},
			Meta:    node.ReadMeta{Index: 5, KnownLeader: true, ServedBy: "node1"},
		}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/kv?prefix=dir/&consistency=leader", "", nil)
	if response.status != 200 || gotPrefix != "dir/" || gotOptions.Consistency != node.LeaderLocal {
		t.Fatalf("status %d, node got prefix %q options %+v", response.status, gotPrefix, gotOptions)
	}
	want := `[{"create_index":2,"key":"dir/a","modify_index":2,"value":"1"},{"create_index":3,"key":"dir/b","modify_index":5,"value":"2"}]`
	if got := response.field("entries"); got != want {
		t.Fatalf("entries = %s, want %s", got, want)
	}
	if response.header.Get(HeaderIndex) != "5" || response.header.Get(HeaderServedBy) != "node1" {
		t.Fatalf("headers %v", response.header)
	}
}

func TestListWithoutEntriesIsEmptyArray(t *testing.T) {
	fake := newLeaderFake()
	fake.list = func(context.Context, string, node.ReadOptions) (node.ListResult, error) {
		return node.ListResult{}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/kv", "", nil)
	if response.status != 200 || response.field("entries") != "[]" {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
}

func TestStatusReturnsNodeStatus(t *testing.T) {
	fake := newFollowerFake()
	fake.status = node.Status{ID: "node2", State: "Follower", Term: 4, LeaderID: "node1", LastContactMillis: -1}
	response := serveTest(t, newTestAPI(fake), "GET", "/status", "", nil)
	if response.status != 200 || response.field("id") != "node2" || response.field("term") != "4" || response.field("leader_id") != "node1" || response.field("last_contact_ms") != "-1" {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
}

func TestHealthEndpoints(t *testing.T) {
	notReady := &node.RejectedError{Reason: node.ReasonNotReady}
	tests := []struct {
		name       string
		fake       *fakeNode
		path       string
		wantStatus int
		wantBody   map[string]string
	}{
		{name: "live", fake: &fakeNode{id: "node3", readyErr: &node.RejectedError{Reason: node.ReasonNoLeader}}, path: "/health/live", wantStatus: 200, wantBody: map[string]string{"status": "ok"}},
		{name: "ready follower", fake: newFollowerFake(), path: "/health/ready", wantStatus: 200, wantBody: map[string]string{"status": "ready"}},
		{name: "not ready without leader", fake: &fakeNode{id: "node3", readyErr: &node.RejectedError{Reason: node.ReasonNoLeader}}, path: "/health/ready", wantStatus: 503, wantBody: map[string]string{"status": "not_ready", "reason": "no_leader"}},
		{name: "ready leader", fake: newLeaderFake(), path: "/health/leader", wantStatus: 200, wantBody: map[string]string{"status": "leader"}},
		{name: "leader before its barrier", fake: &fakeNode{id: "node1", isLeader: true, leaderID: "node1", readyErr: notReady}, path: "/health/leader", wantStatus: 503, wantBody: map[string]string{"status": "not_ready", "reason": "not_ready"}},
		{name: "follower", fake: newFollowerFake(), path: "/health/leader", wantStatus: 429, wantBody: map[string]string{"status": "follower", "leader_id": "node1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveTest(t, newTestAPI(test.fake), "GET", test.path, "", nil)
			if response.status != test.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", response.status, test.wantStatus, response.text)
			}
			for name, value := range test.wantBody {
				if got := response.field(name); got != value {
					t.Errorf("body %s = %q, want %q", name, got, value)
				}
			}
		})
	}
}

func TestListServersOnLeader(t *testing.T) {
	fake := newLeaderFake()
	fake.configuration = func() ([]node.ServerInfo, error) {
		return []node.ServerInfo{{ID: "node1", Address: "127.0.0.1:7001", Suffrage: node.SuffrageVoter}, {ID: "node4", Address: "127.0.0.1:7004", Suffrage: node.SuffrageNonvoter}}, nil
	}
	response := serveTest(t, newTestAPI(fake), "GET", "/cluster/servers", "", nil)
	want := `[{"address":"127.0.0.1:7001","id":"node1","suffrage":"voter"},{"address":"127.0.0.1:7004","id":"node4","suffrage":"nonvoter"}]`
	if response.status != 200 || response.field("servers") != want {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
}

func TestAddServerChoosesVoterOrNonvoter(t *testing.T) {
	for _, voter := range []bool{true, false} {
		t.Run(fmt.Sprint(voter), func(t *testing.T) {
			fake := newLeaderFake()
			var added string
			record := func(kind string) func(context.Context, string, string) error {
				return func(_ context.Context, id, address string) error {
					added = kind + " " + id + " " + address
					return nil
				}
			}
			fake.addVoter, fake.addNonvoter = record("voter"), record("nonvoter")
			body := fmt.Sprintf(`{"id":"node4","address":"127.0.0.1:7004","voter":%t}`, voter)
			response := serveTest(t, newTestAPI(fake), "POST", "/cluster/servers", body, adminHeader())

			wantSuffrage := map[bool]string{true: "voter", false: "nonvoter"}[voter]
			if response.status != 200 || added != wantSuffrage+" node4 127.0.0.1:7004" || response.field("suffrage") != wantSuffrage || response.field("id") != "node4" {
				t.Fatalf("status %d body %s, node call %q", response.status, response.text, added)
			}
		})
	}
}

func TestAddServerRejectsInvalidJSONLocally(t *testing.T) {
	response := serveTest(t, newTestAPI(newLeaderFake()), "POST", "/cluster/servers", `{"id":`, adminHeader())
	if response.status != 400 || response.field("field") != "body" {
		t.Fatalf("status %d body %s", response.status, response.text)
	}
}

func TestRemoveServer(t *testing.T) {
	fake := newLeaderFake()
	var removed string
	fake.removeServer = func(_ context.Context, id string) error {
		removed = id
		return nil
	}
	response := serveTest(t, newTestAPI(fake), "DELETE", "/cluster/servers/node3", "", adminHeader())
	if response.status != 200 || removed != "node3" || response.field("status") != "ok" {
		t.Fatalf("status %d body %s, removed %q", response.status, response.text, removed)
	}
}

func TestTransferLeadershipTarget(t *testing.T) {
	tests := []struct{ name, body, want string }{
		{name: "no body", body: "", want: ""},
		{name: "empty object", body: `{}`, want: ""},
		{name: "target", body: `{"id":"node3"}`, want: "node3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newLeaderFake()
			target := "not called"
			fake.transferLeadership = func(_ context.Context, targetID string) error {
				target = targetID
				return nil
			}
			response := serveTest(t, newTestAPI(fake), "POST", "/cluster/transfer-leadership", test.body, adminHeader())
			if response.status != 200 || target != test.want {
				t.Fatalf("status %d body %s, target %q, want %q", response.status, response.text, target, test.want)
			}
		})
	}
}

func TestClusterCallErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantReason string
	}{
		{name: "rejected", err: &node.RejectedError{Reason: node.ReasonEnqueueTimeout}, wantStatus: 503, wantReason: "enqueue_timeout"},
		{name: "outcome unknown", err: &node.OutcomeUnknownError{Reason: node.ReasonTimeout}, wantStatus: 504, wantReason: "timeout"},
		{name: "validation", err: &node.ValidationError{Field: "id", Message: "must not be empty"}, wantStatus: 400},
	}
	calls := []struct{ method, path, body string }{
		{method: "POST", path: "/cluster/servers", body: `{"id":"node4","address":"127.0.0.1:7004","voter":true}`},
		{method: "DELETE", path: "/cluster/servers/node4"},
		{method: "POST", path: "/cluster/transfer-leadership", body: `{"id":"node2"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newLeaderFake()
			fake.addVoter = func(context.Context, string, string) error { return test.err }
			fake.removeServer = func(context.Context, string) error { return test.err }
			fake.transferLeadership = func(context.Context, string) error { return test.err }
			for _, clusterCall := range calls {
				response := serveTest(t, newTestAPI(fake), clusterCall.method, clusterCall.path, clusterCall.body, adminHeader())
				if response.status != test.wantStatus || response.field("reason") != test.wantReason {
					t.Fatalf("%s %s: status %d body %s", clusterCall.method, clusterCall.path, response.status, response.text)
				}
			}
		})
	}
	t.Run("list servers rejected", func(t *testing.T) {
		fake := newLeaderFake()
		fake.configuration = func() ([]node.ServerInfo, error) { return nil, &node.RejectedError{Reason: node.ReasonShutdown} }
		response := serveTest(t, newTestAPI(fake), "GET", "/cluster/servers", "", nil)
		if response.status != 503 || response.field("reason") != "shutdown" {
			t.Fatalf("status %d body %s", response.status, response.text)
		}
	})
}

func TestSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantError  string
	}{
		{name: "taken", wantStatus: 200},
		{name: "nothing new", err: fmt.Errorf("snapshot: %w", raft.ErrNothingNewToSnapshot), wantStatus: 409, wantError: "nothing_to_snapshot"},
		{name: "shutdown", err: &node.RejectedError{Reason: node.ReasonShutdown}, wantStatus: 503, wantError: "command_rejected"},
		{name: "timeout", err: &node.OutcomeUnknownError{Reason: node.ReasonTimeout}, wantStatus: 504, wantError: "outcome_unknown"},
		{name: "disk error", err: errors.New("snapshot: no space left"), wantStatus: 500, wantError: "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newLeaderFake()
			fake.snapshot = func(context.Context) error { return test.err }
			response := serveTest(t, newTestAPI(fake), "POST", "/admin/snapshot", "", adminHeader())
			if response.status != test.wantStatus || response.field("error") != test.wantError {
				t.Fatalf("status %d body %s", response.status, response.text)
			}
		})
	}
}

func TestMetricsServesPrometheusText(t *testing.T) {
	response := serveTest(t, newTestAPI(newLeaderFake()), "GET", "/metrics", "", nil)
	if response.status != 200 || !strings.Contains(response.text, "go_goroutines") {
		t.Fatalf("status %d body %.300s", response.status, response.text)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	response := serveTest(t, newTestAPI(newLeaderFake()), "POST", "/kv/a", `{"value":"x"}`, nil)
	if response.status != 405 {
		t.Fatalf("status = %d, want 405", response.status)
	}
}
