package httpapi

import (
	"context"
	"net/http"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const testAdminToken = "test-admin-token"

func adminHeader() http.Header {
	return headerWith("Authorization", "Bearer "+testAdminToken)
}

type adminCall struct{ method, path, body string }

// adminCalls change the cluster or write a snapshot. They need the token.
var adminCalls = []adminCall{
	{method: "POST", path: "/cluster/servers", body: `{"id":"node4","address":"127.0.0.1:7004"}`},
	{method: "DELETE", path: "/cluster/servers/node4"},
	{method: "POST", path: "/cluster/transfer-leadership", body: `{"id":"node2"}`},
	{method: "POST", path: "/admin/snapshot"},
}

// newAdminLeaderFake is a ready leader whose admin calls all succeed.
func newAdminLeaderFake() *fakeNode {
	fake := newLeaderFake()
	fake.addNonvoter = func(context.Context, string, string) error { return nil }
	fake.removeServer = func(context.Context, string) error { return nil }
	fake.transferLeadership = func(context.Context, string) error { return nil }
	fake.snapshot = func(context.Context) error { return nil }
	return fake
}

func TestAdminCallWithTheTokenRuns(t *testing.T) {
	for _, call := range adminCalls {
		t.Run(call.method+" "+call.path, func(t *testing.T) {
			fake := newAdminLeaderFake()
			response := serveTest(t, newTestAPI(fake), call.method, call.path, call.body, adminHeader())
			if response.status != 200 || fake.calls.Load() != 1 {
				t.Fatalf("status %d body %s, node calls %d: want 200 and 1 node call", response.status, response.text, fake.calls.Load())
			}
		})
	}
}

func TestAdminCallWithoutAValidTokenIs401(t *testing.T) {
	authorizations := map[string]string{
		"no header":         "",
		"wrong token":       "Bearer wrong-token",
		"token prefix":      "Bearer " + testAdminToken[:len(testAdminToken)-1],
		"token with suffix": "Bearer " + testAdminToken + "x",
		"no scheme":         testAdminToken,
		"basic scheme":      "Basic " + testAdminToken,
		"empty bearer":      "Bearer ",
	}
	for _, call := range adminCalls {
		for name, authorization := range authorizations {
			t.Run(call.method+" "+call.path+" "+name, func(t *testing.T) {
				fake := newAdminLeaderFake()
				header := http.Header{}
				if authorization != "" {
					header.Set("Authorization", authorization)
				}
				response := serveTest(t, newTestAPI(fake), call.method, call.path, call.body, header)
				if response.status != 401 || response.field("error") != "unauthorized" || response.header.Get("WWW-Authenticate") == "" {
					t.Fatalf("status %d header %v body %s: want 401 unauthorized with WWW-Authenticate", response.status, response.header, response.text)
				}
				if fake.calls.Load() != 0 {
					t.Fatalf("the node was called %d times, want 0", fake.calls.Load())
				}
			})
		}
	}
}

// The scheme name is case-insensitive (RFC 9110 section 11.1).
func TestAdminTokenSchemeIgnoresCase(t *testing.T) {
	fake := newAdminLeaderFake()
	response := serveTest(t, newTestAPI(fake), "POST", "/admin/snapshot", "", headerWith("Authorization", "bearer "+testAdminToken))
	if response.status != 200 {
		t.Fatalf("status %d body %s, want 200", response.status, response.text)
	}
}

// Fail closed: a node without a token runs no admin call, also when the
// client sends a token.
func TestAdminCallWithoutAConfiguredTokenIs403(t *testing.T) {
	for _, call := range adminCalls {
		for name, header := range map[string]http.Header{"no header": nil, "with a token": adminHeader()} {
			t.Run(call.method+" "+call.path+" "+name, func(t *testing.T) {
				fake := newAdminLeaderFake()
				handler := New(fake, PeerDirectory{}, Options{})
				response := serveTest(t, handler, call.method, call.path, call.body, header)
				if response.status != 403 || response.field("error") != "admin_disabled" {
					t.Fatalf("status %d body %s: want 403 admin_disabled", response.status, response.text)
				}
				if fake.calls.Load() != 0 {
					t.Fatalf("the node was called %d times, want 0", fake.calls.Load())
				}
			})
		}
	}
}

func TestListServersNeedsNoToken(t *testing.T) {
	for name, opts := range map[string]Options{"token configured": {AdminToken: testAdminToken}, "no token configured": {}} {
		t.Run(name, func(t *testing.T) {
			fake := newLeaderFake()
			fake.configuration = func() ([]node.ServerInfo, error) { return nil, nil }
			response := serveTest(t, New(fake, PeerDirectory{}, opts), "GET", "/cluster/servers", "", nil)
			if response.status != 200 {
				t.Fatalf("status %d body %s, want 200", response.status, response.text)
			}
		})
	}
}

// A follower checks the token before it forwards. A bad token never reaches
// the leader.
func TestFollowerDoesNotForwardAnAdminCallWithABadToken(t *testing.T) {
	leader := newFakeLeaderServer(t, answerJSON(200, `{"status":"ok"}`))
	handler := New(newFollowerFake(), PeerDirectory{"node1": leader.server.URL}, Options{AdminToken: testAdminToken})
	response := serveTest(t, handler, "POST", "/admin/snapshot", "", headerWith("Authorization", "Bearer wrong-token"))
	if response.status != 401 {
		t.Fatalf("status %d body %s, want 401", response.status, response.text)
	}
	if received := leader.requests(); len(received) != 0 {
		t.Fatalf("the leader got %d requests, want 0", len(received))
	}
}
