package cluster

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/httpapi"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

var httpTestClient = &http.Client{Timeout: 10 * time.Second}

// httpStableTiming uses 500 ms timeouts instead of node.TestTiming. These
// tests check which node serves a call, so a leader change in the middle
// (a process pause of 230 ms under parallel -race load) would break them.
func httpStableTiming() node.Timing {
	return node.Timing{
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}
}

// httpStartServers starts one httptest.Server per running node with the
// httpapi handler of that node. Every handler gets the URLs of all nodes as
// its PeerDirectory. t.Cleanup closes the servers before the nodes stop.
func httpStartServers(t *testing.T, cluster *Cluster, opts httpapi.Options) map[string]*httptest.Server {
	t.Helper()
	servers := map[string]*httptest.Server{}
	peers := httpapi.PeerDirectory{}
	for _, running := range cluster.Nodes() {
		// The listener exists before Start, so every URL is known before
		// the first handler is built.
		server := httptest.NewUnstartedServer(nil)
		servers[running.ID()] = server
		peers[running.ID()] = "http://" + server.Listener.Addr().String()
	}
	for _, running := range cluster.Nodes() {
		server := servers[running.ID()]
		server.Config.Handler = httpapi.New(running, peers, opts)
		server.Start()
		t.Cleanup(server.Close)
	}
	return servers
}

type httpAnswer struct {
	status int
	header http.Header
	body   map[string]any
}

// number returns a JSON number field of the body (0 when it is missing).
func (answer httpAnswer) number(name string) uint64 {
	value, _ := answer.body[name].(float64)
	return uint64(value)
}

func httpCall(t *testing.T, method, url, body string) httpAnswer {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build %s %s: %v", method, url, err)
	}
	response, err := httpTestClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read the answer of %s %s: %v", method, url, err)
	}
	answer := httpAnswer{status: response.StatusCode, header: response.Header}
	if err := json.Unmarshal(data, &answer.body); err != nil {
		t.Fatalf("%s %s: status %d, body %q is not JSON: %v", method, url, response.StatusCode, data, err)
	}
	return answer
}

// Scenario: a client sends a PUT to a follower's HTTP API. Then it reads
// the key with the default consistency (linearizable) through every node.
// Expected: the follower forwards the write to the leader, and the answer
// has the Raft index. Every node applies the write. Every read returns the
// value, and the leader served each one (X-KV-Served-By).
// Raft lesson: hashicorp/raft accepts Apply only on the leader and does not
// forward. The HTTP layer must find the leader and forward, as Consul's
// ForwardRPC does. A linearizable read must run on the leader too.
func TestHTTPWriteThroughFollowerIsForwardedToLeader(t *testing.T) {
	cluster := New(t, Options{Timing: httpStableTiming()})
	leader := cluster.WaitForLeader()
	servers := httpStartServers(t, cluster, httpapi.Options{HoldTimeout: 2 * time.Second, ForwardTimeout: 5 * time.Second})
	follower := cluster.Followers(leader.ID())[0]

	written := httpCall(t, "PUT", servers[follower.ID()].URL+"/kv/greeting", `{"value":"hello"}`)
	if written.status != 200 || written.body["value"] != "hello" || written.number("index") == 0 || written.number("modify_index") != written.number("index") {
		t.Fatalf("PUT through follower %s: status %d body %v", follower.ID(), written.status, written.body)
	}
	cluster.WaitForValueEverywhere("greeting", "hello")

	for _, id := range cluster.IDs() {
		read := httpCall(t, "GET", servers[id].URL+"/kv/greeting", "")
		if read.status != 200 || read.body["value"] != "hello" {
			t.Fatalf("GET through %s: status %d body %v", id, read.status, read.body)
		}
		if servedBy := read.header.Get(httpapi.HeaderServedBy); servedBy != leader.ID() {
			t.Fatalf("GET through %s was served by %q, want the leader %s", id, servedBy, leader.ID())
		}
	}
}

// Scenario: write a key, wait until every node applied it, and close the
// leader's HTTP server. Then read the key through each follower, once with
// consistency=stale and once with the default consistency.
// Expected: the stale read is 200 with the value. X-KV-Served-By names the
// follower itself, X-KV-Known-Leader is true and X-KV-Index is at least the
// index of the write. The linearizable read must reach the leader. The
// follower holds it for HoldTimeout (a dial error changed nothing), then it
// fails with 503 leader_unreachable.
// Raft lesson: a stale read skips the leader. It is cheap and works without
// the leader, but it can be old. The headers tell the client how old.
func TestHTTPStaleReadOnFollowerIsServedLocally(t *testing.T) {
	cluster := New(t, Options{Timing: httpStableTiming()})
	leader := cluster.WaitForLeader()
	// A short hold: the linearizable reads below wait for all of it.
	const holdTimeout = 300 * time.Millisecond
	servers := httpStartServers(t, cluster, httpapi.Options{HoldTimeout: holdTimeout, ForwardTimeout: 5 * time.Second})

	written := httpCall(t, "PUT", servers[leader.ID()].URL+"/kv/color", `{"value":"blue"}`)
	if written.status != 200 {
		t.Fatalf("PUT on the leader: status %d body %v", written.status, written.body)
	}
	cluster.WaitForValueEverywhere("color", "blue")
	servers[leader.ID()].Close()

	for _, follower := range cluster.Followers(leader.ID()) {
		stale := httpCall(t, "GET", servers[follower.ID()].URL+"/kv/color?consistency=stale", "")
		if stale.status != 200 || stale.body["value"] != "blue" {
			t.Fatalf("stale GET on %s: status %d body %v", follower.ID(), stale.status, stale.body)
		}
		readIndex, _ := strconv.ParseUint(stale.header.Get(httpapi.HeaderIndex), 10, 64)
		lastContact, _ := strconv.Atoi(stale.header.Get(httpapi.HeaderLastContact))
		if stale.header.Get(httpapi.HeaderServedBy) != follower.ID() || stale.header.Get(httpapi.HeaderKnownLeader) != "true" || readIndex < written.number("index") || lastContact < 0 {
			t.Fatalf("stale GET on %s: headers %v, want served by itself at index >= %d", follower.ID(), stale.header, written.number("index"))
		}

		started := time.Now()
		linearizable := httpCall(t, "GET", servers[follower.ID()].URL+"/kv/color", "")
		if linearizable.status != 503 || linearizable.body["reason"] != httpapi.ReasonLeaderUnreachable {
			t.Fatalf("linearizable GET on %s without the leader's HTTP server: status %d body %v", follower.ID(), linearizable.status, linearizable.body)
		}
		if waited := time.Since(started); waited < holdTimeout {
			t.Fatalf("linearizable GET on %s answered after %s, want a hold of at least %s", follower.ID(), waited, holdTimeout)
		}
	}
}
