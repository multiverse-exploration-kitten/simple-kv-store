package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/config"
)

const serveTestTimeout = 20 * time.Second

// TestServeStopsCleanlyAndKeepsTheDataForTheNextStart starts a 1-node
// cluster, writes a key, stops it as on SIGTERM, and starts it again on the
// same data dir and ports. The second start only works when the first stop
// released the raft.db lock and both ports.
//
// The single node is the leader, and it has no peer to hand leadership to.
// So the stop also proves that a failed leadership transfer does not block
// the shutdown.
func TestServeStopsCleanlyAndKeepsTheDataForTheNextStart(t *testing.T) {
	raftAddress, httpAddress := freeAddress(t), freeAddress(t)
	serverConfig, err := config.Load(mapLookup(map[string]string{
		"KV_NODE_ID":                 "node1",
		"KV_RAFT_ADDRESS":            raftAddress,
		"KV_RAFT_BIND_ADDRESS":       raftAddress,
		"KV_HTTP_ADDRESS":            httpAddress,
		"KV_DATA_DIR":                t.TempDir(),
		"KV_BOOTSTRAP":               "true",
		"KV_PEERS":                   "node1=" + raftAddress,
		"KV_PEER_HTTP":               "node1=http://" + httpAddress,
		"KV_RAFT_INSECURE_PLAINTEXT": "true",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	baseURL := "http://" + httpAddress
	client := &http.Client{Timeout: 10 * time.Second}

	stopFirst := startServe(t, serverConfig)
	waitForStatus(t, client, baseURL+"/health/leader", http.StatusOK)
	putResponse := request(t, client, http.MethodPut, baseURL+"/kv/greeting", `{"value":"hello"}`)
	if putResponse.status != http.StatusOK {
		t.Fatalf("PUT /kv/greeting = %d %s, want 200", putResponse.status, putResponse.body)
	}
	stopFirst()

	stopSecond := startServe(t, serverConfig)
	defer stopSecond()
	waitForStatus(t, client, baseURL+"/health/leader", http.StatusOK)
	getResponse := request(t, client, http.MethodGet, baseURL+"/kv/greeting", "")
	var entry struct{ Value string }
	if getResponse.status != http.StatusOK || json.Unmarshal(getResponse.body, &entry) != nil || entry.Value != "hello" {
		t.Fatalf("GET /kv/greeting after restart = %d %s, want 200 with value hello", getResponse.status, getResponse.body)
	}
}

// startServe runs serve in a goroutine. The returned stop function cancels
// it, as SIGTERM does, and fails the test when serve returns an error or
// does not return in time. It runs at most once.
func startServe(t *testing.T, serverConfig config.Config) (stop func()) {
	t.Helper()
	var logs bytes.Buffer // hclog serializes the writes of one logger tree
	logger := hclog.New(&hclog.LoggerOptions{Name: "kvstore", Level: hclog.Debug, Output: &logs})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- serve(ctx, serverConfig, logger) }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("serve returned %v\nlogs:\n%s", err, logs.String())
			}
		case <-time.After(serveTestTimeout):
			t.Fatalf("serve did not return %s after the stop", serveTestTimeout)
		}
		if t.Failed() {
			t.Logf("logs:\n%s", logs.String())
		}
	}
	t.Cleanup(stop)
	return stop
}

type response struct {
	status int
	body   []byte
}

func request(t *testing.T, client *http.Client, method, url, body string) response {
	t.Helper()
	httpRequest, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer httpResponse.Body.Close()
	responseBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		t.Fatalf("read the body of %s %s: %v", method, url, err)
	}
	return response{status: httpResponse.StatusCode, body: responseBody}
}

func waitForStatus(t *testing.T, client *http.Client, url string, want int) {
	t.Helper()
	deadline := time.Now().Add(serveTestTimeout)
	lastResult := "no answer yet"
	for time.Now().Before(deadline) {
		httpResponse, err := client.Get(url)
		if err == nil {
			_ = httpResponse.Body.Close()
			if httpResponse.StatusCode == want {
				return
			}
			lastResult = httpResponse.Status
		} else {
			lastResult = err.Error()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for GET %s to return %d (last: %s)", serveTestTimeout, url, want, lastResult)
}

// freeAddress returns a 127.0.0.1 address with a port that was free a
// moment ago.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, found := values[name]
		return value, found
	}
}
