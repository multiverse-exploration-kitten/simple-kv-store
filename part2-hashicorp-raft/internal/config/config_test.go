package config

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

func lookupFrom(environment map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, found := environment[name]
		return value, found
	}
}

func minimalEnvironment() map[string]string {
	return map[string]string{
		"KV_NODE_ID":                 "node1",
		"KV_RAFT_ADDRESS":            "172.28.2.11:7000",
		"KV_RAFT_INSECURE_PLAINTEXT": "true",
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	loaded, err := Load(lookupFrom(minimalEnvironment()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		NodeID:                "node1",
		RaftAddress:           "172.28.2.11:7000",
		RaftBindAddress:       "0.0.0.0:7000",
		HTTPAddress:           ":8000",
		DataDir:               "/data",
		TimingMultiplier:      1,
		CommandTimeout:        5 * time.Second,
		RaftInsecurePlaintext: true,
		LogLevel:              "info",
		PeerHTTP:              map[string]string{},
	}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("Load = %+v\nwant   %+v", loaded, want)
	}
}

func TestLoadReadsEveryVariable(t *testing.T) {
	environment := map[string]string{
		"KV_NODE_ID":            "node2",
		"KV_RAFT_ADDRESS":       "172.28.2.12:7000",
		"KV_RAFT_BIND_ADDRESS":  "0.0.0.0:7100",
		"KV_HTTP_ADDRESS":       "127.0.0.1:8102",
		"KV_DATA_DIR":           "/var/kv",
		"KV_BOOTSTRAP":          "true",
		"KV_PEERS":              "node1=172.28.2.11:7000, node2=172.28.2.12:7000,node3=172.28.2.13:7000",
		"KV_PEER_HTTP":          "node1=http://172.28.2.11:8000,node3=https://kv3.example:8443",
		"KV_TIMING_MULTIPLIER":  "3",
		"KV_COMMAND_TIMEOUT":    "750ms",
		"KV_RAFT_TLS_CA_FILE":   "/certs/ca.pem",
		"KV_RAFT_TLS_CERT_FILE": "/certs/node2.pem",
		"KV_RAFT_TLS_KEY_FILE":  "/certs/node2-key.pem",
		"KV_LOG_LEVEL":          "DEBUG",
		"KV_LOG_JSON":           "true",
		"KV_ADMIN_TOKEN":        " 5f2b-admin ",
	}
	loaded, err := Load(lookupFrom(environment))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		NodeID:          "node2",
		RaftAddress:     "172.28.2.12:7000",
		RaftBindAddress: "0.0.0.0:7100",
		HTTPAddress:     "127.0.0.1:8102",
		DataDir:         "/var/kv",
		Bootstrap:       true,
		Peers: []node.Server{
			{ID: "node1", Address: "172.28.2.11:7000"},
			{ID: "node2", Address: "172.28.2.12:7000"},
			{ID: "node3", Address: "172.28.2.13:7000"},
		},
		PeerHTTP:         map[string]string{"node1": "http://172.28.2.11:8000", "node3": "https://kv3.example:8443"},
		TimingMultiplier: 3,
		CommandTimeout:   750 * time.Millisecond,
		RaftTLSCAFile:    "/certs/ca.pem",
		RaftTLSCertFile:  "/certs/node2.pem",
		RaftTLSKeyFile:   "/certs/node2-key.pem",
		LogLevel:         "debug",
		LogJSON:          true,
		AdminToken:       "5f2b-admin",
	}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("Load = %+v\nwant   %+v", loaded, want)
	}
}

func TestLoadErrorNamesTheVariable(t *testing.T) {
	testCases := []struct {
		variable string
		value    string
	}{
		{"KV_NODE_ID", ""},
		{"KV_RAFT_ADDRESS", ""},
		{"KV_RAFT_ADDRESS", "node1:7000"},
		{"KV_RAFT_ADDRESS", "0.0.0.0:7000"},
		{"KV_RAFT_ADDRESS", "172.28.2.11"},
		{"KV_RAFT_BIND_ADDRESS", "0.0.0.0:http"},
		{"KV_HTTP_ADDRESS", "8000"},
		{"KV_BOOTSTRAP", "maybe"},
		{"KV_PEERS", "node1"},
		{"KV_PEERS", "node1=172.28.2.11:7000,node1=172.28.2.12:7000"},
		{"KV_PEERS", "node1=localhost:7000"},
		{"KV_PEER_HTTP", "node1=172.28.2.11:8000"},
		{"KV_TIMING_MULTIPLIER", "0"},
		{"KV_TIMING_MULTIPLIER", "11"},
		{"KV_TIMING_MULTIPLIER", "fast"},
		{"KV_COMMAND_TIMEOUT", "5"},
		{"KV_COMMAND_TIMEOUT", "0s"},
		{"KV_LOG_LEVEL", "loud"},
		{"KV_LOG_JSON", "yes please"},
		{"KV_RAFT_INSECURE_PLAINTEXT", "nope"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.variable+"="+testCase.value, func(t *testing.T) {
			environment := minimalEnvironment()
			environment[testCase.variable] = testCase.value
			_, err := Load(lookupFrom(environment))
			if err == nil {
				t.Fatalf("Load accepted %s=%q", testCase.variable, testCase.value)
			}
			if !strings.Contains(err.Error(), testCase.variable) {
				t.Fatalf("error %q does not name %s", err, testCase.variable)
			}
		})
	}
}

func TestLoadRequiresTLSFilesUnlessPlaintextIsAllowed(t *testing.T) {
	environment := minimalEnvironment()
	delete(environment, "KV_RAFT_INSECURE_PLAINTEXT")
	environment["KV_RAFT_TLS_CA_FILE"] = "/certs/ca.pem"
	_, err := Load(lookupFrom(environment))
	if err == nil {
		t.Fatal("Load accepted a config without TLS files and without KV_RAFT_INSECURE_PLAINTEXT")
	}
	for _, variable := range []string{"KV_RAFT_TLS_CERT_FILE", "KV_RAFT_TLS_KEY_FILE"} {
		if !strings.Contains(err.Error(), variable) {
			t.Fatalf("error %q does not name %s", err, variable)
		}
	}
}

func TestLoadBootstrapNeedsThisNodeInPeers(t *testing.T) {
	testCases := map[string]string{
		"no peers":               "",
		"node missing":           "node2=172.28.2.12:7000,node3=172.28.2.13:7000",
		"other address for node": "node1=172.28.2.99:7000,node2=172.28.2.12:7000",
	}
	for name, peers := range testCases {
		t.Run(name, func(t *testing.T) {
			environment := minimalEnvironment()
			environment["KV_BOOTSTRAP"] = "true"
			environment["KV_PEERS"] = peers
			_, err := Load(lookupFrom(environment))
			if err == nil || !strings.Contains(err.Error(), "KV_PEERS") {
				t.Fatalf("Load error = %v, want an error that names KV_PEERS", err)
			}
		})
	}
}
