// Package config reads the node configuration from KV_* environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// Config is the configuration of one kvstore process.
type Config struct {
	NodeID          string
	RaftAddress     string // advertised IP:port of the Raft transport
	RaftBindAddress string
	HTTPAddress     string
	DataDir         string
	Bootstrap       bool
	// Peers are the initial servers, this node included. Every initial node
	// gets the same list.
	Peers []node.Server
	// PeerHTTP maps a node ID to its HTTP base URL, for forwarding to the leader.
	PeerHTTP              map[string]string
	TimingMultiplier      int
	CommandTimeout        time.Duration
	RaftTLSCAFile         string
	RaftTLSCertFile       string
	RaftTLSKeyFile        string
	RaftInsecurePlaintext bool
	LogLevel              string // trace, debug, info, warn or error
	LogJSON               bool
	// AdminToken guards the HTTP calls that change the cluster or take a
	// snapshot. Empty turns these calls off.
	AdminToken string
}

const (
	minTimingMultiplier = 1
	maxTimingMultiplier = 10 // Consul's and Nomad's limit
)

var logLevels = []string{"trace", "debug", "info", "warn", "error"}

// FromEnvironment reads the configuration from the process environment.
func FromEnvironment() (Config, error) {
	return Load(os.LookupEnv)
}

// Load reads the configuration through lookup. An empty value counts as
// unset. The error lists every bad variable by name.
func Load(lookup func(name string) (string, bool)) (Config, error) {
	reader := &environmentReader{lookup: lookup}
	loaded := Config{
		NodeID:                reader.required("KV_NODE_ID"),
		RaftAddress:           reader.advertisedAddress("KV_RAFT_ADDRESS"),
		RaftBindAddress:       reader.listenAddress("KV_RAFT_BIND_ADDRESS", "0.0.0.0:7000"),
		HTTPAddress:           reader.listenAddress("KV_HTTP_ADDRESS", ":8000"),
		DataDir:               reader.value("KV_DATA_DIR", "/data"),
		Bootstrap:             reader.boolean("KV_BOOTSTRAP"),
		Peers:                 reader.peers("KV_PEERS"),
		PeerHTTP:              reader.peerURLs("KV_PEER_HTTP"),
		TimingMultiplier:      reader.timingMultiplier("KV_TIMING_MULTIPLIER"),
		CommandTimeout:        reader.positiveDuration("KV_COMMAND_TIMEOUT", 5*time.Second),
		RaftTLSCAFile:         reader.value("KV_RAFT_TLS_CA_FILE", ""),
		RaftTLSCertFile:       reader.value("KV_RAFT_TLS_CERT_FILE", ""),
		RaftTLSKeyFile:        reader.value("KV_RAFT_TLS_KEY_FILE", ""),
		RaftInsecurePlaintext: reader.boolean("KV_RAFT_INSECURE_PLAINTEXT"),
		LogLevel:              reader.logLevel("KV_LOG_LEVEL"),
		LogJSON:               reader.boolean("KV_LOG_JSON"),
		AdminToken:            reader.value("KV_ADMIN_TOKEN", ""),
	}
	if !loaded.RaftInsecurePlaintext {
		// Fail closed: Raft traffic without TLS needs an explicit opt-in.
		for name, file := range map[string]string{
			"KV_RAFT_TLS_CA_FILE":   loaded.RaftTLSCAFile,
			"KV_RAFT_TLS_CERT_FILE": loaded.RaftTLSCertFile,
			"KV_RAFT_TLS_KEY_FILE":  loaded.RaftTLSKeyFile,
		} {
			if file == "" {
				reader.fail(name, "is required unless KV_RAFT_INSECURE_PLAINTEXT=true")
			}
		}
	}
	if loaded.Bootstrap && loaded.NodeID != "" && loaded.RaftAddress != "" {
		reader.checkBootstrapPeers(loaded)
	}
	if err := errors.Join(reader.problems...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return loaded, nil
}

type environmentReader struct {
	lookup   func(name string) (string, bool)
	problems []error
}

func (reader *environmentReader) fail(name, format string, arguments ...any) {
	reader.problems = append(reader.problems, fmt.Errorf("%s: %s", name, fmt.Sprintf(format, arguments...)))
}

func (reader *environmentReader) value(name, fallback string) string {
	value, _ := reader.lookup(name)
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func (reader *environmentReader) required(name string) string {
	value := reader.value(name, "")
	if value == "" {
		reader.fail(name, "is required")
	}
	return value
}

func (reader *environmentReader) advertisedAddress(name string) string {
	address := reader.required(name)
	if address == "" {
		return ""
	}
	if err := checkAdvertisedAddress(address); err != nil {
		reader.fail(name, "%v", err)
	}
	return address
}

func (reader *environmentReader) listenAddress(name, fallback string) string {
	address := reader.value(name, fallback)
	_, port, err := net.SplitHostPort(address)
	if err == nil {
		err = checkPort(port)
	}
	if err != nil {
		reader.fail(name, "%v", err)
	}
	return address
}

func (reader *environmentReader) boolean(name string) bool {
	text := reader.value(name, "false")
	parsed, err := strconv.ParseBool(text)
	if err != nil {
		reader.fail(name, "must be true or false, got %q", text)
	}
	return parsed
}

func (reader *environmentReader) timingMultiplier(name string) int {
	text := reader.value(name, strconv.Itoa(minTimingMultiplier))
	multiplier, err := strconv.Atoi(text)
	if err != nil || multiplier < minTimingMultiplier || multiplier > maxTimingMultiplier {
		reader.fail(name, "must be a whole number from %d to %d, got %q", minTimingMultiplier, maxTimingMultiplier, text)
	}
	return multiplier
}

func (reader *environmentReader) positiveDuration(name string, fallback time.Duration) time.Duration {
	text := reader.value(name, fallback.String())
	duration, err := time.ParseDuration(text)
	if err != nil || duration <= 0 {
		reader.fail(name, "must be a positive duration such as 5s, got %q", text)
	}
	return duration
}

func (reader *environmentReader) logLevel(name string) string {
	level := strings.ToLower(reader.value(name, "info"))
	for _, known := range logLevels {
		if level == known {
			return level
		}
	}
	reader.fail(name, "must be one of %s, got %q", strings.Join(logLevels, ", "), level)
	return level
}

// peers parses "id=ip:port,id=ip:port".
func (reader *environmentReader) peers(name string) []node.Server {
	var servers []node.Server
	seenIDs := map[string]bool{}
	seenAddresses := map[string]bool{}
	for id, address := range reader.pairs(name) {
		if err := checkAdvertisedAddress(address); err != nil {
			reader.fail(name, "server %s: %v", id, err)
			continue
		}
		if seenIDs[id] || seenAddresses[address] {
			reader.fail(name, "server %s=%s: duplicate ID or address", id, address)
			continue
		}
		seenIDs[id] = true
		seenAddresses[address] = true
		servers = append(servers, node.Server{ID: id, Address: address})
	}
	return servers
}

// peerURLs parses "id=http://host:port,...".
func (reader *environmentReader) peerURLs(name string) map[string]string {
	urls := map[string]string{}
	for id, rawURL := range reader.pairs(name) {
		parsed, err := url.Parse(rawURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			reader.fail(name, "server %s: want a URL such as http://host:8000, got %q", id, rawURL)
			continue
		}
		if _, duplicate := urls[id]; duplicate {
			reader.fail(name, "server %s: duplicate ID", id)
			continue
		}
		urls[id] = rawURL
	}
	return urls
}

// pairs yields the "id=value" items of a comma-separated list in order.
func (reader *environmentReader) pairs(name string) func(yield func(id, value string) bool) {
	return func(yield func(id, value string) bool) {
		for item := range strings.SplitSeq(reader.value(name, ""), ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			id, value, found := strings.Cut(item, "=")
			id, value = strings.TrimSpace(id), strings.TrimSpace(value)
			if !found || id == "" || value == "" {
				reader.fail(name, "want id=value, got %q", item)
				continue
			}
			if !yield(id, value) {
				return
			}
		}
	}
}

func (reader *environmentReader) checkBootstrapPeers(loaded Config) {
	for _, server := range loaded.Peers {
		if server.ID != loaded.NodeID {
			continue
		}
		if server.Address != loaded.RaftAddress {
			reader.fail("KV_PEERS", "server %s has address %s, but KV_RAFT_ADDRESS is %s", server.ID, server.Address, loaded.RaftAddress)
		}
		return
	}
	reader.fail("KV_PEERS", "must list this node (%s) when KV_BOOTSTRAP=true", loaded.NodeID)
}

// checkAdvertisedAddress requires IP:port. raft's TCP transport cannot
// advertise a host name, and one form avoids two names for one server.
func checkAdvertisedAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("want an IP address and a port, got %q", address)
	}
	if ip.IsUnspecified() {
		return fmt.Errorf("%q is not an address that other nodes can reach", address)
	}
	return checkPort(port)
}

func checkPort(port string) error {
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("want a port from 1 to 65535, got %q", port)
	}
	return nil
}
