// Package cluster runs real Raft nodes (TCP on 127.0.0.1, BoltDB and file
// snapshots in t.TempDir()) inside the test process, with fault injection.
package cluster

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const (
	pollInterval          = 10 * time.Millisecond
	transportTimeout      = 2 * time.Second
	transportMaxPool      = 3
	defaultCommandTimeout = 2 * time.Second
	logLinesOnFailure     = 60

	// raft logs this once when the transport does not implement
	// raft.WithPreVote. Every cluster test fails when it appears.
	preVoteDisabledWarning = "pre-vote is disabled because it is not supported by the Transport"
)

// WaitUntil calls probe until it returns true. It fails the test after
// timeout with the description of what it waited for. Call it from the test
// goroutine.
func WaitUntil(t testing.TB, timeout time.Duration, description string, probe func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !probe() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, description)
		}
		time.Sleep(pollInterval)
	}
}

// Options configure a test cluster. Zero values get defaults: Size 3,
// Timing node.TestTiming(), Snapshots node.DefaultSnapshotPolicy(),
// CommandTimeout 2 s, NewTransport = plain TCP.
type Options struct {
	Size            int
	Timing          node.Timing
	Snapshots       node.SnapshotPolicy
	CommandTimeout  time.Duration
	PreVoteDisabled bool
	// NewTransport opens the inner transport of a node. The harness wraps it
	// in a FaultTransport. bindAddress is 127.0.0.1:0 on the first start and
	// the same address on a restart.
	NewTransport func(nodeID, bindAddress string) (*raft.NetworkTransport, error)
	// WrapLogStore wraps the BoltDB log store of each node, for example to
	// stall the disk of one node.
	WrapLogStore func(nodeID string, store raft.LogStore) raft.LogStore
}

// Cluster is a set of nodes in one test. All methods are safe for
// concurrent use. Methods that fail the test must run on the test goroutine.
type Cluster struct {
	t           *testing.T
	options     Options
	faults      *Faults
	dataRoot    string
	waitTimeout time.Duration

	mutex          sync.Mutex
	members        map[string]*member
	memberIDs      []string // in creation order
	initialServers []node.Server
	closed         bool
}

type member struct {
	id        string
	address   string // the Raft address; it stays the same over restarts
	dataDir   string
	bootstrap bool
	logs      *logBuffer
	logger    hclog.Logger
	transport *FaultTransport // nil while stopped
	node      *node.Node      // nil while stopped
}

// logBuffer collects the hclog output of one node across restarts.
type logBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (logs *logBuffer) Write(data []byte) (int, error) {
	logs.mutex.Lock()
	defer logs.mutex.Unlock()
	return logs.buffer.Write(data)
}

func (logs *logBuffer) String() string {
	logs.mutex.Lock()
	defer logs.mutex.Unlock()
	return logs.buffer.String()
}

// New starts opts.Size nodes, node1 to nodeN, that bootstrap with the same
// configuration. t.Cleanup stops every node, also when the test fails.
func New(t *testing.T, opts Options) *Cluster {
	t.Helper()
	opts = withDefaults(opts)
	cluster := &Cluster{
		t:           t,
		options:     opts,
		faults:      NewFaults(),
		dataRoot:    t.TempDir(),
		waitTimeout: 10*time.Second + 20*opts.Timing.ElectionTimeout,
		members:     map[string]*member{},
	}
	t.Cleanup(cluster.Close)

	// Bind every transport first: the bootstrap configuration needs every address.
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	for number := 1; number <= opts.Size; number++ {
		created := cluster.newMember(fmt.Sprintf("node%d", number), true)
		if err := cluster.openTransport(created, "127.0.0.1:0"); err != nil {
			t.Fatalf("open transport of %s: %v", created.id, err)
		}
		cluster.initialServers = append(cluster.initialServers, node.Server{ID: created.id, Address: created.address})
	}
	for _, id := range cluster.memberIDs {
		if err := cluster.startNode(cluster.members[id]); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	return cluster
}

func withDefaults(opts Options) Options {
	if opts.Size == 0 {
		opts.Size = 3
	}
	if opts.Timing == (node.Timing{}) {
		opts.Timing = node.TestTiming()
	}
	if opts.Snapshots == (node.SnapshotPolicy{}) {
		opts.Snapshots = node.DefaultSnapshotPolicy()
	}
	if opts.CommandTimeout == 0 {
		opts.CommandTimeout = defaultCommandTimeout
	}
	return opts
}

func (cluster *Cluster) newMember(id string, bootstrap bool) *member {
	logs := &logBuffer{}
	created := &member{
		id:        id,
		dataDir:   filepath.Join(cluster.dataRoot, id),
		bootstrap: bootstrap,
		logs:      logs,
		logger:    hclog.New(&hclog.LoggerOptions{Name: id, Level: hclog.Debug, Output: logs}),
	}
	cluster.members[id] = created
	cluster.memberIDs = append(cluster.memberIDs, id)
	return created
}

func (cluster *Cluster) openTransport(target *member, bindAddress string) error {
	var inner *raft.NetworkTransport
	var err error
	if cluster.options.NewTransport != nil {
		inner, err = cluster.options.NewTransport(target.id, bindAddress)
	} else {
		inner, err = raft.NewTCPTransportWithConfig(bindAddress, nil, &raft.NetworkTransportConfig{
			MaxPool: transportMaxPool,
			Timeout: transportTimeout,
			Logger:  target.logger.Named("transport"),
		})
	}
	if err != nil {
		return err
	}
	target.transport = NewFaultTransport(inner, target.id, cluster.faults)
	target.address = string(target.transport.LocalAddr())
	return nil
}

func (cluster *Cluster) nodeConfig(target *member) node.Config {
	return node.Config{
		NodeID:         target.id,
		RaftAddress:    target.address,
		DataDir:        target.dataDir,
		Bootstrap:      target.bootstrap,
		InitialServers: slices.Clone(cluster.initialServers),
		Timing:         cluster.options.Timing,
		Snapshots:      cluster.options.Snapshots,
		CommandTimeout: cluster.options.CommandTimeout,
		// Equal to ReadyMaxLastContact's default; set so tests can read it.
		ReadyMaxLastContact: cluster.options.Timing.HeartbeatTimeout,
		PreVoteDisabled:     cluster.options.PreVoteDisabled,
		Logger:              target.logger,
	}
}

func (cluster *Cluster) startNode(target *member) error {
	dependencies := node.Dependencies{Transport: target.transport}
	if wrap := cluster.options.WrapLogStore; wrap != nil {
		dependencies.WrapLogStore = func(store raft.LogStore) raft.LogStore { return wrap(target.id, store) }
	}
	started, err := node.New(cluster.nodeConfig(target), dependencies)
	if err != nil {
		_ = target.transport.Close()
		target.transport = nil
		return err
	}
	target.node = started
	return nil
}

func (cluster *Cluster) stopNode(target *member) {
	if target.node != nil {
		if err := target.node.Close(); err != nil {
			cluster.t.Errorf("close %s: %v", target.id, err)
		}
	} else if target.transport != nil {
		_ = target.transport.Close()
	}
	target.node = nil
	target.transport = nil
}

// member returns the member with this ID. The caller holds the mutex.
func (cluster *Cluster) member(id string) *member {
	found, exists := cluster.members[id]
	if !exists {
		cluster.t.Fatalf("no node %q in the cluster (nodes: %v)", id, cluster.memberIDs)
	}
	return found
}

// Nodes returns the running nodes in creation order.
func (cluster *Cluster) Nodes() []*node.Node {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	var running []*node.Node
	for _, id := range cluster.memberIDs {
		if started := cluster.members[id].node; started != nil {
			running = append(running, started)
		}
	}
	return running
}

// Node returns the running node with this ID, or nil while it is stopped.
func (cluster *Cluster) Node(id string) *node.Node {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	return cluster.member(id).node
}

// IDs returns the IDs of all nodes, running or stopped, in creation order.
func (cluster *Cluster) IDs() []string {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	return slices.Clone(cluster.memberIDs)
}

// Start starts a stopped node on its old address and data dir.
func (cluster *Cluster) Start(id string) *node.Node {
	cluster.t.Helper()
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	target := cluster.member(id)
	if target.node != nil {
		cluster.t.Fatalf("start %s: it is already running", id)
	}
	if err := cluster.openTransport(target, target.address); err != nil {
		cluster.t.Fatalf("open transport of %s on %s: %v", id, target.address, err)
	}
	if err := cluster.startNode(target); err != nil {
		cluster.t.Fatalf("start %s: %v", id, err)
	}
	return target.node
}

// Stop stops a node like a crash: raft shuts down without a leadership
// transfer. The data dir stays.
func (cluster *Cluster) Stop(id string) {
	cluster.t.Helper()
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	target := cluster.member(id)
	if target.node == nil {
		cluster.t.Fatalf("stop %s: it is not running", id)
	}
	cluster.stopNode(target)
}

// Restart stops and starts a node.
func (cluster *Cluster) Restart(id string) *node.Node {
	cluster.t.Helper()
	cluster.Stop(id)
	return cluster.Start(id)
}

// Close stops every node. New registers it with t.Cleanup. It fails the
// test if a node ran without pre-vote support, and prints the last log lines
// of every node when the test failed.
func (cluster *Cluster) Close() {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	if cluster.closed {
		return
	}
	cluster.closed = true
	for _, id := range cluster.memberIDs {
		cluster.stopNode(cluster.members[id])
	}
	for _, id := range cluster.memberIDs {
		if strings.Contains(cluster.members[id].logs.String(), preVoteDisabledWarning) {
			cluster.t.Errorf("%s ran without pre-vote: the transport does not implement raft.WithPreVote", id)
		}
	}
	if cluster.t.Failed() {
		for _, id := range cluster.memberIDs {
			lines := strings.Split(strings.TrimSpace(cluster.members[id].logs.String()), "\n")
			lines = lines[max(0, len(lines)-logLinesOnFailure):]
			cluster.t.Logf("---- last %d log lines of %s ----\n%s", len(lines), id, strings.Join(lines, "\n"))
		}
	}
}

// WaitForLeader waits until exactly one node of the group is leader, every
// node of the group reports it as leader, and it is Ready (its Barrier of the
// term is done). The group is all running nodes when among is empty.
func (cluster *Cluster) WaitForLeader(among ...string) *node.Node {
	cluster.t.Helper()
	group := among
	if len(group) == 0 {
		for _, running := range cluster.Nodes() {
			group = append(group, running.ID())
		}
	}
	var leader *node.Node
	WaitUntil(cluster.t, cluster.waitTimeout, fmt.Sprintf("one ready leader that all of %v agree on", group), func() bool {
		leader = nil
		var members []*node.Node
		for _, id := range group {
			running := cluster.Node(id)
			if running == nil {
				return false
			}
			members = append(members, running)
			if running.IsLeader() {
				if leader != nil {
					return false
				}
				leader = running
			}
		}
		if leader == nil || leader.Ready() != nil {
			return false
		}
		for _, running := range members {
			if leaderID, _ := running.Leader(); leaderID != leader.ID() {
				return false
			}
		}
		return true
	})
	return leader
}

// Followers returns the running nodes other than leaderID.
func (cluster *Cluster) Followers(leaderID string) []*node.Node {
	var followers []*node.Node
	for _, running := range cluster.Nodes() {
		if running.ID() != leaderID {
			followers = append(followers, running)
		}
	}
	return followers
}

// Partition blocks all requests in both directions between the two groups.
func (cluster *Cluster) Partition(groupA, groupB []string) {
	cluster.faults.Partition(groupA, groupB)
}

// Isolate cuts one node off from every other node, running or stopped. It
// returns the other node IDs.
func (cluster *Cluster) Isolate(id string) (others []string) {
	for _, other := range cluster.IDs() {
		if other != id {
			others = append(others, other)
		}
	}
	cluster.faults.Partition([]string{id}, others)
	return others
}

// Block is a one-way fault: requests from "from" to "to" fail before they
// are sent. Requests from "to" to "from" and their responses still work.
func (cluster *Cluster) Block(from, to string) { cluster.faults.Block(from, to) }

// DropResponses delivers requests from "from" to "to", and "to" handles
// them, but "from" never gets the responses. Use it for ambiguous writes:
// DropResponses(leader, follower) for both followers.
func (cluster *Cluster) DropResponses(from, to string) { cluster.faults.DropResponses(from, to) }

// Heal removes every network fault.
func (cluster *Cluster) Heal() { cluster.faults.Heal() }

// AddNode starts a new node with an empty data dir and no bootstrap. It is
// not in the Raft configuration: add it through the leader with its
// Status().RaftAddress.
func (cluster *Cluster) AddNode(id string) *node.Node {
	cluster.t.Helper()
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	if _, exists := cluster.members[id]; exists {
		cluster.t.Fatalf("add node %s: the ID exists", id)
	}
	created := cluster.newMember(id, false)
	if err := cluster.openTransport(created, "127.0.0.1:0"); err != nil {
		cluster.t.Fatalf("open transport of %s: %v", id, err)
	}
	if err := cluster.startNode(created); err != nil {
		cluster.t.Fatalf("start %s: %v", id, err)
	}
	return created.node
}

// WaitForValueEverywhere waits until every running node has applied
// key=value in its local FSM.
func (cluster *Cluster) WaitForValueEverywhere(key, value string) {
	cluster.t.Helper()
	WaitUntil(cluster.t, cluster.waitTimeout, fmt.Sprintf("%s=%q on every running node", key, value), func() bool {
		for _, running := range cluster.Nodes() {
			result, err := running.Get(context.Background(), key, node.ReadOptions{Consistency: node.Stale})
			if err != nil || !result.Found || result.Entry.Value != value {
				return false
			}
		}
		return true
	})
}

// Logs returns the hclog output of a node across all its restarts.
func (cluster *Cluster) Logs(id string) string {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	return cluster.member(id).logs.String()
}

// Transport returns the fault transport of a running node (for its
// counters), or nil while it is stopped.
func (cluster *Cluster) Transport(id string) *FaultTransport {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	return cluster.member(id).transport
}

// NodeConfig returns the node.Config that the harness uses for a node, for
// example to run node.RecoverCluster on its data dir while it is stopped.
func (cluster *Cluster) NodeConfig(id string) node.Config {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	return cluster.nodeConfig(cluster.member(id))
}
