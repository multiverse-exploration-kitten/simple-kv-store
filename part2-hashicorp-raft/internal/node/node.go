// Package node runs one Raft node of the key-value store: hashicorp/raft,
// the raft-boltdb log and stable store, file snapshots and the FSM.
package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
)

// Input limits (Consul KV values). Big entries delay heartbeats.
const (
	MaxKeyBytes            = 512
	MaxValueBytes          = 512 * 1024
	MaxIdempotencyKeyBytes = 128
	// MaxCommandBytes limits the encoded Raft entry. JSON writes <, >, & and
	// control characters as 6 bytes, so a value at MaxValueBytes could make
	// a 3 MiB entry. Twice MaxValueBytes leaves room for a plain value at the
	// limit with the longest escaped keys (about 516 KiB), and for JSON text
	// at the limit, where " and \ take 2 bytes.
	MaxCommandBytes = 2 * MaxValueBytes
)

const (
	logCacheEntries       = 512 // Consul, Nomad and Vault use 512
	boltOpenTimeout       = time.Second
	leaderNotifyBuffer    = 10 // Consul; raft blocks on a full NotifyCh
	defaultCommandTimeout = 5 * time.Second
)

// Timing holds the Raft timeouts.
type Timing struct {
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
}

// DefaultTiming scales the hashicorp/raft defaults (heartbeat 1 s, election
// 1 s, lease 500 ms) by multiplier, like Consul's raft_multiplier. The commit
// timeout stays 50 ms. A multiplier below 1 counts as 1.
func DefaultTiming(multiplier int) Timing {
	scale := time.Duration(max(multiplier, 1))
	return Timing{
		HeartbeatTimeout:   scale * time.Second,
		ElectionTimeout:    scale * time.Second,
		LeaderLeaseTimeout: scale * 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
	}
}

// TestTiming is for in-process cluster tests on 127.0.0.1.
func TestTiming() Timing {
	return Timing{
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    100 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}
}

// SnapshotPolicy controls when raft takes snapshots and how many it keeps.
type SnapshotPolicy struct {
	Threshold    uint64        // new log entries since the last snapshot
	Interval     time.Duration // how often raft checks the threshold
	TrailingLogs uint64        // entries kept after a snapshot, for slow followers
	Retain       int           // snapshot files kept on disk
}

// DefaultSnapshotPolicy returns the hashicorp/raft defaults and Consul's
// retain count.
func DefaultSnapshotPolicy() SnapshotPolicy {
	return SnapshotPolicy{Threshold: 8192, Interval: 120 * time.Second, TrailingLogs: 10240, Retain: 2}
}

// Server is one member of the Raft configuration.
type Server struct {
	ID      string
	Address string
}

// Suffrage values in ServerInfo.
const (
	SuffrageVoter    = "voter"
	SuffrageNonvoter = "nonvoter"
	SuffrageStaging  = "staging"
)

// ServerInfo is one server of the current Raft configuration.
type ServerInfo struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

// Config configures one node. Zero values get defaults: Timing =
// DefaultTiming(1), Snapshots = DefaultSnapshotPolicy() (a partly set policy
// gets defaults only for Interval and Retain), CommandTimeout = 5 s,
// ReadyMaxLastContact = Timing.HeartbeatTimeout, RaftAddress = the
// transport's address, Logger = an INFO logger on stderr.
type Config struct {
	NodeID      string
	RaftAddress string
	DataDir     string
	// Bootstrap: write InitialServers as the first configuration when the
	// stores are empty. Every initial node gets the same list.
	Bootstrap      bool
	InitialServers []Server
	Timing         Timing
	Snapshots      SnapshotPolicy
	// CommandTimeout limits every wait on raft, enqueue included. A new log
	// entry gets half of it to enter the log.
	CommandTimeout time.Duration
	// ReadyMaxLastContact: a follower is ready when it heard from the leader
	// within this time.
	ReadyMaxLastContact time.Duration
	PreVoteDisabled     bool
	Logger              hclog.Logger
}

// Dependencies are the parts that tests replace.
type Dependencies struct {
	// Transport is required. Node.Close closes it.
	Transport raft.Transport
	// WrapLogStore wraps the BoltDB log store, for example to slow down
	// StoreLogs. The LogCache sits in front of the wrapper.
	WrapLogStore func(raft.LogStore) raft.LogStore
}

// WriteOptions are the options of Put and Delete.
type WriteOptions struct {
	// CASIndex nil = no check. 0 = the key must not exist. N = the key's
	// ModifyIndex must be N.
	CASIndex       *uint64
	IdempotencyKey string
}

// WriteResult is the FSM result of a write. Applied=false is a failed CAS.
type WriteResult = fsm.ApplyResult

// Node is one running Raft node. All methods are safe for concurrent use.
type Node struct {
	config    Config
	raft      *raft.Raft
	fsm       *fsm.FSM
	boltStore *raftboltdb.BoltStore
	transport raft.Transport
	logger    hclog.Logger

	// readyTerm is the term in which this node, as leader, finished its
	// Barrier. Linearizable and leader reads need readyTerm == the current
	// term.
	readyTerm     atomic.Uint64
	leaderChanges chan bool

	lifetime       context.Context
	endLifetime    context.CancelFunc
	backgroundWork sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// New opens the stores in cfg.DataDir, bootstraps if asked and the stores
// are empty, and starts raft. On error New closes what it opened. The caller
// still owns deps.Transport then.
func New(cfg Config, deps Dependencies) (*Node, error) {
	if deps.Transport == nil {
		return nil, errors.New("node: Dependencies.Transport is required")
	}
	cfg = withDefaults(cfg, deps.Transport)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("node: create data dir: %w", err)
	}

	stores, err := openStores(cfg, deps)
	if err != nil {
		return nil, err
	}
	created, err := start(cfg, deps, stores)
	if err != nil {
		_ = stores.boltStore.Close()
		return nil, err
	}
	return created, nil
}

// raftStores are the stores of one data dir. logStore is the BoltDB store
// behind deps.WrapLogStore, without a LogCache.
type raftStores struct {
	boltStore *raftboltdb.BoltStore
	logStore  raft.LogStore
	snapshots *raft.FileSnapshotStore
}

// openStores opens <DataDir>/raft.db and the file snapshot store in DataDir,
// for New and RecoverCluster. On error it closes what it opened.
func openStores(cfg Config, deps Dependencies) (raftStores, error) {
	// NoSync stays false: every StoreLogs fsyncs before raft acknowledges.
	// The open timeout makes the call fail instead of waiting forever while
	// a running node holds the raft.db lock.
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(cfg.DataDir, "raft.db"),
		BoltOptions: &bbolt.Options{Timeout: boltOpenTimeout},
	})
	if err != nil {
		return raftStores{}, fmt.Errorf("node: open raft.db: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStoreWithLogger(cfg.DataDir, cfg.Snapshots.Retain, cfg.Logger.Named("snapshot"))
	if err != nil {
		_ = boltStore.Close()
		return raftStores{}, fmt.Errorf("node: snapshot store: %w", err)
	}
	var logStore raft.LogStore = boltStore
	if deps.WrapLogStore != nil {
		logStore = deps.WrapLogStore(logStore)
	}
	return raftStores{boltStore: boltStore, logStore: logStore, snapshots: snapshots}, nil
}

func start(cfg Config, deps Dependencies, stores raftStores) (*Node, error) {
	cachedLogStore, err := raft.NewLogCache(logCacheEntries, stores.logStore)
	if err != nil {
		return nil, fmt.Errorf("node: log cache: %w", err)
	}

	leaderChanges := make(chan bool, leaderNotifyBuffer)
	raftConfig := buildRaftConfig(cfg, leaderChanges)
	if cfg.Bootstrap {
		if err := bootstrapIfNew(cfg, raftConfig, cachedLogStore, stores.boltStore, stores.snapshots, deps.Transport); err != nil {
			return nil, err
		}
	}
	machine := fsm.New()
	raftNode, err := raft.NewRaft(raftConfig, machine, cachedLogStore, stores.boltStore, stores.snapshots, deps.Transport)
	if err != nil {
		return nil, fmt.Errorf("node: start raft: %w", err)
	}

	lifetime, endLifetime := context.WithCancel(context.Background())
	created := &Node{
		config:        cfg,
		raft:          raftNode,
		fsm:           machine,
		boltStore:     stores.boltStore,
		transport:     deps.Transport,
		logger:        cfg.Logger,
		leaderChanges: leaderChanges,
		lifetime:      lifetime,
		endLifetime:   endLifetime,
	}
	created.backgroundWork.Add(2)
	go func() {
		defer created.backgroundWork.Done()
		created.runLeaderLoop()
	}()
	go func() {
		defer created.backgroundWork.Done()
		stores.boltStore.RunMetrics(lifetime, 0) // freelist gauges; the global metrics sink is a no-op unless cmd sets one
	}()
	return created, nil
}

func withDefaults(cfg Config, transport raft.Transport) Config {
	if cfg.Timing == (Timing{}) {
		cfg.Timing = DefaultTiming(1)
	}
	if cfg.Snapshots == (SnapshotPolicy{}) {
		cfg.Snapshots = DefaultSnapshotPolicy()
	}
	if cfg.Snapshots.Interval == 0 {
		cfg.Snapshots.Interval = DefaultSnapshotPolicy().Interval
	}
	if cfg.Snapshots.Retain == 0 {
		cfg.Snapshots.Retain = DefaultSnapshotPolicy().Retain
	}
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = defaultCommandTimeout
	}
	if cfg.ReadyMaxLastContact == 0 {
		cfg.ReadyMaxLastContact = cfg.Timing.HeartbeatTimeout
	}
	if cfg.RaftAddress == "" {
		cfg.RaftAddress = string(transport.LocalAddr())
	}
	if cfg.Logger == nil {
		cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "kvstore", Level: hclog.Info})
	}
	return cfg
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.NodeID == "":
		return errors.New("node: NodeID is required")
	case cfg.DataDir == "":
		return errors.New("node: DataDir is required")
	case cfg.CommandTimeout < 0:
		return errors.New("node: CommandTimeout must not be negative")
	}
	if cfg.Bootstrap {
		for _, server := range cfg.InitialServers {
			if server.ID == cfg.NodeID {
				return nil
			}
		}
		return fmt.Errorf("node: Bootstrap needs InitialServers that include %s", cfg.NodeID)
	}
	return nil
}

func buildRaftConfig(cfg Config, leaderChanges chan bool) *raft.Config {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = cfg.Timing.HeartbeatTimeout
	raftConfig.ElectionTimeout = cfg.Timing.ElectionTimeout
	raftConfig.LeaderLeaseTimeout = cfg.Timing.LeaderLeaseTimeout
	raftConfig.CommitTimeout = cfg.Timing.CommitTimeout
	raftConfig.SnapshotThreshold = cfg.Snapshots.Threshold
	raftConfig.SnapshotInterval = cfg.Snapshots.Interval
	raftConfig.TrailingLogs = cfg.Snapshots.TrailingLogs
	raftConfig.PreVoteDisabled = cfg.PreVoteDisabled
	raftConfig.NotifyCh = leaderChanges
	// Consul and Nomad: a removed leader becomes a follower instead of
	// shutting raft down.
	raftConfig.ShutdownOnRemove = false
	// No duplicate metric names with the peer ID inside the name.
	raftConfig.NoLegacyTelemetry = true
	raftConfig.Logger = cfg.Logger.Named("raft")
	return raftConfig
}

// bootstrapIfNew writes the initial configuration on empty stores. Every
// initial node writes the same entry, so this is safe (reference Q8). A node
// that already got entries from a leader answers ErrCantBootstrap.
func bootstrapIfNew(cfg Config, raftConfig *raft.Config, logs raft.LogStore, stable raft.StableStore, snapshots raft.SnapshotStore, transport raft.Transport) error {
	hasState, err := raft.HasExistingState(logs, stable, snapshots)
	if err != nil {
		return fmt.Errorf("node: check existing state: %w", err)
	}
	if hasState {
		return nil
	}
	configuration := raft.Configuration{}
	for _, server := range cfg.InitialServers {
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(server.ID),
			Address:  raft.ServerAddress(server.Address),
		})
	}
	err = raft.BootstrapCluster(raftConfig, logs, stable, snapshots, transport, configuration)
	if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return fmt.Errorf("node: bootstrap: %w", err)
	}
	return nil
}

// Close stops raft, then closes the stores, then the transport. raft's
// Shutdown never closes the stores. Close is safe to call twice.
func (node *Node) Close() error {
	node.closeOnce.Do(func() {
		shutdownErr := node.raft.Shutdown().Error()
		node.endLifetime()
		node.backgroundWork.Wait()
		storeErr := node.boltStore.Close()
		var transportErr error
		if closer, closable := node.transport.(raft.WithClose); closable {
			transportErr = closer.Close()
		}
		node.closeErr = errors.Join(shutdownErr, storeErr, transportErr)
	})
	return node.closeErr
}

// ID returns the node ID.
func (node *Node) ID() string { return node.config.NodeID }

// Raft returns the raft instance. Only for tests and metrics.
func (node *Node) Raft() *raft.Raft { return node.raft }

// IsLeader reports whether raft is in the leader state now. A leader is not
// ready for linearizable reads until its first Barrier in the term is done.
func (node *Node) IsLeader() bool { return node.raft.State() == raft.Leader }

// Leader returns the leader that this node knows. Both are "" when it knows none.
func (node *Node) Leader() (id, address string) {
	leaderAddress, leaderID := node.raft.LeaderWithID()
	return string(leaderID), string(leaderAddress)
}

func (node *Node) leaderInfo() leaderInfo {
	leaderID, leaderAddress := node.Leader()
	return leaderInfo{localID: node.config.NodeID, leaderID: leaderID, leaderAddress: leaderAddress}
}

// Put writes key=value through raft.
func (node *Node) Put(ctx context.Context, key, value string, opts WriteOptions) (WriteResult, error) {
	if err := validateKey("key", key); err != nil {
		return WriteResult{}, err
	}
	if err := validateValue(value); err != nil {
		return WriteResult{}, err
	}
	if err := validateIdempotencyKey(opts.IdempotencyKey); err != nil {
		return WriteResult{}, err
	}
	return node.apply(ctx, fsm.Command{Type: fsm.CommandPut, Key: key, Value: value, CASIndex: opts.CASIndex, IdempotencyKey: opts.IdempotencyKey})
}

// Delete removes key through raft. Deleting a missing key succeeds with
// Existed=false.
func (node *Node) Delete(ctx context.Context, key string, opts WriteOptions) (WriteResult, error) {
	if err := validateKey("key", key); err != nil {
		return WriteResult{}, err
	}
	if err := validateIdempotencyKey(opts.IdempotencyKey); err != nil {
		return WriteResult{}, err
	}
	return node.apply(ctx, fsm.Command{Type: fsm.CommandDelete, Key: key, CASIndex: opts.CASIndex, IdempotencyKey: opts.IdempotencyKey})
}

// rejectUnlessLeader returns not_leader or no_leader on a node that is not the
// leader. A follower answers Apply, membership changes and leadership
// transfers with ErrNotLeader only from its main loop. While a slow disk
// blocks that loop, these calls end with ErrEnqueueTimeout, and the HTTP
// layer does not forward them. State() does not need the loop.
func (node *Node) rejectUnlessLeader() error {
	if node.raft.State() != raft.Leader {
		return node.leaderInfo().notLeader(raft.ErrNotLeader)
	}
	return nil
}

// apply sends one command to raft and waits under the command deadline.
// The errors are RejectedError (not applied), OutcomeUnknownError (may
// commit) or ErrIdempotencyKeyReused (with the result).
func (node *Node) apply(ctx context.Context, command fsm.Command) (WriteResult, error) {
	data, err := fsm.EncodeCommand(command)
	if err != nil {
		return WriteResult{}, err
	}
	if len(data) > MaxCommandBytes {
		return WriteResult{}, &ValidationError{
			Field:    "value",
			Message:  fmt.Sprintf("encodes to a Raft entry of %d bytes, the limit is %d bytes (JSON writes <, >, & and control characters as 6 bytes)", len(data), MaxCommandBytes),
			TooLarge: true,
		}
	}
	if err := node.rejectUnlessLeader(); err != nil {
		return WriteResult{}, err
	}
	deadline := node.deadline(ctx)
	enqueueTimeout := enqueueTimeoutFor(deadline)
	if enqueueTimeout <= 0 || ctx.Err() != nil {
		return WriteResult{}, &RejectedError{Reason: ReasonEnqueueTimeout, cause: context.Cause(ctx)}
	}
	// Apply's timeout limits only the enqueue (reference Q2).
	future, err := waitForCall(ctx, deadline, func() raft.ApplyFuture { return node.raft.Apply(data, enqueueTimeout) })
	if err != nil {
		return WriteResult{}, translateWriteError(err, node.leaderInfo())
	}
	result := future.Response().(fsm.ApplyResult)
	return result, result.Err
}

// enqueueTimeoutFor gives raft half of the time until the deadline to take a
// new log entry. Only raft's ErrEnqueueTimeout tells that the entry never
// entered the log (503). With the whole budget, our deadline fires first and
// the answer is "outcome unknown" (504). Half leaves a gap far bigger than
// scheduling pauses (230 ms under -race load), and half the budget for
// replication and fsync. An entry that waited longer would rarely commit.
// The result is 0 or less when the deadline is too near: then do not call
// raft, because raft reads 0 as "no timeout".
func enqueueTimeoutFor(deadline time.Time) time.Duration {
	return time.Until(deadline) / 2
}

// deadline is now + CommandTimeout, or the context deadline if sooner.
func (node *Node) deadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(node.config.CommandTimeout)
	if contextDeadline, hasDeadline := ctx.Deadline(); hasDeadline && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

// callAnswer is the future of a raft call and the result of its Error().
type callAnswer[FutureType raft.Future] struct {
	future FutureType
	err    error
}

// waitForCall runs call and then future.Error() in one goroutine, and stops
// waiting at the deadline or when ctx ends. raft futures have no deadline of
// their own (reference Q3). Some raft calls also block before they return a
// future: VerifyLeader when its queue of 64 is full, Snapshot while an
// earlier snapshot waits for the main loop. So the call runs in the goroutine
// too. The goroutine stays until raft answers: one small goroutine per stuck
// call. The future is the zero value when the error is errWaitDeadline.
func waitForCall[FutureType raft.Future](ctx context.Context, deadline time.Time, call func() FutureType) (FutureType, error) {
	answers := make(chan callAnswer[FutureType], 1)
	go func() {
		future := call()
		answers <- callAnswer[FutureType]{future: future, err: future.Error()}
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case answer := <-answers:
		return answer.future, answer.err
	case <-timer.C:
	case <-ctx.Done():
	}
	// An answer that came at the same moment as the deadline still counts.
	select {
	case answer := <-answers:
		return answer.future, answer.err
	default:
	}
	var noFuture FutureType
	if ctx.Err() != nil {
		return noFuture, fmt.Errorf("%w: %w", errWaitDeadline, context.Cause(ctx))
	}
	return noFuture, errWaitDeadline
}

// Snapshot takes a user snapshot now. raft writes one even when nothing is
// new since the last one (storage doc Q4).
func (node *Node) Snapshot(ctx context.Context) error {
	_, err := waitForCall(ctx, node.deadline(ctx), node.raft.Snapshot)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrRaftShutdown):
		return &RejectedError{Reason: ReasonShutdown, cause: err}
	case errors.Is(err, errWaitDeadline):
		return &OutcomeUnknownError{Reason: ReasonTimeout, cause: err}
	default:
		return fmt.Errorf("snapshot: %w", err)
	}
}

// Status is a summary of the node for /status and tests.
type Status struct {
	ID            string `json:"id"`
	RaftAddress   string `json:"raft_address"`
	State         string `json:"state"`
	Term          uint64 `json:"term"`
	LeaderID      string `json:"leader_id"`
	LeaderAddress string `json:"leader_address"`
	Ready         bool   `json:"ready"`
	// NotReadyReason is a RejectedError reason when Ready is false.
	NotReadyReason string `json:"not_ready_reason,omitempty"`
	// LastContactMillis: 0 on the leader, -1 when no leader was ever heard.
	LastContactMillis int64  `json:"last_contact_ms"`
	CommitIndex       uint64 `json:"commit_index"`
	LastLogIndex      uint64 `json:"last_log_index"`
	LastAppliedIndex  uint64 `json:"last_applied_index"` // the FSM's own index
	LastSnapshotIndex uint64 `json:"last_snapshot_index"`
	KeyCount          int    `json:"key_count"`
}

// Status returns the current status.
func (node *Node) Status() Status {
	leaderID, leaderAddress := node.Leader()
	status := Status{
		ID:                node.config.NodeID,
		RaftAddress:       node.config.RaftAddress,
		State:             node.raft.State().String(),
		Term:              node.raft.CurrentTerm(),
		LeaderID:          leaderID,
		LeaderAddress:     leaderAddress,
		Ready:             true,
		LastContactMillis: node.lastContact().Milliseconds(),
		CommitIndex:       node.raft.CommitIndex(),
		LastLogIndex:      node.raft.LastIndex(),
		LastAppliedIndex:  node.fsm.LastAppliedIndex(),
		KeyCount:          node.fsm.Len(),
	}
	if node.lastContact() == neverContacted {
		status.LastContactMillis = -1
	}
	var notReady *RejectedError
	if errors.As(node.Ready(), &notReady) {
		status.Ready = false
		status.NotReadyReason = notReady.Reason
	}
	// Stats is the only accessor for the snapshot index.
	status.LastSnapshotIndex, _ = strconv.ParseUint(node.raft.Stats()["last_snapshot_index"], 10, 64)
	return status
}

func validateKey(field, key string) error {
	switch {
	case key == "":
		return &ValidationError{Field: field, Message: "must not be empty"}
	case len(key) > MaxKeyBytes:
		return &ValidationError{Field: field, Message: fmt.Sprintf("must be at most %d bytes", MaxKeyBytes)}
	case !utf8.ValidString(key):
		// JSON would replace the bad bytes, and the stored key would differ.
		return &ValidationError{Field: field, Message: "must be valid UTF-8"}
	}
	return nil
}

func validateValue(value string) error {
	switch {
	case len(value) > MaxValueBytes:
		return &ValidationError{Field: "value", Message: fmt.Sprintf("must be at most %d bytes", MaxValueBytes), TooLarge: true}
	case !utf8.ValidString(value):
		return &ValidationError{Field: "value", Message: "must be valid UTF-8"}
	}
	return nil
}

func validateIdempotencyKey(idempotencyKey string) error {
	switch {
	case len(idempotencyKey) > MaxIdempotencyKeyBytes:
		return &ValidationError{Field: "idempotency_key", Message: fmt.Sprintf("must be at most %d bytes", MaxIdempotencyKeyBytes)}
	case !utf8.ValidString(idempotencyKey):
		return &ValidationError{Field: "idempotency_key", Message: "must be valid UTF-8"}
	}
	return nil
}
