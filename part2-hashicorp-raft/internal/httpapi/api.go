// Package httpapi is the HTTP API of one node.
//
// hashicorp/raft does not forward: Apply works only on the leader. So a
// follower forwards writes, linearizable and leader reads, and cluster and
// admin calls to the leader over HTTP, one hop (Consul ForwardRPC). While no
// leader is known or reachable, a call waits and retries until HoldTimeout
// (Consul canRetry). A stale read is always served by the node that got it.
//
// Status codes: 503 = not applied, a retry is safe. 504 = the outcome is
// unknown, the write may still commit (AGENTS.md Raft rules 3 and 6).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

// Headers of the API.
const (
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderForwardedFrom is the loop guard: a request with it is never
	// forwarded again and never held.
	HeaderForwardedFrom = "X-KV-Forwarded-From"
	HeaderDuplicate     = "X-KV-Duplicate"
	HeaderIndex         = "X-KV-Index"
	HeaderKnownLeader   = "X-KV-Known-Leader"
	// HeaderLastContact is in milliseconds: 0 on the leader, -1 when the
	// node never heard a leader.
	HeaderLastContact = "X-KV-Last-Contact"
	HeaderServedBy    = "X-KV-Served-By"
)

// Reasons of forwarding errors, in addition to the node reasons.
const (
	// ReasonLeaderUnreachable: the request never reached the leader (dial
	// error, or the leader has no URL in the PeerDirectory). HTTP 503.
	ReasonLeaderUnreachable = "leader_unreachable"
	// ReasonForwardFailed: the forward failed after the connection to the
	// leader was made. HTTP 504 for a write, 503 for a read.
	ReasonForwardFailed = "forward_failed"
)

const (
	defaultHoldTimeout    = 5 * time.Second
	defaultForwardTimeout = 10 * time.Second
	// JSON can write one value byte as 6 bytes (\u0000). The node checks
	// the decoded value against MaxValueBytes (413).
	maxRequestBodyBytes = 6*node.MaxValueBytes + 4096
)

// NodeAPI is the part of *node.Node that the API uses. Unit tests pass a fake.
type NodeAPI interface {
	ID() string
	IsLeader() bool
	Leader() (id, address string)
	Ready() error
	Status() node.Status
	Put(ctx context.Context, key, value string, opts node.WriteOptions) (node.WriteResult, error)
	Delete(ctx context.Context, key string, opts node.WriteOptions) (node.WriteResult, error)
	Get(ctx context.Context, key string, opts node.ReadOptions) (node.ReadResult, error)
	List(ctx context.Context, prefix string, opts node.ReadOptions) (node.ListResult, error)
	Configuration() ([]node.ServerInfo, error)
	AddVoter(ctx context.Context, id, address string) error
	AddNonvoter(ctx context.Context, id, address string) error
	RemoveServer(ctx context.Context, id string) error
	TransferLeadership(ctx context.Context, targetID string) error
	Snapshot(ctx context.Context) error
}

var _ NodeAPI = (*node.Node)(nil)

// PeerDirectory maps a node ID to its HTTP base URL, for example
// "http://172.28.2.11:8000". The API forwards to the URL of the leader.
type PeerDirectory map[string]string

// Options configure the API. Zero values get defaults.
type Options struct {
	// HoldTimeout: how long a call waits and retries while no leader is
	// known, the leader cannot be dialed, the leader is not ready or a
	// leadership transfer runs. Default 5 s (Nomad's RPCHoldTimeout).
	HoldTimeout time.Duration
	// ForwardTimeout limits one forwarded request. Keep it above the
	// leader's CommandTimeout, so that the leader's own answer comes first.
	// Default 10 s.
	ForwardTimeout time.Duration
	// Logger default: no logs.
	Logger hclog.Logger
}

type api struct {
	node    NodeAPI
	peers   PeerDirectory
	options Options
	logger  hclog.Logger
	client  *http.Client
}

// New returns the HTTP handler of one node. One node needs one handler: do
// not run more than one web worker per Raft node (AGENTS.md rule 8).
func New(localNode NodeAPI, peers PeerDirectory, opts Options) http.Handler {
	if opts.HoldTimeout == 0 {
		opts.HoldTimeout = defaultHoldTimeout
	}
	if opts.ForwardTimeout == 0 {
		opts.ForwardTimeout = defaultForwardTimeout
	}
	if opts.Logger == nil {
		opts.Logger = hclog.NewNullLogger()
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // peers talk directly, also when HTTP_PROXY is set
	server := &api{
		node:    localNode,
		peers:   peers,
		options: opts,
		logger:  opts.Logger,
		client:  &http.Client{Transport: transport},
	}

	mux := http.NewServeMux()
	// {key...} allows "/" in keys, so a prefix list can group them.
	mux.HandleFunc("PUT /kv/{key...}", server.handlePut)
	mux.HandleFunc("DELETE /kv/{key...}", server.handleDelete)
	mux.HandleFunc("GET /kv/{key...}", server.handleGet)
	mux.HandleFunc("GET /kv", server.handleList)
	mux.HandleFunc("GET /status", server.handleStatus)
	mux.HandleFunc("GET /health/live", server.handleLive)
	mux.HandleFunc("GET /health/ready", server.handleReady)
	mux.HandleFunc("GET /health/leader", server.handleLeaderHealth)
	mux.HandleFunc("GET /cluster/servers", server.handleListServers)
	mux.HandleFunc("POST /cluster/servers", server.handleAddServer)
	mux.HandleFunc("DELETE /cluster/servers/{id}", server.handleRemoveServer)
	mux.HandleFunc("POST /cluster/transfer-leadership", server.handleTransferLeadership)
	mux.HandleFunc("POST /admin/snapshot", server.handleSnapshot)
	// The default registry. cmd/kvstore sets the go-metrics sink that fills it.
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

type putRequest struct {
	Value *string `json:"value"`
}

type putResponse struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	CreateIndex uint64 `json:"create_index"`
	ModifyIndex uint64 `json:"modify_index"`
	Index       uint64 `json:"index"`
}

type deleteResponse struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
	Index   uint64 `json:"index"`
}

type listResponse struct {
	Entries []entryResponse `json:"entries"`
}

type entryResponse struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	CreateIndex uint64 `json:"create_index"`
	ModifyIndex uint64 `json:"modify_index"`
}

type addServerRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Voter   bool   `json:"voter"`
}

type transferRequest struct {
	ID string `json:"id"`
}

type serversResponse struct {
	Servers []node.ServerInfo `json:"servers"`
}

type healthResponse struct {
	Status   string `json:"status"`
	Reason   string `json:"reason,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
}

var okResponse = healthResponse{Status: "ok"}

func (server *api) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := readBody(w, r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	var request putRequest
	if err := decodeBody(body, &request); err != nil {
		server.writeError(w, err, operation{})
		return
	}
	if request.Value == nil {
		server.writeError(w, &node.ValidationError{Field: "value", Message: "is required"}, operation{})
		return
	}
	opts, err := writeOptions(r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	server.serve(w, r, operation{write: true, idempotencyKey: opts.IdempotencyKey, body: body, run: func(w http.ResponseWriter) error {
		result, err := server.node.Put(r.Context(), key, *request.Value, opts)
		if err != nil {
			return err
		}
		if result.Duplicate {
			w.Header().Set(HeaderDuplicate, "true")
		}
		if !result.Applied {
			writeCASFailed(w, result)
			return nil
		}
		writeJSON(w, http.StatusOK, putResponse{
			Key:         result.Entry.Key,
			Value:       result.Entry.Value,
			CreateIndex: result.Entry.CreateIndex,
			ModifyIndex: result.Entry.ModifyIndex,
			Index:       result.Index,
		})
		return nil
	}})
}

func (server *api) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	opts, err := writeOptions(r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	server.serve(w, r, operation{write: true, idempotencyKey: opts.IdempotencyKey, run: func(w http.ResponseWriter) error {
		result, err := server.node.Delete(r.Context(), key, opts)
		if err != nil {
			return err
		}
		if result.Duplicate {
			w.Header().Set(HeaderDuplicate, "true")
		}
		if !result.Applied {
			writeCASFailed(w, result)
			return nil
		}
		writeJSON(w, http.StatusOK, deleteResponse{Key: key, Deleted: result.Existed, Index: result.Index})
		return nil
	}})
}

func (server *api) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	opts, err := readOptions(r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	server.serve(w, r, operation{run: func(w http.ResponseWriter) error {
		result, err := server.node.Get(r.Context(), key, opts)
		if err != nil {
			return err
		}
		setReadHeaders(w, result.Meta)
		if !result.Found {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "key_not_found", Key: key})
			return nil
		}
		writeJSON(w, http.StatusOK, entryResponse(result.Entry))
		return nil
	}})
}

func (server *api) handleList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	opts, err := readOptions(r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	server.serve(w, r, operation{run: func(w http.ResponseWriter) error {
		result, err := server.node.List(r.Context(), prefix, opts)
		if err != nil {
			return err
		}
		setReadHeaders(w, result.Meta)
		entries := make([]entryResponse, 0, len(result.Entries))
		for _, entry := range result.Entries {
			entries = append(entries, entryResponse(entry))
		}
		writeJSON(w, http.StatusOK, listResponse{Entries: entries})
		return nil
	}})
}

func (server *api) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, server.node.Status())
}

func (server *api) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, okResponse)
}

// handleReady: a follower is ready when it heard from a known leader
// recently. A leader is ready after its Barrier in the current term.
func (server *api) handleReady(w http.ResponseWriter, _ *http.Request) {
	if err := server.node.Ready(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "not_ready", Reason: rejectionReason(err)})
		return
	}
	writeJSON(w, http.StatusOK, healthResponse{Status: "ready"})
}

// handleLeaderHealth uses Vault's codes: 200 on the active node, 429 on a
// standby. A leader before its Barrier is 503.
func (server *api) handleLeaderHealth(w http.ResponseWriter, _ *http.Request) {
	if !server.node.IsLeader() {
		leaderID, _ := server.node.Leader()
		writeJSON(w, http.StatusTooManyRequests, healthResponse{Status: "follower", LeaderID: leaderID})
		return
	}
	if err := server.node.Ready(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "not_ready", Reason: rejectionReason(err)})
		return
	}
	writeJSON(w, http.StatusOK, healthResponse{Status: "leader"})
}

// handleListServers asks the leader: a follower's configuration can be old.
func (server *api) handleListServers(w http.ResponseWriter, r *http.Request) {
	server.serve(w, r, operation{run: func(w http.ResponseWriter) error {
		if err := server.requireLeader(); err != nil {
			return err
		}
		servers, err := server.node.Configuration()
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, serversResponse{Servers: servers})
		return nil
	}})
}

// handleAddServer adds a non-voter unless "voter" is true. Add a non-voter
// first and promote it after it caught up (reference Q9).
func (server *api) handleAddServer(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	var request addServerRequest
	if err := decodeBody(body, &request); err != nil {
		server.writeError(w, err, operation{})
		return
	}
	server.serve(w, r, operation{write: true, body: body, run: func(w http.ResponseWriter) error {
		if err := server.requireLeader(); err != nil {
			return err
		}
		add, suffrage := server.node.AddNonvoter, node.SuffrageNonvoter
		if request.Voter {
			add, suffrage = server.node.AddVoter, node.SuffrageVoter
		}
		if err := add(r.Context(), request.ID, request.Address); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, node.ServerInfo{ID: request.ID, Address: request.Address, Suffrage: suffrage})
		return nil
	}})
}

func (server *api) handleRemoveServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	server.serve(w, r, operation{write: true, run: func(w http.ResponseWriter) error {
		if err := server.requireLeader(); err != nil {
			return err
		}
		if err := server.node.RemoveServer(r.Context(), id); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, okResponse)
		return nil
	}})
}

// handleTransferLeadership runs on the leader, also when the target is the
// follower that got the request.
func (server *api) handleTransferLeadership(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r)
	if err != nil {
		server.writeError(w, err, operation{})
		return
	}
	var request transferRequest
	if len(body) > 0 {
		if err := decodeBody(body, &request); err != nil {
			server.writeError(w, err, operation{})
			return
		}
	}
	server.serve(w, r, operation{write: true, body: body, run: func(w http.ResponseWriter) error {
		if err := server.requireLeader(); err != nil {
			return err
		}
		if err := server.node.TransferLeadership(r.Context(), request.ID); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, okResponse)
		return nil
	}})
}

// handleSnapshot takes a user snapshot on the leader (Consul forwards
// snapshot saves to the leader too). A timeout is 504: raft can still write
// the snapshot.
func (server *api) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	server.serve(w, r, operation{write: true, run: func(w http.ResponseWriter) error {
		if err := server.requireLeader(); err != nil {
			return err
		}
		if err := server.node.Snapshot(r.Context()); err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, okResponse)
		return nil
	}})
}

// requireLeader gives the error that a node method gives on a follower, so
// that serve forwards the call.
func (server *api) requireLeader() error {
	if server.node.IsLeader() {
		return nil
	}
	leaderID, leaderAddress := server.node.Leader()
	if leaderID == "" || leaderID == server.node.ID() {
		return &node.RejectedError{Reason: node.ReasonNoLeader}
	}
	return &node.RejectedError{Reason: node.ReasonNotLeader, LeaderID: leaderID, LeaderAddress: leaderAddress}
}

// readBody reads the whole body: serve sends it again when it forwards.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return nil, &node.ValidationError{Field: "body", Message: "must be at most " + strconv.Itoa(maxRequestBodyBytes) + " bytes", TooLarge: true}
	case err != nil:
		return nil, &node.ValidationError{Field: "body", Message: "cannot be read: " + err.Error()}
	}
	return body, nil
}

func decodeBody(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return &node.ValidationError{Field: "body", Message: "must be a JSON object: " + err.Error()}
	}
	return nil
}

func writeOptions(r *http.Request) (node.WriteOptions, error) {
	opts := node.WriteOptions{IdempotencyKey: r.Header.Get(HeaderIdempotencyKey)}
	if text := r.URL.Query().Get("cas"); text != "" {
		index, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return node.WriteOptions{}, &node.ValidationError{Field: "cas", Message: "must be a modify index (0 = create only)"}
		}
		opts.CASIndex = &index
	}
	return opts, nil
}

func readOptions(r *http.Request) (node.ReadOptions, error) {
	query := r.URL.Query()
	consistency, err := node.ParseReadConsistency(query.Get("consistency"))
	if err != nil {
		return node.ReadOptions{}, err
	}
	opts := node.ReadOptions{Consistency: consistency}
	if text := query.Get("max_stale"); text != "" {
		opts.MaxStale, err = time.ParseDuration(text)
		if err != nil {
			return node.ReadOptions{}, &node.ValidationError{Field: "max_stale", Message: "must be a duration such as 5s"}
		}
	}
	return opts, nil
}

func setReadHeaders(w http.ResponseWriter, meta node.ReadMeta) {
	lastContactMillis := meta.LastContact.Milliseconds()
	if meta.LastContact < 0 {
		lastContactMillis = -1 // never heard a leader; Milliseconds() rounds -1 ns to 0
	}
	header := w.Header()
	header.Set(HeaderIndex, strconv.FormatUint(meta.Index, 10))
	header.Set(HeaderKnownLeader, strconv.FormatBool(meta.KnownLeader))
	header.Set(HeaderLastContact, strconv.FormatInt(lastContactMillis, 10))
	header.Set(HeaderServedBy, meta.ServedBy)
}
