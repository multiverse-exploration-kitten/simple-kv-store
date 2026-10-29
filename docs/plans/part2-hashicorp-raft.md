# Part 2 plan: KV store on hashicorp/raft

## Status

- Wave 0 (research) done: `part2-hashicorp-raft/docs/{hashicorp-raft-reference,hashicorp-raft-storage-and-transport,production-patterns}.md`. Plan written.
- Wave 1 (phase 1, core) done: 73 unit tests, 4 smoke tests, lint clean. Contract changes are in "Phase 1 changes to the contracts".
- Wave 2 (phases 2-7) done: 215 unit tests, 38 cluster tests (Porcupine check included), lint clean.
- Phase 1 fixes done: every raft call runs under the deadline, enqueue timeout = 503, one store opener.
- Phases 8 and 9 done: binary, Docker (mutual TLS, distroless), Makefile, 6 e2e subtests, README, 27 lessons. Forward dial timeout 1 s (lesson 27).
- Last full run: `go test -race ./...` all 7 packages ok (cluster 105 s), `make e2e` 6/6 ok, lint clean.
- Next: one code review of part 2. The user decides what to fix.
- Gotcha: under parallel `-race` load a test process can pause for 230 ms. Tests that assert "no leader change" use 500 ms timeouts.

## Goal

A 3-node replicated key-value store in Go with hashicorp/raft, an HTTP API, Docker Compose, and Go tests that demonstrate the Raft failure scenarios.
Part 2 is "more established" than part 1. It copies the patterns that Consul, Nomad and Vault use (see `docs/production-patterns.md`, section "Patterns for part 2").
Read `AGENTS.md` (repo root) first. Its Raft rules and testing rules are part of this plan.
Read the three research docs in `part2-hashicorp-raft/docs/` before you write code. They give the library facts with file:line citations.

## Decisions (recommended options, chosen without questions)

| Topic | Decision | Why |
|---|---|---|
| Versions | Go 1.25, `github.com/hashicorp/raft v1.8.0`, `github.com/hashicorp/raft-boltdb/v2 v2.4.1` | Latest raft release. Read reference Q15 (open bugs) before an upgrade. raft-boltdb v2.4.2 and raft-wal v0.5.0 need Go 1.26. raft-wal v0.4.2 does not build with raft v1.8.0 (storage doc Q1, Q2). |
| Log and stable store | One raft-boltdb file `<data>/raft.db` for both, `NoSync=false`, a bbolt open `Timeout`, wrapped in `raft.NewLogCache(512, …)` | Consul values. 2 fdatasync per `StoreLogs`. The file never shrinks (storage doc Q1). |
| Snapshots | `raft.NewFileSnapshotStore(dir, 2, …)` | Consul keeps 2. It fsyncs files and the directory. |
| Timeouts | One `TimingMultiplier` (Consul `raft_multiplier`). Base = hashicorp defaults (heartbeat 1 s, election 1 s, lease 500 ms, commit 50 ms). Default multiplier 1. Tests use `node.TestTiming()` (100 ms, 100 ms, 100 ms, 10 ms). | One knob. HashiCorp recommends 1 in production. |
| Pre-vote | On (library default). `PreVoteDisabled` exists only for the disruptive-rejoin demo. | Reference Q5, Q6. |
| Bootstrap | Every initial node calls `BootstrapCluster` with the same server list when `raft.HasExistingState` is false. `ErrCantBootstrap` counts as success. | Reference Q8: safe with an identical configuration. |
| Raft addresses | An IP and a port everywhere (config, advertise, peers). Docker uses static IPs. | The advertise address must be a `*net.TCPAddr` with an IP (storage doc Q7). One form avoids two names for one server. |
| Command format | 1 type byte + JSON body. Bit 128 = "ignorable" (Consul `IgnoreUnknownTypeFlag`). | Versioned commands (Raft rule 2). |
| FSM errors | Business results (CAS failed, idempotency key reused) are values in the response. An unknown non-ignorable type or a decode error panics (fail-stop, as Consul and Nomad). | A node that skips a command it does not understand diverges. The leader validates all input before `Apply`, so user input never reaches a panic. |
| Versions of keys | `CreateIndex` and `ModifyIndex` = the Raft log index. `?cas=N` on PUT and DELETE. `cas=0` = create only. | Consul KV. Rule 5. |
| Duplicate writes | `Idempotency-Key` header. The FSM keeps the last 10,000 keys (FIFO by log index, deterministic) with a request fingerprint and the result. Same key + other request = 422. | No HashiCorp product dedupes all writes (production-patterns section 7). This is the Raft thesis 6.3 fix for ambiguous writes. |
| Reads | `linearizable` (default) = leader + readiness flag of the current term + `VerifyLeader()`. `leader` = local read on the leader, no check (Consul "default"). `stale` = local read on any node, optional `max_stale`. | Consul `consistentRead`. Rule 4. |
| Leader setup | Buffered `NotifyCh`, one goroutine. On "leader": `Barrier()`, then set `readyTerm` = current term. On "follower": clear it. | Consul `leaderLoop`. Reference Q4, Q11. |
| Write path on a follower | The node returns `RejectedError{Reason: "not_leader", LeaderID}`. The HTTP layer forwards once to the leader's HTTP address. While no leader is known, it retries "nothing changed" errors until `HoldTimeout`. | Consul `ForwardRPC` + `canRetry`. hashicorp/raft does not forward. |
| Error mapping | 503 = known not applied, retry is safe. 504 = outcome unknown (`ErrLeadershipLost`, `ErrRaftShutdown`, deadline, broken forward). | Rules 3 and 6. Consul returns 500 for both, part 2 separates them. |
| Deadline | Every `future.Error()` waits under `CommandTimeout` (default 5 s). A miss is 504. | Reference Q3: `Error()` can block forever. |
| Value limits | Key 1..512 bytes, value up to 512 KiB (413 above), idempotency key up to 128 bytes. | Consul KV. Big entries delay heartbeats. |
| Peer security | Mutual TLS `raft.StreamLayer` (TLS 1.3, client certs required). A `certs` init container makes a CA and node certs in a volume. Plain TCP only with `KV_RAFT_INSECURE_PLAINTEXT=true`. | Fail closed. Study guide topic 13. |
| HTTP | `net/http` with Go 1.22 patterns. No framework. JSON bodies. | Standard library is enough. |
| Logs, metrics | `hclog` (JSON in Docker). `hashicorp/go-metrics` Prometheus sink at `/metrics`. | Library's own stack. |
| Lint | `gofmt -l`, `go vet`, `go tool staticcheck` (tool dependency in go.mod). | No global install. |
| Tests | Unit tests next to the code (`internal/**/*_test.go`, Go convention). Cluster tests in `tests/cluster/` with real TCP on 127.0.0.1, real raft-boltdb stores in `t.TempDir()`, and a fault-injecting transport. E2E in `tests/e2e/` with build tag `e2e`. Linearizability check with Porcupine. | AGENTS.md test levels. |
| Docker | Multi-stage build, `gcr.io/distroless/static-debian12:nonroot`. `kvstore healthcheck` subcommand for the healthcheck. Project `kvstore-part2`, subnet `172.28.11.0/24` (e2e: `172.28.12.0/24`), HTTP ports 8101-8103, Raft port 7000 (internal). | No shell in the image. Part 1 and part 2 can run at the same time. |

## Architecture

```
HTTP client ─> httpapi (forward to leader if needed) ─> node.Node ─> raft.Raft ─mTLS─> 2 peers
                                                          │            ├─ raft-boltdb (log + stable)
                                                          │            └─ FileSnapshotStore
                                                          └─> fsm.FSM ─> fsm.State (pure, deterministic)
```

## Layout

All paths are relative to `part2-hashicorp-raft/`.

```
go.mod, go.sum                module github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft
cmd/kvstore/                  main: subcommands serve (default), healthcheck, gencerts
internal/config/              Config from environment variables
internal/fsm/                 command.go, state.go, fsm.go (raft.FSM + snapshot)
internal/node/                node.go, errors.go, leader.go, reads.go, membership.go (phase 1), recovery.go (phase 5)
internal/httpapi/             HTTP handlers, forwarding, error mapping
internal/tlsstream/           mutual TLS raft.StreamLayer, cert generation
tests/cluster/                harness.go, faults.go, *_test.go
tests/e2e/                    //go:build e2e
docs/                         research docs, lessons-from-tests.md
Dockerfile, .dockerignore, docker-compose.yml, Makefile, README.md
```

## Contracts (phase 1 writes these. Later phases use them and do not change them.)

### `internal/fsm`

- `CommandType` (byte): `CommandPut = 1`, `CommandDelete = 2`. `IgnorableFlag = 128`.
- `Command{Type, Key, Value string, CASIndex *uint64, IdempotencyKey string}`. `EncodeCommand(Command) ([]byte, error)`, `DecodeCommand([]byte) (Command, error)`.
- `Entry{Key, Value string, CreateIndex, ModifyIndex uint64}`.
- `ApplyResult{Index uint64, Applied bool, Existed bool, Entry *Entry, Duplicate bool, Err error}`.
  - Put applied: `Applied=true`, `Entry` = new entry. CAS failed: `Applied=false`, `Entry` = current entry or nil.
  - Delete: `Applied=true` when the key was deleted or did not exist. `Existed` says which. CAS failed: `Applied=false`.
  - `Err` holds only business errors: `ErrIdempotencyKeyReused`.
  - A duplicate returns the stored result with `Duplicate=true`.
- `FSM` implements `raft.FSM`: `New() *FSM`, `Get(key) (Entry, bool)`, `List(prefix) []Entry` (sorted by key), `LastAppliedIndex() uint64`, `Len() int`.
- `MaxIdempotencyKeys = 10000`. Changing it changes the replicated state, so it is a versioned change.
- `Snapshot()` copies the map and the dedupe table under the lock. `Persist` writes JSON. `Restore` replaces all state.

### `internal/node`

- `Timing{HeartbeatTimeout, ElectionTimeout, LeaderLeaseTimeout, CommitTimeout time.Duration}`. `DefaultTiming(multiplier int) Timing`, `TestTiming() Timing`.
- `SnapshotPolicy{Threshold uint64, Interval time.Duration, TrailingLogs uint64, Retain int}`. `DefaultSnapshotPolicy()`.
- `Server{ID, Address string}`. `ServerInfo{ID, Address, Suffrage string}`.
- `Config{NodeID, RaftAddress, DataDir string, Bootstrap bool, InitialServers []Server, Timing Timing, Snapshots SnapshotPolicy, CommandTimeout time.Duration, ReadyMaxLastContact time.Duration, PreVoteDisabled bool, Logger hclog.Logger}`.
- `Dependencies{Transport raft.Transport, WrapLogStore func(raft.LogStore) raft.LogStore}`. `Transport` is required. `WrapLogStore` is optional (slow-disk tests).
- `New(cfg Config, deps Dependencies) (*Node, error)`.
- `ReadConsistency` (string): `Linearizable = "linearizable"`, `LeaderLocal = "leader"`, `Stale = "stale"`. `ParseReadConsistency(string) (ReadConsistency, error)` ("" = linearizable).
- `WriteOptions{CASIndex *uint64, IdempotencyKey string}`. `WriteResult = fsm.ApplyResult`.
- `ReadOptions{Consistency ReadConsistency, MaxStale time.Duration}`.
- `ReadMeta{Index uint64, KnownLeader bool, LastContact time.Duration, ServedBy string}`.
- `ReadResult{Entry fsm.Entry, Found bool, Meta ReadMeta}`. `ListResult{Entries []fsm.Entry, Meta ReadMeta}`.
- Methods:
  - `Put(ctx, key, value string, opts WriteOptions) (WriteResult, error)`, `Delete(ctx, key string, opts WriteOptions) (WriteResult, error)`
  - `Get(ctx, key string, opts ReadOptions) (ReadResult, error)`, `List(ctx, prefix string, opts ReadOptions) (ListResult, error)`
  - `ID() string`, `IsLeader() bool`, `Leader() (id, address string)`, `Ready() error`, `Status() Status` (JSON tags)
  - `Configuration() ([]ServerInfo, error)`, `AddVoter(ctx, id, address string) error`, `AddNonvoter(ctx, id, address string) error`, `RemoveServer(ctx, id string) error`, `TransferLeadership(ctx, targetID string) error` ("" = any)
  - `Snapshot(ctx) error` (user snapshot), `Raft() *raft.Raft` (tests and metrics only), `Close() error` (`Shutdown().Error()`, then close the stores, then close the transport. Safe to call twice.)
- Errors (use `errors.As`):
  - `RejectedError{Reason, LeaderID, LeaderAddress string}`: known not applied. HTTP 503. Reasons: `not_leader`, `no_leader`, `enqueue_timeout`, `leadership_transfer_in_progress`, `not_ready`, `too_stale`, `verify_failed`, `shutdown`.
  - `OutcomeUnknownError{Reason string}`: may commit. HTTP 504. Reasons: `timeout`, `leadership_lost`, `shutdown`.
  - `ValidationError{Field, Message string, TooLarge bool}`: HTTP 400, or 413 when `TooLarge`.
  - Re-export `fsm.ErrIdempotencyKeyReused` for HTTP 422.
  - A read never returns `OutcomeUnknownError`. A read is always safe to retry.
- `Ready()`: leader = `readyTerm` equals the current term. Follower = a leader is known and `LastContact <= ReadyMaxLastContact`.

### `tests/cluster` harness (package `cluster`)

- `Options{Size int, Timing node.Timing, Snapshots node.SnapshotPolicy, CommandTimeout time.Duration, PreVoteDisabled bool, NewTransport func(nodeID, bindAddress string) (*raft.NetworkTransport, error), WrapLogStore func(nodeID string, store raft.LogStore) raft.LogStore}`.
- `New(t *testing.T, opts Options) *Cluster`. It registers `t.Cleanup(Close)`.
- `Cluster` methods: `Nodes()`, `Node(id)`, `IDs()`, `Start(id)`, `Stop(id)`, `Restart(id)`, `Close()`, `WaitForLeader(among ...string) *node.Node`, `Followers(leaderID)`, `Partition(groupA, groupB []string)`, `Isolate(id) (others []string)`, `Block(from, to string)` (one-way: requests from `from` to `to` fail), `DropResponses(from, to string)` (requests from `from` reach `to`, the responses are lost), `Heal()`, `AddNode(id string) *node.Node` (new node, no bootstrap), `WaitForValueEverywhere(key, value string)`.
- `WaitUntil(t, timeout, description, probe func() bool)`.
- `faults.go`: `Faults` (shared rule table) and `FaultTransport` that wraps `*raft.NetworkTransport`. It must keep pre-vote, `Close` and pipelining behavior (see the storage doc, Q10). A test fails when the "pre-vote not supported" warning appears.
  Start from the tested wrapper of the research: `/private/tmp/claude-501/-Users-william-workspace-simple-kv-store/2ee5234f-11ad-496c-90b3-250d84527b4f/scratchpad/hraft-storage/faultnet/faultnet.go` (and its test). The storage doc appendix has the same code.

### Phase 1 changes to the contracts (the code is the truth)

- Errors are pointer types (`*node.RejectedError`, `*node.OutcomeUnknownError`, `*node.ValidationError`). `errors.Is(err, raft.ErrLeadershipLost)` works through `Unwrap()`.
- New reason `unknown_error` (`OutcomeUnknownError`): an unknown raft error on a write is "outcome unknown".
- Fault rules work per request. `Block(from, to)` blocks only the requests that `from` sends to `to`. A lost-response scenario is `DropResponses(leader, follower)` for each follower.
- `WaitForLeader` also waits until the leader is `Ready()`.
- Harness extras: `Logs(id)`, `Transport(id)`, `NodeConfig(id)`. `Start`, `Restart` and `AddNode` return `*node.Node`.
- FSM: a CAS failure has `Existed=false`. A duplicate returns the original index. `DELETE ?cas=0` means "the key must not exist". A key over 512 bytes is a 400, not a 413.
- Raft settings: `ShutdownOnRemove=false` (as Consul and Nomad), `NoLegacyTelemetry=true`. `node.New` starts the metrics loop.
- Builder notes: `.claude/phase-memory/part2-hashicorp-raft/phase-1.md`.

## Library facts that every builder must know

Sources: the three research docs. Read the cited section before you depend on a fact.

1. `Apply`'s timeout limits only the enqueue. `future.Error()` can block forever. Wait under your own deadline (reference Q2, Q3).
2. `ErrLeadershipLost` and `ErrRaftShutdown` are "outcome unknown". `ErrNotLeader`, `ErrEnqueueTimeout` and `ErrLeadershipTransferInProgress` are "not applied" (reference Q2).
3. `AppliedIndex()` counts entries sent to the FSM queue, not applied ones. Use `fsm.LastAppliedIndex()` (reference Q11).
4. A panic in `FSM.Apply` kills the process. Unit tests check it with `recover` or a child process. Never panic in a cluster test (reference Q13).
5. `Shutdown()` never closes the stores. It closes the transport only through `raft.WithClose` (reference Q14).
6. `GetConfiguration().Index()` is always 0 in v1.8.0 (#722). Pass 0 as `prevIndex` (reference Q9).
7. `AddVoter` adds a full voter at once. Add a non-voter first and promote it after it caught up (reference Q9). `VerifyLeader` counts non-voters (#725).
8. A removed follower is not shut down. It keeps running without the leader's traffic (reference Q9).
9. After a restart, `NewRaft` restores the newest snapshot but does not apply the log after it until it learns the commit index (storage doc Q5).
10. `Snapshot()` with nothing new still writes a snapshot. It can push the older good snapshot out of the retain count (storage doc Q4).
11. The transport wrapper must keep `WithClose` and `WithPreVote`, wrap `SetHeartbeatHandler`, return `ErrPipelineReplicationNotSupported` unwrapped, and return a zero response for a lost pipelined response. Injected errors must not contain the text "unexpected command" (storage doc Q10).
12. The global go-metrics sink mixes all nodes in one process. Only `cmd/kvstore` sets a sink (storage doc Q11).
13. Lost responses + a short lease: replication backoff can grow and the write can really be lost. The ambiguous-write test uses a 1 s lease (storage doc Q10).

## HTTP API contract (phase 4)

| Method | Path | Body / query | Success | Errors |
|---|---|---|---|---|
| PUT | `/kv/{key}` | `{"value": str}`, `?cas=N`, header `Idempotency-Key` | 200 `{"key","value","create_index","modify_index","index"}`, header `X-KV-Duplicate: true` on a duplicate | 400, 409, 413, 422, 503, 504 |
| GET | `/kv/{key}` | `?consistency=linearizable\|leader\|stale`, `?max_stale=5s` | 200 `{"key","value","create_index","modify_index"}` | 400, 404, 503 |
| GET | `/kv` | `?prefix=`, `?consistency=` | 200 `{"entries": [...]}` | 400, 503 |
| DELETE | `/kv/{key}` | `?cas=N`, header `Idempotency-Key` | 200 `{"key","deleted": bool,"index"}` | 400, 409, 422, 503, 504 |
| GET | `/status` | | 200 node status | |
| GET | `/health/live` | | 200 | |
| GET | `/health/ready` | | 200, or 503 with the reason | |
| GET | `/health/leader` | | 200 on a ready leader, 429 on a follower | |
| GET | `/cluster/servers` | | 200 `{"servers": [{"id","address","suffrage"}]}` | 503 |
| POST | `/cluster/servers` | `{"id","address","voter": bool}` | 200 | 400, 503, 504 |
| DELETE | `/cluster/servers/{id}` | | 200 | 503, 504 |
| POST | `/cluster/transfer-leadership` | `{"id": str}` (optional) | 200 | 503, 504 |
| POST | `/admin/snapshot` | | 200 | 503 |
| GET | `/metrics` | | Prometheus text | |

- Every read response has headers `X-KV-Index`, `X-KV-Known-Leader`, `X-KV-Last-Contact` (ms), `X-KV-Served-By`.
- 409 body: `{"error": "cas_failed", "current_modify_index": N}` (0 when the key does not exist).
- 503 body: `{"error": "command_rejected", "reason": str, "retry_safe": true}` + header `Retry-After: 1`.
- 504 body: `{"error": "outcome_unknown", "reason": str, "retry_safe": false}`. With an `Idempotency-Key`, add `"retry_with_same_idempotency_key": true`.
- Forwarding: a follower forwards writes, `linearizable` and `leader` reads, and `/cluster/*` and `/admin/*` calls to the leader, once. Header `X-KV-Forwarded-From: <node id>`. A forwarded request that meets `not_leader` again returns 503. A `stale` read never forwards.
- A dial error before the request is sent = 503 `leader_unreachable`. A failure after the request was sent = 504 `forward_failed` for writes, 503 for reads.
- `httpapi.New(node NodeAPI, peers PeerDirectory, opts Options) http.Handler`. `NodeAPI` is an interface with the `node.Node` methods the API uses. `PeerDirectory` maps a node ID to an HTTP base URL. `Options{HoldTimeout, ForwardTimeout time.Duration, Logger hclog.Logger}`.

## Environment (phase 8, `cmd/kvstore`)

`KV_NODE_ID` (required), `KV_RAFT_ADDRESS` (advertised, required), `KV_RAFT_BIND_ADDRESS` (default `0.0.0.0:7000`), `KV_HTTP_ADDRESS` (default `:8000`), `KV_DATA_DIR` (default `/data`), `KV_BOOTSTRAP` (default `false`), `KV_PEERS` (`id=raftAddress,…`), `KV_PEER_HTTP` (`id=http://host:port,…`), `KV_TIMING_MULTIPLIER` (default 1), `KV_COMMAND_TIMEOUT` (default `5s`), `KV_RAFT_TLS_CA_FILE`, `KV_RAFT_TLS_CERT_FILE`, `KV_RAFT_TLS_KEY_FILE`, `KV_RAFT_INSECURE_PLAINTEXT` (default `false`), `KV_LOG_LEVEL` (default `info`), `KV_LOG_JSON` (default `false`).
Phase 1 writes `internal/config` with these fields and its unit test. A bad value is an error that names the variable.

## Phases and parallel map

| Phase | Wave | Tier | Files owned |
|---|---|---|---|
| 1 Core | 1 | Complex | `go.mod`, `go.sum`, `internal/config/**`, `internal/fsm/**`, `internal/node/**` (not `recovery.go`), `tests/cluster/{harness,faults,smoke_test}.go` |
| 2 Cluster tests A | 2 | Complex | `tests/cluster/{election,replication,durability,snapshot,transfer}_test.go` |
| 3 Cluster tests B | 2 | Complex | `tests/cluster/{quorum_loss,partition,read_consistency,ambiguous_write,concurrency,prevote,disk_stall}_test.go` |
| 4 HTTP API | 2 | Complex | `internal/httpapi/**`, `tests/cluster/http_test.go` |
| 5 Membership + recovery | 2 | Complex | `internal/node/recovery.go`, `internal/node/recovery_test.go`, `tests/cluster/{membership,recovery}_test.go` |
| 6 Mutual TLS | 2 | Complex | `internal/tlsstream/**`, `tests/cluster/tls_test.go` |
| 7 Linearizability | 2 | Complex | `tests/cluster/{history.go,linearizability_test.go}` |
| 8 Binary + Docker + e2e | 3 | Complex | `cmd/kvstore/**`, `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `Makefile`, `tests/e2e/**` |
| 9 Docs | 4 | Simple | `README.md`, `docs/lessons-from-tests.md` |

Rules for every builder:
- Edit only the files you own. If you need a change in another file, stop and report it.
- Phase 1 adds every module dependency (`go get`). Later phases do not edit `go.mod` or `go.sum` and do not run `go mod tidy`. The main session tidies after each wave.
- Run only your own tests (`go test ./internal/fsm/`, `go test ./tests/cluster/ -run 'TestName'`). Six builders run at the same time, so the machine is busy. If a timing test fails, run it 5 more times before you change the code. A flaky test is a bug (AGENTS.md 5.6): find the cause.
- Run `gofmt -l .`, `go vet ./...` and `go tool staticcheck ./...` on your packages before you report.
- Each failure-scenario test has a doc comment with **Scenario**, **Expected**, **Raft lesson**.
- Report library gotchas that your tests found. The main session puts them in `docs/lessons-from-tests.md`.

### Phase 1: Core

- [x] `go.mod` with Go 1.25 and every dependency: raft v1.8.0, raft-boltdb/v2 v2.4.1, hclog, go-metrics (+ Prometheus sink), porcupine, staticcheck as a tool (a version that builds with Go 1.25).
- [x] `internal/fsm`: the contract above. Unit tests: put, delete, CAS (create only, match, mismatch, delete-CAS), dedupe (duplicate, key reuse with another request = error, FIFO eviction at the limit), snapshot round trip, determinism (two FSMs that apply the same log have equal state), unknown ignorable type is skipped, unknown non-ignorable type panics.
- [x] `internal/config`: env parsing and its unit test.
- [x] `internal/node`: the contract above. Unit test for the error translation table (each raft error → `RejectedError` or `OutcomeUnknownError`).
- [x] `tests/cluster/faults.go`, `harness.go`, `smoke_test.go`: 3 nodes elect one leader. A write replicates to all. A partition heals. A fault-transport test proves that pre-vote stays on.

### Phase 2: Cluster tests A (election, replication, durability, snapshots, transfer)

- [x] Exactly one leader. All nodes agree on the leader and the term.
- [x] Leader stop: the 2 survivors elect a new leader in a higher term. Committed data survives. Writes continue.
- [x] A write on a follower returns `RejectedError{not_leader}` with the leader ID.
- [x] One follower down: writes still commit.
- [x] A stopped follower restarts and catches up from the log.
- [x] Low `TrailingLogs` and `Threshold`: a follower that missed many writes catches up through `InstallSnapshot`. Assert the snapshot index on that follower.
- [x] Full cluster restart: all data is back. Linearizable reads work again only after the new leader's barrier.
- [x] Leadership transfer: the target becomes leader. Writes continue. Writes during the transfer get `leadership_transfer_in_progress` (503) or succeed.
- [x] Each write returns the Raft index. `ModifyIndex` grows. The FSM `LastAppliedIndex` matches on all nodes after the writes.

### Phase 3: Cluster tests B (quorum loss, partitions, reads, ambiguity, concurrency, pre-vote, disk stall)

- [x] Majority down: writes fail with 503 or 504 within `CommandTimeout`. They do not hang. A linearizable read fails. A stale read returns the old value.
- [x] Leader isolated: the majority elects a new leader. The old leader steps down after `LeaderLeaseTimeout`. Its write fails with `leadership_lost` and is gone after the heal.
- [x] Read modes on an isolated old leader before it steps down: `leader` returns the old value, `linearizable` fails (`VerifyLeader`).
- [x] Stale read on a partitioned follower: returns the old value. `max_stale` rejects it with `too_stale`.
- [x] Ambiguous write: `DropResponses` from the followers to the leader. The write gets `OutcomeUnknownError`. After the heal the write is committed.
- [x] Idempotent retry: in the same scenario, a CAS write without a key and its retry gives a false `cas_failed`. With an `Idempotency-Key` the retry returns the original success with `Duplicate=true`.
- [x] CAS under concurrency: many goroutines increment one counter with CAS through different nodes. Final value = number of successful CAS calls.
- [x] Pre-vote: an isolated follower does not raise its term. After the heal the leader stays. With `PreVoteDisabled` the leader steps down (show the term jump).
- [x] Disk stall: `WrapLogStore` makes `StoreLogs` block on the leader. Test and record what the cluster does (reference Q15: #503, #666). Write the test for the observed behavior.

### Phase 4: HTTP API

- [x] `internal/httpapi`: the contract above. Unit tests with a fake `NodeAPI` for every route and every error mapping. Forwarding tests with a fake leader (`httptest.Server`): forward once, loop guard, dial error = 503, broken forward on a write = 504, hold while no leader.
- [x] `tests/cluster/http_test.go`: real cluster + one `httptest.Server` per node. A write through a follower is forwarded and readable on all nodes. A `stale` read on a follower is served locally (`X-KV-Served-By`).

### Phase 5: Membership and recovery

- [x] Add a 4th node: `AddNonvoter`, wait until it caught up, `AddVoter`. Then 4 voters: stopping 2 stops writes (quorum 3).
- [x] Replace a failed node: `RemoveServer(dead)`, then add a new node with a new ID and an empty data dir.
- [x] Remove a node: the 2-node cluster keeps writing. The removed node does not get new writes (reference: it is not shut down).
- [x] Known bug #725: `VerifyLeader` counts non-voters. Demonstrate it if you can (isolate leader + non-voter). Name the issue in the doc comment.
- [x] `internal/node/recovery.go`: `RecoverCluster(cfg Config, deps Dependencies, servers []Server) error` (wraps `raft.RecoverCluster` with the same stores). Test: lose 2 of 3 nodes, recover the survivor as a 1-node cluster, writes work. Show the data loss case from reference Q10.

### Phase 6: Mutual TLS

- [x] `internal/tlsstream`: `StreamLayer` (implements `raft.StreamLayer`), `NewStreamLayer(bindAddress string, advertise net.Addr, tlsConfig *tls.Config)`, `LoadMutualTLS(caFile, certFile, keyFile string) (*tls.Config, error)` (TLS 1.3, `RequireAndVerifyClientCert`, server name from the dial address), `GenerateCertificates(dir string, nodes []NodeIdentity) error` (CA + one cert per node, DNS and IP SANs, key files 0600).
- [x] Unit tests: handshake works. A client with a cert from another CA is rejected. A client without a cert is rejected.
- [x] `tests/cluster/tls_test.go`: a 3-node cluster over mutual TLS (harness `NewTransport` option) elects a leader and replicates.

### Phase 7: Linearizability check

- [x] `tests/cluster/history.go`: record each client operation (client, input, call time, return time, output). An operation with an unknown outcome is open (no return).
- [x] `tests/cluster/linearizability_test.go`: clients run put and get through random nodes. A nemesis partitions, heals, stops and restarts nodes. Porcupine checks the history. Print the seed. Write the Porcupine visualization to `t.TempDir()` on failure.
- [x] A second test: `leader` or `stale` reads during a leader partition. Porcupine must find the violation. This proves the checker works.

### Phase 8: Binary, Docker, e2e

- [x] `cmd/kvstore`: `serve` (config → transport → node → httpapi → `http.Server`. On SIGTERM: leadership transfer if leader, graceful HTTP stop, `node.Close()`), `healthcheck` (GET `/health/live`, exit code), `gencerts` (calls `tlsstream.GenerateCertificates`).
- [x] `Dockerfile`, `.dockerignore`, `docker-compose.yml` (3 nodes, `certs` init service, static IPs, one volume per node, healthcheck), `Makefile` (help, lint, test, e2e, up, down, logs, status, kill-leader, partition, heal, pause, unpause, start).
- [x] `tests/e2e/` (`//go:build e2e`): own compose project and ports. Replicate across nodes, write through a follower, kill the leader, pause a follower, disconnect the leader from the network, stop 2 nodes then expect 503 or 504.

### Phase 9: Docs

- [x] `README.md`: what it is, architecture, API, run, test, chaos commands, a table "failure scenario → test → Raft lesson", and "part 1 vs part 2".
- [x] `docs/lessons-from-tests.md`: what the tests found about hashicorp/raft.
