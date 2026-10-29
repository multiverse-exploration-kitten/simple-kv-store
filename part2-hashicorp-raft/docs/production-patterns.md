# Production patterns: how Consul, Nomad and Vault use hashicorp/raft

HashiCorp builds three products on `github.com/hashicorp/raft`: Consul, Nomad and Vault. This doc shows how they set up the library, forward writes, serve reads, change membership and recover from outages. Part 2 of this repo copies the patterns that fit a 3-node learning KV store.

Other docs in this folder describe the library itself (`hashicorp-raft-reference.md`, `hashicorp-raft-storage-and-transport.md`). This doc is only about how the products use it.

## Versions read

I read the source code and the docs in October 2026.

| Product | Tag read | hashicorp/raft | raft-autopilot | raft-wal | raft-boltdb |
|---|---|---|---|---|---|
| Consul | [v2.1.0][c-gomod] | v1.8.0 | v0.1.6 | v0.5.0 | v2.4.2 |
| Nomad | [v2.1.0][n-gomod] | v1.8.0 | v0.1.6 | v0.5.0 | v2.4.2 |
| Vault | [v2.2.1][v-gomod] | v1.7.3 | v0.3.0 | v0.4.0 | v2.3.0 |

Each fact has a link to a pinned source line or to a docs page. **My reading** marks a conclusion that I took from the code and that no doc states.

## Key takeaways

1. All three products wrap the log store in a 512-entry `LogCache`. Consul and Nomad keep 2 file snapshots. New Consul servers use raft-wal by default.
2. One multiplier scales all Raft timeouts. Consul and Vault default to 5. Nomad defaults to 1. HashiCorp recommends 1 for Consul in production.
3. hashicorp/raft does not forward writes. Each product sends the request to the leader and holds it for a few seconds while no leader exists. Consul and Nomad retry only errors that prove that nothing changed.
4. A write that fails after it reached the leader is not retried. Consul returns HTTP 500 for it. Part 2 must return 504, because the outcome is unknown.
5. A Consul "consistent" read calls `VerifyLeader()` and then waits for a readiness flag. The new leader sets the flag after its `Barrier()` and after its leader setup. Consul's "default" read skips both checks, so it can be stale for a short time.
6. The FSM reads a type byte first. Bit 128 (`IgnoreUnknownTypeFlag`) lets an old node skip a new command. Any other unknown type makes the FSM panic, so that the nodes cannot diverge.
7. The Raft log index is the version number. Consul stores it as `ModifyIndex`. `?cas=<index>` gives compare-and-set, and `cas=0` means "create only".
8. No product dedupes all retried writes. Nomad dedupes only job dispatch with an idempotency token. Consul tells clients to use CAS.
9. New servers join as non-voters. Autopilot promotes a server after 10 s of good health. It removes dead servers, but never a majority of the voters.
10. Recovery without quorum uses `peers.json` and `raft.RecoverCluster`. The library says that this can commit entries that were never committed.

---

## 1. Raft setup

### Stores and transport

| Setting | Consul v2.1.0 | Nomad v2.1.0 | Vault v2.2.1 |
|---|---|---|---|
| Log and stable store | raft-wal when `raft.db` does not exist. BoltDB when it exists ([server.go][c-setup-wal]). | BoltDB by default. raft-wal is opt-in. With raft-wal, the server refuses to start if `raft.db` exists and tells you to run `nomad operator raft migrate-backend` ([server.go][n-setup-store], [config.go][n-config]). | BoltDB by default. `raft_wal` is experimental and off. Vault ignores it when `raft.db` exists ([raft.go][v-stores], [docs][v-storage-docs]). |
| fsync | raft-boltdb and raft-wal sync each append. The Consul docs say servers are I/O bound for writes ([server docs][c-server-docs]). | Sets `NoSync: false` with the comment "fsync each log write" ([server.go][n-nosync]). | Library default. |
| BoltDB freelist | `raft_logstore.boltdb.no_freelist_sync` option ([raft config docs][c-raft-docs]). | `boltdb.no_freelist_sync` option. | `NoFreelistSync: true` and a map freelist by default ([config.go][v-bolt]). |
| Log verifier | On by default with raft-wal, every 1 minute ([server.go][c-setup-wal]). | Optional. | Optional `raft_log_verifier_enabled`, 60 s ([docs][v-storage-docs]). |
| `LogCache` | 512 entries ([server.go][c-consts]) | 512 entries, can be disabled ([server.go][n-consts]) | 512 entries ([raft.go][v-consts]) |
| Snapshot store | `FileSnapshotStore`, keeps 2 ([server.go][c-snapstore]) | `FileSnapshotStore`, keeps 2 ([server.go][n-consts]) | Own `BoltSnapshotStore` that reads the on-disk FSM ([raft.go][v-stores]) |
| Transport | `NetworkTransport`, `MaxPool` 3, `Timeout` 10 s ([server.go][c-transport]) | `NetworkTransport`, `MaxPool` 3, `RaftTimeout` 10 s ([config.go][n-config]) | `NetworkTransport`, `MaxPool` 3, `Timeout` 10 s ([raft.go][v-transport]) |
| Stream layer and TLS | Raft shares the server RPC port. The first byte selects Raft. A TLS byte switches the connection to TLS first. The server checks the peer certificate before it hands the connection to Raft ([raft_rpc.go][c-raftrpc], [rpc.go][c-raftrpc-auth]). | Same design. Nomad checks that the certificate names `server.<region>.nomad` ([rpc.go][n-raft-tls]). | Raft uses the cluster port with its own ALPN name. Vault creates ECDSA P-521 certificates for mutual TLS ([streamlayer.go][v-streamlayer]). |
| Leader signal | `NotifyCh`, buffered 10 ([server.go][c-notify]) | `LeaderCh()` ([leader.go][n-monitor]) | `NotifyCh` for the HA lock ([raft.go][v-raftlock]) |
| `ShutdownOnRemove` | `false` ([config.go][c-config-raft]) | `false` ([config.go][n-config]) | Library default |
| Server ID | Node ID from the `node-id` file (protocol 3) ([server.go][c-setup]) | Node ID (protocol 3) | `node_id` or a random GUID in `data/node-id` ([docs][v-storage-docs]) |

### Timeouts

hashicorp/raft v1.8.0 defaults to a 1 s heartbeat timeout, a 1 s election timeout and a 500 ms leader lease timeout ([config.go][r-defaults]). Each product multiplies these values by one number.

| | Consul | Nomad | Vault |
|---|---|---|---|
| Name | `performance.raft_multiplier` | `server.raft_multiplier` | `performance_multiplier` |
| Default | 5 ([config.go][c-mult]) | 1 ([agent.go][n-mult]) | 5 ([raft.go][v-applycfg]) |
| Allowed range | 1 to 10 ([builder.go][c-builder]) | 1 to 10 ([agent.go][n-mult]) | greater than 0 |
| Scales | heartbeat, election, leader lease ([builder.go][c-builder]) | heartbeat, election, leader lease, **and** `CommitTimeout` ([agent.go][n-mult-apply]) | heartbeat, election, leader lease ([raft.go][v-applycfg]) |
| Advice | "In production, we recommend configuring the Consul server performance parameters to `1`" ([server docs][c-server-docs]). | Default is already 1. | Keep the default unless telemetry shows a need ([docs][v-storage-docs]). |

At multiplier 5 the timeouts are 5 s, 5 s and 2.5 s. HashiCorp says that wide networks with more latency do better with a larger multiplier ([server docs][c-server-docs]).

Vault adds one more trick. At startup it multiplies the election and heartbeat timeouts by 3 again. It reloads the normal values when the node becomes candidate or leader, or after one random timeout. The code comment says this stops all nodes from calling an election at once when they start together ([raft.go][v-initial-timeout], [raft.go][v-reload]).

### Snapshot settings

| Setting | raft v1.8.0 default ([config.go][r-defaults]) | Consul | Nomad | Vault |
|---|---|---|---|---|
| `SnapshotThreshold` | 8192 | 16384 ([config.go][c-config-raft]) | library default, configurable ([agent.go][n-mult-apply]) | 8192 ([docs][v-storage-docs]) |
| `SnapshotInterval` | 120 s | 30 s ([config.go][c-config-raft]) | library default, configurable | 120 s |
| `TrailingLogs` | 10240 | library default ([default.go][c-default]). The docs say 10000. | library default, configurable | library default. The docs say 10000. |

Vault also sets `NoSnapshotRestoreOnStart = true`, `MaxAppendEntries = 64` and `BatchApplyCh = true` ([raft.go][v-applycfg]). Vault can skip the restore at start because its FSM is a BoltDB file that survives a restart.

The Consul docs give the rule for `raft_trailing_logs`: change it only "when followers cannot catch up to the leader" ([raft config docs][c-raft-docs]). The Vault docs say that a higher `snapshot_threshold` trades disk I/O for disk space, and recovery can take longer ([docs][v-storage-docs]).

**For part 2.** Use raft-wal v0.4.1 or later, a 512-entry `LogCache` and a `FileSnapshotStore` that keeps 2 snapshots. Put the multiplier, the snapshot threshold, the snapshot interval and the trailing logs in config. Use small values in tests, so that snapshots happen in seconds.

---

## 2. Write forwarding

hashicorp/raft accepts `Apply()` only on the leader. Every product adds its own forwarding.

### How Consul forwards

Every RPC handler first calls `ForwardRPC()`. `KVS.Apply` is an example ([kvs_endpoint.go][c-kvs-rpc]). The steps are ([rpc.go][c-forward]):

1. If the request is for another datacenter, send it there.
2. If the request is a stale read and this server has heard from a leader at least once, serve it locally ([rpc.go][c-stale]).
3. If this server is the leader (`raft.State() == raft.Leader`), serve it locally ([server.go][c-isleader]).
4. Otherwise send the RPC to the known leader.
5. If that fails, call `canRetry()`. When it says yes, wait with backoff and go to step 1.

The wait starts at `RPCHoldTimeout / 16` and doubles each time, with jitter ([rpc.go][c-canretry]). `rpc_hold_timeout` defaults to 7 s ([default.go][c-default]). The source comment gives the reason. During a leader election, the caller "experiences a slow response rather than an error" ([config.go][c-hold]).

### Which errors Consul retries

`canRetry()` is short ([rpc.go][c-canretry]):

| Condition | Retry? | Reason in the code |
|---|---|---|
| Time since the first try is more than `RPCHoldTimeout` (writes) or `RPCHoldTimeout` + blocking wait (reads) ([structs.go][c-timeout]) | No | Deadline passed |
| Error contains "No cluster leader" | Yes | "No leader errors are always safe to retry since no state could have been changed." |
| `ErrChunkingResubmit`, `rate.ErrRetryLater` | Yes | The chunked write did not complete, or the rate limiter rejected it |
| EOF on a **read** | Yes | "Reads are safe to retry for stream errors" |
| EOF on a **write** | No | The write may have reached the leader |
| Any other error | No | |

Nomad's client agent uses the same rule: retry "no leader" errors, and retry EOF only for reads ([client/rpc.go][n-canretry]).

### What happens when the leader changes during a forwarded write

My reading of `canRetry()` and `raftApplyEncoded()` ([rpc.go][c-apply]) gives three cases:

| Case | Library error | Consul result | Outcome |
|---|---|---|---|
| The old leader stepped down before `Apply()` queued the entry | `ErrNotLeader` ("node is not the leader") | HTTP 500, no retry | Not applied |
| Leadership was lost after the entry was appended | `ErrLeadershipLost` | HTTP 500, no retry | **Unknown.** The `Apply` docs say "There is no way to guarantee whether the write succeeded or failed in this case" ([api.go][r-apply]). |
| The forwarding connection broke | EOF | HTTP 500, no retry for writes | Unknown |

The HTTP layer maps only rate-limit errors to 429 and 503. Every other RPC error becomes HTTP 500 ([http.go][c-http-errors]). So Consul does not tell "not applied" from "unknown". Part 2 must make that difference (AGENTS.md Raft rule 3).

Consul calls `raft.Apply(buf, enqueueLimit)` with `enqueueLimit = 30 s`. Nomad uses the same constant ([rpc.go][c-consts-rpc], [rpc.go][n-consts-rpc]). The timeout only limits the time to queue the entry. Vault passes 0, so it waits as long as needed ([raft.go][v-applylog]).

### Nomad and Vault

- **Nomad** forwards to the leader like Consul. One difference matters. A Nomad leader serves a non-stale RPC only after its readiness flag is set. Until then, it waits up to `RPCHoldTimeout` (5 s) and then returns `ErrNotReadyForConsistentReads` ([rpc.go][n-forward], [config.go][n-config]).
- **Vault** forwards at the HTTP layer. A standby sends the full client request to the active node over mutual TLS, or it redirects the client with HTTP 307 ([Vault HA docs][v-ha-docs]). The active node is always the Raft leader. The `RaftLock` comment says "Vault's active duty matches raft's leadership" ([raft.go][v-raftlock]).

**For part 2.** A follower forwards the HTTP write to the leader. While no leader is known, hold the request for a short time (for example 2 election timeouts) and poll with backoff. Retry only when the leader is unknown or the leader answered "not leader before append". Map the result: no leader after the hold time gives 503, `ErrLeadershipLost` or a broken connection gives 504.

---

## 3. Consistent reads

### Consul's three modes

| Mode | Query | What the server does | Docs say |
|---|---|---|---|
| `default` | none | Forward to the leader. The leader reads its local state store. It does not call `VerifyLeader()` and does not check the readiness flag (my reading of [rpc.go][c-forward] and [blockingquery.go][c-bq]). | "strongly consistent in almost all cases". An old leader can return stale values during a short window after a new election ([consistency docs][c-consistency]). |
| `consistent` | `?consistent` | Forward to the leader. The leader calls `VerifyLeader()`, then waits for the readiness flag ([rpc.go][c-consistent]). | "strongly consistent without caveats". Costs a quorum round trip. |
| `stale` | `?stale` | Any server reads its local state if it has heard from a leader at least once ([rpc.go][c-stale]). | "generally consistent to within 50 milliseconds of the leader, though there is no upper limit on this staleness". Works without quorum. |

Consul rejects `?stale` together with `?consistent` with HTTP 400 ([http.go][c-http-consistency]). Writes have no mode. They always act like `consistent` ([consistency docs][c-consistency]).

### The consistent read in code

`consistentReadWithContext()` does two checks ([rpc.go][c-consistent]):

1. `raft.VerifyLeader()`. If it fails, return the error at once.
2. If `isReadyForConsistentReads()` is false, poll it every `RPCHoldTimeout / 16`. Stop at the deadline with `ErrNotReadyForConsistentReads`.

The flag is an atomic integer ([server.go][c-readready]). `establishLeadership()` sets it as its last setup step, after the leader loop's `Barrier()` ([leader.go][c-establish]). `revokeLeadership()` clears it ([leader.go][c-revoke]). A blocking query with `?consistent` repeats the check on each loop ([blockingquery.go][c-bq]).

### max_stale

`?max_stale=<duration>` turns on stale mode. After the answer comes back, the agent compares it with the server's last contact. If `LastContact` is more than `max_stale`, the agent retries once in default mode ([catalog_endpoint.go][c-maxstale]). The docs warn that `discovery_max_stale` "may result in a prolonged outage if your Consul servers become overloaded". An overloaded server falls behind, and then every read goes to the leader ([consistency docs][c-consistency]).

### Response headers

`SetQueryMeta()` fills the values ([rpc.go][c-querymeta]). `setMeta()` writes the headers ([http.go][c-http-meta]).

| Header | Value |
|---|---|
| `X-Consul-Index` | The index of the result. Never 0, because `?index=0` never blocks and clients would loop. |
| `X-Consul-LastContact` | Milliseconds since this server heard from the leader. 0 on the leader. |
| `X-Consul-KnownLeader` | `true` when the server knows a leader |
| `X-Consul-Effective-Consistency` | The mode that the server used: `leader`, `consistent` or `stale` |

### Nomad and Vault

- **Nomad** has only `default` and `stale` ([Nomad API docs][n-api-docs]). Its leader waits for the readiness flag, but it never calls `VerifyLeader()` ([rpc.go][n-forward]). So an old leader can still serve a stale read. The docs mention this window.
- **Vault** reads from the local BoltDB FSM on the active node. `RaftBackend.Get()` does not call `VerifyLeader()` ([raft.go][v-get]). Before a node becomes active, `RaftLock.Lock()` writes a key through Raft and waits for it ([raft.go][v-raftlock]). My reading: that write acts like a barrier, because the FSM applies entries in order.

**For part 2.** Copy Consul's `consistent` mode, but make it the default (AGENTS.md Raft rule 4). Offer `?stale` with the `X-LastContact` and `X-KnownLeader` headers. `max_stale` is a cheap extra. Do not copy Consul's `default` mode, because it is not linearizable.

---

## 4. Leader establishment

### Consul's leader loop

`monitorLeadership()` reads `NotifyCh`, not `LeaderCh()`. The comment says that `LeaderCh` "is only notified best-effort". `NotifyCh` delivers every signal in order. Consul needs that order for cleanup and to never run two leader loops ([leader.go][c-monitor]). The channel has a buffer of 10 ([server.go][c-notify]). The library says that Raft blocks while it writes to `NotifyCh`, so the channel "should either be buffered or aggressively consumed" ([config.go][r-notifych]).

On `true`, Consul starts `leaderLoop()` in a goroutine. On `false`, it closes the stop channel and waits for the loop to end ([leader.go][c-monitor]). The loop does this ([leader.go][c-leaderloop]):

1. `raft.Barrier(2 * time.Minute)`. Wait until the FSM applied every earlier entry. On failure, wait and try again.
2. `establishLeadership()` one time. It starts leader-only work (session timers, tombstone GC, replication, autopilot) and sets the readiness flag last ([leader.go][c-establish]).
3. If `establishLeadership()` fails, call `revokeLeadership()` and then `LeadershipTransfer()` up to 3 times ([leader.go][c-transfer]). If the transfer also fails, retry in 5 s.
4. Reconcile membership on a timer until the stop channel closes.
5. `defer revokeLeadership()` stops all leader-only work and clears the flag ([leader.go][c-revoke]).

One comment in `establishLeadership()` shows why the barrier comes first. Session timers "MUST be done after the initial barrier to ensure the latest Sessions are available". Otherwise the setup reads stale data ([leader.go][c-establish]).

### Nomad's leader loop

Nomad reads `LeaderCh()` ([leader.go][n-monitor]). The library says that a reader can miss signals and see `true` twice ([api.go][r-leaderch]). Nomad handles that case. Two `true` values in a row restart the leader loop, so the FSM catches up again. Nomad logs that this "Could indicate network issues, memory paging, or high CPU load". The leader loop runs `Barrier()` and then sets the readiness flag ([leader.go][n-barrier], [leader.go][n-ready]).

**For part 2.** Copy Consul: a buffered `NotifyCh`, one leader goroutine at a time, `Barrier()` first, then the readiness flag, and a revoke step that clears the flag. A leadership transfer on setup failure is a small and useful extra.

---

## 5. FSM design

### Command format

`structs.Encode()` writes one type byte and then the msgpack body. The FSM reads `buf[0]` ([fsm.go][c-fsm-apply]):

1. If bit 128 (`IgnoreUnknownTypeFlag`) is set, clear it and remember "ignore if unknown".
2. Look up the type in a dispatch table and call the handler with `buf[1:]` and `log.Index`.
3. If no handler exists and the flag was set, log a warning and return `nil`.
4. Otherwise `panic`. The comment says: "If not, we have to panic so that we crash and our state doesn't diverge."

The flag comment explains the goal: "new commands can be added in a way that won't cause old servers to crash" ([structs.go][c-flag]). The type list has the comment "entries must only ever be added", because the numbers are stored in logs and snapshots ([structs.go][c-types]). Nomad uses the same byte and the same flag ([structs.go][n-flag], [fsm.go][n-fsm-apply]).

So a developer decides per command. A command that old nodes may safely skip gets the flag. A command that changes state that every node must have does not get the flag, so an old node crashes instead of diverging. Consul also has a downgrade mode that skips enterprise types (64 and above) ([fsm.go][c-fsm-apply]).

### When the FSM panics on purpose

| Product | Panic when |
|---|---|
| Consul | Unknown type without the flag. A request body that does not decode ([commands_ce.go][c-fsm-decode]). |
| Nomad | Unknown type without the flag. A request body that does not decode ([fsm.go][n-fsm-apply]). |
| Vault | A log that does not unmarshal. An unexpected log type. A failed BoltDB write transaction, which includes an unsupported operation type ([fsm.go][v-fsm]). |

### How apply errors reach the caller

The FSM does not panic on a business error. It returns the error as the response value. `applyKVSOperation()` returns `false` for a failed CAS and an `error` for an invalid operation ([commands_ce.go][c-fsm-kvs]). On the leader, `raftApplyEncoded()` first checks `future.Error()` for Raft errors. Then it checks whether `future.Response()` is an `error` and returns it as a normal error ([rpc.go][c-apply]). Nomad does the same and also returns `future.Index()` ([rpc.go][n-raftapply]).

### Snapshots with memdb

Consul keeps state in go-memdb, an in-memory database with immutable radix trees. A write transaction uses the Raft index as its index (`s.db.WriteTxn(idx)`) ([kvs.go][c-state-cas]).

- `Snapshot()` runs on the FSM goroutine and must be fast. It only takes a memdb snapshot, which is a pointer to the current trees ([fsm.go][c-fsm-snapshot]).
- `Persist()` runs while new applies continue. It writes a header with `LastIndex` and then one type byte and one msgpack record per object ([snapshot.go][c-fsm-persist]).
- `Restore()` builds a new state store from the stream and commits it. Then it swaps the pointer under a lock and calls `Abandon()` on the old store. That wakes every blocking query, because indexes can go backwards after a restore ([fsm.go][c-fsm-restore]). An unknown type in a snapshot is an error, not a skip.

Vault uses a different design. Its FSM is a BoltDB file. The snapshot store reads that file directly, so a snapshot only truncates the Raft log ([raft.go][v-stores], [integrated storage docs][v-is-docs]).

**For part 2.** Copy the type byte, the flag and the panic rule. Return business errors as the response and check for them on the leader. For snapshots, copy the cheap-copy-then-stream split. A Go map needs a copy under a lock in `Snapshot()`, or an immutable map.

---

## 6. KV API semantics

### Indexes

Each entry has `CreateIndex` and `ModifyIndex` ([structs.go][c-raftindex]). The FSM passes `log.Index` to the state store ([commands_ce.go][c-fsm-kvs]). So both values are Raft log indexes. They grow, and they skip numbers, because all writes share one log.

| Field | Rule ([kvs.go][c-state-set]) |
|---|---|
| `CreateIndex` | The index of the write that created the key. Kept on later writes. |
| `ModifyIndex` | The index of the last write that changed the key. A write of an equal entry does not change it. |
| `LockIndex` | Goes up by 1 each time a new session acquires the key ([kvs.go][c-state-lock]). |

### CAS

`PUT /v1/kv/<key>?cas=<index>` sets the operation to `KVCAS` ([kvs_endpoint.go][c-kvput]). The FSM checks it in one transaction ([kvs.go][c-state-cas]):

- `cas=0` and the key exists: no write.
- `cas=N` and the key does not exist: no write.
- `cas=N` and `ModifyIndex != N`: no write.
- Otherwise: write.

The response body is `true` or `false`. A failed CAS is HTTP 200 with `false` ([KV docs][c-kv-docs]). The PUT response does not carry the new `ModifyIndex` (my reading of [kvs_endpoint.go][c-kvput]). The client must read again to learn it. `DELETE ?cas=0` does nothing. Only a non-zero index deletes ([KV docs][c-kv-docs]).

### Blocking queries

`GET ?index=<N>&wait=<duration>` blocks until the index is greater than N or the wait ends ([blockingquery.go][c-bq]).

- `wait` defaults to 5 minutes and stops at 10 minutes ([config.go][c-config-defaults]).
- The server adds jitter of up to `wait / 16` ([rpc.go][c-querytimeout]).
- For one key, `X-Consul-Index` is the key's `ModifyIndex`. For a missing key, it is the table index ([kvs_endpoint.go][c-kvs-rpc]).
- The index is never 0 ([http.go][c-http-meta]).
- A return does not prove a change. If the index goes backwards, the client must reset to 0. A snapshot restore can cause that ([blocking docs][c-blocking]).

Nomad has the same API with `X-Nomad-Index` ([Nomad API docs][n-api-docs], [rpc.go][n-blocking]).

### Maximum value size

Consul sets `kv_max_value_size` to `raft.SuggestedMaxDataSize`, which is 512 KiB ([default.go][c-default], [api.go][r-maxsize]). A bigger body gets HTTP 413 ([kvs_endpoint.go][c-kvput-size]). The library comment gives the reason: big entries risk "RPC IO taking too long and preventing timely heartbeat signals". Heartbeats "are sent in serial in current transports", so this can cause "leadership instability" ([api.go][r-maxsize]). Consul also logs a warning for any Raft entry above 1 MiB ([rpc.go][c-consts-rpc]).

Vault's `max_entry_size` defaults to 1 MiB, which is 2 chunks of 512 KiB ([raft.go][v-consts], [docs][v-storage-docs]). Vault splits entries above 512 KiB with go-raftchunking ([raft.go][v-applylog]).

**For part 2.** Copy `CreateIndex` and `ModifyIndex` from the Raft index, and `?cas=` with `cas=0` as create-only. Unlike Consul, return the new `ModifyIndex` on a successful write, as Nomad's `raftApply` does. Limit values to 512 KiB and return 413.

---

## 7. Duplicate writes

The Raft thesis (section 6.3) gives each client an ID and each command a serial number. The FSM stores the last result per client. I found no such general table in the code that I read.

| Product | What it does |
|---|---|
| Consul | No dedupe. Internal forwarding retries only errors that prove "nothing changed" ([rpc.go][c-canretry]). A repeated PUT of the same value does not change `ModifyIndex` ([kvs.go][c-state-set]). The docs offer CAS for read-modify-write ([KV docs][c-kv-docs]). |
| Nomad | Job dispatch takes an `idempotency_token` ([jobs API docs][n-jobs-docs]). The leader checks it before Raft ([job_endpoint.go][n-dispatch]). The FSM checks it again inside `applyUpsertJob()` ([fsm.go][n-fsm-token]). The check scans existing jobs ([state_store.go][n-token-check]). My reading: the protection ends when garbage collection removes the earlier job. Job register has `EnforceIndex` with `JobModifyIndex`, a "check-and-set style" update ([jobs API docs][n-jobs-docs]). |
| Vault | The Go client retries up to 2 times by default ([client.go][v-client-retries]). Its retry policy retries 412, 429, 5xx (except 501) and connection errors ([client.go][v-client-policy]). My reading: the policy does not look at the HTTP method, so a write can run twice. KV v2 offers a `cas` option and a `cas_required` setting ([KV v2 docs][v-kv2-docs]). |

Two details in Nomad are worth copying. The FSM check is the real guarantee, because the FSM applies entries in log order on every node. The leader reads the clock (`time.Now()`) before Raft and puts the value into the command, so the FSM stays deterministic ([job_endpoint.go][n-dispatch]).

**For part 2.** Go beyond the products. Add an optional `client_id` and `seq` to each write. Keep the last `seq` and result per client inside the FSM, and include the table in snapshots. Without these fields, tell clients to use CAS. This matches the study guide's duplicate test.

---

## 8. Membership

### Autopilot

Consul and Nomad use raft-autopilot v0.1.6. Vault uses v0.3.0.

| Step | Rule |
|---|---|
| Add | `AddServer()` adds a new server with `AddNonvoter()` ([raft.go][ap-add]). A non-voter gets the log but does not count toward quorum. |
| Health | Four checks ([types.go][ap-health]). The server is alive. Its last contact is below `LastContactThreshold`. Its term is the leader's term. It is at most `MaxTrailingLogs` behind. |
| Promote | A non-voter that stays healthy for `ServerStabilizationTime` becomes a voter ([stable_promoter.go][ap-promote]). |
| Remove dead servers | Runs on each reconcile tick and after a new server joins ([run.go][ap-run]). It removes non-voters first. It never removes more than `(voters - 1) / 2` voters, and it never goes below `MinQuorum` ([reconcile.go][ap-prune]). |
| Promote or demote order (v0.3.0) | With an odd number of voters, autopilot demotes one failed voter **before** any promotion, because a promotion "temporarily inflates quorum" ([reconcile.go][ap3-reconcile]). |

| Default | Consul ([config.go][c-config-defaults]) | Nomad ([config.go][n-config]) | Vault ([raft_autopilot.go][v-autopilot-defaults]) |
|---|---|---|---|
| `CleanupDeadServers` | true | true | **false** (needs `min_quorum`) |
| `LastContactThreshold` | 200 ms | 200 ms | 10 s |
| `MaxTrailingLogs` | 250 | 250 | 1000 |
| `ServerStabilizationTime` | 10 s | 10 s | 10 s |
| Dead server last contact | n/a | n/a | 24 h |

### Order of operations to replace a failed server

**Consul** ([outage tutorial][c-outage], [autopilot docs][c-autopilot-docs]):

1. Check that the cluster still has a leader: `consul operator raft list-peers`.
2. Best case: bring the server back with the same IP address and node name.
3. Else start a new server with an empty data directory.
4. Autopilot adds it as a non-voter and promotes it after 10 s of good health.
5. Autopilot removes the failed server when the replacement joins.
6. Without autopilot, run `consul force-leave` if the node is still in `consul members`, or `consul operator raft remove-peer` for a peer that is only in Raft ([operator raft docs][c-operator-raft]).

**Vault** ([operator raft docs][v-operator-raft]):

1. Run `vault operator raft remove-peer <node-id>` for the failed server. Dead server cleanup is off by default.
2. Start the new node and join it with `vault operator raft join` or `retry_join`.
3. Autopilot keeps it as a non-voter until `server_stabilization_time` passes.
4. A removed node must delete its Raft data before it can join again.

The Vault docs also say: without autopilot, joining nodes count toward quorum while they sync. "Adding several at once can cause quorum loss" ([integrated storage docs][v-is-docs]).

**For part 2.** raft-autopilot is a library. But it needs a delegate that gets stats from each server, and that is a medium-sized task. A simpler version teaches the same lesson. Add a new node with `AddNonvoter()`. Poll until its last index is close to the leader's. Then call `AddVoter()`. Remove a dead voter with `RemoveServer()` only while a majority stays alive.

---

## 9. Disaster recovery

### peers.json and RecoverCluster

Consul, Nomad and Vault all read `raft/peers.json` at startup and call `raft.RecoverCluster()` ([server.go][c-peers], [server.go][n-setup-store], [raft.go][v-peers]).

- The format for protocol 3 is a JSON array of `{"id", "address", "non_voter"}` ([server.go][c-peersinfo]).
- Consul and Nomad write a `peers.info` file on the first start. If `peers.info` does not exist yet, they **delete** any `peers.json`. So an old file cannot start a recovery by accident ([server.go][c-peers]).
- After a successful recovery, the server deletes `peers.json`.
- `peers.info` says that you must stop all servers before you create `peers.json`. Every server must get the same file ([server.go][c-peersinfo]).

The library comment warns ([api.go][r-recover]): "This operation implicitly commits all entries in the Raft log, so in general this is an extremely unsafe operation." Entries that were "in the process of being replicated but not yet be committed" can become committed. The Consul tutorial also says that a multi-server loss can lose data ([outage tutorial][c-outage]).

Consul refuses a startup when the server was offline for more than `ServerRejoinAgeMax`, which defaults to 7 days ([config.go][c-config-defaults]).

### Snapshot save and restore

**Consul** ([snapshot_endpoint.go][c-snapshot-ep], [snapshot.go][c-snapshot-new], [snapshot API docs][c-snapshot-docs]):

- `GET /v1/snapshot` goes to the leader. Without `?stale`, the leader first runs a consistent read. Then it calls `raft.Snapshot()` and writes a gzipped archive with SHA-256 sums. `X-Consul-Index` holds the snapshot index.
- `?stale` lets a follower answer. The docs say that this helps when the cluster has no leader.
- `PUT /v1/snapshot` calls `raft.Restore()`. Then it runs `Barrier()` and asks the leader loop to run `revokeLeadership()` and `establishLeadership()` again, because the state store changed under it.
- The docs say that restore is "not designed to handle server failures during a restore". The new cluster must run the same Consul version.

**Vault**: `vault operator raft snapshot save` and `restore`. The restore continues in the background after the command returns ([operator raft docs][v-operator-raft]).

### What data can be lost

| Action | Lost |
|---|---|
| Restore a snapshot | Every write after the snapshot index. In-flight writes get `ErrAbortedByRestore` ([api.go][r-apply]). |
| `peers.json` recovery | Nothing that a surviving server stored. But uncommitted entries on the survivors become committed. Entries that only lost servers had are gone. |
| Single node failure with quorum | Nothing. |

**For part 2.** Copy `peers.json` with the `peers.info` sentinel. Add `GET /v1/snapshot` and `PUT /v1/snapshot` with the barrier after restore. Write a test that restores a snapshot and checks that later writes are gone.

---

## 10. Health and readiness endpoints

| Endpoint | Product | Answer |
|---|---|---|
| `/v1/status/leader` | Consul | The leader address, or `""` when no leader is known ([status_endpoint.go][c-status]) |
| `/v1/status/peers` | Consul | Addresses in the Raft configuration |
| `/v1/operator/raft/configuration` | Consul | Each server with ID, address, leader flag and voter flag |
| `/v1/operator/autopilot/health` | Consul | HTTP 429 when any server is unhealthy, with `FailureTolerance` ([operator_endpoint.go][c-op-health]) |
| `/v1/sys/health` | Vault | 200 active, 429 standby, 472 DR secondary, 473 performance standby, 474 standby that cannot reach the active node, 501 not initialized, 503 sealed, 530 removed ([sys_health.go][v-health], [health docs][v-health-docs]) |

The Vault docs explain 429: "a 429 indicates a standby node that doesn't process the request itself, read or write". `?standbyok=true` returns 200 on a standby for load balancers that only accept 2xx ([health docs][v-health-docs]).

What HashiCorp's own Kubernetes charts check:

- **Consul server readiness**: `curl /v1/status/leader | grep -E '".+"'`. A server is ready when it knows a non-empty leader. Period 3 s, failure threshold 2 ([consul-k8s][k8s-consul]).
- **Vault**: readiness runs `vault status` by default, which succeeds when the node is unsealed. The liveness probe path is `/v1/sys/health?standbyok=true` ([vault-helm values][k8s-vault-values], [vault-helm statefulset][k8s-vault-sts]).

Neither chart restarts a pod because it is a follower or because the cluster has no leader.

**For part 2.**

- `/health/live`: 200 when the process runs. Use it for liveness.
- `/health/ready`: 200 when the node knows a leader. On the leader, also require the readiness flag. Else 503.
- `/health/leader`: 200 on the leader, 429 on a follower (the Vault codes).
- `/v1/status/leader` and `/v1/raft/configuration`, as in Consul.

---

## 11. Metrics that operators alert on

The Consul telemetry docs give these "what to look for" rules ([telemetry docs][c-telemetry]):

| Metric | Rule |
|---|---|
| `consul.raft.leader.lastContact` | "lower than 200ms" |
| `consul.raft.state.leader`, `consul.raft.state.candidate` | "`leader` > 0 and `candidate` == 0" |
| `consul.raft.apply`, `consul.raft.commitTime`, `consul.kvs.apply` | Alert on "Deviations ... of more than 50% from baseline over the previous hour" |
| `consul.autopilot.healthy` | "Alert if `healthy` is `0`" |
| `consul.raft.thread.main.saturation`, `consul.raft.thread.fsm.saturation` | "steady-state saturation should be less than 50%" |
| `consul.raft.leader.oldestLogAge` | The lowest point should stay "2x or more" above the snapshot restore time. Compare with `consul.raft.fsm.lastRestoreDuration` and `consul.raft.rpc.installSnapshot`. |
| `consul.raft.boltdb.storeLogs`, `logsPerBatch` | Watch for growth. `logsPerBatch` near 64 with rising `storeLogs` means write latency and errors may follow. |
| `consul.raft.boltdb.freelistBytes` | No number. Use it to explain high `storeLogs` times. |
| `consul.runtime.sys_bytes` | Alert above 90% of system memory |
| `consul.runtime.total_gc_pause_ns` | Warning above 2 s per minute, critical above 5 s per minute |

The current reference page lists the metric names but gives no thresholds ([telemetry reference][c-telemetry-ref]). The numbers above come from the agent telemetry page.

Vault's key-metrics page adds `vault.core.leadership_lost` ("A consistently low value ... suggests a high leadership turnover") and `vault.core.leadership_setup_failed` ([Vault key metrics][v-key-metrics]).

**For part 2.** Expose `raft.Stats()` and a few counters (leader changes, apply latency, apply errors by class) on `/v1/metrics`. Document the Consul thresholds next to them. A full Prometheus setup is optional.

---

## 12. Production gotchas that HashiCorp documents

1. **The BoltDB file only grows.** BoltDB is a copy-on-write B-tree. Deleted log space goes to a freelist. After a write burst, the file can be several times its normal size. Each write must also store freelist data, so "latencies tend to increase" ([WAL docs][c-wal-docs]). Fixes: raft-wal, or `no_freelist_sync`, which costs a slower start ([raft config docs][c-raft-docs]). The Vault docs describe the same freelist problem ([integrated storage docs][v-is-docs]).
2. **raft-wal had real bugs.** v0.2.3 fixed concurrent `GetLog` calls that "could return corrupted log entries". v0.4.1 fixed a startup error after a tail truncation, which happens when a new leader overwrites an uncommitted entry ([raft-wal releases][wal-releases]). Use a recent version.
3. **Big values cause leader flapping.** Heartbeats wait behind big `AppendEntries` calls ([api.go][r-maxsize]). Keep entries at or below 512 KiB.
4. **Big snapshots cause a catch-up loop.** A follower can need longer to install a snapshot than the leader keeps the logs after it. Then the follower must start again with a newer snapshot. Raise `raft_trailing_logs` only for that case. The Consul docs add that the cluster is then likely overloaded ([raft config docs][c-raft-docs]). Watch `oldestLogAge` (section 11).
5. **Slow CPU, disk or network causes spurious elections.** "Spurious leader elections can be caused by networking issues between the servers or insufficient CPU resources" ([server docs][c-server-docs]). The minimum disk is 7500+ IOPS. Nomad logs a lost-and-regained leadership as a sign of "network issues, memory paging, or high CPU load" ([leader.go][n-monitor]). Vault strongly recommends disabling swap ([docs][v-storage-docs]).
6. **A short multiplier fails on a slow network.** A low `raft_multiplier` "may be triggered frequently in high latency situations" ([server docs][c-server-docs]).
7. **`max_stale` can turn overload into an outage.** See section 3.
8. **Blocking clients can busy-loop.** An index of 0 never blocks, so Consul never returns 0. Clients must also reset when the index goes backwards ([blocking docs][c-blocking]).
9. **A restore needs the same version and a stable cluster.** See section 9.
10. **A removed node keeps old data.** Vault requires you to delete it before the node joins again ([operator raft docs][v-operator-raft]).

---

## Patterns for part 2

| Pattern | Product source | Decision | Reason |
|---|---|---|---|
| raft-wal + 512-entry `LogCache` + `FileSnapshotStore` that keeps 2 | Consul [setupRaft][c-setup] | copy | Three lines of setup. It avoids the BoltDB freelist problem. |
| One multiplier for all Raft timeouts | Consul [builder.go][c-builder], Nomad [agent.go][n-mult-apply] | copy | One knob for Docker, tests and slow laptops. Default 1. |
| Configurable snapshot threshold, interval and trailing logs | Consul [config.go][c-config-raft] | copy | Tests need small values to force snapshots. |
| Buffered `NotifyCh` + one leader goroutine + `Barrier()` + revoke on loss | Consul [leader.go][c-leaderloop] | copy | Core of a correct leader setup. Small code. |
| Readiness flag set after barrier, cleared on revoke | Consul [server.go][c-readready], Nomad [rpc.go][n-forward] | copy | Prevents reads from a new leader whose FSM is behind. |
| Linearizable read = `VerifyLeader()` + readiness flag, as the default | Consul [consistentRead][c-consistent] | copy | Rule 4 needs a safe default. Consul's own default mode is not safe. |
| `?stale` + `X-LastContact` + `X-KnownLeader`, optional `max_stale` | Consul [http.go][c-http-meta], [catalog_endpoint.go][c-maxstale] | copy | Makes stale reads explicit and measurable. |
| Follower forwards writes. Hold while no leader. Retry only "nothing changed" errors. | Consul [ForwardRPC][c-forward], [canRetry][c-canretry] | adapt | Forward over HTTP, not a custom RPC port. Keep the retry rule. |
| Error mapping: no leader 503, leadership lost or broken forward 504 | Consul [http.go][c-http-errors] returns 500 | adapt | Rule 3 and rule 6 need distinct codes. Consul does not separate them. |
| Leadership transfer when leader setup fails | Consul [leader.go][c-leaderloop] | adapt | Small. Shows `LeadershipTransfer()` in a real use. |
| Type byte + `IgnoreUnknownTypeFlag` + panic on other unknown types | Consul [fsm.go][c-fsm-apply], Nomad [fsm.go][n-fsm-apply] | copy | Rule 2 (versioned commands) with a proven design. |
| Business errors as the FSM response, panic on decode or storage errors | Consul [rpc.go][c-apply], Vault [fsm.go][v-fsm] | copy | Separates "rejected" from "Raft failed" and never diverges. |
| `CreateIndex`/`ModifyIndex` from the Raft index, `?cas=`, `cas=0` create-only | Consul [kvs.go][c-state-cas] | copy | Rule 5. The Raft index is a free, unique version. |
| Return the new index from each write | Nomad [raftApply][n-raftapply] | adapt | Consul returns only `true`/`false`. The index lets the client chain CAS writes. |
| Blocking `GET ?index=&wait=`, jitter `wait/16`, index never 0 | Consul [blockingquery.go][c-bq] | adapt | Nice watch feature. Needs a notify channel per apply. Optional. |
| 512 KiB value limit with HTTP 413 | Consul [kvs_endpoint.go][c-kvput-size] | copy | Protects heartbeats. One check. |
| Dedupe table keyed by `client_id` + `seq`, checked inside the FSM | Nomad [fsm.go][n-fsm-token] (dispatch only) | adapt | Generalize Nomad's FSM-side check to all writes. Proves the duplicate test. |
| Cheap snapshot copy, streamed `Persist()`, swap on `Restore()` | Consul [fsm.go][c-fsm-snapshot] | adapt | No memdb. Copy the map under a lock, or use an immutable map. |
| `peers.json` recovery with the `peers.info` sentinel | Consul [server.go][c-peers] | copy | Small code. Teaches the most dangerous recovery path. |
| Snapshot save and restore API with `Barrier()` after restore | Consul [snapshot_endpoint.go][c-snapshot-ep] | adapt | Use `raft.Snapshot()` and `raft.Restore()`. Skip the archive format. |
| Non-voter first, promote when caught up, guarded dead-server removal | raft-autopilot [reconcile.go][ap-prune] | adapt | Do it by hand with `AddNonvoter()`/`AddVoter()`. The full library needs a stats delegate. |
| Health endpoints: ready = known leader, leader = 200/429 | consul-k8s [probe][k8s-consul], Vault [sys_health.go][v-health] | copy | Matches what HashiCorp's own probes check. |
| Larger timeouts at first start | Vault [raft.go][v-initial-timeout] | skip | Fixes a startup election storm that 3 nodes rarely hit. Adds a config reload path. |
| Metrics with Consul's alert thresholds | Consul [telemetry docs][c-telemetry] | adapt | Expose `raft.Stats()` and a few counters. Document the thresholds. |
| Serf gossip for membership and failure detection | Consul, Nomad | skip | A big subsystem. Part 2 has a static 3-node cluster. |
| go-raftchunking for values above 512 KiB | Consul [rpc.go][c-apply], Vault [raft.go][v-applylog] | skip | The size limit is enough. Chunking adds an FSM wrapper and resubmit logic. |
| On-disk BoltDB FSM with `NoSnapshotRestoreOnStart` | Vault [raft.go][v-applycfg] | skip | A second storage engine. An in-memory map plus snapshots teaches the same Raft lessons. |
| Raft log verifier (checksummed checkpoints) | Consul [server.go][c-setup-wal] | skip | Needs a checkpoint command and a report path. Worth a later experiment. |
| Shared port with a first-byte protocol switch and mTLS | Consul [raft_rpc.go][c-raftrpc], Vault [streamlayer.go][v-streamlayer] | skip | Part 2 runs inside one Docker network. A plain TCP transport is enough for learning. |

## Sources

Each link in this doc is a reference link. The targets are below, in the raw Markdown. Every source link pins a release tag.

Consul v2.1.0 source:

[c-gomod]: https://github.com/hashicorp/consul/blob/v2.1.0/go.mod
[c-setup]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L946-L1184
[c-consts]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L98-L110
[c-transport]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L962-L968
[c-setup-wal]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1026-L1072
[c-snapstore]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1075-L1086
[c-peers]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1088-L1150
[c-notify]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1175-L1183
[c-isleader]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1570-L1572
[c-readready]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1882-L1894
[c-peersinfo]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/server.go#L1990-L2064
[c-mult]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/config.go#L36-L42
[c-hold]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/config.go#L359-L364
[c-config-defaults]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/config.go#L614-L644
[c-config-raft]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/config.go#L666-L676
[c-default]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/config/default.go#L117-L158
[c-builder]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/config/builder.go#L730-L736
[c-consts-rpc]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L82-L92
[c-raftrpc-auth]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L519-L531
[c-canretry]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L610-L657
[c-forward]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L700-L870
[c-stale]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L772-L777
[c-apply]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L981-L1027
[c-querymeta]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L1065-L1088
[c-consistent]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L1090-L1137
[c-querytimeout]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/rpc.go#L1139-L1153
[c-raftrpc]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/raft_rpc.go#L98-L127
[c-monitor]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/leader.go#L70-L119
[c-transfer]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/leader.go#L121-L141
[c-leaderloop]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/leader.go#L145-L275
[c-establish]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/leader.go#L277-L347
[c-revoke]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/leader.go#L351-L387
[c-fsm-apply]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/fsm.go#L169-L203
[c-fsm-snapshot]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/fsm.go#L205-L225
[c-fsm-restore]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/fsm.go#L227-L317
[c-fsm-persist]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/snapshot.go
[c-fsm-decode]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/commands_ce.go#L160-L190
[c-fsm-kvs]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/fsm/commands_ce.go#L210-L257
[c-raftindex]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/structs/structs.go#L40-L45
[c-types]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/structs/structs.go#L47-L96
[c-flag]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/structs/structs.go#L169-L175
[c-timeout]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/structs/structs.go#L386-L417
[c-state-set]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/state/kvs.go#L135-L175
[c-state-cas]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/state/kvs.go#L348-L387
[c-state-lock]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/state/kvs.go#L420-L470
[c-kvs-rpc]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/kvs_endpoint.go#L103-L180
[c-kvput]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/kvs_endpoint.go#L187-L296
[c-kvput-size]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/kvs_endpoint.go#L240-L279
[c-http-errors]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/http.go#L455-L566
[c-http-meta]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/http.go#L893-L948
[c-http-consistency]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/http.go#L1080-L1140
[c-maxstale]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/catalog_endpoint.go#L254-L258
[c-bq]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/blockingquery/blockingquery.go#L117-L217
[c-snapshot-ep]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/snapshot_endpoint.go#L51-L130
[c-snapshot-new]: https://github.com/hashicorp/consul/blob/v2.1.0/snapshot/snapshot.go#L30-L100
[c-status]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/consul/status_endpoint.go#L24-L85
[c-op-health]: https://github.com/hashicorp/consul/blob/v2.1.0/agent/operator_endpoint.go#L318-L332

Consul docs:

[c-consistency]: https://developer.hashicorp.com/consul/api-docs/features/consistency
[c-blocking]: https://developer.hashicorp.com/consul/api-docs/features/blocking
[c-kv-docs]: https://developer.hashicorp.com/consul/api-docs/kv
[c-snapshot-docs]: https://developer.hashicorp.com/consul/api-docs/snapshot
[c-server-docs]: https://developer.hashicorp.com/consul/docs/reference/architecture/server
[c-raft-docs]: https://developer.hashicorp.com/consul/docs/reference/agent/configuration-file/raft
[c-wal-docs]: https://developer.hashicorp.com/consul/docs/deploy/server/wal
[c-autopilot-docs]: https://developer.hashicorp.com/consul/docs/manage/scale/autopilot
[c-outage]: https://developer.hashicorp.com/consul/tutorials/operate-consul/recovery-outage
[c-operator-raft]: https://developer.hashicorp.com/consul/commands/operator/raft
[c-telemetry]: https://developer.hashicorp.com/consul/docs/monitor/telemetry/agent
[c-telemetry-ref]: https://developer.hashicorp.com/consul/docs/reference/agent/telemetry

Nomad v2.1.0 source and docs:

[n-gomod]: https://github.com/hashicorp/nomad/blob/v2.1.0/go.mod
[n-consts]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/server.go#L76-L90
[n-setup-store]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/server.go#L1373-L1625
[n-nosync]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/server.go#L1495-L1503
[n-config]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/config.go#L631-L753
[n-mult]: https://github.com/hashicorp/nomad/blob/v2.1.0/command/agent/agent.go#L58-L66
[n-mult-apply]: https://github.com/hashicorp/nomad/blob/v2.1.0/command/agent/agent.go#L243-L279
[n-consts-rpc]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/rpc.go#L36-L45
[n-forward]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/rpc.go#L585-L660
[n-raftapply]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/rpc.go#L807-L826
[n-blocking]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/rpc.go#L851-L906
[n-raft-tls]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/rpc.go#L909-L930
[n-canretry]: https://github.com/hashicorp/nomad/blob/v2.1.0/client/rpc.go#L172-L187
[n-monitor]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/leader.go#L111-L170
[n-barrier]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/leader.go#L250-L262
[n-ready]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/leader.go#L436-L440
[n-fsm-apply]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/fsm.go#L256-L419
[n-fsm-token]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/fsm.go#L672-L705
[n-dispatch]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/job_endpoint.go#L2050-L2100
[n-token-check]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/state/state_store.go#L1893-L1914
[n-flag]: https://github.com/hashicorp/nomad/blob/v2.1.0/nomad/structs/structs.go#L153-L162
[n-api-docs]: https://developer.hashicorp.com/nomad/api-docs
[n-jobs-docs]: https://developer.hashicorp.com/nomad/api-docs/jobs

Vault v2.2.1 source and docs:

[v-gomod]: https://github.com/hashicorp/vault/blob/v2.2.1/go.mod
[v-consts]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L95-L108
[v-stores]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L520-L612
[v-applycfg]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1073-L1130
[v-initial-timeout]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1225-L1248
[v-transport]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1250-L1262
[v-peers]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1286-L1330
[v-reload]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1381-L1432
[v-get]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L1845-L1880
[v-applylog]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L2032-L2120
[v-raftlock]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft.go#L2163-L2270
[v-fsm]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/fsm.go#L686-L845
[v-bolt]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/config.go#L249-L283
[v-autopilot-defaults]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/raft_autopilot.go#L748-L759
[v-streamlayer]: https://github.com/hashicorp/vault/blob/v2.2.1/physical/raft/streamlayer.go#L93-L120
[v-health]: https://github.com/hashicorp/vault/blob/v2.2.1/http/sys_health.go#L122-L220
[v-client-retries]: https://github.com/hashicorp/vault/blob/v2.2.1/api/client.go#L258-L266
[v-client-policy]: https://github.com/hashicorp/vault/blob/v2.2.1/api/client.go#L1888-L1897
[v-storage-docs]: https://developer.hashicorp.com/vault/docs/configuration/storage/raft
[v-is-docs]: https://developer.hashicorp.com/vault/docs/internals/integrated-storage
[v-ha-docs]: https://developer.hashicorp.com/vault/docs/concepts/ha
[v-health-docs]: https://developer.hashicorp.com/vault/api-docs/system/health
[v-operator-raft]: https://developer.hashicorp.com/vault/docs/commands/operator/raft
[v-kv2-docs]: https://developer.hashicorp.com/vault/api-docs/secret/kv/kv-v2
[v-key-metrics]: https://developer.hashicorp.com/vault/docs/internals/telemetry/key-metrics

Libraries and charts:

[r-defaults]: https://github.com/hashicorp/raft/blob/v1.8.0/config.go#L329-L343
[r-notifych]: https://github.com/hashicorp/raft/blob/v1.8.0/config.go#L212-L214
[r-maxsize]: https://github.com/hashicorp/raft/blob/v1.8.0/api.go#L19-L31
[r-recover]: https://github.com/hashicorp/raft/blob/v1.8.0/api.go#L294-L321
[r-apply]: https://github.com/hashicorp/raft/blob/v1.8.0/api.go#L852-L867
[r-leaderch]: https://github.com/hashicorp/raft/blob/v1.8.0/api.go#L1154-L1165
[ap-add]: https://github.com/hashicorp/raft-autopilot/blob/v0.1.6/raft.go#L95-L115
[ap-health]: https://github.com/hashicorp/raft-autopilot/blob/v0.1.6/types.go#L110-L137
[ap-promote]: https://github.com/hashicorp/raft-autopilot/blob/v0.1.6/stable_promoter.go#L41-L55
[ap-run]: https://github.com/hashicorp/raft-autopilot/blob/v0.1.6/run.go#L150-L170
[ap-prune]: https://github.com/hashicorp/raft-autopilot/blob/v0.1.6/reconcile.go#L219-L289
[ap3-reconcile]: https://github.com/hashicorp/raft-autopilot/blob/v0.3.0/reconcile.go#L15-L76
[wal-releases]: https://github.com/hashicorp/raft-wal/releases
[k8s-consul]: https://github.com/hashicorp/consul-k8s/blob/v2.1.0/charts/consul/templates/server-statefulset.yaml#L601-L629
[k8s-vault-values]: https://github.com/hashicorp/vault-helm/blob/v0.34.1/values.yaml#L581-L610
[k8s-vault-sts]: https://github.com/hashicorp/vault-helm/blob/v0.34.1/templates/server-statefulset.yaml#L168-L190
