# hashicorp/raft storage, snapshots, transport and test support

This file is a checked reference for the storage and network side of
[github.com/hashicorp/raft](https://github.com/hashicorp/raft) and its log
stores. It answers 12 questions that matter when you build a 3-node key-value
store with hashicorp/raft, an HTTP API, Docker and Go tests with fault
injection. The core API (Apply, reads, elections, membership) is in
`hashicorp-raft-reference.md` in this folder.

## How this was checked

- Versions (latest releases on 2026-10-29, from the Go module proxy):

  | Module | Version | Released | `go` directive |
  |---|---|---|---|
  | `github.com/hashicorp/raft` | v1.8.0 | 2026-09-16 | 1.25.0 |
  | `github.com/hashicorp/raft-boltdb/v2` | v2.4.2 | 2026-09-16 | **1.26.0** |
  | `github.com/hashicorp/raft-boltdb/v2` | v2.4.1 | 2026-09-16 | 1.23 |
  | `github.com/hashicorp/raft-wal` | v0.5.0 | 2026-09-16 | **1.26.0** |
  | `go.etcd.io/bbolt` | v1.4.1 (with raft-boltdb), v1.5.0 (with raft-wal v0.5.0) | | |
  | `github.com/hashicorp/go-metrics` | v0.7.0 | | 1.25.0 |

- raft-boltdb v2.4.1 and v2.4.2 have identical `.go` files. Only `go.mod`
  changed (v2.4.2 removed the go-metrics shim, PR #67). [tested: `diff` of
  the module cache, `gh api .../compare/v2.4.1...v2.4.2`]
- The study tests ran with **go1.25.5 darwin/arm64** and `GOTOOLCHAIN=local`,
  against raft v1.8.0, raft-boltdb/v2 **v2.4.1**, bbolt v1.4.1.
- The fsync counts ran on **Linux arm64** (Debian 12 container on Docker
  Desktop 23.0.5) under `strace`. The raft-wal binary was built with go1.26.0.
- Date of the checks: 2026-10-29.
- Each fact has a tag:
  - **[tested]**: a Go test or program ran it and showed the result.
  - **[code]**: read in the source. The file and line are given.
  - **[not verified]**: the fact was not checked. Do not trust it without a test.
- Line numbers refer to the module cache: `raft@v1.8.0`,
  `raft-boltdb/v2@v2.4.2` (same as v2.4.1), `raft-wal@v0.5.0`,
  `bbolt@v1.4.1`. Consul, Nomad and Vault links point to fixed commits.
- The study code is in the scratchpad of the research session:
  `/private/tmp/claude-501/-Users-william-workspace-simple-kv-store/2ee5234f-11ad-496c-90b3-250d84527b4f/scratchpad/hraft-storage/`.
  That folder is temporary. The important code is copied into the appendix.

## The 16 most important facts

1. **The newest stores need Go 1.26.** raft-boltdb/v2 v2.4.2 and raft-wal
   v0.5.0 have `go 1.26.0`. With Go 1.25 and `GOTOOLCHAIN=local`, use raft
   v1.8.0 + raft-boltdb/v2 **v2.4.1** (same code). raft-wal v0.4.2 does not
   build with raft v1.8.0: it imports `go-metrics/compat`, which go-metrics
   v0.7.0 removed. Only the 2023 raft-wal v0.4.1 (and older) builds. [tested]
2. **raft-boltdb fsyncs twice per write call.** Each `StoreLogs` call and each
   StableStore `Set` makes 2 `fdatasync` calls (data pages, then meta page),
   for 1 entry or for 64 entries. raft-wal makes 1 `fsync` per `StoreLogs`.
   [tested: strace] On macOS one synced `StoreLogs` took about 9 ms. [tested]
3. **`BoltOptions.NoSync` does nothing.** `raftboltdb.New` sets
   `handle.NoSync = options.NoSync` after `bbolt.Open` (`bolt_store.go:86`).
   Only `raftboltdb.Options.NoSync` turns fsync off. Keep it `false`. [tested]
4. **A second open of `raft.db` blocks forever** if the first store is still
   open and `bbolt.Options.Timeout` is 0 (the default). `Raft.Shutdown` does
   not close the log store, the stable store or the snapshot store. Close the
   BoltStore yourself, and set a Timeout. [tested]
5. **`raft.db` never shrinks.** The test wrote 20,000 entries of 1 KiB and
   compacted down to 100 entries. The file stayed at 48 MiB, with 2,845 free
   pages in the freelist. [tested]
6. **A lagging follower gets the whole snapshot in one stream.** If the
   leader no longer has the follower's next entry, it sends InstallSnapshot:
   one RPC, no chunks, no resume. While the follower is down, the leader
   retries with the newest snapshot each time. [tested]
7. **A restarted node is stale until it hears the commit index.** `NewRaft`
   restores the newest valid snapshot. It does not apply the log entries after
   that snapshot. They are applied after the node learns the commit index from
   a leader (or wins an election). [tested]
8. **`RestoreCommittedLogs=true` fails with both real stores.** NewRaft
   returns `ErrIncompatibleLogStore`, because neither raft-boltdb nor raft-wal
   implements `raft.CommitTrackingLogStore`. `LogCache` also hides that
   interface. [tested for raft-boltdb] [code for raft-wal and LogCache]
9. **A corrupt newest snapshot can crash the node on every start.** The CRC
   check fails and raft falls back to the older snapshot. If the log entries
   between the two snapshots are already compacted, `NewRaft` panics with
   `log not found`. If every snapshot is corrupt, `NewRaft` returns
   `failed to load any existing snapshots`. [tested]
10. **`Snapshot()` with nothing new still writes a new snapshot.** It returns
    `ErrNothingNewToSnapshot` only if the FSM never applied anything. Two
    user snapshots at the same index push the older, different snapshot out
    of the retain count. [tested]
11. **`FSM.Snapshot()` blocks Apply. `Persist` does not.** A 300 ms
    `Snapshot()` made a write wait 302 ms. 20 writes during a blocked
    `Persist` took at most 70 µs each. [tested]
12. **The transport timeout is one deadline per RPC.** It covers send plus
    the wait for the response (and the dial). A follower that does not answer
    fails the RPC after the timeout. InstallSnapshot gets
    `timeout × size / 256 KiB`. [tested] [code]
13. **Heartbeats skip `Consumer()`.** NetworkTransport calls the heartbeat
    handler directly. A fault wrapper that filters only `Consumer()` lets
    heartbeats through a "partition". [tested]
14. **A wrapper must forward `WithClose` and `WithPreVote`.** Without
    `WithClose`, `Shutdown` never closes the inner transport and the port
    stays bound. Without `WithPreVote`, raft turns pre-vote off with only a
    warning. [code] The working wrapper is in the appendix. [tested]
15. **Lost responses turn a failed write into a committed write.** With
    follower → leader traffic blocked, `Apply` on the leader returned
    `leadership lost while committing log`. The followers then elected a new
    leader and committed the entry. [tested, 5 of 5 runs]
16. **InmemTransport cannot drop responses.** `Disconnect` removes one route
    in one direction. In-flight responses still arrive. For one-way faults and
    lost responses, wrap NetworkTransport instead. [tested]

---

## Q1. raft-boltdb/v2: fsync, Options, known problems, Close

### fsync per call

`StoreLogs` opens one bbolt write transaction for the whole batch and calls
`tx.Commit()` (`bolt_store.go:191-231`). bbolt `Commit` writes the dirty
pages and calls `fdatasync` (`tx.go:524`), then writes the meta page and
calls `fdatasync` again (`tx.go:568`). On Linux this is `fdatasync(2)`
(`bolt_linux.go:9`). On macOS it is `os.File.Sync`, which Go implements with
`F_FULLFSYNC` (`boltsync_unix.go:7`, Go `internal/poll/fd_fsync_darwin.go`).
[code]

Measured with `strace -e trace=fsync,fdatasync`, 100 operations minus the
0-operation baseline [tested]:

| Store | Operation | fdatasync per call | fsync per call |
|---|---|---|---|
| raft-boltdb v2.4.1 | `StoreLogs` (1 entry) | 2 | 0 (1 extra in total for file growth) |
| raft-boltdb v2.4.1 | `StoreLogs` (64 entries) | 2 | 0 (6 extra in total for file growth) |
| raft-boltdb v2.4.1 | `StoreLogs`, `Options.NoSync=true` | 0 | 0 |
| raft-boltdb v2.4.1 | `SetUint64` (StableStore) | 2 | 0 |
| raft-wal v0.5.0 | `StoreLogs` (1 or 64 entries) | 0 | 1 |
| raft-wal v0.5.0 | `SetUint64` (StableStore, bbolt meta DB) | 2 | 0 |

Raft calls `StoreLogs` once per batch. The leader stores up to
`MaxAppendEntries` (default 64) new entries per call. A follower stores the
entries of one AppendEntries RPC per call. So the cost is per batch, not per
entry. Raft writes the StableStore only on term changes and votes
(`raft.go:2154-2166`, keys `CurrentTerm`, `LastVoteTerm`, `LastVoteCand`).
[code]

Timing on macOS, average of 200 single-entry `StoreLogs` calls [tested]:

| Setting | Average |
|---|---|
| default | 9.03 ms |
| `BoltOptions: &bbolt.Options{NoSync: true}` | 8.98 ms (no effect) |
| `raftboltdb.Options{NoSync: true}` | 0.086 ms |

### Options

```go
type Options struct {
    Path                    string         // file path of raft.db
    BoltOptions             *bbolt.Options // passed to bbolt.Open
    NoSync                  bool           // skip fsync. Unsafe.
    MsgpackUseNewTimeFormat bool           // time.Time encoding compat
}
```

`bolt_store.go:47-65`. `New` opens bbolt, then sets
`handle.NoSync = options.NoSync` (`bolt_store.go:82-86`). So
`BoltOptions.NoSync` is always overwritten. [code] [tested]

Useful `bbolt.Options` fields (`bbolt@v1.4.1/db.go`, `type Options`) [code]:

- `Timeout`: how long to wait for the file lock. **0 means wait forever.**
  With `Timeout: 100ms` the second open failed with bbolt `ErrTimeout`. Without
  it, the second open was still blocked after 500 ms and finished only when
  the first store closed. [tested]
- `NoFreelistSync`: do not write the freelist on each commit. Less disk I/O
  per write, but bbolt scans the whole file on open. Consul exposes this as
  `raft_logstore.boltdb.no_freelist_sync`
  ([server.go:1050](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L1050)).
- `FreelistType`: `array` (default) or `hashmap`.
- `NoGrowSync`, `InitialMmapSize`, `MmapFlags`, `PageSize`, `MaxSize`,
  `Mlock`, `ReadOnly`, `Logger`, `NoStatistics`.
- `ReadOnly: true` makes `New` skip bucket creation (`bolt_store.go:96`).

### One file for LogStore and StableStore

One `*BoltStore` implements both `raft.LogStore` and `raft.StableStore`. Logs
go to bucket `logs`, stable keys to bucket `conf` (`bolt_store.go:26-27`).
Pass the same value twice to `NewRaft`. [code] Consul does this
([server.go:1048-1060](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L1048)).

### Known production problems

- **The file only grows.** Log compaction (`DeleteRange`) frees pages inside
  the file but never returns them to the OS. The free pages go to the
  freelist, and with `NoFreelistSync=false` the freelist is written on every
  commit. Test result: 20,032 entries of 1 KiB gave a 49,248 KiB file. Then
  `DeleteRange` left 100 entries, and one more batch followed. The file was
  still 49,248 KiB. It had 2,845 free pages (45,600 KiB) and a 22,816-byte
  freelist encoding. [tested]
- Consul's docs describe the result. After a write burst the file is mostly
  free space. The freelist metadata goes to disk with every commit, and write
  latency can rise
  ([Consul WAL docs](https://github.com/hashicorp/web-unified-docs/blob/a2aceb453baaaeb24b12f559c4b0ae5f094df60d/content/consul/v1.22.x/content/docs/deploy/server/wal/index.mdx)).
  [code: docs]
- The old module `github.com/hashicorp/raft-boltdb` (v1, `boltdb/bolt`) has
  open `-race` checkptr issues (#35, #45). Use `/v2`. All study tests passed
  with `-race` on `/v2`. [tested for /v2] [not verified for v1]
- Issue #61 (open) reports many `Rollback failed: tx closed` error logs. The
  `defer tx.Rollback()` after `Commit` causes it. In v2.4.1 the bbolt logger
  is a no-op by default, and the study tests showed no such lines.
  [tested: no lines with the default logger] [not verified with a bbolt Logger set]

### Metrics

`(*BoltStore).RunMetrics(ctx, interval)` emits freelist and transaction
gauges every interval (default 5 s) until `ctx` ends (`metrics.go:20-37`).
Consul runs it in a goroutine
([server.go:1062](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L1062)).
Watch `raft.boltdb.freelistBytes` and `raft.boltdb.freePageBytes`. [code]

### Close

`Close` closes the bbolt handle (`bolt_store.go:130-132`). After `Close`,
`StoreLogs`, `GetLog` and `GetUint64` return `database not open`. They do
not panic. [tested] `Raft.Shutdown` does not call it (`api.go:1060-1073`).
[code]

## Q2. raft-wal: maturity, fsync, API, and the recommendation

### Maturity

- The README still says "**This library is still considered experimental!**"
  (`README.md:10`). [code]
- Consul uses raft-wal **by default since v1.20.0** for a new server (no
  `raft.db` in the data dir). Before v1.20.0 that default needed the v2
  catalog experiment. The gate went away when PR #21592 removed the v2
  experiments. [tested: `server.go` at tags v1.15.0 … v2.0.0] Consul's docs
  say "It became the default backend in v1.20". [code: docs] Consul also
  turns on log verification (checksums every 1 minute) when it picks WAL by
  default
  ([server.go:1027-1035](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L1027)).
  [code]
- If `raft.db` exists, Consul keeps BoltDB, even with `backend = "wal"`
  (`server.go:1036-1046`). [code]
- Nomad (main, 2026-10-28) defaults to BoltDB. WAL is opt-in
  ([server.go](https://github.com/hashicorp/nomad/blob/7399e1f13154e5a643c1c24432819092006ceb14/nomad/server.go#L1461)).
  Vault has an opt-in `raft_wal` setting
  ([raft.go:529-541](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/physical/raft/raft.go#L529)).
  [code]

### fsync behavior

- `StoreLogs` appends all entries, then a commit frame, then calls
  `Sync` once (`segment/writer.go:361-379`, `:456-463`). There is no NoSync
  option. [code] 1 fsync per call. [tested: strace]
- The first `Sync` of a new segment file also fsyncs the parent directory
  (`fs/file.go:30-39`). [code]
- The StableStore part is a bbolt file (`metadb`). Each `Set` costs 2
  fdatasync, like raft-boltdb. [tested: strace]
- The README assumes "powersafe overwrites" (a partial sector write does not
  damage earlier bytes), like SQLite (`README.md:35-37`). [code]

### API

```go
wal, err := raftwal.Open(dir,
    raftwal.WithSegmentSize(64*1024*1024), // default 64 MiB (wal.go:34)
    raftwal.WithLogger(logger.Named("wal")),
    raftwal.WithMetricsCollector(collector),
)
// *WAL implements raft.LogStore, raft.StableStore, raft.MonotonicLogStore.
defer wal.Close()
```

`wal.go:94` (`Open`), `options.go:19-61`, `wal.go:38-40`, `wal.go:909`
(`Close`). [code]

- `IsMonotonic()` is `true` (`wal.go:955-957`). After InstallSnapshot or a
  user restore, raft then deletes **all** old logs (`raft.go:1965-1968`,
  `snapshot.go:265-279`). [code]
- `DeleteRange` supports only a head or a tail range (`README.md:55-58`).
  Raft needs nothing else. [code]

### Recommendation for part 2: raft-boltdb/v2 v2.4.1

Use **raft-boltdb/v2 v2.4.1** with `NoSync=false`, a bbolt `Timeout`, and a
`LogCache` in front. Reasons:

1. **It builds with Go 1.25 at a current version.** raft-wal v0.5.0 needs
   Go 1.26. raft-wal v0.4.2 does not build with raft v1.8.0. [tested] Only
   raft-wal v0.4.1 and older build with Go 1.25 and raft v1.8.0. [tested:
   v0.3.0, v0.4.0, v0.4.1] v0.4.1 is from 2023-10-20. It pulls in
   `github.com/coreos/etcd v3.3.27+incompatible` and the deprecated
   `armon/go-metrics`. The commits from v0.4.1 to v0.5.0 change only
   dependencies, metrics wiring and CI, no WAL logic. [tested: `gh api
   .../compare/v0.4.1...v0.5.0`] Moving part 2 to Go 1.26 only for the log
   store is not worth it.
2. **It has the longer production record.** Consul used it as the only store
   until v1.20. Nomad and Vault still default to it. Its failure modes
   (freelist growth, file never shrinks) are known and have metrics.
3. **One file is simpler to reason about.** LogStore and StableStore live in
   one `raft.db`. This repo is about Raft lessons, not storage engines.
4. **The fsync contract is clear and testable.** 2 fdatasync per batch,
   before `StoreLogs` returns. That satisfies rule 7 (persist before you
   acknowledge).
5. **Swapping later is cheap.** Both stores implement the same interfaces.
   A move to raft-wal is one constructor. Do not add an abstraction for it now.

What you give up: raft-wal's 1 fsync per batch (versus 2), and no freelist
growth. With a small KV store and default `TrailingLogs`, neither matters.

## Q3. LogCache

`raft.NewLogCache(capacity, store)` wraps any LogStore (`log_cache.go:25`).
[code]

- It is a ring buffer of `capacity` entries. Slot = `index % capacity`
  (`log_cache.go:49`, `:74`).
- `StoreLogs` writes to the store first. It caches the entries only if the
  store succeeded (`log_cache.go:66-78`). So the cache never holds an entry
  that is not on disk.
- `GetLog` serves a hit from memory. A miss goes to the store
  (`log_cache.go:46-60`).
- `DeleteRange` clears the whole cache (`log_cache.go:88-95`).
- `FirstIndex` and `LastIndex` always go to the store.
- It forwards `IsMonotonic` (`log_cache.go:38-44`). It does **not** forward
  `CommitTrackingLogStore`. So `RestoreCommittedLogs` fails with a LogCache in
  front of any store. [code]

Size: Consul, Nomad and Vault all use **512**
([Consul server.go:106](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L106),
[Nomad server.go:90](https://github.com/hashicorp/nomad/blob/7399e1f13154e5a643c1c24432819092006ceb14/nomad/server.go#L90),
[Vault raft.go:98](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/physical/raft/raft.go#L98)).
Consul's docs say they "measured no significant improvement" from the cache,
because the OS page cache already holds recent entries. Consul keeps a flag
to disable it. [code: docs] Use 512 in part 2. It is cheap and it matches the
reference products. Memory is 512 × entry size.

## Q4. FileSnapshotStore

### API

```go
store, err := raft.NewFileSnapshotStoreWithLogger(baseDir, retain, logger)
store, err := raft.NewFileSnapshotStore(baseDir, retain, logOutput) // io.Writer
```

`file_snapshot.go:88-132`. `retain` must be at least 1
(`file_snapshot.go:89-91`). The constructor makes `<base>/snapshots/` and
tests write permission with a `permTest` file (`file_snapshot.go:101-116`).
Consul uses `retain = 2`
([server.go:102](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L102)).
[code]

### How a snapshot is written

1. `Create` makes `<base>/snapshots/<term>-<index>-<unix ms>.tmp/`
   (`file_snapshot.go:153-173`).
2. It writes `meta.json` without the CRC and fsyncs it
   (`file_snapshot.go:200`, `:501-532`).
3. `Write` goes through a buffer into `state.bin` and into a CRC-64 (ECMA)
   hash at the same time (`file_snapshot.go:206-219`, `:392-394`).
4. `Close` flushes and fsyncs `state.bin`, and records size and CRC
   (`file_snapshot.go:468-498`). Then it rewrites `meta.json` with the CRC
   and fsyncs it. Then it renames the dir without `.tmp` and fsyncs the
   parent dir, except on Windows (`file_snapshot.go:397-439`).
5. Then it deletes snapshots beyond `retain` (`file_snapshot.go:442`,
   `:366-382`).
6. `Cancel` closes the file and deletes the `.tmp` dir
   (`file_snapshot.go:450-465`).

[code] The fsync calls skip only when the private `noSync` field is set. Only
raft's own `FileSnapTest` helper sets it (`testing.go:895-907`). [code]

### CRC check

- `Open` reads the whole `state.bin` once to compute the CRC, compares it with
  `meta.json`, seeks back to 0, and returns a buffered reader
  (`file_snapshot.go:314-363`). So every restore and every InstallSnapshot
  reads the file twice. [code]
- A mismatch returns `CRC mismatch` and logs `CRC checksum failed`. [tested]

### What happens on start with a bad snapshot

| Case on disk | What raft does | Tag |
|---|---|---|
| A `*.tmp` dir (crash during write) | `List` skips it with a warning `found temporary snapshot`. **Nothing ever deletes it.** | [tested] |
| A dir with an unreadable `meta.json` | `List` skips it with a warning (`file_snapshot.go:269-273`). | [code] |
| Newest `state.bin` corrupt, older one good, logs still cover the gap | `NewRaft` restores the older one (newest-to-oldest loop, `api.go:652-680`). Later the log fills the gap. | [tested] |
| Newest corrupt, logs between the snapshots already compacted | `NewRaft` panics with `log not found` (`api.go:613-617`). Every restart panics again. | [tested] |
| Every snapshot corrupt | `NewRaft` returns `failed to load any existing snapshots` (`api.go:684-687`). | [tested] |

Two more facts from the same test [tested]:

- The corrupt snapshot stays on disk. It still counts toward `retain`. After
  one new snapshot, the dir held the corrupt one and the new one. The good
  older one was deleted.
- `List` returns at most `retain` snapshots (`file_snapshot.go:237`). So with
  `retain = 1` there is no fallback at all.

What to do: keep `retain = 2`. Watch for `CRC mismatch` and `failed to
restore snapshot` in the logs. Delete leftover `*.tmp` dirs at startup
yourself, if you care about disk space. If a node panics with
`log not found` at start, wipe its data dir and let it rejoin.

## Q5. Snapshot triggers, InstallSnapshot, and restart

### Triggers

- `runSnapshots` wakes up at a random time between `SnapshotInterval` and
  2 × `SnapshotInterval` (`snapshot.go:73-103`, `randomTimeout`). [code]
- It takes a snapshot if `lastLogIndex - lastSnapshotIndex >=
  SnapshotThreshold` (`snapshot.go:107-121`). [code]
- Defaults: `SnapshotInterval` 120 s, `SnapshotThreshold` 8192,
  `TrailingLogs` 10240 (`config.go:337-339`). `ValidateConfig` rejects
  `SnapshotInterval < 5ms` and checks nothing for the other two
  (`config.go:376-378`). All three can change at run time with
  `ReloadConfig`. [code]
- After a snapshot, raft deletes logs up to
  `min(snapshotIndex, lastLogIndex - TrailingLogs)` (`snapshot.go:217-249`).
  So `TrailingLogs` entries always stay. [code]
- `raft.Snapshot()` runs at once on the same goroutine (`snapshot.go:87-97`).
  It returns `ErrNothingNewToSnapshot` **only** when the FSM has applied
  nothing at all (`fsm.go:216-221`). A second `Snapshot()` without new
  entries made a second snapshot at the same index 32. [tested]
- A snapshot fails while a configuration change is not yet applied
  (`snapshot.go:174-177`). [code]

### When the leader sends InstallSnapshot

The leader builds AppendEntries from `nextIndex`. If `GetLog` for the
previous entry or for a new entry returns `ErrLogNotFound`, it jumps to
`SEND_SNAP` (`replication.go:223-224`, `:587-628`). The check is
`err == ErrLogNotFound` with `==`. **A LogStore wrapper must return
`raft.ErrLogNotFound` unwrapped**, or the leader never falls back to a
snapshot. raft-boltdb returns it unwrapped (`bolt_store.go:180`). [code]

Test: `TrailingLogs=10`, `SnapshotThreshold=20`, `SnapshotInterval=50ms`.
Stop a follower at index 6. Write 200 entries. The leader compacts to first
index 177. Restart the follower on the same port and data dir [tested]:

```
leader:   installing snapshot on: peer=node2 id=2-19-...  size=407
leader:   failed to install snapshot: ... connect: connection refused
leader:   installing snapshot on: peer=node2 id=2-38-...  size=882
          ... (one attempt for each newer snapshot while the follower is down)
leader:   installing snapshot on: peer=node2 id=2-205-... size=5057
follower: copied to local snapshot: bytes=5057
follower: Installed remote snapshot
```

The follower restored its FSM once, stored the snapshot (index 205) in its
own snapshot dir, and then applied the 2 newer entries. [tested]

### Is the snapshot one stream?

Yes. [code]

- One InstallSnapshot RPC: the msgpack request, then `io.Copy` of the whole
  `state.bin` on the same TCP connection (`net_transport.go:507-542`).
- The connection is always closed afterwards (`net_transport.go:513`).
- No chunks and no resume. A failure starts again from byte 0 with the
  newest snapshot (`replication.go:299-345`).
- The I/O deadline is `Timeout × (Size / 256 KiB)`, at least `Timeout`
  (`net_transport.go:516-522`, `DefaultTimeoutScale` at `:30`).
- The follower writes the stream into its own snapshot store and checks
  that it got `Size` bytes. It closes the sink (fsync and rename). Then it
  restores the FSM from that local copy (`raft.go:1837+`). This
  runs on the follower's main Raft goroutine. Heartbeats still work through
  the fast path (Q7). [code]

### Restart: snapshot first, log later

`NewRaft` (`api.go:508`) does this [code]:

1. Read `CurrentTerm` and the last log entry.
2. `restoreSnapshot`: try snapshots from newest to oldest. Call `FSM.Restore`
   on the first one that opens and restores. Set `lastApplied` to its index
   (`api.go:645-688`).
3. `restoreFromCommittedLogs`: only with `RestoreCommittedLogs=true` and a
   `CommitTrackingLogStore` (`api.go:724-754`).
4. Scan the log after the snapshot for configuration entries only
   (`api.go:613-619`).

It does **not** apply the command entries after the snapshot. Test: one node,
30 writes, `Snapshot()`, 10 more writes, restart. Right after `NewRaft`: state
`Follower`, 30 keys, 0 applies, 1 restore. After it elected itself: 40 keys,
10 applies. [tested]

`NoSnapshotRestoreOnStart=true` skips `FSM.Restore` but still uses the
snapshot metadata (`api.go:691-694`, `config.go:229-233`). Use it only if the
FSM is durable by itself. [code]

Lesson for part 2: right after a restart, local reads are stale. A
"stale-allowed" read on a restarted follower can return data that is older
than its last acknowledged write.

## Q6. FSMSnapshot: concurrency and patterns

Contract (`fsm.go:25-48`, `:71-81`) [code]:

- `Apply` and `Snapshot` run on the same FSM goroutine. `Snapshot` must be
  fast, because no `Apply` runs while it runs.
- `Persist` runs on the snapshot goroutine, **at the same time as `Apply`**.
- raft always calls `Release`, even if it never calls `Persist`.
- raft calls `sink.Cancel()` if `Persist` returns an error
  (`snapshot.go:191-193`) and `sink.Close()` after `Persist` returns nil
  (`snapshot.go:198`). `FileSnapshotSink` makes both idempotent
  (`file_snapshot.go:399-402`, `:452-455`).

Tests [tested]:

| Test | Result |
|---|---|
| `Persist` blocked, 20 writes | All succeeded, slowest 70 µs |
| `Snapshot()` sleeps 300 ms, 1 write | The write took 302 ms |
| `Persist` writes bytes and returns an error, no Cancel | Snapshot fails with `failed to persist snapshot: ...`. The `snapshots/` dir is empty. |

Patterns:

1. **Copy under a read lock** (used in the study FSM):

   ```go
   func (store *Store) Snapshot() (raft.FSMSnapshot, error) {
       store.mutex.RLock()
       defer store.mutex.RUnlock()
       return &snapshot{values: maps.Clone(store.values)}, nil
   }

   func (copy *snapshot) Persist(sink raft.SnapshotSink) error {
       if err := json.NewEncoder(sink).Encode(copy.values); err != nil {
           _ = sink.Cancel()
           return err
       }
       return sink.Close() // return it: a failed fsync must not be lost
   }
   ```

   Cost: O(n) copy time is Apply latency. Fine for a small store.
2. **Immutable tree.** Consul's state store uses `go-memdb` on
   `go-immutable-radix`. `Snapshot()` opens a read transaction, which is O(1)
   ([state_store.go:180-190](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/state/state_store.go#L180),
   [fsm.go:205](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/fsm/fsm.go#L205)).
   [code] Use it if the map grows big.

Do not share a mutable map between the live FSM and the snapshot. Do not do
I/O in `Snapshot()`. Version the snapshot format, like the commands (rule 2):
`SnapshotMeta.Version` covers only raft's metadata, not your data
(`snapshot.go:17-20`). [code]

## Q7. NetworkTransport and TCP

### Signatures

```go
func NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int,
    timeout time.Duration, logOutput io.Writer) (*NetworkTransport, error)
func NewTCPTransportWithLogger(bindAddr string, advertise net.Addr, maxPool int,
    timeout time.Duration, logger hclog.Logger) (*NetworkTransport, error)
func NewTCPTransportWithConfig(bindAddr string, advertise net.Addr,
    config *NetworkTransportConfig) (*NetworkTransport, error)
func NewNetworkTransportWithConfig(config *NetworkTransportConfig) *NetworkTransport
```

`tcp_transport.go:28-65`, `net_transport.go:211-281`. [code]

### Bind and advertise

- `"127.0.0.1:0"` works. `LocalAddr()` returns the real port, for example
  `127.0.0.1:57614`. [tested]
- `"0.0.0.0:0"` without advertise fails with `local bind address is not
  advertisable`. [tested]
- The advertise address must be a `*net.TCPAddr` with a non-unspecified IP
  (`tcp_transport.go:83-91`). **A host name cannot be advertised** through
  `NewTCPTransport`. [code] With advertise `127.0.0.1:7777`, `LocalAddr()`
  returned `127.0.0.1:7777`. [tested]
- `LocalAddr` goes into `RPCHeader.Addr` and is what followers report as the
  leader address (`raft.go:40`, `replication.go:574`). Raft does not compare
  it with the addresses in the configuration. [code] So in Docker you can
  bootstrap with `node1:7000` and advertise the resolved container IP. Map
  the leader ID to an HTTP address in the app. Do not reuse the leader's Raft
  address for HTTP.

### Config fields

```go
type NetworkTransportConfig struct {
    ServerAddressProvider ServerAddressProvider // ID -> address at dial time
    Logger                hclog.Logger
    Stream                StreamLayer
    MaxPool               int           // idle connections kept per target
    MaxRPCsInFlight       int           // pipelining; 0 -> 2, 1 -> off
    Timeout               time.Duration // I/O deadline per RPC and dial
    MsgpackUseNewTimeFormat bool
}
```

`net_transport.go:116-170`. [code]

- **maxPool** limits the idle connections kept per target after an RPC
  (`returnConn`, `net_transport.go:432-444`). It does not limit concurrent
  connections. A new one is dialed when the pool is empty
  (`net_transport.go:403-411`). Consul uses 3
  ([server.go:964](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L964)).
  [code]
- **timeout** is used three ways [code]:
  - dial timeout (`net_transport.go:410`)
  - one deadline for send plus response of each RPC
    (`net_transport.go:489-491`)
  - per-request write deadline and per-response read deadline in the pipeline
    (`net_transport.go:843-845`, `:871-873`)

  Test: a server that never answers. The RPC failed after 301 ms with a
  300 ms timeout: `read tcp ...: i/o timeout`. [tested] Consul uses 10 s
  ([server.go:965](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L965)).
  The follower's fsync is inside this deadline. With 0 there is no deadline
  at all.
- **Pipelining.** `MaxRPCsInFlight` default 2. `1` makes
  `AppendEntriesPipeline` return `ErrPipelineReplicationNotSupported`, and
  every AppendEntries is synchronous (`net_transport.go:448-453`). The comment
  says values above 2 help only on high-latency networks
  (`net_transport.go:128-159`). [code]
- **Heartbeat fast path.** An AppendEntries with a term, a leader address,
  `PrevLogEntry == 0`, `PrevLogTerm == 0`, no entries and
  `LeaderCommitIndex == 0` is a heartbeat (`net_transport.go:673-677`). It goes
  straight to the handler from `SetHeartbeatHandler` and skips `Consumer()`
  (`net_transport.go:722-730`). Raft sets the handler in `NewRaft`
  (`api.go:630`). Test: 1 heartbeat went to the handler, 1 normal
  AppendEntries went to the consumer. [tested] Heartbeats never carry the
  commit index. The commit index travels in the replication RPCs, sent at
  least every `CommitTimeout` (`replication.go:164-171`). [code]
- **ServerAddressProvider** maps a server ID to an address for each new
  connection (`net_transport.go:383-400`). If it fails, the transport logs a
  warning and uses the configured address. Consul uses it with protocol
  version 3 ([server.go:955-958](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/server.go#L955)).
  [code]

### StreamLayer

```go
type StreamLayer interface {
    net.Listener // Accept, Close, Addr
    Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}
```

`net_transport.go:179-184`. `Addr()` becomes `LocalAddr()`. With your own
StreamLayer and `NewNetworkTransportWithConfig` there is no TCPAddr check, so
`Addr()` may return a host-name address. [code]

### DNS

`TCPStreamLayer.Dial` calls `net.DialTimeout("tcp", address, timeout)` with
the raw address string (`tcp_transport.go:99-101`). So DNS is resolved on
each **new** connection. A pooled connection keeps the old IP until it
fails. [code] An RPC to `localhost:<port>` worked. [tested]

### Close and restart on the same port

- `Close` closes the shutdown channel and the listener only
  (`net_transport.go:333-343`). Inbound handlers return
  `ErrTransportShutdown` on their next RPC. Pooled outbound connections are
  not closed. `CloseStreams` closes the pool and cancels the inbound handlers
  (`net_transport.go:308-330`). [code]
- `Raft.Shutdown().Error()` closes the transport, because NetworkTransport
  implements `WithClose` (`future.go:170-178`). It does this inside
  `Error()`. If you call `Shutdown()` and never call `Error()`, the
  transport stays open. [code]
- A new transport on the same port right after `Close` works. The peer's
  first RPC on its stale pooled connection fails with `EOF`. The next RPC
  dials again and succeeds. [tested] Raft retries, so this is harmless. All
  restart tests in this study reused the old port. [tested]

### Replication backoff

After a failed replication RPC, raft waits `backoff(10ms, failures, 12)`:
10 ms, 10 ms, 20 ms, 40 ms … up to 10.24 s (`replication.go:17-18`,
`:211-216`, `util.go:129-136`). Heartbeats back off up to
`HeartbeatTimeout/2` (`replication.go:413`). [code] So after a long
partition, a follower can wait up to about 10 s for the next replication
attempt. Do not assert "caught up within 1 s after heal" in tests.

## Q8. TLS: a mutual-TLS StreamLayer

Raft RPCs carry no authentication. Anybody who reaches the port can send
AppendEntries. Use mutual TLS outside a trusted network.

The study StreamLayer (full code in the appendix) [tested]:

- `Listen`: `tls.NewListener(net.Listen("tcp", bind), serverConfig)` with
  `ClientAuth: tls.RequireAndVerifyClientCert`, `ClientCAs: pool`,
  `MinVersion: tls.VersionTLS13`.
- `Accept`: returns the `*tls.Conn`. The handshake runs on the first `Read`,
  in NetworkTransport's per-connection goroutine, so it does not block the
  accept loop.
- `Dial`: `tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr,
  clientConfig)`. The Go docs say the dialer timeout covers connect plus
  handshake. `ServerName` is the host part of the Raft address, so each node
  certificate needs a SAN for its name or IP.
- Result: 3 nodes elected a leader and replicated a write over mTLS. A TLS
  client without a certificate got `remote error: tls: certificate
  required`. A plain TCP transport got `EOF`. [tested]

Real examples [code]:

- **Vault**: Raft runs on the cluster port, chosen by ALPN
  `raft_storage_v1`, with `RequireAndVerifyClientCert` and self-made
  certificates in a keyring.
  [streamlayer.go:381-384 (Dial)](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/physical/raft/streamlayer.go#L381),
  [streamlayer.go:329-361 (Handoff, Accept)](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/physical/raft/streamlayer.go#L329),
  [cluster.go:233](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/vault/cluster/cluster.go#L233),
  [cluster.go:451-491 (GetDialerFunc)](https://github.com/hashicorp/vault/blob/352089582444ea84feb03bc97eda97b35729e8ec/vault/cluster/cluster.go#L451).
- **Nomad**: `RaftLayer` dials TCP, writes the `RpcTLS` byte, wraps the conn
  in TLS, then writes the `RpcRaft` byte. The server side reads the first
  byte and hands the conn to the layer.
  [raft_rpc.go:107-136](https://github.com/hashicorp/nomad/blob/7399e1f13154e5a643c1c24432819092006ceb14/nomad/raft_rpc.go#L107),
  [rpc.go:323-329](https://github.com/hashicorp/nomad/blob/7399e1f13154e5a643c1c24432819092006ceb14/nomad/rpc.go#L323).
- **Consul**: the same first-byte scheme. `RPCRaft = 1`, `RPCTLS = 3`. The
  server checks the peer certificate (`AuthorizeServerConn`) before
  `Handoff`.
  [raft_rpc.go:98-126](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/raft_rpc.go#L98),
  [pool/conn.go:35-41](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/pool/conn.go#L35),
  [rpc.go:519-531](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/consul/rpc.go#L519).

Note: Nomad and Consul write the mode byte inside `Dial`, after the TLS wrap.
So the handshake runs inside `Dial`, and that code sets no deadline for it.
[code] [not verified: whether their TLS wrapper sets one]
`tls.DialWithDialer` avoids that question.

## Q9. InmemTransport and raft's test helpers

### InmemTransport

| Method | Effect | Source |
|---|---|---|
| `Connect(peer, t)` | Adds a route **from this transport to** `peer`. | `inmem_transport.go:214-219` |
| `Disconnect(peer)` | Removes that one route and closes pipelines to `peer`. | `inmem_transport.go:222-238` |
| `DisconnectAll()` | Removes all routes from this transport. | `inmem_transport.go:241-251` |
| `Close()` | Same as `DisconnectAll`. Others can still send to it. | `inmem_transport.go:254-257` |

[code]

- **A partition is one-way per call.** After `A.Disconnect(B)`, A → B failed
  with `failed to connect to peer`. B → A worked and B got A's response.
  [tested]
- **Responses still flow after `Disconnect`.** A got a request from B, then
  called `Disconnect(B)` and `DisconnectAll()`, then answered. B got the
  answer. [tested] The response goes back on a channel inside the RPC, not
  through a route (`inmem_transport.go:176-199`). [code] So InmemTransport
  cannot model "request delivered, response lost".
- No heartbeat fast path: `SetHeartbeatHandler` is empty
  (`inmem_transport.go:74-75`). [code]
- Default timeout 500 ms for enqueue and for the response. InstallSnapshot and
  TimeoutNow use 10 × (`inmem_transport.go:69`, `:142`, `:155`). The consumer
  channel buffers 16 RPCs (`inmem_transport.go:58`). [code]
- Routes use the address, not the server ID.

### The cluster helpers in `testing.go`

`testing.go` is a normal file of package `raft` (not `_test.go`), so it is
compiled into every program that imports raft. [code]

- Exported: `MakeCluster`, `MakeClusterNoBootstrap`, `MakeClusterCustom`,
  `MakeClusterCustomWithErr`, `MakeClusterOpts`, `MockFSM`, `MockSnapshot`,
  `MockFSMConfigStore`, `MockMonotonicLogStore`, `FileSnapTest`
  (`testing.go:38-907`). Each has the comment "exposed for middleware testing
  purposes and is not a stable API" (`testing.go:854`). [code]
- `MakeCluster` returns the unexported `*raft.cluster`. From another module
  you can call it and use its methods (`Leader`, `Followers`, `Partition`,
  `Disconnect`, `FullyConnect`, `WaitForReplication`, `EnsureSame`,
  `GetInState`, `Close`) with `cluster := raft.MakeCluster(...)`. You cannot
  name the type. [tested]
- It always uses `InmemStore`, `InmemTransport` and a `FileSnapshotStore`
  with fsync off (`testing.go:735-853`, `:906`). `MakeFSMFunc` lets you plug
  in your FSM. It takes `*testing.T`, not `testing.TB`. [code]
- `Followers()` waits for N-1 followers and fails the test otherwise. After
  `Partition` of the leader it failed with `timeout waiting for 2
  followers`. [tested]

Verdict: good for quick protocol tests. It cannot test your real stores, your
transport, restarts with disk state, or one-way faults. Part 2 needs its own
small harness (see `testkit` in the appendix list).

## Q10. Fault injection with a transport wrapper

### What the wrapper must implement

| Interface or method | Why | How raft finds it |
|---|---|---|
| `raft.Transport` (all 10 methods) | required | `NewRaft` parameter |
| `raft.WithClose` | `Shutdown().Error()` closes the transport only through it. Without it the inner listener stays open and the port stays bound. | type assertion, `future.go:176` |
| `raft.WithPreVote` | Without it raft sets `preVoteDisabled` and logs `pre-vote is disabled because it is not supported by the Transport`. | type assertion, `api.go:554`, `:584`, `:588-590` |
| `SetHeartbeatHandler` (part of Transport) | NetworkTransport calls the handler directly for heartbeats. Wrap the callback, or heartbeats ignore your filter. | `api.go:630`, `net_transport.go:722-730` |
| `AppendEntriesPipeline` | Return the inner `ErrPipelineReplicationNotSupported` **unwrapped**. Raft compares with `!=` and logs an error for any other value. | `replication.go:189` |
| `raft.WithPeers` | Not needed. Raft core never asserts it. Only `LoopbackTransport` (test helpers) uses it. | `transport.go:93-107` |

[code] The wrapper in the appendix asserts the first three at compile time.

Three more traps [code]:

- Raft reads `trans.Consumer()` **once** in `NewRaft` (`api.go:569`). Create
  the wrapper's channel and its forwarding goroutine before `NewRaft`.
- `pipelineDecode` never calls `future.Error()`. It reads `Response()` only
  (`replication.go:544-559`). A "lost" pipelined response must return a
  zero `AppendEntriesResponse` (`Success=false`), or raft counts it as a
  success. Note: raft calls `setLastContact()` before it checks `Success`
  (`replication.go:554`), so even a failed pipelined response refreshes
  "last contact". The same happens with a real decode error.
- A pre-vote error whose text contains `unexpected command` counts as a
  **granted** vote (`raft.go:2105-2114`, constant at `raft.go:24`). Never put
  that text in an injected error.

### Who sent an RPC

Every request embeds `RPCHeader{ProtocolVersion, ID, Addr}`. `ID` is the
sender's `ServerID`, `Addr` its encoded address (`commands.go:10-18`, set in
`raft.go:36-42`). All requests implement `raft.WithRPCHeader`
(`commands.go:21-23`). [code]

```go
header := rpc.Command.(raft.WithRPCHeader).GetRPCHeader()
senderID := raft.ServerID(header.ID)
```

The outgoing methods get the target ID as the first argument, for example
`AppendEntries(id ServerID, target ServerAddress, ...)` (`transport.go:41-66`).
[code]

### Dropping incoming RPCs

Read the inner `Consumer()` in a goroutine. Forward allowed RPCs to the
wrapper's own channel. For a dropped RPC, **answer it with an error**:
`rpc.Respond(nil, err)`. If you never answer, the sender waits until its I/O
deadline. The inner transport also keeps a goroutine blocked on the response
channel until shutdown (`net_transport.go:740-762`). For InstallSnapshot,
drain `rpc.Reader` first. Apply the same filter inside the wrapped heartbeat
handler. [code] The study wrapper does all of this. [tested]

### Request delivered, response lost

Model the network as a set of blocked one-way links. For an outgoing RPC
from `local` to `peer`:

1. If `local → peer` is blocked: fail at once. The request is lost.
2. Else call the inner transport. The peer processes the request.
3. If `peer → local` is blocked: throw the response away, zero it, and return
   an error. The response is lost.

For the pipeline, check step 1 in `AppendEntries` and step 3 in a goroutine
that copies the inner `Consumer()` futures into the wrapper's channel.

### Tests with the wrapper (3 nodes, real TCP, BoltDB) [tested]

| Test | Result |
|---|---|
| Two-way partition of the leader | New leader on the majority side. Old leader stepped down after 190 ms (`LeaderLeaseTimeout` 100 ms) and returned `node is not the leader`. After heal, all FSMs equal. |
| Followers' responses lost | `Apply` on the old leader: `leadership lost while committing log`. New leader elected among the followers. The key is there. After heal, also on the old leader. 5 of 5 runs with `-race`. |
| One-way cut leader → follower | The cut follower lost the leader and ran pre-vote. The term stayed 2. The leader stayed leader. 20 writes succeeded. |

All three passed 10 of 10 runs with `-race`. [tested]

A flake found on the way, now fixed: with a 100 ms lease the "lost
responses" test failed once under load. The replication backoff (Q7) delayed
the entry past the lease, so the write was really lost. Both outcomes are
legal for the same error. The fixed test uses a 1 s lease and waits until
both followers stored the entry. [tested]

Another flake: `BootstrapCluster` on the third node returned `bootstrap only
works on new clusters`. The first two nodes had already elected a leader that
replicated to it. When you bootstrap every node with the same configuration,
treat `raft.ErrCantBootstrap` as success. [tested]

## Q11. Metrics

- raft, raft-boltdb and the transport call package functions of
  `github.com/hashicorp/go-metrics` (`metrics.IncrCounter`,
  `metrics.MeasureSince` …). They write to one process-wide global
  (`start.go:47-58`). The default global is a `BlackholeSink`, so metrics
  cost little until you set one (`start.go:49-52`). [code]
- raft v1.8.0 and raft-boltdb v2.4.x import `hashicorp/go-metrics` directly.
  No build tags are needed (raft v1.8.0 release notes, raft-boltdb v2.4.2
  release notes). The raft-boltdb README still describes the old build tags.
  It is out of date. [code]
- Prometheus: package `github.com/hashicorp/go-metrics/prometheus`. [code]

  ```go
  registry := prometheus.NewRegistry()
  sink, err := metricsprometheus.NewPrometheusSinkFrom(metricsprometheus.PrometheusOpts{
      Expiration: time.Minute, // DefaultPrometheusOpts uses 60s
      Registerer: registry,
  })
  config := metrics.DefaultConfig("kvstore")
  config.EnableHostname = false
  _, err = metrics.NewGlobal(config, sink)
  http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
  ```

  `NewPrometheusSinkFrom` registers the sink with the registerer
  (`prometheus/prometheus.go:116-145`). The default registerer accepts it only
  once: the second sink failed with `duplicate metrics collector
  registration attempted` (`AlreadyRegisteredError`). A separate registry per
  sink worked. [tested]
- Consul: `lib/telemetry.go` builds a fanout sink (statsd, dogstatsd,
  circonus, Prometheus, plus an in-memory sink) and calls `NewGlobal`
  ([telemetry.go:348-380](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/lib/telemetry.go#L348)).
  It ignores `AlreadyRegisteredError` for tests
  ([telemetry.go:286-300](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/lib/telemetry.go#L286)).
  `/v1/agent/metrics?format=prometheus` serves
  `promhttp.HandlerFor(prometheus.DefaultGatherer, ...)`
  ([agent_endpoint.go:215](https://github.com/hashicorp/consul/blob/da3684fffeddb81dfaa0e648b51193c866f51ce8/agent/agent_endpoint.go#L215)).
  [code]
- **Many nodes in one test process share the global.** Three nodes with one
  in-memory sink gave one series `raft.state.follower = 3` with no labels.
  [tested] Most raft keys have no node label (`raft.go:163`, `:289`, `:471`).
  [code] Correctness is not affected. Only per-node assertions are. In
  production there is one node per process, so this does not matter.
- `Config.NoLegacyTelemetry=true` drops the old duplicate metrics that put the
  peer ID inside the metric name (`config.go:238-241`). [code]

## Q12. Logging with hclog

- `raft.Config.Logger` takes an `hclog.Logger`. If it is nil, raft makes one
  from `LogOutput` (default stderr) and `LogLevel` (`config.go:217-227`,
  `:260-271`). `DefaultConfig` sets `LogLevel: "DEBUG"`
  (`config.go:341`), so without your own logger raft logs at DEBUG. [code]
- Other components take their own logger [code]:
  - transport: `NewTCPTransportWithLogger` or `NetworkTransportConfig.Logger`
  - snapshots: `NewFileSnapshotStoreWithLogger`
  - raft-wal: `WithLogger` (default `hclog.Default().Named("wal")`,
    `options.go:70-72`)
  - raft-boltdb: no hclog. bbolt has its own `Options.Logger` interface.
    Issue #46 asks for hclog support.
- Setup for part 2:

  ```go
  logger := hclog.New(&hclog.LoggerOptions{
      Name:       "kvstore",
      Level:      hclog.LevelFromString(os.Getenv("LOG_LEVEL")), // "" -> NoLevel
      JSONFormat: true,
      Output:     os.Stderr,
  }).With("node_id", nodeID)
  config.Logger = logger.Named("raft")
  ```

  `LevelFromString("")` returns `NoLevel`. Default to `"INFO"` yourself.
- What each level shows in raft [code]. The core files have 0 Trace, 27
  Debug, 31 Info, 34 Warn and 63 Error calls (`api.go`, `raft.go`,
  `replication.go`, `net_transport.go`, snapshot and FSM files).
  - **INFO**: `entering follower state`, `entering candidate state`,
    `election won`, `entering leader state`, `pipelining replication`,
    `starting snapshot up to`, `compacting logs`, `installing snapshot on`,
    `Installed remote snapshot`, `initial configuration`. Good default.
  - **WARN**: `heartbeat timeout reached, starting election`,
    `failed to contact quorum of nodes, stepping down`,
    `rejecting vote request since we have a leader`, `previous log term
    mis-match`. Alert on the step-down line.
  - **ERROR**: failed RPCs during partitions (`failed to heartbeat to`,
    `failed to appendEntries to`), and `failed to get log` before a snapshot
    send (normal, see Q5).
  - **DEBUG**: votes and pre-votes (`asking for vote`, `vote granted`),
    `accepted connection`. Use it in tests. The study harness logs DEBUG
    into a per-node buffer and greps it.

---

## What this means for part 2

1. Pin Go 1.25, raft v1.8.0, raft-boltdb/v2 v2.4.1. Add a CI check that
   builds with `GOTOOLCHAIN=local`, so a dependency bump to a Go 1.26 module
   fails loudly.
2. Store setup:

   ```go
   boltStore, err := raftboltdb.New(raftboltdb.Options{
       Path:        filepath.Join(dataDir, "raft.db"),
       BoltOptions: &bbolt.Options{Timeout: time.Second},
       // NoSync stays false: fsync before acknowledge (rule 7).
   })
   logStore, err := raft.NewLogCache(512, boltStore)
   snapshots, err := raft.NewFileSnapshotStoreWithLogger(dataDir, 2, logger.Named("snapshot"))
   transport, err := raft.NewTCPTransportWithConfig(bindAddress, advertise, &raft.NetworkTransportConfig{
       MaxPool: 3,
       Timeout: 10 * time.Second, // tests: 2s
       Logger:  logger.Named("raft-net"),
   })
   node, err := raft.NewRaft(config, fsm, logStore, boltStore, snapshots, transport)
   // Stop: node.Shutdown().Error(), then boltStore.Close().
   ```

3. Docker: bind `0.0.0.0:7000`, advertise `net.ResolveTCPAddr("tcp",
   "nodeN:7000")`, bootstrap with the `nodeN:7000` names. Keep a map from
   server ID to HTTP address for leader forwarding.
4. Tests: wrap NetworkTransport with the fault wrapper, not InmemTransport.
   Use free ports (`127.0.0.1:0`) and reuse the port on restart. Tolerate
   `ErrCantBootstrap`. Allow about 10 s for catch-up after a long partition.
5. Failure-scenario tests worth porting: lost responses (rule 3), catch-up
   through InstallSnapshot, restart staleness, corrupt newest snapshot.
6. The API must report `Apply` errors such as `ErrLeadershipLost` as
   "outcome unknown" (HTTP 504). The lost-response test proves why.
7. Expose `/metrics` with a Prometheus sink on its own registry. Run
   `boltStore.RunMetrics`. Watch `raft.boltdb.freelistBytes`.

## Appendix A: study tests and programs

All paths are under
`/private/tmp/claude-501/-Users-william-workspace-simple-kv-store/2ee5234f-11ad-496c-90b3-250d84527b4f/scratchpad/`.
Run with `GOTOOLCHAIN=local go test -race ./...` in `hraft-storage/`. The last
full run passed all packages. [tested]

| File | Tests | Questions |
|---|---|---|
| `hraft-storage/boltstore_test.go` | second open blocks, NoSync settings, file does not shrink, use after Close | Q1 |
| `hraft-storage/cmd/fsynccount/main.go` | raft-boltdb fsync counter for strace | Q1 |
| `hraft-wal126/main.go` | raft-wal fsync counter for strace (Go 1.26 module) | Q2 |
| `fsyncbin/count.sh` | runs both counters under strace in a Debian container | Q1, Q2 |
| `hraft-go125-buildcheck/` | Go 1.25 build check of raft v1.8.0 + raft-boltdb v2.4.1, the raft-wal v0.4.2 failure, and raft-wal v0.3.0 to v0.4.1 builds | Q1, Q2 |
| `hraft-storage/snapshot_test.go` | catch-up through InstallSnapshot, restart staleness, RestoreCommittedLogs, corrupt snapshot fallback, panic with compacted logs (child process) | Q4, Q5 |
| `hraft-storage/fsmsnapshot_test.go` | Apply during Persist, slow Snapshot blocks Apply, Persist error cancels the sink | Q6 |
| `hraft-storage/transport_test.go` | bind and advertise, restart on same port, timeout, heartbeat fast path, host-name dial | Q7 |
| `hraft-storage/tlslayer/` | mutual-TLS StreamLayer and a 3-node test | Q8 |
| `hraft-storage/inmem_test.go` | one-way Disconnect, response after Disconnect, MakeCluster from another module | Q9 |
| `hraft-storage/faultnet/` | the fault wrapper and 3 partition tests | Q10 |
| `hraft-storage/metrics_test.go` | Prometheus double registration, shared global sink | Q11 |
| `hraft-storage/testkit/testkit.go` | harness: real BoltDB, file snapshots, TCP, optional wrapper | all |
| `hraft-storage/kvfsm/kvfsm.go` | small KV FSM with apply, restore and snapshot counters | all |

## Appendix B: the fault-injecting transport wrapper

`faultnet/faultnet.go`, tested with raft v1.8.0 and NetworkTransport.

```go
package faultnet

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// The text must not contain "unexpected command" (raft.go:2107).
var ErrLinkBlocked = errors.New("faultnet: link blocked")

type link struct{ from, to raft.ServerID }

// Network is the shared fault state for all wrapped transports in one test.
type Network struct {
	mutex        sync.RWMutex
	blockedLinks map[link]bool
}

func NewNetwork() *Network { return &Network{blockedLinks: map[link]bool{}} }

// Block drops all traffic from -> to. Traffic to -> from still flows.
func (network *Network) Block(from, to raft.ServerID) {
	network.mutex.Lock()
	defer network.mutex.Unlock()
	network.blockedLinks[link{from: from, to: to}] = true
}

// Partition blocks both directions between the two groups.
func (network *Network) Partition(groupA, groupB []raft.ServerID) {
	for _, nodeA := range groupA {
		for _, nodeB := range groupB {
			network.Block(nodeA, nodeB)
			network.Block(nodeB, nodeA)
		}
	}
}

func (network *Network) HealAll() {
	network.mutex.Lock()
	defer network.mutex.Unlock()
	network.blockedLinks = map[link]bool{}
}

func (network *Network) IsBlocked(from, to raft.ServerID) bool {
	network.mutex.RLock()
	defer network.mutex.RUnlock()
	return network.blockedLinks[link{from: from, to: to}]
}

// InnerTransport: NetworkTransport and InmemTransport both satisfy it.
type InnerTransport interface {
	raft.Transport
	raft.WithPreVote
}

type Transport struct {
	inner           InnerTransport
	localID         raft.ServerID
	network         *Network
	consumerChannel chan raft.RPC
	stopChannel     chan struct{}
	closeOnce       sync.Once

	DroppedRequests  atomic.Int64
	DroppedResponses atomic.Int64
	RejectedIncoming atomic.Int64
}

var (
	_ raft.Transport   = (*Transport)(nil)
	_ raft.WithClose   = (*Transport)(nil) // future.go:176
	_ raft.WithPreVote = (*Transport)(nil) // api.go:554
)

// Wrap must run before raft.NewRaft, which reads Consumer() once (api.go:569).
func Wrap(inner InnerTransport, localID raft.ServerID, network *Network) *Transport {
	transport := &Transport{
		inner:           inner,
		localID:         localID,
		network:         network,
		consumerChannel: make(chan raft.RPC),
		stopChannel:     make(chan struct{}),
	}
	go transport.forwardIncoming()
	return transport
}

func (transport *Transport) forwardIncoming() {
	innerConsumer := transport.inner.Consumer()
	for {
		select {
		case rpc := <-innerConsumer:
			if transport.rejectIncoming(rpc) {
				continue
			}
			select {
			case transport.consumerChannel <- rpc:
			case <-transport.stopChannel:
				return
			}
		case <-transport.stopChannel:
			return
		}
	}
}

// rejectIncoming must answer the RPC. Without an answer the sender waits
// for its deadline and the inner transport keeps a goroutine blocked.
func (transport *Transport) rejectIncoming(rpc raft.RPC) bool {
	headerCarrier, hasHeader := rpc.Command.(raft.WithRPCHeader)
	if !hasHeader {
		return false
	}
	senderID := raft.ServerID(headerCarrier.GetRPCHeader().ID)
	if !transport.network.IsBlocked(senderID, transport.localID) {
		return false
	}
	transport.RejectedIncoming.Add(1)
	if rpc.Reader != nil {
		_, _ = io.Copy(io.Discard, rpc.Reader)
	}
	rpc.Respond(nil, fmt.Errorf("%w: %s -> %s (incoming)", ErrLinkBlocked, senderID, transport.localID))
	return true
}

func (transport *Transport) send(peerID raft.ServerID, resetResponse func(), call func() error) error {
	if transport.network.IsBlocked(transport.localID, peerID) {
		transport.DroppedRequests.Add(1)
		return fmt.Errorf("%w: %s -> %s (request)", ErrLinkBlocked, transport.localID, peerID)
	}
	err := call()
	if transport.network.IsBlocked(peerID, transport.localID) {
		transport.DroppedResponses.Add(1)
		resetResponse()
		return fmt.Errorf("%w: %s -> %s (response, request was delivered)", ErrLinkBlocked, peerID, transport.localID)
	}
	return err
}

func (transport *Transport) Consumer() <-chan raft.RPC     { return transport.consumerChannel }
func (transport *Transport) LocalAddr() raft.ServerAddress { return transport.inner.LocalAddr() }
func (transport *Transport) DecodePeer(b []byte) raft.ServerAddress {
	return transport.inner.DecodePeer(b)
}
func (transport *Transport) EncodePeer(id raft.ServerID, address raft.ServerAddress) []byte {
	return transport.inner.EncodePeer(id, address)
}

// NetworkTransport skips Consumer() for heartbeats (net_transport.go:722).
func (transport *Transport) SetHeartbeatHandler(handler func(rpc raft.RPC)) {
	if handler == nil {
		transport.inner.SetHeartbeatHandler(nil)
		return
	}
	transport.inner.SetHeartbeatHandler(func(rpc raft.RPC) {
		if transport.rejectIncoming(rpc) {
			return
		}
		handler(rpc)
	})
}

func (transport *Transport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return transport.send(id, func() { *resp = raft.AppendEntriesResponse{} }, func() error {
		return transport.inner.AppendEntries(id, target, args, resp)
	})
}

func (transport *Transport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return transport.send(id, func() { *resp = raft.RequestVoteResponse{} }, func() error {
		return transport.inner.RequestVote(id, target, args, resp)
	})
}

func (transport *Transport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	return transport.send(id, func() { *resp = raft.RequestPreVoteResponse{} }, func() error {
		return transport.inner.RequestPreVote(id, target, args, resp)
	})
}

func (transport *Transport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return transport.send(id, func() { *resp = raft.InstallSnapshotResponse{} }, func() error {
		return transport.inner.InstallSnapshot(id, target, args, resp, data)
	})
}

func (transport *Transport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return transport.send(id, func() { *resp = raft.TimeoutNowResponse{} }, func() error {
		return transport.inner.TimeoutNow(id, target, args, resp)
	})
}

// Return the inner error unchanged: raft compares it with != (replication.go:189).
func (transport *Transport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if transport.network.IsBlocked(transport.localID, id) {
		transport.DroppedRequests.Add(1)
		return nil, fmt.Errorf("%w: %s -> %s (pipeline)", ErrLinkBlocked, transport.localID, id)
	}
	innerPipeline, err := transport.inner.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return newPipeline(transport, id, innerPipeline), nil
}

func (transport *Transport) Close() error {
	var closeErr error
	transport.closeOnce.Do(func() {
		close(transport.stopChannel)
		if closer, ok := transport.inner.(raft.WithClose); ok {
			closeErr = closer.Close()
		}
	})
	return closeErr
}

type pipeline struct {
	transport   *Transport
	peerID      raft.ServerID
	inner       raft.AppendPipeline
	doneChannel chan raft.AppendFuture
	stopChannel chan struct{}
	closeOnce   sync.Once
}

func newPipeline(transport *Transport, peerID raft.ServerID, inner raft.AppendPipeline) *pipeline {
	wrapped := &pipeline{
		transport:   transport,
		peerID:      peerID,
		inner:       inner,
		doneChannel: make(chan raft.AppendFuture),
		stopChannel: make(chan struct{}),
	}
	go wrapped.forwardResponses()
	return wrapped
}

func (wrapped *pipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	if wrapped.transport.network.IsBlocked(wrapped.transport.localID, wrapped.peerID) {
		wrapped.transport.DroppedRequests.Add(1)
		return nil, fmt.Errorf("%w: %s -> %s (pipeline request)", ErrLinkBlocked, wrapped.transport.localID, wrapped.peerID)
	}
	return wrapped.inner.AppendEntries(args, resp)
}

// pipelineDecode reads Response() and never Error() (replication.go:544),
// so a lost response must look like Success=false.
func (wrapped *pipeline) forwardResponses() {
	innerConsumer := wrapped.inner.Consumer()
	for {
		select {
		case future := <-innerConsumer:
			if wrapped.transport.network.IsBlocked(wrapped.peerID, wrapped.transport.localID) {
				wrapped.transport.DroppedResponses.Add(1)
				future = lostResponseFuture{AppendFuture: future}
			}
			select {
			case wrapped.doneChannel <- future:
			case <-wrapped.stopChannel:
				return
			}
		case <-wrapped.stopChannel:
			return
		}
	}
}

func (wrapped *pipeline) Consumer() <-chan raft.AppendFuture { return wrapped.doneChannel }

func (wrapped *pipeline) Close() error {
	var closeErr error
	wrapped.closeOnce.Do(func() {
		close(wrapped.stopChannel)
		closeErr = wrapped.inner.Close()
	})
	return closeErr
}

type lostResponseFuture struct{ raft.AppendFuture }

func (lostResponseFuture) Error() error { return ErrLinkBlocked }
func (lostResponseFuture) Response() *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{}
}
```

Use in a test:

```go
network := faultnet.NewNetwork()
tcpTransport, _ := raft.NewTCPTransportWithLogger("127.0.0.1:0", nil, 3, 2*time.Second, logger)
transport := faultnet.Wrap(tcpTransport, "node1", network)
node, _ := raft.NewRaft(config, fsm, logStore, boltStore, snapshots, transport)

network.Partition([]raft.ServerID{"node1"}, []raft.ServerID{"node2", "node3"}) // two-way
network.Block("node2", "node1") // one-way: node2's requests and responses to node1 are lost
network.HealAll()
```

## Appendix C: the mutual-TLS StreamLayer

`tlslayer/tlslayer.go`, tested with 3 nodes.

```go
package tlslayer

import (
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type StreamLayer struct {
	listener     net.Listener
	advertise    net.Addr
	clientConfig *tls.Config
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

func Listen(bindAddress string, advertise net.Addr, serverConfig, clientConfig *tls.Config) (*StreamLayer, error) {
	if serverConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, errors.New("tlslayer: server config must require and verify client certificates")
	}
	tcpListener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}
	return &StreamLayer{
		listener:     tls.NewListener(tcpListener, serverConfig),
		advertise:    advertise,
		clientConfig: clientConfig,
	}, nil
}

// The handshake runs on the first Read, in NetworkTransport's connection goroutine.
func (layer *StreamLayer) Accept() (net.Conn, error) { return layer.listener.Accept() }
func (layer *StreamLayer) Close() error              { return layer.listener.Close() }

func (layer *StreamLayer) Addr() net.Addr {
	if layer.advertise != nil {
		return layer.advertise
	}
	return layer.listener.Addr()
}

// The dialer timeout covers TCP connect and TLS handshake together.
func (layer *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, err
	}
	config := layer.clientConfig.Clone()
	if config.ServerName == "" {
		config.ServerName = host
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", string(address), config)
}
```

Configs used in the test:

```go
serverConfig := &tls.Config{
	Certificates: []tls.Certificate{nodeCertificate}, // ExtKeyUsage server + client
	ClientCAs:    caPool,
	ClientAuth:   tls.RequireAndVerifyClientCert,
	MinVersion:   tls.VersionTLS13,
}
clientConfig := &tls.Config{
	Certificates: []tls.Certificate{nodeCertificate},
	RootCAs:      caPool,
	MinVersion:   tls.VersionTLS13,
}
layer, _ := tlslayer.Listen("127.0.0.1:0", nil, serverConfig, clientConfig)
transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
	Stream: layer, MaxPool: 3, Timeout: 2 * time.Second, Logger: logger,
})
```
