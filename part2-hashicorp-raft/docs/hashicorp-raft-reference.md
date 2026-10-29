# hashicorp/raft reference (version 1.8.0)

This file is a checked reference for the Go library
[github.com/hashicorp/raft](https://github.com/hashicorp/raft). It answers 15
questions that matter when you build a 3-node key-value store with
hashicorp/raft, an HTTP API, Docker and Go tests with fault injection.

## How this was checked

- Library version: **v1.8.0**, tag commit `0f543c0`. The `main` branch is 2
  commits ahead (`871f108`, `a5b4692`). Those commits change only license
  headers and CI. [tested: `gh api .../compare/v1.8.0...main`]
- Go: **go1.25.5 darwin/arm64**.
- Date of the checks: 2026-10-29.
- Each fact has a tag:
  - **[tested]**: a Go test ran it and showed the result.
  - **[code]**: read in the library source. The file and line are given.
  - **[not verified]**: the fact was not checked. Do not trust it without a test.
- Line numbers refer to the module source
  `$(go env GOMODCACHE)/github.com/hashicorp/raft@v1.8.0/`.
- The study tests are in the scratchpad module of the research session:
  `/private/tmp/claude-501/-Users-william-workspace-simple-kv-store/2ee5234f-11ad-496c-90b3-250d84527b4f/scratchpad/hraft-core/`.
  That folder is temporary. The important code is copied into this file.
- Most tests use `InmemTransport` and `InmemStore` in one process. The timing
  and restart tests use the real `NetworkTransport` over TCP on `127.0.0.1`,
  but with in-memory stores. **No test used a real disk.** Latency numbers do
  not include fsync.
- All tests passed 3 times in a row (`go test -count=3`) and once with
  `-race`.

## The 15 most important facts

1. **`Apply` timeout limits only the enqueue step.** It does not limit commit
   or `FSM.Apply`. A 10 ms timeout with a 300 ms `FSM.Apply` returned success
   after 300 ms. [tested] `ErrEnqueueTimeout` means that no log holds the entry.
   So a retry is safe. [tested]
2. **`future.Error()` has no deadline and can block forever.** It blocked for
   more than 3 s while the leader kept its lease and the follower disks failed.
   It returned `nil` 2.1 s after the disks recovered. [tested] After
   `Shutdown()`, 1 of 5 committed futures was still blocked after 2 s.
   [tested] Put your own deadline around `Error()` and report 504.
3. **`ErrLeadershipLost` does not mean "not written".** The leader sent the
   entry, lost the responses, stepped down after 50 ms and returned
   `ErrLeadershipLost`. The entry then committed on all 3 nodes. [tested]
4. **`ErrRaftShutdown` is also ambiguous.** It can come before the enqueue
   (not written) or for an entry that is already committed (`raft.go:1304-1313`).
   [code]
5. **`Error()` returns after `FSM.Apply` ran on the leader.** `Response()`
   holds the return value of `FSM.Apply`. An error value from `FSM.Apply` goes to
   `Response()`, not to `Error()`, and the cluster continues. [tested]
6. **A panic in `FSM.Apply` kills the whole process.** The library does not
   recover it. The test child process exited with code 2. [tested] The entry
   stays in the log, so a restart applies it again. [code]
7. **There is no ReadIndex and no lease-read API.** `VerifyLeader()` costs one
   heartbeat round to a quorum and no log entry. `Barrier()` costs one log entry
   per call. [tested] The leader "lease" is not a read lease (`raft.go:1016-1023`).
   [code]
8. **Pre-vote is on by default (since v1.7.0), but only if the transport
   implements `raft.WithPreVote`.** A wrapper that embeds the `raft.Transport`
   interface hides `RequestPreVote`. The node then runs without pre-vote and
   logs only one warning. [tested]
9. **Pre-vote stops the disruptive rejoin. Leader stickiness alone does not.**
   With pre-vote, an isolated follower kept term 2 and the leader stayed in
   5 of 5 runs. Without pre-vote, the isolated term rose to 14-16. After the
   heal the leader stepped down in 5 of 5 runs (term about 20, 51-98 ms with no
   leader). [tested]
10. **`AddVoter` adds a full voter at once, not a staging server.** The doc
    comment says "staging" (`api.go:986-993`), but the code adds a `Voter`
    (`configuration.go:231-251`). After `AddVoter` of an unreachable node, the
    quorum was 3 of 4, and losing one more node stopped writes. [tested]
11. **`GetConfiguration().Index()` always returns 0 in v1.8.0** (issue #722).
    You cannot feed it to the `prevIndex` guard of membership calls. [tested]
12. **`RecoverCluster` commits every entry in the local log.** In the test it
    brought back an entry that was never committed and lost a write that 2 of 3
    nodes had committed. [tested]
13. **Validation minimums are 5 ms for the timeouts and 1 ms for
    `CommitTimeout`.** `LeaderLeaseTimeout` must be at most `HeartbeatTimeout`,
    and `ElectionTimeout` at least `HeartbeatTimeout`. [tested] On localhost TCP,
    10 ms timings ran with no term change. The library's own tests use 50 ms.
    [tested] [code]
14. **`Shutdown()` closes the transport only if it implements `raft.WithClose`.
    It never closes the stores.** A wrapper that hides `Close` left the TCP port
    bound. [tested] Close the stores yourself, after `Shutdown().Error()`
    (and see issue #717).
15. **v1.8.0 has open safety bugs.** Examples:
    - data divergence after a partition heal (#695)
    - a follower that applies never-committed entries (#733, #721)
    - `VerifyLeader` counts non-voters (#725)
    - a cluster stall after a leadership transfer (#696)

    [tested: `gh issue list`]

---

## Q1. Latest version, release date, Go version, metrics library

- Latest release: **v1.8.0**, published on GitHub on **2026-09-16**. The Go
  proxy time is `2026-09-16T14:40:33Z`. [tested: `gh api releases`, `go list -m -json`]
- Recent releases: v1.7.0 (2024-06-06), v1.7.1 (2024-08-29), v1.7.2
  (2025-01-09), v1.7.3 (2025-03-20), v1.8.0 (2026-09-16). [tested]
- `go.mod` of v1.8.0 says `go 1.25.0`. So you need **Go 1.25 or later**. v1.7.3
  said `go 1.20`. [code: `go.mod:3`] [tested: `go list -m -json` shows
  `GoVersion` 1.25.0]
- Direct dependencies: `hashicorp/go-hclog v1.6.3`, `hashicorp/go-metrics v0.7.0`,
  `hashicorp/go-msgpack/v2 v2.1.5`. [code: `go.mod`]
- **Metrics library: `github.com/hashicorp/go-metrics`.** The source imports it
  directly (`api.go:16`, `raft.go:18`). The v1.8.0 release notes say: "This
  release removes the long-standing compatibility shim" to `armon/go-metrics`.
  No build tags are needed. [code] [tested: release notes]
  - `go-metrics v0.7.0` has no reference to `armon`. [tested: grep]
  - `README.md:122-145` is **stale**. It still says that `armon/go-metrics` is
    the default and that you need build tags. Ignore it.
  - Your application must set up the sink with `hashicorp/go-metrics` (for
    example its `prometheus` sub-package), not with `armon/go-metrics`. [code]
- `CHANGELOG.md` in the module stops at 1.7.0. Use the GitHub release notes for
  1.7.1 and later. [code]
- What v1.8.0 changed: the metrics shim removal (#712) and an optional commit
  index in the log store (`RestoreCommittedLogs`, PR #613). [tested: compare]
- Maintenance: 47 open issues and 26 open PRs on 2026-10-29. Since July 2026,
  outside testers (Antithesis and others) filed several safety reports. Most
  have a fix PR, but none is merged. Code ownership moved to the Consul and
  Nomad teams (commit `d128548`, 2026-05-21). [tested: `gh issue list`, `gh pr list`]

## Q2. Apply, ApplyLog and their errors

Signature: `Apply(cmd []byte, timeout time.Duration) ApplyFuture`. `ApplyLog`
is the same, but takes a `Log`. Only `Data` and `Extensions` are used
(`api.go:871-889`). [code]

### What the timeout limits

The timeout limits only the wait to hand the future to the main loop
(`api.go:877-899`). After the enqueue, nothing has a deadline. [code]

- `TestApplyTimeoutOnlyLimitsEnqueue`: the leader disk was slow (500 ms per
  `StoreLogs`). A second `Apply(..., 50ms)` returned `ErrEnqueueTimeout` after
  50.6 ms. That command was in no log and no FSM afterwards. [tested]
- `TestErrorReturnsAfterLeaderFSMApply`: `Apply(..., 10ms)` with a 300 ms
  `FSM.Apply` returned `nil` after 300.4 ms. [tested]
- With `BatchApplyCh = true`, `applyCh` gets a buffer of `MaxAppendEntries`
  (`api.go:548-552`). The doc says this "breaks the timeout guarantee on Apply"
  (`config.go:169-174`). An entry can sit in the buffer after the timeout.
  [code]

### When Error() returns

- For a command, the FSM goroutine sets `future.response` and then calls
  `respond(nil)` after `FSM.Apply` returns (`fsm.go:92-107`). So `Error()`
  returns **after commit and after `FSM.Apply` on the leader**. [code] [tested:
  the leader FSM had the value at once after `Error()`]
- `Response()` is the return value of `FSM.Apply` (`future.go:35-45`). In the
  test it was `"ok:42"`. [tested]
- `Index()` is the log index of the entry. Call it only after `Error()`
  (`future.go:30-32`). [code]

### Every error an ApplyFuture can return

| Error | Where it comes from | Is the entry in a log? | Retry |
|---|---|---|---|
| `ErrNotLeader` | follower or candidate loop (`raft.go:179-182`, `392-395`), or a leader that is removing itself (`raft.go:926-930`) | No. It is rejected before any write. [tested: in no log] | Safe on the leader |
| `ErrEnqueueTimeout` | the enqueue `select` (`api.go:892-894`) | No. [tested] | Safe |
| `ErrLeadershipTransferInProgress` | leader loop during a transfer (`raft.go:908-912`) | No. It is rejected before `dispatchLogs`. [code] [tested: seen 28,399 times during a slow transfer] | Safe, after the transfer |
| `ErrRaftShutdown` (case A) | the enqueue `select` sees shutdown (`api.go:895-896`) | No | Safe |
| `ErrRaftShutdown` (case B) | `processLogs` cannot hand a committed batch to the FSM at shutdown (`raft.go:1304-1313`) | **Yes, it is committed** | Unknown. Treat as 504 |
| `ErrLeadershipLost` | the leader steps down. The cleanup fails every in-flight future (`raft.go:518-521`). This also runs when a leader shuts down (`raft.go:958-959`). | **Maybe.** Other nodes can have it. [tested: it committed on all nodes] | Unknown. Treat as 504 |
| `ErrAbortedByRestore` | a user `Restore()` cancels in-flight entries (`raft.go:1124-1132`) | It was in the log, but the restored snapshot replaces the state. The doc says the write "effectively failed" (`api.go:864-866`). | Decide in the application |
| a `LogStore` error | the leader's `StoreLogs` fails. The leader becomes a follower (`raft.go:1270-1277`). | Not on the leader. Replication starts only after a good write (`raft.go:1283-1286`). | Probably safe. It depends on the atomicity of your store. [code] |
| no answer at all | see Q3 | Unknown | Your deadline gives 504 |

- `Error()` never returns a timeout error by itself. [code]
- An error value from `FSM.Apply` is not in this table. It goes to
  `Response()` (`future.go:39-44`). [tested, see Q13]
- `SuggestedMaxDataSize` is 512 KiB (`api.go:30`). Larger entries can delay
  heartbeats. The library does not check it. [code]

## Q3. Can Error() block for a long time? Leader step-down

### Yes, Error() can block for a long time

`deferError.Error()` waits on `errCh`. It also waits on `ShutdownCh`, but only
if the future has one (`future.go:103-119`). `ApplyLog` never sets
`ShutdownCh` on its future (`api.go:883-890`). [code]

- `TestErrorBlocksWhileLeaderKeepsLease`: both followers failed every
  `StoreLogs`. Heartbeats do not write to disk, so the leader kept its lease.
  `Error()` blocked for more than 3 s. The node stayed leader in the same term.
  After the disks recovered, `Error()` returned `nil` after 2.1 s. The delay
  comes from the retry backoff, which grows to `10ms * 2^10` (`util.go:129-136`,
  `replication.go:255-263`). [tested]
- Issue #503 reports the same hang when the **leader's** disk hangs. Issue #498
  ("Apply future deadlocks if quorum cannot be reached") is open since 2022.
  PR #683 (open) wants to fix the leader-disk case. [tested: `gh issue view`]
- `TestShutdownBehaviour`: 5 entries were committed and waited for a slow
  leader FSM. After `Shutdown()`, 4 futures returned `nil` and 1 was still
  blocked after 2 s. The FSM goroutine exits on `shutdownCh` and leaves queued
  batches without an answer (`fsm.go:237-262`). [tested]

### When a leader steps down

- **Lease check.** The leader loop runs `checkLeaderLease()` on a timer
  (`raft.go:935-948`). It counts voters whose last contact is not older than
  `LeaderLeaseTimeout`. If that count is below the quorum, the leader becomes a
  follower and increments `raft.transition.leader_lease_timeout`
  (`raft.go:1037-1082`). The next check comes after `LeaderLeaseTimeout - maxDiff`,
  but at least 10 ms (`raft.go:22`, `raft.go:942-945`). [code]
- "Contact" is any response from the follower, also a failed one, and also a
  heartbeat (`replication.go:69-72`, `245`, `427`). So a follower that answers
  heartbeats but cannot store entries keeps the lease alive. [code] [tested]
- **Higher term.** A response with a higher term makes the leader step down
  (`replication.go:238-242`, `646-650`). [code] [tested: disruptive rejoin]
- **VerifyLeader.** One `false` vote makes the leader step down
  (`future.go:289-292`, `raft.go:854-863`). [code]
- `TestIsolatedLeaderStepsDownAndFailsInflight`: with `LeaderLeaseTimeout = 250ms`
  the isolated leader returned `ErrLeadershipLost` after 259 ms. With 50 ms it
  took 50 ms (`TestErrLeadershipLostButEntryCommits`). [tested]

### What happens to in-flight futures

At step-down, every uncommitted in-flight future gets `ErrLeadershipLost`, and
every pending `VerifyLeader` future gets `ErrLeadershipLost`
(`raft.go:518-526`). [code] [tested]

### Can the caller add its own deadline?

Yes. `Error()` must not run twice at the same time on one future
(`future.go:15-18`). So call it in one goroutine and wait on a channel:

```go
// waitForApply waits for the future, but never longer than timeout.
// On timeout the write may still commit later: report "outcome unknown".
func waitForApply(future raft.ApplyFuture, timeout time.Duration) (interface{}, error) {
	resultCh := make(chan error, 1)
	go func() { resultCh <- future.Error() }()
	select {
	case err := <-resultCh:
		if err != nil {
			return nil, err
		}
		return future.Response(), nil
	case <-time.After(timeout):
		return nil, errOutcomeUnknown // HTTP 504
	}
}
```

- The helper goroutine leaks if the future never resolves (see the shutdown
  test). The leak is one small goroutine per stuck request. [tested]
- A write that timed out at the caller can still commit. In the disk test it
  committed 2.1 s after the disks recovered. [tested]

## Q4. Reads: VerifyLeader, Barrier, ReadIndex, leases

### VerifyLeader

- It sends one immediate heartbeat to every follower (`raft.go:966-988`). It
  succeeds when a quorum answers `Success = true` (`future.go:274-293`). It
  writes nothing to the log. [code]
- Cost in the test: 17.7 µs on average (in-memory transport, 200 calls). On a
  real network, expect one round trip. [tested]
- On a follower: `ErrNotLeader` at once (`raft.go:184-187`). [tested]
- On an isolated leader: `ErrLeadershipLost` after about `LeaderLeaseTimeout`
  (51 ms with 50 ms). The future waits until the step-down cleanup answers it.
  [tested]
- Issue #725 (open): `VerifyLeader` also counts **non-voter** answers toward the
  quorum. With non-voters it can succeed without a voter majority. [tested:
  `gh issue view`] [code: `raft.go:982-987` registers every replication stream]

### Barrier

- It appends a `LogBarrier` entry (`api.go:902-926`). `Error()` returns after
  that entry is committed and all earlier entries went through the FSM
  (`fsm.go:104-124`). [code]
- Cost: one log entry per call. 200 barriers added 200 entries. Average 29 µs
  in memory. On disk it costs a quorum fsync. [tested]
- A `BatchingFSM` does not receive barrier entries (`fsm.go:135-143`). [code]

### What a new leader must do first

- A new leader appends a no-op entry when it starts (`raft.go:566-573`). Its
  commit index and FSM can be behind until that no-op commits and applies.
  [code]
- `VerifyLeader()` does not wait for that. So run `Barrier()` once after you
  become leader, before you serve linearizable reads. [code]
- `AppliedIndex()` is not proof that the FSM applied the entry. It counts
  entries **sent** to the FSM queue (`api.go:1286-1295`). In the test the
  follower `AppliedIndex()` reached the last write while its slow FSM had not
  applied it. [tested]

### ReadIndex or lease reads

- There is no ReadIndex API and no lease-read API. [code]
- `CommitIndex()` exists. Its doc says it "maybe helpful" to build ReadIndex
  (`api.go:1279-1284`). Issue #621 asks if that is "actually (easily) possible"
  and is open. PR #625 (open) wants an API for leadership checks. [code]
  [tested: `gh issue list`]
- The library's lease is **not** a read lease. The code says so: "it serves
  another purpose" (`raft.go:1016-1023`). [code]

### Is a local read on the leader safe?

No, not for a linearizable read. [code]

- The step-down happens only at the next lease check. Until then
  `State()` is still `Leader`. [code] [tested: the isolated leader needed about
  one lease period to step down]
- Validation forces `LeaderLeaseTimeout <= HeartbeatTimeout`
  (`config.go:382-384`). So the old leader usually steps down before followers
  time out (after 1x to 2x `HeartbeatTimeout`, `util.go:28-35`). But it is a
  timer race, not a guarantee. Issue #666 shows a stale leader that keeps its
  lease during a disk stall. [code]

### Read options for part 2

| Read | Cost | Safe? |
|---|---|---|
| `Barrier()` then read the leader FSM | one log entry (quorum write) | Linearizable |
| `VerifyLeader()` then read the leader FSM, after one `Barrier()` in this term | one heartbeat round | Linearizable with voters only (issue #725) |
| local FSM read on any node | none | Stale. On an idle follower the lag after the leader ack was 1x to 2x `CommitTimeout` (see Q13). Under a partition it has no bound. |

## Q5. Pre-vote

- Added in **v1.7.0** (PR #530), "activated by default". [tested: release
  notes, `CHANGELOG.md`]
- **v1.7.1** fixed two pre-vote bugs (#605 observed in production, #609
  theoretical). The release notes "strongly recommend all 1.7.0 users upgrade".
  Use 1.7.1 or later. [tested: release notes]
- Switch it off with `Config.PreVoteDisabled = true` (`config.go:235-236`).
  The default is `false`. [code] [tested]
- The transport must implement the extra interface `raft.WithPreVote`
  (`transport.go:69-77`). `NewRaft` checks it with a type assertion:
  `preVoteDisabled = conf.PreVoteDisabled || !transportSupportPreVote`
  (`api.go:554`, `api.go:584`). [code]
- `NetworkTransport` and `InmemTransport` implement it. [tested: compile-time
  assertions]
- **A wrapper that embeds the `raft.Transport` interface hides
  `RequestPreVote`.** The node then does real elections without pre-vote. It
  logs only one warning: `pre-vote is disabled because it is not supported by
  the Transport` (`api.go:588-590`). [tested: `TestPreVoteNeedsTransportInterface`]
- A wrapper that embeds the concrete `*raft.NetworkTransport` (or
  `*raft.InmemTransport`) keeps `RequestPreVote` and `Close` through method
  promotion. [tested]
- The wrapper only changes outbound pre-votes. Inbound pre-vote RPCs are
  decoded by the inner transport (`net_transport.go:691-697`) and answered by
  `requestPreVote` (`raft.go:1759-1831`). [code]
- If a peer answers a pre-vote with "unexpected command" (an old version), the
  candidate counts it as **granted** (`raft.go:2105-2113`). [code]
- A leadership transfer skips pre-vote on purpose (`raft.go:295-301`). [code]

Fault-injection wrappers for part 2 must embed the concrete transport type, or
implement `RequestPreVote` and `Close` themselves.

## Q6. Leader stickiness and the disruptive rejoin

### What the code does

- `requestVote` rejects a vote while the node knows a leader that is not the
  candidate, unless the request has the `LeadershipTransfer` flag
  (`raft.go:1673-1679`). This check runs **before** the term check, so the
  receiver does not adopt the higher term. [code]
- `requestPreVote` rejects a pre-vote while the node knows a leader
  (`raft.go:1786-1792`). [code]
- A follower forgets the leader when its heartbeat timer fires without contact
  (`raft.go:214-228`). [code]
- `docs/divergence.md` says the library rejects votes "when there's already an
  established leader". This follows the liveness paper of Howard and Abraham.
  [code]

### Test: disruptive rejoin (`TestDisruptiveRejoin`)

Scenario: 3 nodes, 50 ms timeouts. Isolate one follower in both directions.
Write 5 entries through the majority. Wait 1 s (20 election timeouts). Heal.
Watch for 1 s more. 5 runs per variant. [tested]

| Variant | Isolated node term | Leader after heal | Time with no leader |
|---|---|---|---|
| pre-vote on (default) | stayed 2 (5 of 5) | same leader, same term 2 (5 of 5) | 0 |
| `PreVoteDisabled = true` | rose to 14-16 | term changed in 5 of 5 runs, to term 19-23 | 51-98 ms |

Without pre-vote, the other nodes did reject the votes ("rejecting vote
request since we have a leader", 7-12 log lines per run). But the leader still
stepped down. The reason [code]:

1. The leader sends `AppendEntries` to the healed node.
2. The response has the higher term (14).
3. `replicateTo` sees `resp.Term > req.Term` and asks the leader to step down
   (`replication.go:238-242`, `646-650`). It does not adopt term 14.
4. Each later candidate with a lower term loses, and adopts the higher term from
   the vote replies (`raft.go:364-372`). This explains the many elections after
   the heal.

**Raft lesson:** leader stickiness protects only the vote path. It does not
protect the replication path. Pre-vote is the real protection. Keep it on, and
make sure every transport wrapper implements `WithPreVote`.

## Q7. Leadership transfer

- `LeadershipTransfer()` picks the voter with the highest `nextIndex`
  (`raft.go:2186-2205`). `LeadershipTransferToServer(id, address)` picks a given
  server. Both need protocol version 3 (`api.go:1309-1330`). [code]
- Steps: the leader stops new writes, brings the target up to date, and sends
  `TimeoutNow` (`raft.go:991-1031`). The target becomes a candidate at once
  and skips pre-vote (`raft.go:2232-2237`). [code]
- Success means the old leader left its leader loop. The future then returns
  `nil` (`raft.go:735-740`, `754-756`). [code] In memory this took 28-39 µs.
  [tested]

Errors [tested unless marked]:

| Case | Result |
|---|---|
| transfer to itself | `cannot transfer leadership to itself` (`raft.go:2214-2219`) |
| call on a follower | `ErrNotLeader` |
| a second transfer while one runs | `ErrLeadershipTransferInProgress` |
| transfer channel full | `ErrEnqueueTimeout`. The send does not wait (`raft.go:2221-2228`). [code] |
| target too slow to catch up | `leadership transfer timeout` after `ElectionTimeout` (50 ms) |
| target isolated | `leadership transfer timeout` after `ElectionTimeout`. The node stays leader. |
| no other voter | `cannot find peer` (`raft.go:768-776`) [code] |
| `TimeoutNow` RPC fails | `failed to make TimeoutNow RPC to ...` (`raft.go:1026-1030`) [code] |
| protocol version below 3 | `ErrUnsupportedProtocol` [code] |

What happens to other calls during a transfer:

- `Apply` and `Barrier` return `ErrLeadershipTransferInProgress`
  (`raft.go:906-912`). During a slow transfer the test saw that error 28,399
  times and 3,893 successes. [tested]
- User `Restore()` and membership changes are also rejected
  (`raft.go:873-900`). [code]
- Snapshots fail during a transfer, because `takeSnapshot` uses
  `configurationsCh` (`snapshot.go:152-162`, `raft.go:883-889`). [code]
- `Raft.GetConfiguration()` is not affected. It reads an atomic copy
  (`api.go:943-951`). [code]
- After a failed transfer, an `Apply` can still get
  `ErrLeadershipTransferInProgress` for a short time. The future answers before
  the flag is reset. The code comment says so (`raft.go:719-725`). In one test
  run the next `Apply` failed this way. [tested]
- After a timeout the transfer goroutine can still send `TimeoutNow`. It checks
  `stopCh` only inside the catch-up loop (`raft.go:1000-1031`). So leadership
  can move after the caller saw "timeout". [code]

Known issues: #696 (cluster stall after an interrupted transfer, fix PR #708
open), #727 (a non-voter target is accepted, fix PR #732 open). v1.6.1 fixed
"rare leadership transfer failures when writes happen during transfer"
(#581). [tested: `gh issue view`]

## Q8. Bootstrap

- `BootstrapCluster(conf, logs, stable, snaps, trans, configuration)` writes
  term 1 and one configuration entry at index 1 (`api.go:247-291`). It refuses
  if `HasExistingState` is true and returns `ErrCantBootstrap`
  (`api.go:260-267`). [code]
- The doc says: call it "with an identical configuration listing all Voter
  servers". It also says that a further attempt returns "an error that can be
  safely ignored" (`api.go:235-246`). [code]
- **Calling it on all 3 nodes with the same configuration is safe.** Each node
  writes the same entry. Every test in this study did that. [tested:
  `TestBootstrapSemantics` and all cluster tests]
- A second call returns `ErrCantBootstrap`, offline and live. [tested]
- The live method `Raft.BootstrapCluster(configuration)` (`api.go:817-827`)
  works only in follower state. First it checks that the local node is a voter
  in the given configuration, else `ErrNotVoter` (`raft.go:262-266`). A leader
  or candidate answers `ErrCantBootstrap` (`raft.go:417-419`, `902-904`).
  [code] [tested: `ErrNotVoter` with a configuration that did not list the node]
- `HasExistingState(logs, stable, snaps)` is true if there is a current term,
  a log entry, or a snapshot (`api.go:470-502`). [code] [tested: true after
  bootstrap, false on empty stores]
- Risk for Docker: if a node loses its volume, `HasExistingState` is false and
  your start-up code bootstraps it again. It also lost its vote record, so it
  can vote twice in one term. [code] Bootstrap only on the first start of the
  whole cluster.

## Q9. Membership changes

| Method | Effect | Code |
|---|---|---|
| `AddVoter(id, addr, prevIndex, timeout)` | adds a **Voter at once** (the doc says "staging"). If the ID exists as a voter, it updates the address. | `api.go:994-1005`, `configuration.go:231-251` |
| `AddNonvoter(...)` | adds a Nonvoter. It gets entries but has no vote. | `api.go:1012-1023` |
| `DemoteVoter(id, prevIndex, timeout)` | makes a voter a Nonvoter. | `api.go:1045-1055` |
| `RemoveServer(id, prevIndex, timeout)` | removes the server. | `api.go:1028-1038` |

- **`prevIndex`:** if not zero, the change fails unless the latest
  configuration index equals it. The error is `configuration changed since 999
  (latest is 1)` (`configuration.go:225-227`). [tested]
- **`GetConfiguration().Index()` returns 0** (issue #722, fix PR #723 open).
  `GetConfiguration()` builds `configurations{latest: ...}` without the index
  (`api.go:948`). `Stats()["latest_configuration_index"]` is also `"0"`. So
  you cannot use the read value as `prevIndex`. [tested]
- **`timeout`:** it limits only the enqueue (`raft.go:116-133`). With 0 the
  call waits for the enqueue without a limit. [code]
- **One change at a time:** the leader reads the change channel only in one
  state. The latest configuration and its own no-op must both be committed
  (`raft.go:654-664`). A second change waits. [code]
- **AddVoter of an unreachable node:** `AddVoter` returned `nil` after 69 µs.
  The new server was a `Voter` and `num_peers` became 3. Then one old follower
  was isolated, and a write got `ErrLeadershipLost` and the leader stepped down.
  [tested: `TestMembershipChanges`] Safe pattern: `AddNonvoter`, wait until it
  catches up, then `AddVoter` to promote it.
- **A removed leader:** with `ShutdownOnRemove = true` (the default) it shuts
  down. With `false` it becomes a follower. [tested: `TestRemoveServer`]
  (`raft.go:839-846`)
- **A removed follower:** it is not shut down, also with
  `ShutdownOnRemove = true`. The flag is used only at `raft.go:840`. The node
  stays a follower and does not start elections. Its term stayed the same for
  1 s. [tested] [code: `raft.go:235-250`] Your application must stop it.
- `GetConfiguration()` returns the **latest** configuration, which may not be
  committed (`api.go:943-951`). [code]
- Open bug #726: if the leader fails to write a configuration entry, it can
  still install that configuration (fix PR #738 open). [tested: `gh issue view`]

## Q10. Disaster recovery: RecoverCluster and peers.json

- `RecoverCluster(conf, fsm, logs, stable, snaps, trans, configuration)` does
  these steps (`api.go:321-448`): [code]
  1. It restores the newest snapshot into the FSM you pass.
  2. It applies **every** log entry to that FSM.
  3. It writes a new snapshot with the new configuration.
  4. It removes the whole log.
- Warnings in the code (`api.go:302-312`):
  - It "implicitly commits all entries in the Raft log". It is "an extremely
    unsafe operation".
  - Discard the FSM you passed. Give a fresh FSM to `NewRaft`.
- It refuses when the stores are empty: `refused to recover cluster with no
  initial state, this is probably an operator error` (`api.go:339-343`).
  [tested]
- `TestRecoverClusterResurrectsUncommittedEntry`: the old leader was isolated
  and wrote one entry that never committed. The majority committed another
  write. Then `RecoverCluster` ran on the old leader alone. After the restart its
  FSM had the **never-committed** entry and did **not** have the majority
  write. [tested]
- `ReadConfigJSON(path)` reads the new peers.json format:
  `[{"id": "...", "address": "...", "non_voter": false}]`
  (`peersjson.go:51-101`). [tested] `ReadPeersJSON` reads the old format
  (a list of addresses) and must not be used with server IDs
  (`peersjson.go:12-17`). [code]
- The library does not look for a peers.json file itself. Your start-up code
  must check for the file and call `RecoverCluster` (Consul does this).
  [code: no caller in the library] [not verified: Consul details]
- Recovery steps: stop all nodes. Pick the node with the most recent data. Run
  `RecoverCluster` on it, or on every node with the same configuration
  (`api.go:314-320`). Start the nodes. Accept that some writes can come back or
  get lost.

## Q11. Observability

### Stats()

Keys [tested]: `applied_index`, `commit_index`, `fsm_pending`, `last_contact`,
`last_log_index`, `last_log_term`, `last_snapshot_index`, `last_snapshot_term`,
`latest_configuration`, `latest_configuration_index`, `num_peers`,
`protocol_version`, `protocol_version_max`, `protocol_version_min`,
`snapshot_version_max`, `snapshot_version_min`, `state`, `term`.

- All values are strings (`api.go:1208-1266`). [code]
- `state` is `Follower`, `Candidate`, `Leader` or `Shutdown`. [tested]
- `last_contact` is `"0"` on the leader, `"never"` before any contact, else a
  duration such as `1.69ms`. [tested]
- `num_peers` counts the other **voters**. It is 0 if this node is not in the
  configuration. [code] [tested: a non-voter did not count]
- `latest_configuration_index` is always `"0"` (issue #722). [tested]
- The doc calls `Stats()` "informative purposes or debugging" only
  (`api.go:1183-1184`). [code]

### LeaderCh and NotifyCh

- `LeaderCh()` has a buffer of 1 (`api.go:562`). The library overwrites an
  unread value, so only the **latest** value stays (`util.go:88-105`,
  `api.go:1154-1167`). [code] In the test the node changed leadership 3 times
  and the channel held only `[false]`. [tested]
- `LeaderCh()` returns the same channel for every caller. Two readers steal
  events from each other (PR #427, open). [code]
- `Config.NotifyCh` is different: the library **blocks** on the send
  (`config.go:212-215`). A new leader sends `true` before it sets up its leader
  state (`raft.go:481-492`). If nobody reads, the new leader hangs. [code] Use a
  buffered channel and read it all the time, or use `LeaderCh()`.

### Observers

- `RegisterObserver(raft.NewObserver(channel, blocking, filter))`
  (`observer.go:87-111`). A non-blocking observer drops events when the channel
  is full and counts them in `GetNumDropped()` (`observer.go:137-147`). [code]
- Observation types: `raft.RaftState`, `raft.LeaderObservation`,
  `raft.PeerObservation`, `raft.FailedHeartbeatObservation`,
  `raft.ResumedHeartbeatObservation`, `raft.RequestVoteRequest`,
  `raft.RequestPreVoteRequest` (`observer.go:11-46`, `raft.go:109`, `615`,
  `641`, `1628`, `1761`, `2180`, `replication.go:416`, `425`). [code] The comment
  in `observer.go:15-19` lists only 4 of them.
- A leader isolation and heal produced 6 of the 7 types (not
  `ResumedHeartbeatObservation`). 0 events were dropped with a 1024 buffer.
  [tested]
- `RequestVoteRequest` and `RequestPreVoteRequest` are reported on the node
  that **receives** the request. [code]

### Other accessors

- `LeaderWithID()` returns address and ID. Both can be empty (`api.go:841-850`).
  `Leader()` is deprecated. [code]
- `LastContact()` is the time of the last good contact from a leader. It is
  meaningful only on a follower (`api.go:1174-1181`). [code] [tested: 1.7 ms
  on a follower]
- `CommitIndex()`, `AppliedIndex()`, `LastIndex()`, `CurrentTerm()` exist
  (`api.go:1268-1295`). `CurrentTerm()` came in v1.7.2. [code]
- `AppliedIndex()` runs ahead of the FSM (see Q4). [tested]

### Main metric names

All names start with `raft.` [code: grep of all `metrics.*` calls]

- Leadership: `state.follower`, `state.candidate`, `state.leader`,
  `transition.heartbeat_timeout`, `transition.leader_lease_timeout`,
  `leader.lastContact` (sample, ms), `peers` (gauge).
- Writes: `apply` (counter), `barrier`, `verify_leader`, `commitTime`,
  `leader.dispatchLog`, `leader.dispatchNumLogs`, `commitNumLogs`,
  `leader.oldestLogAge`.
- FSM: `fsm.apply`, `fsm.applyBatch`, `fsm.applyBatchNum`, `fsm.enqueue`,
  `fsm.snapshot`, `fsm.restore`, `fsm.lastRestoreDuration`.
- Replication (label `peer_id`): `replication.appendEntries.rpc`,
  `replication.appendEntries.logs`, `replication.heartbeat`,
  `replication.installSnapshot`.
- RPC handling: `rpc.appendEntries`, `rpc.appendEntries.storeLogs`,
  `rpc.appendEntries.processLogs`, `rpc.requestVote`, `rpc.requestPreVote`,
  `rpc.installSnapshot`, `rpc.processHeartbeat`, `net.rpcDecode`,
  `net.rpcEnqueue`, `net.rpcRespond`, `net.getRPCType`.
- Snapshots: `snapshot.takeSnapshot`, `snapshot.create`, `snapshot.persist`,
  `compactLogs`, `restore`, `restoreUserSnapshot`.
- Saturation: `thread.main.saturation`, `thread.fsm.saturation`.
- `Config.NoLegacyTelemetry = true` stops the old duplicate names with the peer
  ID as a suffix, such as `raft.replication.heartbeat.node2`
  (`config.go:238-241`). [code]

## Q12. Config

### DefaultConfig values (`config.go:329-343`) [tested]

| Field | Default |
|---|---|
| `ProtocolVersion` | 3 |
| `HeartbeatTimeout` | 1s |
| `ElectionTimeout` | 1s |
| `CommitTimeout` | 50ms |
| `MaxAppendEntries` | 64 |
| `BatchApplyCh` | false |
| `ShutdownOnRemove` | true |
| `TrailingLogs` | 10240 |
| `SnapshotInterval` | 2m |
| `SnapshotThreshold` | 8192 |
| `LeaderLeaseTimeout` | 500ms |
| `LogLevel` | `"DEBUG"` |
| `PreVoteDisabled`, `NoSnapshotRestoreOnStart`, `RestoreCommittedLogs`, `NoLegacyTelemetry` | false |

`DefaultConfig()` alone fails validation: `LocalID cannot be empty`. [tested]

### ValidateConfig rules (`config.go:346-389`) [tested: table test]

- `ProtocolVersion` from 1 to 3. 0 fails.
- `LocalID` not empty.
- `HeartbeatTimeout`, `ElectionTimeout`, `LeaderLeaseTimeout` and
  `SnapshotInterval` at least 5 ms. 4 ms fails.
- `CommitTimeout` at least 1 ms. 999 µs fails.
- `MaxAppendEntries` from 1 to 1024.
- `LeaderLeaseTimeout <= HeartbeatTimeout`. Equal is allowed.
- `ElectionTimeout >= HeartbeatTimeout`.
- `TrailingLogs = 0` and `SnapshotThreshold = 0` pass. Issue #86 asks about
  `TrailingLogs = 0` and is open. [code]

### What each timeout does [code]

- `HeartbeatTimeout`: a follower without contact for a random 1x to 2x of it
  starts an election (`raft.go:164`, `214-246`, `util.go:28-35`). The leader
  sends heartbeats every random 0.1x to 0.2x of it (`replication.go:402`).
- `ElectionTimeout`: how long a candidate waits before a new round
  (`raft.go:310-311`). It also limits a leadership transfer (`raft.go:729`).
- `LeaderLeaseTimeout`: see Q3.
- `CommitTimeout`: when no new entries come, the leader still sends
  `AppendEntries` after a random 1x to 2x of it. So followers learn the commit
  index (`replication.go:164-171`). Heartbeats do not carry the commit
  index. Write latency does not wait for it. A write starts replication at
  once (`raft.go:1283-1286`).

### Fastest stable timings on localhost (`TestTimingsOnLocalhostTCP`)

3 nodes, real TCP on `127.0.0.1`, in-memory stores, sequential writes for
4 s, then the leader is shut down. [tested]

| Heartbeat / Election / Lease / Commit | First leader | Term changes in 4 s | Write p50 | Failover to next write |
|---|---|---|---|---|
| 1s / 1s / 500ms / 50ms (default) | 1.09-1.48 s | 0 | 54 µs | 2.7-2.9 s |
| 100 / 100 / 100 / 10 ms | 108-157 ms | 0 | 54 µs | 176-280 ms |
| 50 / 50 / 50 / 5 ms | 54-66 ms | 0 | 54 µs | 64-87 ms |
| 20 / 20 / 20 / 2 ms | 24-35 ms | 0 | 55 µs | 31-57 ms |
| 10 / 10 / 10 / 1 ms | 14-20 ms | 0 | 55 µs | 15-31 ms |

- With `-race`, writes were 4x slower (p50 about 240 µs). There was still no
  term change. But at 10 ms and 20 ms, some followers briefly lost the leader
  (1-6 `LeaderObservation` events). Pre-vote stopped those near-misses from
  becoming elections. [tested]
- The library's own tests use 50 / 50 / 50 / 5 ms (`testing.go:24-32`). [code]
- Advice: use 50 ms for in-process cluster tests. It gives a margin for
  `-race`, CI machines and real fsync. These numbers do not include a real disk.

### Other fields [code]

- `LocalID`: not empty. It must be unique for all time (`config.go:207-210`).
  `checkConfiguration` rejects duplicate IDs and duplicate addresses
  (`configuration.go:192-220`). With protocol version below 3 it must equal the
  address (`api.go:544-546`).
- `ProtocolVersion`: keep 3. It is not negotiated. A node rejects RPCs more than
  one version older than its own (`raft.go:67-69`).
- `BatchApplyCh`: see Q2. It weakens the `Apply` timeout. Keep it false.
- `MaxAppendEntries`: the maximum entries per `AppendEntries`, per group commit
  in the leader loop (`raft.go:915-923`) and per FSM batch (`raft.go:1318-1348`).
- `ShutdownOnRemove`: see Q9.
- `NoSnapshotRestoreOnStart`: `NewRaft` does not call `FSM.Restore` with the
  newest snapshot (`api.go:691-694`). Your FSM must then recover its own state.
- `RestoreCommittedLogs` (new in v1.8.0): replays known-committed entries in
  `NewRaft`. It needs a `CommitTrackingLogStore` (`log.go:194-221`), or
  `NewRaft` fails with `ErrIncompatibleLogStore` (`api.go:731-735`). Open bugs:
  #729 (`NewRaft` can hang) and #728 (configuration entries can be skipped).
  **Do not use it in part 2.**
- `Logger`: an `hclog.Logger`. Without it the library writes `DEBUG` logs to
  `os.Stderr` (`config.go:260-273`, `341`). Set a level.
- `ReloadConfig` can change only `TrailingLogs`, `SnapshotInterval`,
  `SnapshotThreshold`, `HeartbeatTimeout` and `ElectionTimeout`
  (`config.go:280-317`, `api.go:765-789`).
- TCP transport: binding `0.0.0.0` without an advertise address fails with
  `local bind address is not advertisable` (`tcp_transport.go:88-91`). In
  Docker, pass the container address as `advertise`. [tested]

## Q13. FSM contract

### Interfaces (`fsm.go:16-81`) [code]

```go
type FSM interface {
	Apply(*raft.Log) interface{}
	Snapshot() (raft.FSMSnapshot, error)
	Restore(snapshot io.ReadCloser) error
}

type BatchingFSM interface {
	ApplyBatch([]*raft.Log) []interface{}
	FSM
}

type FSMSnapshot interface {
	Persist(sink raft.SnapshotSink) error
	Release()
}
```

- `Apply` "must be deterministic and produce the same result on all peers"
  (`fsm.go:19-20`).
- `ApplyBatch` gets `LogCommand` and `LogConfiguration` entries, never barriers.
  It must return exactly one response per entry, or the library panics
  (`fsm.go:135-165`).
- A plain `FSM` sees only `LogCommand` entries. Configuration entries go to an
  optional `ConfigurationStore` (`fsm.go:104-120`).

### FSM.Apply returns an error value

`Error()` is `nil`. `Response()` is the error. The next write committed on all
nodes. [tested: `TestFSMApplyErrorIsOnlyAResponse`] The entry is committed and
every node applies it. A business rule such as "CAS failed" can be such a
response. The FSM must return the same error on every node.

### FSM.Apply panics

The FSM goroutine runs without `recover` (`state.go:145-151`, `fsm.go:86`).
The panic kills the process. The child test process exited with code 2. The
stack trace showed `raft.(*Raft).runFSM.func1`. [tested:
`TestFSMApplyPanicKillsProcess`] Every node applies the same entry, so every
node crashes. After a restart the node applies the entry again (see the
restart test), so it crashes again. [code] Never panic in `Apply`. Return an
error value instead.

### Concurrency

- `Apply` and `Snapshot` run in the same goroutine. They never overlap
  (`fsm.go:34-36`). [tested: 0 applies during a 300 ms `Snapshot()`, and the
  slowest write took 301 ms]
- `Persist` runs in the snapshot goroutine, while `Apply` continues
  (`snapshot.go:189-195`). [tested: 28,092 applies during a 500 ms `Persist()`]
  So `Snapshot()` must copy the state fast (or take an immutable view), and
  `Persist` must work on that copy.
- `Release()` is always called. `Persist` may never be called (`fsm.go:38-42`).
- `Restore` "is not called concurrently with any other command". The FSM must
  discard all old state first (`fsm.go:45-48`). It runs in the FSM goroutine
  (`fsm.go:188-214`), or in `NewRaft` before the goroutines start
  (`api.go:601-604`).

### When Restore is called [code]

1. In `NewRaft`, with the newest snapshot, unless `NoSnapshotRestoreOnStart`
   (`api.go:645-689`).
2. On a follower that gets `InstallSnapshot` from the leader
   (`raft.go:1935-1951`).
3. On the leader for a user `Restore()`. If that fails, the library panics
   (`raft.go:1177-1179`).

### When a follower calls FSM.Apply

- When an `AppendEntries` request carries a higher leader commit index, the
  follower sends the new entries to the FSM goroutine (`raft.go:1588-1598`).
  [code]
- Heartbeats do not carry the commit index (`replication.go:390-395`). So an
  idle follower learns about a commit after 1x to 2x `CommitTimeout`.
  `TestFollowerApplyLag`: with `CommitTimeout = 5ms` the lag after the leader
  ack was 9 ms on average (max 11 ms). With 50 ms it was 78 ms (max 100 ms).
  [tested]
- After a restart, the FSM is empty until the leader sends the commit index. In
  `TestRestartReappliesLogToFreshFSM` the fresh FSM had 0 entries right after
  `NewRaft`, and later all 20 again. [tested] The library replays the log from
  the last snapshot, so `Apply` must give the same result when it runs again on
  a fresh state.

## Q14. Shutdown and restart

- `Shutdown()` closes `shutdownCh` and sets the state to `Shutdown`
  (`api.go:1060-1073`). Its future waits for all library goroutines and then
  calls `Close()` on the transport, but only if the transport implements
  `raft.WithClose` (`future.go:171-180`). [code]
- "This is not a graceful operation" (`api.go:1057-1059`). For a planned stop of
  the leader, call `LeadershipTransfer()` first. [code]
- `NetworkTransport` implements `WithClose`. After `Shutdown().Error()` the port
  was free again, and a new transport bound the same address. [tested:
  `TestRestartReappliesLogToFreshFSM`]
- A wrapper that embeds the `raft.Transport` interface hides `Close`. The port
  stayed bound (`bind: address already in use`) until the test closed the inner
  transport. [tested: `TestShutdownBehaviour`]
- **The library never closes the `LogStore`, `StableStore` or
  `SnapshotStore`.** You must close them. [code: no `Close` call on stores]
- Order: `Shutdown().Error()`, then close the transport if you wrapped it, then
  close the stores. Issue #717: a heartbeat on a `NetworkTransport` goroutine
  can still reach the store after that, and then `setCurrentTerm` panics with
  `database not open` (`raft.go:2164-2170`). Fix PR #720 is open. [tested:
  `gh issue view`]
- A second `Shutdown()` is harmless and returns `nil`. `Apply` after
  `Shutdown()` returns `ErrRaftShutdown`. [tested]
- In-flight futures at shutdown: see Q3. One can block forever. [tested]
- Restart in the same process: `NewRaft` with the same stores, a fresh FSM and
  a new transport on the same address worked. `HasExistingState` was true, so no
  bootstrap was needed. The FSM got all 20 entries again. [tested]
- With file stores (for example `raft-boltdb`) on the same data dir: close the
  old store before you open it again. [not verified]

## Q15. Known bugs and open issues that matter in production

Status on 2026-10-29. All are open, and all affect v1.8.0 unless marked.
[tested: `gh issue view`, `gh pr list`]

### Safety (wrong data)

- **#695** Data divergence after a partition heal. The cause is a race with
  the asynchronous heartbeat. Found with Antithesis on a 3-node cluster. A
  maintainer wrote that it seems legitimate at first glance. No fix PR yet.
- **#733** A follower can apply entries that were never committed. The commit
  index is capped at the follower's own last index, not at the last new entry
  (`raft.go:1591`). Fix PR #734.
- **#721** `InstallSnapshot` keeps divergent entries below the snapshot. A later
  leader can send them to a new server, which applies never-committed writes.
  Fix PR #735. Related livelock: #697.
- **#725** `VerifyLeader` counts non-voters toward the quorum. No fix PR.
- **#726** A failed configuration write can still install the new
  configuration. Fix PR #738.

### Liveness (the cluster stops)

- **#696** The cluster stalls after an interrupted leadership transfer. The
  leader keeps answering `leadership transfer in progress`. Fix PR #708.
- **#666** A stale leader keeps its lease during a disk stall. **#503**: a
  hanging leader disk hangs the whole cluster, and no election starts. Fix PR
  #683.
- **#498** An `Apply` future can wait forever. [tested: Q3]
- **#614** A node that cannot write its log is elected again and again.
- **#729** `NewRaft` can hang with `RestoreCommittedLogs`. Fix PR #730.

### API and operations

- **#722** `GetConfiguration().Index()` is 0. Fix PR #723. [tested]
- **#727** `LeadershipTransferToServer` accepts a non-voter target. Fix PR #732.
- **#728** `RestoreCommittedLogs` may skip configuration entries. Fix PR #731.
- **#717** A panic at shutdown when the store is closed too early. Fix PR #720.
- **#714** A failed `FileSnapshotSink` write leaves `state.bin` open and the
  `.tmp` directory on disk. Fix PR #715.
- **#174** (since 2016) "Quiet a server that's no longer in the cluster". See Q9.

### Pre-vote and AppliedIndex

- Pre-vote: v1.7.0 had two bugs, fixed in v1.7.1 (#605, #609). No open
  pre-vote issue was found. [tested: release notes, issue list]
- `AppliedIndex`: no open bug. It is defined as "sent to the FSM", not
  "applied" (`api.go:1286-1295`). [tested: Q11] Issue #523 (closed): a node
  replays the log on every restart. That is the expected behavior.

### Snapshot restore

- `FileSnapshotStore` fsyncs the state file, the meta file and the parent
  directory, unless `noSync` is set (`file_snapshot.go:427-435`, `474-476`,
  `525-526`). [code]
- The library does not fsync the log itself. Durability of log, term and vote
  depends on your `LogStore` and `StableStore`. `InmemStore` is not durable.
  [code]
- #522 (open since 2022): if the leader cannot load a snapshot, the cluster does
  not recover. #85: a panic after a restore from an older snapshot.

---

## What this means for part 2

These points follow from the facts above.

1. Pin `github.com/hashicorp/raft v1.8.0` and Go 1.25. Read the Q15 list before
   each upgrade.
2. Wrap every `future.Error()` with the deadline helper from Q3. Map a deadline
   miss, `ErrLeadershipLost` and `ErrRaftShutdown` to HTTP 504 ("outcome
   unknown"). Map `ErrNotLeader`, `ErrEnqueueTimeout` and
   `ErrLeadershipTransferInProgress` to a safe retry (redirect or 503).
3. Keep pre-vote on. Fault-injection transport wrappers must embed the concrete
   transport, or implement `RequestPreVote` and `Close`. Add a test that fails
   when the pre-vote warning appears in the log.
4. Linearizable read: `Barrier()` once per term (after `LeaderCh` says `true`),
   then `VerifyLeader()` per read. Do not add non-voters (issue #725). Stale
   read: local FSM, documented as stale.
5. The FSM tracks its own last applied index. Do not use `AppliedIndex()` as
   proof of apply.
6. `FSM.Apply` never panics. Bad commands return an error value. Version the
   command format inside `Log.Data`.
7. `Snapshot()` copies the map under a lock. `Persist` writes the copy.
8. Membership: `AddNonvoter`, wait for catch-up, then `AddVoter`. Do not rely on
   `GetConfiguration().Index()`.
9. Use `NotifyCh` only with a buffer, or use `LeaderCh()` with one reader.
10. Shutdown order: `LeadershipTransfer()` if leader, `Shutdown().Error()`, then
    close the stores.
11. Tests: 50 ms timings in process. Use the real defaults or larger in Docker.

## Appendix: study tests

Module: `hraftcore`, Go 1.25.5, `github.com/hashicorp/raft v1.8.0`.
Run with `go test -count=1 -v .` in the scratchpad folder.

| File | Tests |
|---|---|
| `helpers_test.go` | `kvFSM`, `faultyStore` (slow or failing `StoreLogs`), `lossyTransport` (drops `AppendEntries` responses after delivery), `interfaceOnlyTransport`, in-memory cluster with `isolate` and `heal`, `waitUntil`, `errorWithin` |
| `apply_test.go` | `TestApplyOnFollowerReturnsErrNotLeaderAndWritesNothing`, `TestApplyTimeoutOnlyLimitsEnqueue`, `TestErrorReturnsAfterLeaderFSMApply`, `TestErrLeadershipLostButEntryCommits`, `TestErrorBlocksWhileLeaderKeepsLease`, `TestIsolatedLeaderStepsDownAndFailsInflight` |
| `election_test.go` | `TestPreVoteNeedsTransportInterface`, `TestDisruptiveRejoin`, `TestVerifyLeaderAndBarrier`, `TestFollowerApplyLag` |
| `config_test.go` | `TestDefaultConfigAndValidateConfig`, `TestTimingsOnLocalhostTCP`, `TestTCPTransportBindAllNeedsAdvertise` |
| `fsm_test.go` | `TestFSMApplyErrorIsOnlyAResponse`, `TestFSMApplyPanicKillsProcess`, `TestSnapshotAndPersistConcurrency`, `TestRestartReappliesLogToFreshFSM` |
| `operations_test.go` | `TestBootstrapSemantics`, `TestMembershipChanges`, `TestRemoveServer`, `TestLeadershipTransfer`, `TestLeaderChKeepsOnlyLatest`, `TestObservability`, `TestShutdownBehaviour` |
| `recovery_test.go` | `TestRecoverClusterResurrectsUncommittedEntry` |

The two fault injectors that matter most:

```go
// faultyStore wraps InmemStore and can delay or fail log writes.
type faultyStore struct {
	*raft.InmemStore
	failWrites      atomic.Bool
	writeDelayNanos atomic.Int64
}

func (store *faultyStore) StoreLog(entry *raft.Log) error {
	return store.StoreLogs([]*raft.Log{entry})
}

func (store *faultyStore) StoreLogs(entries []*raft.Log) error {
	if delay := store.writeDelayNanos.Load(); delay > 0 {
		time.Sleep(time.Duration(delay))
	}
	if store.failWrites.Load() {
		return errors.New("injected disk failure")
	}
	return store.InmemStore.StoreLogs(entries)
}

// lossyTransport embeds the concrete *InmemTransport, so RequestPreVote and
// Close are promoted. It delivers AppendEntries, then can drop the response.
type lossyTransport struct {
	*raft.InmemTransport
	dropAppendResponses atomic.Bool
}

func (transport *lossyTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress,
	args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	err := transport.InmemTransport.AppendEntries(id, target, args, resp)
	if err == nil && transport.dropAppendResponses.Load() {
		return errors.New("injected: response lost after delivery")
	}
	return err
}

// No pipeline, so every AppendEntries goes through the method above.
func (transport *lossyTransport) AppendEntriesPipeline(raft.ServerID, raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}
```

Partitions with `InmemTransport`: `Disconnect(peer)` cuts only the outbound
direction of one transport (`inmem_transport.go:221-238`). Call it on both
sides for a full partition, or on one side for a one-way partition. [code]
