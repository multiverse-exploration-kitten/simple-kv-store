# Lessons from the tests (hashicorp/raft v1.8.0)

Each section is one finding. Each has three parts: what happened, the test that shows it, and what to do in production.
Test names are in `tests/cluster/` unless the name has a path. For library facts and issue numbers, see [hashicorp-raft-reference.md](hashicorp-raft-reference.md) and [hashicorp-raft-storage-and-transport.md](hashicorp-raft-storage-and-transport.md).
General rules: [../../docs/raft-in-production.md](../../docs/raft-in-production.md).
A finding marked "research" comes from the research phase, not from a test of this repo.

## 1. A write with lost responses still commits

**What happened.** The leader's requests reached both followers, but every response was lost. The leader could not count a majority.
It stepped down when its lease ended, and the write failed with `ErrLeadershipLost`. The followers elected a new leader, which had the entry and committed it.
After the heal, every node had the value, at the log index of the "failed" write.

**Test.** `TestAmbiguousWriteLostResponsesStillCommits` (`ambiguous_write_test.go`).

**Production.** Treat `ErrLeadershipLost`, `ErrRaftShutdown` and every timeout after the enqueue as "outcome unknown". Return 504, not "failed".
Only `ErrNotLeader`, `ErrEnqueueTimeout` and `ErrLeadershipTransferInProgress` are safe to retry.

## 2. A blind retry needs CAS or an idempotency key

**What happened.** After finding 1, a CAS retry without an idempotency key got a false `cas_failed`. The "conflict" was with the client itself.
With an `Idempotency-Key`, the retry returned the first result with `Duplicate=true`. The key got one write.

**Tests.** `TestAmbiguousWriteCASRetryWithoutIdempotencyKeyReportsFalseConflict` and `TestAmbiguousWriteIdempotentRetryReturnsOriginalResult`.

**Production.** Keep the dedupe table inside the FSM, so every node gives the same answer (Raft thesis 6.3). Bound it. Here the FSM keeps the last 10,000 keys in FIFO order by log index.
Changing that limit changes the replicated state, so it is a versioned change.

## 3. A 504 can mean "never written"

**What happened.** The leader was cut off from both followers. It accepted a write, because it did not know yet that it lost the quorum. It stepped down after `LeaderLeaseTimeout`.
The write failed with `leadership_lost`. After the heal, no node had the key.

**Test.** `TestPartitionIsolatedLeaderStepsDownAndLosesItsWrite`.

**Production.** The client cannot tell "lost" from "committed" (finding 1). Do not retry a 504 write blindly. Use CAS, an idempotency key, or a read.

## 4. Recovery can bring back a failed write and lose an acknowledged write

**What happened.** The old leader got a 504 for a write that stayed only in its own log. The other two nodes committed another write and returned 200. Then both nodes were lost.
After `RecoverCluster` on the old leader alone, the 504 write was visible and the 200 write was gone.

**Test.** `TestRecoveryCommitsUncommittedEntryAndLosesAcknowledgedWrite` (`recovery_test.go`).

**Production.** `RecoverCluster` commits every entry of the local log, because the commit information of the lost majority is gone. Recover the survivor with the highest last log term and index.
Run it only on stopped nodes, with the same server list on every node that stays. Tell clients that recent writes can change.
Related: `TestRecoveryLoseTwoOfThreeRecoverSurvivorAlone` shows the normal exit from quorum loss. `TestRecoverClusterFailsWhileTheNodeRuns` (`internal/node/recovery_test.go`) shows that a running node holds the bbolt lock, so recovery fails.

## 5. `VerifyLeader()` counts non-voters (#725)

**What happened.** The cluster had 3 voters and 1 non-voter. The two other voters stopped. A linearizable read on the leader succeeded in about 1 ms, because `VerifyLeader()` counted the heartbeat reply of the non-voter.
A write could not commit (504), because commit counts voters only. With the non-voter cut off too, the read failed with `verify_failed`.
The test could not show a stale value. The lease check counts voters only, and raft requires `LeaderLeaseTimeout <= HeartbeatTimeout`. So the old leader steps down before the other voters can elect.

**Test.** `TestMembershipVerifyLeaderCountsNonvoterIssue725` (`membership_test.go`).

**Production.** In v1.8.0 a linearizable read rests on the leader lease (a clock), not only on a quorum round. Keep non-voters only for a short catch-up, as autopilot does. The issue is open, with no fix PR.

## 6. A hung leader disk freezes the cluster and starts no election (#503, #666)

**What happened.** `StoreLogs` blocked on the leader. The leader's main loop was stuck, so its lease check never ran.
The heartbeat goroutines do not touch the disk, so the followers kept the leader. There was no election for 13 election timeouts.
The leader stayed leader in the same term. Every node reported `Ready`. Stale reads with `max_stale` passed on the followers.
The first write failed with 504 at `CommandTimeout`. It committed about 70 ms after the disk recovered.
Also, `raft.Shutdown()` blocks while a `StoreLogs` call hangs, so the test must release the disk before it closes the nodes.

**Test.** `TestDiskStallOnLeaderFreezesClusterWithoutElection` (`disk_stall_test.go`).

**Production.** Health checks on `Ready` and on last contact cannot see this fault. Watch the latency of `raft.leader.dispatchLog` and of writes.
Put a deadline on every raft call. Release the disk or kill the process, because the cluster will not heal itself.

## 7. `VerifyLeader()` blocks past the deadline when its queue is full

**What happened.** raft's verify queue holds 64 requests. During a disk stall, the 65th concurrent linearizable read blocked inside `VerifyLeader()` with no deadline.
In an early experiment, 64 of 70 reads returned `verify_failed` and 6 hung until the disk recovered.
The first fix waited only on `future.Error()`. That was not enough, because the call itself blocks. Now the call and the wait run in one goroutine under the deadline (`waitForCall` in `internal/node/node.go`).

**Test.** `TestDeadlineLinearizableReadsDuringLeaderDiskStall` (`deadline_test.go`). 80 reads must all fail with 503 within `CommandTimeout` plus a small margin.

**Production.** A deadline on `future.Error()` is not enough. Run every raft call under the deadline.
Each stuck call keeps one goroutine until raft answers. With steady read traffic during a long stall, these goroutines grow without a bound. Add load shedding.

## 8. `raft.Snapshot()` also blocks during a disk stall

**What happened.** `Snapshot()` waits on an unbuffered channel. The first user snapshot waited for the stuck main loop, so a second `Snapshot()` call hung until the disk recovered.
It is the same class as finding 7.

**Test.** `TestDeadlineSnapshotDuringLeaderDiskStall`.

**Production.** Wrap `Snapshot()` like every other raft call. `GetConfiguration()` answers inline and does not block. This repo wraps it anyway, for one rule.

## 9. The enqueue timeout is the only proof of "not in the log"

**What happened.** During a disk stall, a second write could not enter the log. It first came back as 504 `timeout`, not 503 `enqueue_timeout`.
`Apply(data, enqueueTimeout)` used the whole budget, and our own deadline fired at the same time. The write was safe, but the class was wrong.
After the first fix, the two timers raced. A test that checked only the class passed even with the bug.
The fix gives raft half of the time until the deadline as the enqueue timeout (`enqueueTimeoutFor`). The code refuses a value of 0 or less before the call, because raft reads 0 as "no timeout".

**Test.** `TestDeadlineWriteThatNeverEnteredTheLogIsRejected`. It asserts that the 503 arrives before `CommandTimeout`, with no slack. A write and a membership change must both be rejected, and the new key and server must not exist after the disk recovers.

**Production.** Only `ErrEnqueueTimeout` proves that an entry never entered the log. It must fire well before your own deadline. Else the client gets 504 for a write that cannot commit.

## 10. After a full restart the FSM is empty until an entry of the new term commits

**What happened.** All 3 nodes stopped and started again. The data was on disk, but every FSM stayed empty (last applied index 0).
raft does not apply the log before it learns the commit index. A new leader learns it only when an entry of its own term commits.
The test blocked `StoreLogs` to hold this window open. A linearizable read failed with `not_ready`. After the entries committed, all 10 keys came back with their old indexes.

**Test.** `TestDurabilityFullClusterRestartServesLinearizableReadsOnlyAfterBarrier` (`durability_test.go`).

**Production.** After the election, run `Barrier()` and set a ready flag for that term (Consul `leaderLoop`). Serve linearizable reads only after that.
Do not wait for `LastAppliedIndex >= raft.LastIndex()`. The FSM sees only command entries, not barrier, no-op and configuration entries.

## 11. Pre-vote stops the term jump

**What happened.** A follower cut off for a while campaigned again and again. With pre-vote on, every campaign failed and the term stayed the same. After the heal, the same leader led in the same term.
With `PreVoteDisabled`, the cut node climbed at least 3 terms. After the heal, the leader stepped down. In one run the cut node went from term 2 to 5, and the leader was in term 9 after the heal.
Also: the fault-injection wrapper must forward `WithPreVote`. A wrapper that hides it turns pre-vote off, and a mutation run showed a cut follower at term 47.

**Tests.** `TestPreVoteDisruptiveRejoinKeepsLeaderAndTerm`, `TestPreVoteDisabledDisruptiveRejoinForcesNewElection`, `TestSmokePreVoteStaysOnThroughFaultTransport`.

**Production.** Keep pre-vote on (the default). Make every transport wrapper implement `RequestPreVote`. The harness fails a test when raft logs a pre-vote warning about the transport.

## 12. The leader state is not proof of leadership

**What happened.** The old leader was cut off. A `TimeoutNow` RPC made one follower start an election at once, which stands for an old leader whose process paused. The new leader committed a new value.
Two nodes were in the leader state at the same time. A `leader` read on the old leader returned the old value. A `linearizable` read on it failed, because `VerifyLeader()` got no quorum.
A forged `TimeoutNow` from a non-member also passed the fault rules in an early run.

**Test.** `TestReadConsistencyIsolatedOldLeaderServesStaleLeaderReads` (`read_consistency_test.go`).

**Production.** `State() == Leader` can be old. Use `VerifyLeader()` for every linearizable read. Document that the `leader` mode (Consul "default") can be stale.

## 13. A stale read has no bound on missed writes

**What happened.** A cut follower returned the old value. `max_stale` rejected the read with `too_stale` after the follower had not heard the leader for long enough.

**Tests.** `TestReadConsistencyStaleReadOnPartitionedFollower`, `TestQuorumLossWritesFailFastAndStaleReadsServeOldData`.

**Production.** `max_stale` bounds the time since the last leader contact. It does not bound how many writes the node missed. Return the age in headers (`X-KV-Last-Contact`, `X-KV-Index`) and let the client decide.

## 14. Without a majority the cluster fails closed, if every wait has a deadline

**What happened.** With 2 of 3 nodes stopped, the write failed within `CommandTimeout` (503 or 504). The node stepped down after the lease. A later write failed at once with `no_leader`.
Linearizable and `leader` reads failed. A stale read returned the old value.
In the Docker cluster the write got 504 `leadership_lost` after about 0.4 s in 3 of 4 runs. In 1 run it got 503 after the 5 s hold. The linearizable read was always 503 after the 5 s hold.

**Tests.** `TestQuorumLossWritesFailFastAndStaleReadsServeOldData`, `TestDockerCluster/WithoutMajorityRequestsFailFast` (`tests/e2e/e2e_test.go`).

**Production.** Plan for both 503 and 504 in this state. Set the HTTP hold time and the command timeout so that a request ends in a known time.

## 15. A removed follower is not shut down

**What happened.** After `RemoveServer`, the follower kept running. It logged that it was not in the configuration and started no election. Its term stayed the same, `Ready()` was `no_leader`, and a stale read returned old data.

**Test.** `TestMembershipRemovedNodeGetsNoNewWrites`.

**Production.** Stop a removed node yourself. This repo sets `ShutdownOnRemove=false` (as Consul and Nomad), so a removed leader becomes a follower and does not exit.

## 16. `AddVoter` adds a full voter at once

**What happened.** The test added node4 as a non-voter, waited until it had all data, then promoted it. With 4 voters the quorum is 3. With 2 of 4 stopped, writes failed fast.
Replacing a dead node needs `RemoveServer` and a new ID with an empty disk.

**Tests.** `TestMembershipAddFourthNodeAsNonvoterThenPromote`, `TestMembershipReplaceFailedNode`.

**Production.** Add a non-voter first. Promote it after it caught up. A 4th voter does not add fault tolerance. Do not bring a dead server back under its old ID with an empty disk.

## 17. Only CAS makes read-modify-write safe

**What happened.** 6 goroutines incremented one counter through different nodes with stale reads. The final value equaled the number of successful CAS writes on every node.
A mutation without CAS gave a counter of 5 after 30 "successes".
The test uses 5 increments for each worker. On macOS, fsync is slow, and 10 increments took 11.7 s.

**Test.** `TestConcurrencyCASIncrementsThroughDifferentNodes`.

**Production.** Use the log index as the version (Consul `ModifyIndex`). Tell clients to use `?cas=`.

## 18. A leadership transfer is a short gap, not an outage

**What happened.** A hot writer ran on the old leader during the transfer. Each transfer gave 228 to 1998 `leadership_transfer_in_progress` rejections. Then `no_leader` (the old leader does not know the new leader yet), never `not_leader`.
Every successful put was on the new leader. No rejected put was.

**Test.** `TestTransferLeadershipToChosenFollower` (`transfer_test.go`).

**Production.** Transfer leadership before you stop a leader on purpose. Map `ErrLeadershipTransferInProgress` to 503, not 504. The binary does this on SIGTERM. In Docker the transfer took about 8 ms.

## 19. A lagging follower gets a snapshot, and the leader tries once per snapshot

**What happened.** With `TrailingLogs` 10 and a threshold of 20, a follower that missed 100 writes got `InstallSnapshot`. It logged "Installed remote snapshot".
Research: while a follower is down, the leader tries `InstallSnapshot` once for each newer snapshot and logs "connection refused". Replication backoff starts at 10 ms and doubles up to 10.24 s.
After the heal, allow seconds for catch-up.

**Tests.** `TestSnapshotLaggingFollowerCatchesUpThroughInstallSnapshot`, `TestReplicationRestartedFollowerCatchesUpFromLog`.

**Production.** Size `TrailingLogs` to cover a normal restart. A raft snapshot index counts barrier, no-op and configuration entries. It is not the FSM applied index.

## 20. `raft.Snapshot()` with nothing new still writes a snapshot

**What happened (research).** `ErrNothingNewToSnapshot` comes only when the FSM applied nothing at all. A second `Snapshot()` without new entries made a second snapshot at the same index. With `retain` 2, it can push the older good snapshot out.
This repo maps the error to 409 `nothing_to_snapshot` (`TestSnapshot` in `internal/httpapi/api_test.go`).

**Production.** Do not call `Snapshot()` on a timer. Call it only after new writes.

## 21. Go's HTTP client replays a request that has an `Idempotency-Key`

**What happened.** Found while reading the Go code in the HTTP phase. `http.Transport` replays a request that has an `Idempotency-Key` header on a reused connection after a read error.
A forwarded write with that header can reach the leader twice. The FSM dedupes it. Without the header, Go does not replay it. No test sends this replay.

**Production.** Put the idempotency key on every write, so a hidden replay is harmless. Do not rely on "one request is one write" over a pooled connection.

## 22. A forward over a dead pooled connection gives 504

**What happened.** After the leader dies, a follower can pick a pooled dead connection. That counts as "after the request left", so a write gets 504, not 503. A SIGKILLed container's IP also black-holes: dials end with `i/o timeout` after the full transport timeout (10 s), not with "connection refused".

**Test.** `TestDockerCluster/KilledLeaderIsReplaced` (`tests/e2e/e2e_test.go`). The unit test `TestForwardBrokenAfterSendIs504ForWrites503ForReads` (`internal/httpapi/forward_test.go`) pins the 504 for writes.

**Production.** Expect short 504 answers from followers after a crash. Retry with the same idempotency key.

## 23. TLS 1.3 rejects a bad client late

**What happened.** In TLS 1.3 the client finishes its handshake before the server checks the client certificate. A rejected client gets a working `Dial`. The error comes on the first read.
In raft it looks like `msgpack decode error [pos 0]: remote error: tls: unknown certificate authority`. The server logs `failed to verify certificate: x509: certificate signed by unknown authority`.
Go also checks the host name before the chain. A certificate for the wrong IP gives `x509.HostnameError` even when the CA is right.

**Tests.** `TestClientWithCertificateFromAnotherCAIsRejected`, `TestClientWithoutCertificateIsRejected`, `TestServerCertificateForAnotherIPIsRejected` (`internal/tlsstream/streamlayer_test.go`), and `TestTLSPeerWithCertificateFromAnotherCACannotJoin`.

**Production.** Test a rejection on the first read, not on `Dial`. The log of the leader shows a failed handshake on every retry with a non-voter it cannot verify. The configuration change still commits.

## 24. Raft RPCs have no authentication

**What happened.** A mutation run with the client-certificate check off accepted a forged heartbeat (`AppendEntries`) with a term 100 higher. The response term was 102. raft takes the term and leader of any `AppendEntries` with a higher term.
With mutual TLS, every RPC from a peer with a certificate from another CA failed, and no node took the forged term.

**Test.** `TestTLSPeerWithCertificateFromAnotherCACannotSendRPCs` (`tls_test.go`).

**Production.** Use mutual TLS for the Raft port, and fail closed. `NewStreamLayer` returns an error when client certificates are not required.
Fixed later (lesson 32): the server side of the handshake now has a deadline. Plain TCP still has no deadline.

## 25. Process pauses under `-race` load break short lease tests

**What happened.** With 4 parallel `-race` test processes, one process logged nothing for 230 ms. A leader with a 100 ms lease and one follower stepped down. This is right for Raft. But it broke tests that assert "no leader change".

**Tests.** `TestSmokePreVoteStaysOnThroughFaultTransport` and every test with 500 ms or 1 s timings (`electionStableTiming`, `partitionTiming`).

**Production.** A lease shorter than a possible pause (GC, VM stall, `docker pause`) causes leader changes. Use the default timings (1 s) in production. Scale them with `KV_TIMING_MULTIPLIER`.

## 26. A checker that only passes proves nothing

**What happened.** Porcupine checked 492 to 682 operations under random faults, and found no violation. To test the checker, we made two mutations. A workload with `stale` reads gave `Illegal`.
A recorder that dropped "outcome unknown" writes as failed gave `Illegal` in 3 of 3 runs. Some of those writes always committed, and a later read returned them.
Porcupine 1.3.1 has no pending operations. An open operation gets `Return = math.MaxInt64` and the model accepts any state for its output.

**Tests.** `TestLinearizabilityPutAndGetUnderFaults`, `TestLinearizabilityCheckerFindsStaleRead`, `TestLinearizabilityModelOpenAndRejectedWrites`.

**Production.** Record a timed-out write as "maybe". Drop only a write that got a 503 rejection. A test that can never fail is not a test.

## 27. A dial to a killed leader must time out fast

**What happened.** We killed the leader with SIGKILL and at once sent a read to a follower. The answer was 503 `leader_unreachable`, but a new leader existed after about 2 s. The forward client used a clone of Go's default transport. Its dial to the dead container's IP hung until the 10 s forward timeout, so the 5 s hold window ended before the retry. With a 1 s dial timeout, the same read returned 200 after 2.94 s from the new leader.

**Tests.** `TestHoldRetriesWhenDialToLeaderTimesOut` (`internal/httpapi`), e2e subtest `KilledLeaderIsReplaced` (`tests/e2e/e2e_test.go`).

**Production.** Give the forward path its own short connect timeout. A connect timeout sends no byte, so a retry is safe.
Known gap: a paused leader (`docker pause`) sends no FIN. A pooled keep-alive connection to it waits until the forward timeout. A write then gets 504 and a read gets 503.

## 28. A busy follower does not say "not leader"

**What happened.** A test stalls the `StoreLogs` of one follower. Its raft main loop blocks inside an AppendEntries call. `raft.Apply` on a follower waits for the main loop to answer `ErrNotLeader`. The blocked loop does not answer, so `Apply` ends with `ErrEnqueueTimeout`. The API gave 503 `enqueue_timeout` and did not forward the write. `AddNonvoter`, `RemoveServer` and `LeadershipTransfer` did the same.

**Tests.** `TestBusyFollowerWriteIsForwardedToLeader` (`tests/cluster/busy_follower_test.go`).

**Production.** Check `raft.State()` before `Apply`, before a membership change and before a leadership transfer. `State()` is an atomic read and does not need the main loop. Then a busy follower answers `not_leader` at once, and the HTTP layer forwards the request.

## 29. A new leader may not know the commit index yet

**What happened.** The old leader acknowledged a write. The new leader had the entry in its log but did not apply it yet. A `leader` read on the new leader returned the old value. A follower learns the commit index only from AppendEntries calls with entries. In hashicorp/raft v1.8.0 a heartbeat carries only the term and the leader (`replication.go:390`). The new leader applies the entries of old terms only after it commits an entry of its own term (the barrier).

**Tests.** `TestLeaderReadOnNewLeaderWaitsForBarrier` (`tests/cluster/leader_read_test.go`).

**Production.** Do not serve a local read on a new leader before the barrier of its term is done. The `leader` mode now answers 503 `not_ready` before the barrier. A `linearizable` read checks the barrier after `VerifyLeader()`, in the term from before the verify, as Consul does. No test can force the bad order from the outside, so that part has no test.

## 30. JSON escaping makes a Raft entry bigger than the value

**What happened.** Go's `json.Marshal` writes `<`, `>`, `&` and control characters as `<` (6 bytes). A 512 KiB value of `<` made a 3 MiB Raft entry. hashicorp/raft has `SuggestedMaxDataSize` = 512 KiB (`api.go:30`), but it does not enforce it.

**Tests.** `TestPutRejectsValueWhoseEncodedCommandIsTooLarge`, `TestPutAcceptsLargestPlainValueWithLongestEscapedKeys` (`internal/node/node_test.go`), `TestEncodeCommandBytesStayTheSame` (`internal/fsm`).

**Production.** Limit the size of the encoded entry, not only the size of the input. Do not change the encoding to fix it, because old nodes must read the new entries during a rolling upgrade. We kept the bytes the same and added a 1 MiB limit on the encoded command (413).

## 31. raft does not check the target of a membership call

**What happened.** `LeadershipTransferToServer` accepts a non-voter (issue #727). It forces an election and returns success. `AddNonvoter` with the ID of an existing voter keeps the server a voter but rewrites its address. A wrong address then breaks the quorum.

**Tests.** `TestTransferLeadershipToNonvoterIsRejected` (`tests/cluster/transfer_test.go`), `TestMembershipAddNonvoterOnExistingVoterIsRejected` (`tests/cluster/membership_test.go`).

**Production.** Read the configuration and check the suffrage before you call raft. Both calls now give 400. Known gap: the check and the call are not atomic. The `prevIndex` argument of raft can make them atomic. We pass 0.

## 32. A silent peer kept a Raft connection open forever

**What happened.** A test opened a TCP connection to the Raft port and sent nothing. The TLS stream layer returned the connection from `Accept`, and the handshake ran at the first read. raft's connection goroutine waited in that read with no deadline, so the connection and the goroutine stayed forever. Many such connections use up file descriptors.

**Tests.** `TestSilentInboundConnectionIsClosedAfterTheHandshakeTimeout`, `TestAcceptDoesNotWaitForTheHandshake`, `TestIdleConnectionAfterTheHandshakeStaysOpen`, `TestDialWithoutTimeoutStillLimitsTheHandshake` (`internal/tlsstream/handshake_test.go`).

**Production.** Do not run the handshake inside `Accept`. raft has one accept loop, so one slow client would stop all new connections. The first read or write of the connection runs the handshake with a 5 s deadline and then clears the deadline. A connection that is idle after the handshake stays open, because raft keeps pooled connections.

## 33. The HTTP server timeouts must fit the hold and the forward

**What happened.** The `http.Server` had no `ReadTimeout`, `WriteTimeout` or `IdleTimeout`. A client that sends a body very slowly kept a connection and a goroutine. A `WriteTimeout` that is too short cuts a request that waits for a leader (hold, 5 s) and then is forwarded (forward timeout).

**Tests.** `TestHTTPServerTimeoutsCoverTheHoldAndTheForward` (`cmd/kvstore/serve_test.go`).

**Production.** `ReadHeaderTimeout` 10 s, `ReadTimeout` 30 s, `IdleTimeout` 2 min. `WriteTimeout` = `ReadTimeout` + hold + forward timeout (45 s), derived from the config, not a fixed guess. `ReadTimeout` does not stop a handler that runs. The server `IdleTimeout` is longer than the 90 s idle timeout of the forward client, so the client closes an idle connection first.
