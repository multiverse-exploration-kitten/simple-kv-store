# Part 2: Raft key-value store on hashicorp/raft

This is part 2 of 3 of a practice project. It is a 3-node replicated key-value store.
It uses Go 1.25, hashicorp/raft v1.8.0, raft-boltdb, mutual TLS and Docker Compose.
The goal is to learn how to run Raft correctly in production, not only how the algorithm works.
Part 2 copies the patterns of Consul, Nomad and Vault.
The tests inject real faults (crash, pause, partition, disk stall) and show what happens.

Run every command on this page from the `part2-hashicorp-raft/` directory.

## Architecture

```
HTTP client -> httpapi (forward to leader if needed) -> node.Node -> raft.Raft --mTLS--> 2 peers
                                                          |            +-- raft-boltdb (log + stable)
                                                          |            +-- FileSnapshotStore
                                                          +-> fsm.FSM -> fsm.State (pure, deterministic)
```

- hashicorp/raft accepts a write only on the leader. It does not forward.
  The HTTP layer forwards a request to the leader once (`X-KV-Forwarded-From` stops loops).
- While the node knows no leader, the HTTP layer holds the request up to 5 s and tries again. It does this only for errors that changed nothing.
- Every raft call runs under a deadline (`KV_COMMAND_TIMEOUT`, default 5 s). A miss is 504, outcome unknown.
- The default read is linearizable: the leader must be ready (barrier done in its term) and `VerifyLeader()` must pass.
- The FSM keeps the last 10,000 idempotency keys, so a retry of a lost write is safe.
- One process per node. The Raft port 7000 uses mutual TLS (TLS 1.3, client certificates required) and stays inside the Docker network.
- Each node keeps `raft.db` (log and stable store, fsync on) and its snapshots on its own Docker volume.
- 3 nodes: a majority of 3 is 2, so one node can fail. A 4th node raises the majority to 3 and still tolerates only one failure.

## Quick start

Start the cluster. It builds the image and waits until all 3 nodes are healthy.

```bash
make up
```

Show the status of each node.

```bash
make status
```

Write through any node. A follower forwards the write.

```bash
curl -X PUT http://127.0.0.1:8101/kv/greeting -H 'Content-Type: application/json' -d '{"value": "hello"}'
```

Read it. The default read is linearizable.

```bash
curl http://127.0.0.1:8102/kv/greeting
```

Read from local memory of one node (stale read).

```bash
curl 'http://127.0.0.1:8102/kv/greeting?consistency=stale'
```

Compare-and-set. It succeeds only if `modify_index` is still 7. Use `cas=0` for "the key must not exist".

```bash
curl -X PUT 'http://127.0.0.1:8101/kv/greeting?cas=7' -H 'Content-Type: application/json' -d '{"value": "hi"}'
```

A write with an idempotency key. A retry with the same key and the same request returns the first result.

```bash
curl -X PUT http://127.0.0.1:8101/kv/order-1 -H 'Idempotency-Key: abc-1' -H 'Content-Type: application/json' -d '{"value": "x"}'
```

Stop the cluster and delete its data and certificates.

```bash
make down
```

## API

The HTTP ports on the host are 8101 (node1), 8102 (node2) and 8103 (node3).

| Method | Path | Notes |
|---|---|---|
| PUT | `/kv/{key}` | Body `{"value": "..."}`. Query `?cas=N`. Header `Idempotency-Key`. |
| GET | `/kv/{key}` | Query `?consistency=linearizable\|leader\|stale`, `?max_stale=5s`. |
| GET | `/kv` | Query `?prefix=`, `?consistency=`. Returns `{"entries": [...]}`. |
| DELETE | `/kv/{key}` | Query `?cas=N`. Header `Idempotency-Key`. `cas=0` means "the key must not exist". |
| GET | `/status` | State, term, leader, indexes, key count. |
| GET | `/health/live` | 200 when the process runs. |
| GET | `/health/ready` | 200, or 503 with the reason. |
| GET | `/health/leader` | 200 on a ready leader, 503 on a leader before its barrier, 429 on a follower. |
| GET | `/cluster/servers` | List the Raft configuration. |
| POST | `/cluster/servers` | Body `{"id","address","voter"}`. `voter` defaults to false (non-voter first). |
| DELETE | `/cluster/servers/{id}` | Delete a server from the configuration. |
| POST | `/cluster/transfer-leadership` | Optional body `{"id"}`. |
| POST | `/admin/snapshot` | Take a snapshot now. |
| GET | `/metrics` | Prometheus text. |

Keys may contain `/`. A key has 1 to 512 bytes. A value has up to 512 KiB.

### Read consistency

| Mode | Where it runs | Can it be stale? |
|---|---|---|
| `linearizable` (default) | Leader only. Needs a ready leader and `VerifyLeader()`. A follower forwards it. | No. |
| `leader` | Leader only. No quorum check (Consul "default"). | Yes, on an old leader that does not know it lost the lease. |
| `stale` | Any node, from local memory. `max_stale` bounds the time since the last leader contact. | Yes. `max_stale` does not bound how many writes the node missed. |

Every read answer has the headers `X-KV-Index`, `X-KV-Known-Leader`, `X-KV-Last-Contact` (ms) and `X-KV-Served-By`.

### Version of a key and CAS

`create_index` and `modify_index` are Raft log indexes. Use `?cas=<modify_index>` for read-modify-write.
Do not read, edit and write back without CAS.

### Error codes

| Code | Meaning | Body |
|---|---|---|
| 400 | Bad input (key, `cas`, `max_stale`, UTF-8). | `{"error":"invalid_request","field","message"}` |
| 404 | Key not found. | `{"error":"key_not_found","key"}` |
| 409 | CAS failed. Also `nothing_to_snapshot` on `/admin/snapshot`. | `{"error":"cas_failed","current_modify_index":N}` (0 when the key does not exist) |
| 413 | Value too large. | `{"error":"too_large",...}` |
| 422 | Same `Idempotency-Key` with another request. | `{"error":"idempotency_key_reused"}` |
| 503 | Known not applied. A retry is safe. Header `Retry-After: 1`. | `{"error":"command_rejected","reason","retry_safe":true}` |
| 504 | Outcome unknown. The write may still commit. | `{"error":"outcome_unknown","reason","retry_safe":false}` |

- With an `Idempotency-Key`, a 504 body adds `"retry_with_same_idempotency_key": true`.
- A duplicate answer has the header `X-KV-Duplicate: true`.
- 503 reasons include `not_leader`, `no_leader`, `enqueue_timeout`, `leadership_transfer_in_progress`, `not_ready`, `too_stale`, `verify_failed`, `shutdown`, `leader_unreachable`.
- 504 reasons include `timeout`, `leadership_lost`, `shutdown`, `forward_failed`, `unknown_error`.
- A forward that fails before the request left (dial error) is 503. A forward that fails after it left is 504 for writes and 503 for reads.

## Configuration

The node reads these environment variables. A bad value is an error that names the variable.

| Variable | Default | Meaning |
|---|---|---|
| `KV_NODE_ID` | required | Node ID. |
| `KV_RAFT_ADDRESS` | required | Advertised Raft address. An IP and a port. |
| `KV_RAFT_BIND_ADDRESS` | `0.0.0.0:7000` | Raft listen address. |
| `KV_HTTP_ADDRESS` | `:8000` | HTTP listen address. |
| `KV_DATA_DIR` | `/data` | Log, stable store and snapshots. |
| `KV_BOOTSTRAP` | `false` | Bootstrap with `KV_PEERS` when the node has no state. |
| `KV_PEERS` | none | `id=raftAddress,...` |
| `KV_PEER_HTTP` | none | `id=http://host:port,...` Used for forwarding. |
| `KV_TIMING_MULTIPLIER` | `1` | Scales all Raft timeouts (Consul `raft_multiplier`). |
| `KV_COMMAND_TIMEOUT` | `5s` | Deadline for each write and each raft call. |
| `KV_RAFT_TLS_CA_FILE`, `KV_RAFT_TLS_CERT_FILE`, `KV_RAFT_TLS_KEY_FILE` | empty | Mutual TLS files. |
| `KV_RAFT_INSECURE_PLAINTEXT` | `false` | Plain TCP for Raft. Without it, you must set the TLS files. |
| `KV_LOG_LEVEL`, `KV_LOG_JSON` | `info`, `false` | Logging. |

## Tests

Three levels, as in `../AGENTS.md`.

| Level | Where | Needs |
|---|---|---|
| Unit | `internal/**/*_test.go` | Nothing. |
| Cluster | `tests/cluster/` | Real Raft nodes in one process on `127.0.0.1`, with fault injection. Real stores in `t.TempDir()`. |
| E2E | `tests/e2e/` (build tag `e2e`) | Docker. Own project `kvstore-part2-e2e`, subnet 172.28.12.0/24, ports 18101-18103. |

```bash
make lint      # gofmt, go vet, staticcheck (the e2e package too)
make format    # gofmt -w
make test      # go test -race ./...   (unit + cluster, no Docker)
make e2e       # builds the image, starts its own cluster, runs the scenarios, deletes it
```

Run one cluster test.

```bash
go test -race -count=1 ./tests/cluster/ -run 'TestDiskStall' -v
```

The Porcupine linearizability test prints its seed. Set `LINEARIZABILITY_SEED` to replay it.
Set `LINEARIZABILITY_VISUALIZATION_DIR` to keep the HTML visualization.

Tests that assert "no leader change" use 500 ms timeouts. Under parallel `-race` load a test process can pause for 230 ms.

## Chaos commands

Run `make` alone to see all targets. `NODE` is `node1`, `node2` or `node3`.

```bash
make status                  # /status of each node, one line each
make logs                    # follow the logs (all nodes, or NODE=node2)
make kill-leader             # SIGKILL the current leader
make start NODE=node1        # start a stopped or killed node
make pause NODE=node2        # docker pause (freeze the process)
make unpause NODE=node2
make partition NODE=node1    # disconnect NODE from the Raft network
make heal NODE=node1         # connect NODE again with its static IP
```

Example session.

```bash
make up
make kill-leader
make status                  # a new leader in a higher term
make start NODE=node1        # use the node that make kill-leader printed
make down
```

The Raft network `kvstore-part2_raft` uses the subnet 172.28.11.0/24. Change it with `KV_SUBNET_PREFIX`.
Change the HTTP host ports with `KV_NODE1_HTTP_PORT`, `KV_NODE2_HTTP_PORT` and `KV_NODE3_HTTP_PORT`.

## Failure scenarios and tests

Each failure test has a doc comment with Scenario, Expected and Raft lesson.
Cluster tests are in `tests/cluster/`. E2E tests are in `tests/e2e/e2e_test.go` (one `TestDockerCluster`, six subtests).
The findings with more detail are in [docs/lessons-from-tests.md](docs/lessons-from-tests.md).

| Failure scenario | Test | Raft lesson |
|---|---|---|
| 3 or 5 nodes start | `TestElectionExactlyOneLeaderThatAllNodesAgreeOn` | A term has at most one leader. |
| Same start, simple check | `TestSmokeThreeNodesElectOneLeader` | All initial nodes can bootstrap with one identical list. |
| Write on the leader | `TestSmokeWriteReplicatesToAllNodes` | Apply returns after commit. Followers apply later. |
| Leader crash | `TestElectionLeaderStopSurvivorsElectNewLeaderInHigherTerm` | 2 of 3 is a majority. The new leader has every committed entry. |
| Write sent to a follower | `TestReplicationWriteOnFollowerIsRejectedWithLeaderID` | Only the leader appends. `not_leader` is safe to retry. |
| One follower down | `TestReplicationOneFollowerDownWritesStillCommit` | A majority commits. This is why cluster sizes are odd. |
| Follower restart | `TestReplicationRestartedFollowerCatchesUpFromLog` | The leader sends the missing entries from its log. |
| Log index as version | `TestReplicationWriteIndexesGrowAndAllNodesApplySameIndex` | The log index is one total order. Use the FSM applied index as proof of apply. |
| Follower misses many writes | `TestSnapshotLaggingFollowerCatchesUpThroughInstallSnapshot` | Compaction deletes entries. `TrailingLogs` is the margin. |
| All nodes restart | `TestDurabilityFullClusterRestartServesLinearizableReadsOnlyAfterBarrier` | Data is on disk. The FSM is empty until an entry of the new term commits. |
| 2 of 3 nodes down | `TestQuorumLossWritesFailFastAndStaleReadsServeOldData` | Fail closed and fast. Stale reads still work. |
| Leader isolated | `TestPartitionIsolatedLeaderStepsDownAndLosesItsWrite` | The isolated leader steps down after one lease. Its 504 write is lost. |
| Old leader still in leader state | `TestReadConsistencyIsolatedOldLeaderServesStaleLeaderReads` | `State() == Leader` is not proof. Only `VerifyLeader()` makes a read linearizable. |
| Follower cut off | `TestReadConsistencyStaleReadOnPartitionedFollower` | A stale read can be old. `max_stale` bounds time, not missed writes. |
| Follower cut off, pre-vote on | `TestPreVoteDisruptiveRejoinKeepsLeaderAndTerm` | Pre-vote stops a disruptive rejoin. |
| Follower cut off, pre-vote off | `TestPreVoteDisabledDisruptiveRejoinForcesNewElection` | Without pre-vote the term jumps and the leader steps down. |
| Pre-vote through a wrapper | `TestSmokePreVoteStaysOnThroughFaultTransport` | A transport wrapper must forward `WithPreVote`. |
| Follower catches up after a heal | `TestSmokePartitionedFollowerCatchesUpAfterHeal` | A majority commits. A healed node gets the log. |
| Responses lost | `TestAmbiguousWriteLostResponsesStillCommits` | A timeout is not a failure. The write commits. |
| Retry of a CAS write | `TestAmbiguousWriteCASRetryWithoutIdempotencyKeyReportsFalseConflict` | CAS stops a double write but gives a false conflict. |
| Retry with an idempotency key | `TestAmbiguousWriteIdempotentRetryReturnsOriginalResult` | The dedupe table in the FSM makes a retry safe and exact. |
| Many clients increment a counter | `TestConcurrencyCASIncrementsThroughDifferentNodes` | Only CAS makes read-modify-write safe. |
| Leader disk hangs | `TestDiskStallOnLeaderFreezesClusterWithoutElection` | No failover (#503, #666). Only our own deadline keeps the API from hanging. |
| 80 reads during a disk stall | `TestDeadlineLinearizableReadsDuringLeaderDiskStall` | `VerifyLeader()` blocks when its queue of 64 is full. |
| Write during a disk stall | `TestDeadlineWriteThatNeverEnteredTheLogIsRejected` | Only `ErrEnqueueTimeout` proves "not in the log". It must fire before our deadline. |
| Snapshot during a disk stall | `TestDeadlineSnapshotDuringLeaderDiskStall` | `raft.Snapshot()` blocks too. Run the call under the deadline. |
| Leadership transfer under load | `TestTransferLeadershipToChosenFollower` | Transfer is a short gap. A rejected write is safe to retry. |
| Add a 4th node | `TestMembershipAddFourthNodeAsNonvoterThenPromote` | Add a non-voter first. A 4th voter raises the quorum to 3. |
| Replace a dead node | `TestMembershipReplaceFailedNode` | Delete it from the configuration. Add a new ID with an empty disk. |
| Remove a running node | `TestMembershipRemovedNodeGetsNoNewWrites` | A removed follower is not shut down. |
| Leader with a non-voter | `TestMembershipVerifyLeaderCountsNonvoterIssue725` | `VerifyLeader()` counts non-voters (#725). |
| Lose 2 of 3 nodes for good | `TestRecoveryLoseTwoOfThreeRecoverSurvivorAlone` | `RecoverCluster` is the manual exit from quorum loss. |
| Recovery after a 504 | `TestRecoveryCommitsUncommittedEntryAndLosesAcknowledgedWrite` | Recovery can bring back a failed write and lose an acknowledged write. |
| Write through a follower (HTTP) | `TestHTTPWriteThroughFollowerIsForwardedToLeader` | The application must forward to the leader. |
| Stale read on a follower (HTTP) | `TestHTTPStaleReadOnFollowerIsServedLocally` | A stale read skips the leader. The headers show the age. |
| Wrong CA, forged heartbeat | `TestTLSPeerWithCertificateFromAnotherCACannotSendRPCs` | Raft RPCs have no authentication. Mutual TLS must reject the peer first. |
| Wrong CA, node joins | `TestTLSPeerWithCertificateFromAnotherCACannotJoin` | A node without a cluster certificate gets no log. |
| 3 nodes over mutual TLS | `TestTLSClusterElectsLeaderAndReplicates` | A TLS stream layer secures every RPC. |
| Random faults, checked | `TestLinearizabilityPutAndGetUnderFaults` | The whole error contract must hold. Porcupine checks it. |
| Stale read, checked | `TestLinearizabilityCheckerFindsStaleRead` | The checker can fail. It finds a real stale read. |
| Outcome unknown in the history | `TestLinearizabilityModelOpenAndRejectedWrites` | Keep a timed-out write as "maybe". Drop only a rejected write. |
| E2E: replicate | `TestDockerCluster/ReplicatesToEveryNode` | A late follower can be behind. The default read is linearizable. |
| E2E: write on a follower | `TestDockerCluster/WriteThroughFollowerIsForwarded` | One hop to the leader. A stale read stays local. |
| E2E: leader killed (SIGKILL) | `TestDockerCluster/KilledLeaderIsReplaced` | A crash has no transfer. The followers wait for an election. |
| E2E: follower frozen (`docker pause`) | `TestDockerCluster/PausedFollowerCatchesUp` | A frozen node is a slow follower. |
| E2E: leader disconnected | `TestDockerCluster/DisconnectedLeaderRejoinsAsFollower` | The majority works without the old leader. The old one rejoins as follower. |
| E2E: 2 nodes stopped | `TestDockerCluster/WithoutMajorityRequestsFailFast` | 503 or 504 in under 10 s. Stale reads still work. |

## Part 1 (pysyncobj) vs part 2 (hashicorp/raft)

| Topic | Part 1: pysyncobj | Part 2: hashicorp/raft |
|---|---|---|
| Language, web layer | Python 3.12, FastAPI | Go 1.25, `net/http` |
| Write on a follower | The library forwards. | The application forwards (one hop, hold while no leader). |
| Linearizable read | One log entry for each read. | `VerifyLeader()` after a barrier in the current term. No log entry. |
| Three read modes | linearizable, stale | linearizable, leader, stale with `max_stale` |
| Pre-vote | None. A rejoining node disrupts the leader. | On. `PreVoteDisabled` shows the old behavior. |
| Disk | Memory-mapped journal, no fsync. | raft-boltdb with fsync. Snapshots with fsync. |
| Peer traffic | Pickle with a shared password. | msgpack over mutual TLS 1.3. |
| Duplicate writes | None. A 504 needs CAS or a read. | `Idempotency-Key` in the replicated state. |
| Command format | Methods numbered by sorted name. | 1 type byte plus JSON, with an "ignorable" flag. |
| Bad command in the FSM | Stops the log on every node. | The leader checks input first. An unknown command panics on purpose (fail-stop). |
| Membership change | Not covered. | Non-voter first, promote, remove, transfer, `RecoverCluster`. |
| Leader loss on a cut | Check-quorum, 30 s default (5 s here). | Leader lease ends after `LeaderLeaseTimeout` (500 ms base). |
| Addresses | DNS names. Needs static IPs in Docker. | IPs only. Static IPs in Docker. |
| Linearizability check | None. | Porcupine under random faults. |

## What hashicorp/raft does not give you

The research docs give the evidence with file and line citations.
Details of each test finding are in [docs/lessons-from-tests.md](docs/lessons-from-tests.md).

- `Apply` and `future.Error()` can block for a long time. Some calls block before they return a future. Run every raft call under your own deadline.
- No forwarding, no idempotency, no read-index API. The application builds these.
- `GetConfiguration().Index()` is always 0 (#722). Pass 0 as `prevIndex`.
- `VerifyLeader()` counts non-voters toward the quorum (#725, open, no fix PR).
- A hung leader disk stops the cluster and starts no election (#503, #666, fix PR #683 open).
- A removed follower is not shut down. This repo sets `ShutdownOnRemove=false` like Consul.
- `Shutdown()` never closes the stores. It blocks while a `StoreLogs` call hangs.
- `RecoverCluster` commits the whole local log. It can undo a "failed" write and lose an acknowledged one.
- Open safety bugs in v1.8.0:
  - Data divergence after a partition heal (#695).
  - A follower that applies never-committed entries (#733, #721).
  - A failed configuration write that still installs the configuration (#726).
- Open liveness bug: a stall after an interrupted leadership transfer (#696, fix PR #708).
- `raft.Snapshot()` with nothing new still writes a snapshot. It can push the older good snapshot out of the retain count.
- A panic in `FSM.Apply` kills the process. Check input before `Apply`.
- The Raft RPCs have no authentication. Use mutual TLS.
- The status of these bugs is from 2026-10-29. Read reference Q15 before you upgrade.

## Documents

- [docs/lessons-from-tests.md](docs/lessons-from-tests.md): what the tests found.
- [docs/hashicorp-raft-reference.md](docs/hashicorp-raft-reference.md): library facts and open bugs (Q15).
- [docs/hashicorp-raft-storage-and-transport.md](docs/hashicorp-raft-storage-and-transport.md): stores, snapshots, transport, fault injection.
- [docs/production-patterns.md](docs/production-patterns.md): patterns from Consul, Nomad and Vault.
- [../docs/raft-in-production.md](../docs/raft-in-production.md): the study guide for all parts.
- [../docs/plans/part2-hashicorp-raft.md](../docs/plans/part2-hashicorp-raft.md): the plan.
