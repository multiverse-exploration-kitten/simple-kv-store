# Lessons from the tests (pysyncobj 0.3.17)

Each section is one finding. Each has four parts: what we saw, why it happens, what a production system does, and what this repo does.
Source lines are from pysyncobj 0.3.17 (`syncobj.py`). Facts marked "code reading" were not observed in a run.
Library options and API: [pysyncobj-reference.md](pysyncobj-reference.md). General rules: [../../docs/raft-in-production.md](../../docs/raft-in-production.md).

## 1. LEADER_CHANGED is an unknown outcome

**What we saw.** pysyncobj documents `FAIL_REASON.LEADER_CHANGED` as "similar to NOT_LEADER". That suggests a safe retry.
In `test_write_that_times_out_can_still_commit` (`tests/cluster/test_partitions.py`) a timed-out write commits after heal. A leader change gives the same ambiguity.

**Why.** A follower reports `LEADER_CHANGED` when the leader changes after the follower forwarded the command.
The old leader may already have replicated the entry to a majority. The new leader can then commit it.

**Production.** Treat every error after the command left the client as "unknown". Give each write a client ID and a sequence number, so the state machine drops duplicates.

**This repo.** `node.py` maps `LEADER_CHANGED`, `UNKNOWN_OUTCOME` and timeouts to `CommandOutcomeUnknownError` (HTTP 504, `retry_safe: false`).
Only reasons where the entry never entered the log (`no_leader`, `not_leader`, `queue_full`, `discarded`, `request_denied`) give 503. The client uses CAS or reads the key first.

## 2. `isReady()` is not enough after a full restart (barrier read)

**What we saw.** Test `test_ready_after_full_restart_means_all_committed_data_is_applied` (`tests/cluster/test_durability.py`).
Experiment, 5 runs: write 10 values, stop all nodes, start all nodes. The commit index before the stop was 12.
The stored commit index was 7 to 10. In 4 of 5 runs the first "ready" node had `last_applied` 7 to 9, and a stale read of `key-9` returned `None`.

**Why.** A new leader does not know the real commit index until it commits an entry in its own term (Raft paper, section 5.4.2).
pysyncobj reports its old stored commit index until then, and a node that applied up to it sets ready.

**Production.** Do not serve linearizable reads before the leader has committed an entry in its own term. Consul waits for a `Barrier()` after an election (study guide section 4).

**This repo.** `KeyValueNode.is_ready()` calls `isReady()` and then does one linearizable read of a dummy key. When it returns, the node has applied every earlier entry.
`/health/ready` runs this check and also needs a known leader and `has_quorum`. The read runs on every check, so a node that loses its leader on an open but lossy link stops being ready (test `test_cut_off_follower_stops_being_ready`). Each check costs one log entry. `/status` shows the result of the last check, so its `ready` flag is `false` until the first `/health/ready` call.

## 3. No fsync (mmap journal)

**What we saw.** Test `test_full_cluster_restart_keeps_all_committed_data` and the e2e test `test_full_cluster_restart_keeps_the_data` pass.
But both stop the nodes in a clean way (or by SIGKILL). They cannot show a power loss.

**Why.** The journal is a memory-mapped file. `syncobj.py` never calls `journal.flush()`. Only `_destroy` (a clean close) runs `mmap.flush`.
The meta file uses `f.flush()` and no `os.fsync`.

**Production.** Raft needs the log, the term and the vote on stable storage before a node answers. Without fsync, a node can forget a vote or an acknowledged entry after a power loss.
Then it can vote twice in one term, or a committed entry can be lost. See study guide section 7.

**This repo.** A process crash is safe (the page cache keeps the data). A power loss is not. We document it and do not try to fix it. A part with real fsync is part 2 or part 3.

## 4. The commit index is saved once per second

**What we saw.** Same experiment as section 2. The stored commit index lags the real one by 2 to 5 entries after a quick restart.

**Why.** pysyncobj saves the commit index to `raft.journal.meta` only in `onOneSecondTimer`. `FileJournal._destroy` does not save it.

**Production.** The commit index is not needed for safety, because a node can learn it again from the leader. But code that trusts it right after a restart is wrong (section 2).

**This repo.** The barrier read in section 2.

## 5. No pre-vote (a rejoining node is disruptive)

**What we saw.** Test `test_rejoining_follower_with_a_higher_term_disrupts_the_leader` (`tests/cluster/test_partitions.py`).
An isolated follower went from term 1 to term 5 in 3 s. After heal the cluster jumped to term 7, and the healthy leader had to win again.
In a manual Docker run (`make partition NODE=node1`, then `make heal NODE=node1`) the term went from 3 to 7 and the rejoined node became the leader.

**Why.** `syncobj.py:920-923`: any node that sees a `request_vote` with a higher term becomes a follower.
pysyncobj does not answer an `append_entries` with a lower term (`syncobj.py:945`). So the leader learns about the higher term only from the rejoining node's next vote request.

**Production.** Turn on pre-vote and check-quorum together. With pre-vote a node first asks if it could win, and raises its term only then. etcd/raft has `PreVote`. hashicorp/raft has pre-vote on by default since v1.7.0.

**This repo.** Nothing can fix it in pysyncobj. The test documents the behavior. Each flaky rejoin is a short write outage (503 or 504). Alert on a fast-rising term.

## 6. Check-quorum default is 30 s, we use 5 s

**What we saw.** Test `test_isolated_leader_cannot_commit_and_steps_down_after_check_quorum` (`tests/cluster/test_quorum_loss.py`).
The isolated leader steps down about `leader_fallback_timeout` (1.5 s in tests) after its last ack. Before that, its writes time out (504). After that, `leader_address()` is `None` and writes fail at once with `no_leader` (503).

**Why.** pysyncobj has check-quorum (`leaderFallbackTimeout`), but the default is 30 s. For 30 s a client of the cut-off leader waits for each command and gets "unknown".
Data stays safe, because only the leader with a majority can commit.

**Production.** Set check-quorum to a few election timeouts.

**This repo.** `RaftTimings.leader_fallback_timeout` is 5.0 s (1.5 s in `for_tests()`).

## 7. Stale leader address on a cut-off follower, and the `no_quorum` fast reject

**What we saw.** Test `test_majority_down_fails_closed_within_the_command_timeout` (`tests/cluster/test_quorum_loss.py`) and e2e `test_without_majority_writes_fail_fast_and_stale_reads_still_work`.
A follower whose 2 peers are stopped keeps the dead leader in `leader_address()` forever. Every command waits the full timeout and returns 504. A readiness check that only asks "is a leader known" lies here.
With the fix, a write on the Docker cluster without quorum gives 503 `no_quorum` in about 10 ms.

**Why.** Check-quorum (`leaderFallbackTimeout`) runs on a leader. We saw that a follower keeps the dead leader's address. We did not read the source for the exact reason.

**Production.** Fail closed and fast without a majority (AGENTS.md, Raft rule 6). Do not rely on the library to tell you.

**This repo.** `KeyValueNode._run_command` raises `CommandRejectedError("no_quorum")` when `SyncObj.hasQuorum` is false, before it submits the command. `/health/ready` also needs `has_quorum`.
The check counts open TCP connections only. A link that drops packets but stays open still counts. In the cluster tests the fault harness drops messages and keeps the TCP link open, so it cannot trigger this check.

## 8. Write latency is about 2 heartbeats

**What we saw.** 40 sequential synchronous puts took 4.19 s, so 105 ms each, with a heartbeat of 50 ms.

**Why.** `appendEntriesUseBatch=True` (the default) sends new entries only on the next `appendEntriesPeriod` tick. The ack comes back on the following tick.

**Production.** Know that the heartbeat interval also sets the write latency here. It is not only a failure-detection setting.

**This repo.** `heartbeat_interval` is 0.1 s, so a write takes about 0.2 s. We accept it. Clients must use a timeout well above that (`KV_COMMAND_TIMEOUT_SECONDS` is 3 s).

## 9. The leader compacts without checking followers

**What we saw.** Test `test_lagging_follower_catches_up_from_a_snapshot` (`tests/cluster/test_durability.py`).
The leader's log no longer has the entries that a stopped follower missed, so the follower gets a full snapshot.

**Why.** The leader compacts at its own applied index. It ignores the followers' match index. A follower that misses more than `logCompactionMinEntries` writes always needs a snapshot.
Without `fullDumpFile` (no data dir), the snapshot stays in memory, and InstallSnapshot still works.

**Production.** Keep extra log entries for slow followers (hashicorp/raft `TrailingLogs`, default 10,240). Watch snapshot size and time. See study guide section 8.

**This repo.** Default `log_compaction_min_entries` is 5000. The state is small. We watch `log_length` and `match_index` in `/status`.

## 10. `getStatus()` race

**What we saw.** Code reading only (not observed). During a leader change `SyncObj.getStatus()` can raise `RuntimeError` ("dictionary changed size during iteration").

**Why.** `getStatus()` loops over `__raftNextIndex` and `__raftMatchIndex` in the caller thread. The Raft thread can change them at the same time.

**Production.** Not covered by the study guide. Read shared state with a lock, or ask the Raft thread for it.

**This repo.** `KeyValueNode._read_raw_status` tries 3 times. Tests call `status()` inside `wait_until` probes.

## 11. DNS cache 600 s, and static IPs in Docker

**What we saw.** Config defaults in `pysyncobj/config.py`: `dnsCacheTime` is 600 s and `dnsFailCacheTime` is 30 s.
A DNS failure with no earlier success is cached for 30 s. After a success, a failed lookup keeps the old IPs.

**Why.** pysyncobj resolves each peer name and caches the result. A restarted container with a new IP is unreachable for its peers until the cache expires.

**Production.** Use stable addresses for Raft peers (static IPs, StatefulSet DNS names, or a resolver with short TTL).

**This repo.** `docker-compose.yml` gives each node a static IP (`<prefix>.1N`). `make heal` reconnects a node with the same IP and the alias `nodeN`.
Without `--alias`, a peer that starts later cannot resolve the service name, because Compose adds the alias only when it creates the container.
Also: only the node with the greater address string opens the TCP link (node3 to node1 and node2, node2 to node1).

## 12. Fork with threads (`useFork=False`)

**What we saw.** No test. This is a design decision from the comments in `node.py`.

**Why.** By default pysyncobj forks a child process to write the snapshot. A fork in a process with threads (uvicorn, tests) can deadlock.

**Production.** Not covered by the study guide. Choose the snapshot method with the threads of your process in mind.

**This repo.** `useFork=False`. The cost: the snapshot blocks the Raft thread while it writes, and heartbeats stop for that time. Keep the state small.
Test `test_lagging_follower_catches_up_from_a_snapshot` states this trade-off.

## 13. The one-way partition: an ambiguous write

**What we saw.** Test `test_write_that_times_out_can_still_commit` (`tests/cluster/test_partitions.py`).
With both follower-to-leader links blocked, the write fails with `CommandOutcomeUnknownError` (504). After heal, the write is committed anyway.
With production-like ratios (command timeout, then connection timeout, then check-quorum) the same leader commits it about 0.1 s after heal.
With the fast test timings the leader steps down first, and a new leader in a later term commits it (3 of 3 runs).

**Why.** The leader still sends the entry, and the followers store it. Only the acks are lost, so the leader cannot count a majority.
After heal, the next ack arrives and the entry commits.

**Production.** A timeout is not a failure (AGENTS.md, Raft rule 3). Use an idempotency key, or CAS, or read the value again before a retry.

**This repo.** The API returns 504 with `retry_safe: false`. The test sets `connection_timeout=6.0` and `leader_fallback_timeout=6.0` so that the mechanism is simple.

## 14. An old leader's uncommitted write is discarded

**What we saw.** Test `test_heal_discards_the_old_leaders_uncommitted_write` (`tests/cluster/test_partitions.py`).
After heal the old leader becomes a follower and loses its write. It cannot win again, because its last log term is older.

**Why.** The new leader overwrites uncommitted entries on the old leader.

**Production.** A 504 can mean "lost", not only "late". The client must read the key again, or use CAS.

**This repo.** Same API rule as section 13.

## 15. A stale read can be old on any node

**What we saw.** Test `test_cut_off_follower_serves_stale_reads_but_rejects_linearizable_reads` (`tests/cluster/test_partitions.py`).
A cut-off follower returns the old value with `STALE`. A `LINEARIZABLE` read on it fails with `no_leader` (503).

**Why.** pysyncobj has no ReadIndex or lease read. A linearizable read is a log entry, so it costs a full replication round.

**Production.** Use ReadIndex or lease reads for cheap linearizable reads (etcd `ReadIndex`, hashicorp/raft `VerifyLeader()` with `Barrier()`). See study guide section 4.

**This repo.** The default read is linearizable. `?consistency=stale` is explicit. A forwarded sync write returns after the calling node applied it, so a client can read its own write on the same node, also with stale.

## 16. Peer traffic is pickle, so set a password

**What we saw.** Tests `test_cluster_with_password_elects_leader_and_replicates` and `test_node_with_wrong_password_cannot_join` (`tests/cluster/test_security.py`). Nodes with the same password work normally.

**Why.** pysyncobj sends peer messages as pickles. Without a password, anyone who reaches port 4321 can run code on the node.
With a password, pysyncobj encrypts and signs every peer message (Fernet, needs the `cryptography` package).

**Production.** Authenticate peer traffic with mutual TLS (for example etcd `--peer-client-cert-auth`) and keep the Raft port on a private network.

**This repo.** `KV_RAFT_PASSWORD` sets `raft_password`. `docker-compose.yml` requires it and has no default, so a forgotten password stops the start instead of using a known one. `make up` writes a random password to `.env`. The e2e tests use their own random password.

## 17. A wrong password fails silently

**What we saw.** In `test_node_with_wrong_password_cannot_join` the bad node never sees a leader and never gets the value. It logs nothing.
The operator sees only: no leader, `has_quorum` false, and peers with `connected: false` in `/status`.

**Why.** `tcp_connection.py` in pysyncobj has a bare `except` that silently disconnects the link.

**Production.** Alert on `has_quorum` false and on disconnected peers. Compare the secret on all nodes when a node cannot join.

**This repo.** `/status` and `make status` show `quorum` and `connected`. Check the password first when a new node does not join.

## 18. Replicated method numbers depend on sorted order

**What we saw.** Code reading only (`syncobj.py` line 248). Not tested in a run.

**Why.** pysyncobj numbers replicated methods in sorted (ver, name) order. A new `ver=0` method or a rename changes the numbers.
A mixed-version cluster during a rolling upgrade then applies the wrong method.

**Production.** Version the commands. A format change must not break nodes that run the old version (AGENTS.md, Raft rule 2).

**This repo.** Add a new command with a higher `ver`. Do not rename an existing replicated method.

## 19. An exception in a replicated method stops the whole cluster

**What we saw.** The pysyncobj study (see [pysyncobj-reference.md](pysyncobj-reference.md)) raised an exception inside a `@replicated` method.
After that, no node applied any later entry, and the cluster had no leader. It did not recover.

**Why.** Every node applies the same entry, so every node hits the same exception. pysyncobj does not skip the entry.

**Production.** Keep apply free of exceptions. Validate input before the command enters the log. Return an error value from apply instead of raising.

**This repo.** `KeyValueNode` checks that keys and values are strings before it sends a command (`_require_text` in `node.py`).
Test: `test_bad_argument_is_rejected_before_it_enters_the_log` in `tests/cluster/test_smoke.py`.
