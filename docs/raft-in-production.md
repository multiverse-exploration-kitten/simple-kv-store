# Raft in production

A study guide for this repository. You build the same key-value (KV) store three times:

| Part | Stack | Library version checked |
|---|---|---|
| 1 | Python + pysyncobj | 0.3.17 (PyPI, August 2026) |
| 2 | Go + hashicorp/raft | v1.7.0 and the `main` branch |
| 3 | Go + etcd/raft (`go.etcd.io/raft/v3`) | the `main` branch (latest tag v3.7.0) |

The goal is to run Raft correctly in production. The guide assumes that you know the basic algorithm: terms, elections, log replication and commit. Library facts come from the library docs and from the library source code (read in October 2026). A fact that I could not check is marked **not verified**.

## Key takeaways

1. Run 3 or 5 voters in 3 failure domains. An even number of voters costs more and tolerates no extra failure.
2. Set the election timeout to at least 10 times the peer round-trip time. Also keep it well above the worst disk fsync time.
3. Turn on pre-vote **and** check-quorum. With only one of them, a partial network fault can leave the cluster with no stable leader.
4. A read from local state can be stale. Use ReadIndex or a read through the log when the reader needs the latest value.
5. A write timeout means "unknown", not "failed". Give each write a client ID and a sequence number, so a retry cannot apply twice.
6. The apply function must be deterministic. The proposer puts time, random values and new IDs into the command. Apply never creates them.
7. Fsync the log, the term and the vote before a node answers any Raft message. A node that lost its disk must rejoin with a new identity.
8. Take snapshots at a size or count threshold. Keep extra log entries for slow followers. Watch snapshot time and disk use.
9. Change membership one server at a time. Add new servers as non-voters first. Remove a dead server before you add its replacement.
10. Measure leader changes, commit-to-apply lag, proposal failures and fsync latency. Test with crashes, pauses, partitions and a linearizability checker.

## Words used in this guide

| Word | Meaning |
|---|---|
| Voter | A member that votes in elections and counts toward the quorum. |
| Quorum | A majority of the voters: `floor(N/2) + 1`. |
| Commit | A log entry is stored on a quorum. Raft never loses a committed entry. |
| Apply | A node runs a committed entry on its state machine (here, the KV map). |
| Linearizable | Each operation seems to take effect at one instant between its start and its end. A read sees every write that finished before the read started. |
| Term | A number that names one leader period. It only goes up. |
| FSM | "Finite state machine". hashicorp/raft uses this name for your state machine. |

## The three libraries at a glance

| Feature | pysyncobj 0.3.17 | hashicorp/raft | etcd/raft |
|---|---|---|---|
| Leader steps down without quorum (check-quorum) | `leaderFallbackTimeout`, default 30 s ([config.py][pso-config]) | `LeaderLeaseTimeout`, default 500 ms ([config.go][hr-config]) | `CheckQuorum`, default `false` ([raft.go][er-raftgo]) |
| Pre-vote | No (no pre-vote message in [syncobj.py][pso-syncobj]) | Yes, on by default since v1.7.0 ([CHANGELOG][hr-changelog]) | `PreVote`, default `false` |
| Followers ignore votes while a leader is alive | No ([syncobj.py][pso-syncobj]) | Yes ([raft.go][hr-raftgo]) | Yes, only when `CheckQuorum` is on |
| Linearizable read API | None. Read through the log. | `VerifyLeader()` + `Barrier()` ([api.go][hr-api]) | `ReadIndex()` with `ReadOnlySafe` |
| Lease read | No | No read lease. `LeaderLeaseTimeout` only controls step-down. | `ReadOnlyLeaseBased` (needs `CheckQuorum`) |
| Leadership transfer | `transferLeadership()`, new in 0.3.16 | `LeadershipTransfer()` | `TransferLeadership()` |
| Non-voting members | Read-only nodes (`selfNode=None`) | `AddNonvoter()` | Learners |
| Joint consensus | No | No | Yes (`ConfChangeV2`) |
| Storage | In memory by default. Optional journal file. | You pick a `LogStore` and `StableStore` | You write it |
| Peer encryption | Optional `password` (Fernet) | None in the TCP transport. You write a TLS `StreamLayer`. | You write the transport |

---

## 1. Cluster size and quorum math

**Problem.** Raft needs a quorum to elect a leader and to commit a write. A cluster of N voters survives `floor((N-1)/2)` failed voters. A fourth voter raises the quorum from 2 to 3, but the cluster still survives only one failure. Each extra voter also adds work to every write.

| Voters | Quorum | Failures tolerated |
|---|---|---|
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 4 | 3 | 1 |
| 5 | 3 | 2 |
| 6 | 4 | 2 |
| 7 | 4 | 3 |

**In production.** The [etcd FAQ][etcd-faq] recommends odd sizes and no more than seven members. Consul recommends 3 or 5 servers ([Consul consensus][consul-consensus]). The [Consul reference architecture][consul-refarch] puts 5 servers in 3 availability zones, with at most 2 per zone. Then the loss of a full zone leaves 3 servers, which is still a quorum ([Consul reliability][consul-reliability]). With only 2 zones, the loss of one zone can remove the quorum. To hold more data, CockroachDB and TiKV run many small Raft groups instead of one big group ([Scaling Raft][crdb-scaling], [TiKV Multi-Raft][tikv-multiraft]).

**How to check it in this repo.** All parts. Part 1 already has `test_writes_commit_with_one_follower_down` and `test_majority_down_fails_closed_within_the_command_timeout`. Add a 4-node test: stop 2 nodes and show that writes block, as with 3 nodes. In part 2 or 3, measure write latency with 3, 5 and 7 voters.

## 2. Timeouts

**Problem.** A follower starts an election when it gets no message from the leader for one election timeout. A heartbeat round includes the network round trip. A round with new entries also includes an fsync on the leader and on the followers. A short timeout turns a slow disk, a garbage-collection pause or network jitter into a false election. A long timeout makes the cluster wait longer after a real crash.

The [Raft paper][raft-paper] (section 5.6) states the rule: broadcast time ≪ election timeout ≪ mean time between failures. It gives 0.5–20 ms for a broadcast and 10–500 ms for the election timeout.

**In production.**

| System | Heartbeat | Election timeout | Step-down without quorum |
|---|---|---|---|
| etcd server | 100 ms | 1000 ms | one election timeout |
| Consul | as hashicorp/raft | `raft_multiplier` × 1000 ms | `raft_multiplier` × 500 ms |
| hashicorp/raft defaults | random 100–200 ms (`HeartbeatTimeout / 10`, [replication.go][hr-replication]) | `HeartbeatTimeout` 1 s | `LeaderLeaseTimeout` 500 ms |
| pysyncobj defaults | `appendEntriesPeriod` 0.1 s | random 0.4–1.4 s | `leaderFallbackTimeout` 30 s |

etcd says to set the heartbeat near the round-trip time and the election timeout to at least 10 times the round-trip time ([etcd tuning][etcd-tuning]). All members must use the same values. Consul's default `raft_multiplier` is 5, so its election timeout is 5 s. HashiCorp recommends 1 for production ([Consul performance][consul-perf]).

etcd counts disk latency as part of leader health. The [etcd FAQ][etcd-faq] sets two disk targets. The p99 of `wal_fsync_duration_seconds` must stay below 10 ms. The p99 of `backend_commit_duration_seconds` must stay below 25 ms. When the leader misses two heartbeat intervals, etcd logs "failed to send out heartbeat on time".

**How to check it in this repo.** All parts. Add latency on one link (part 1: extend the fault-injecting transport in `tests/cluster/faults.py` with a delay). Then lower the election timeout to about 3 times that delay and count term changes. In part 2, wrap the `LogStore` and add a sleep to each `StoreLogs` call to act as a slow disk. Watch `raft.state.candidate` go up.

## 3. Leader election problems

**Problem.** Several followers can time out at the same moment and split the votes. Then no candidate wins and a new round starts. A **disruptive server** is a node that was cut off and kept raising its term. When it comes back, its higher term forces a healthy leader to step down. A **stale leader** is a leader that lost contact with the majority but still acts as leader.

**In production.** Raft uses randomized election timeouts against split votes (150–300 ms in the [Raft paper][raft-paper]). Ongaro's [thesis][thesis] adds these tools:

| Mechanism | What it does | Thesis section |
|---|---|---|
| Pre-vote | A candidate first asks if it could win. It raises its term only when a majority says yes. | 9.6 |
| Check-quorum | A leader steps down when it cannot reach a majority within an election timeout. | 6.2 |
| Leader stickiness | A follower that heard from a leader within the minimum election timeout ignores vote requests. | 4.2.3 |
| Leadership transfer | The leader hands over before a planned restart. | 3.10 |

On 2 November 2020, a switch at Cloudflare failed only partly ([Cloudflare post-mortem][cloudflare]). One etcd node could not reach the leader, but it could reach the third node. etcd kept holding elections and stopped accepting writes. A database failover followed, and the API and dashboard had problems for over 6 hours. Howard and Abraham then showed that basic Raft and "Raft with pre-vote only" can lose liveness in such partial faults ([Decentralized Thoughts][howard]). Raft with pre-vote and check-quorum together stays live. Which options Cloudflare's etcd had enabled is **not verified**.

Today the etcd server always sets `CheckQuorum: true` ([bootstrap.go][etcd-bootstrap]), and `--pre-vote` defaults to `true` in v3.6 ([etcd flags][etcd-config]). hashicorp/raft has pre-vote and step-down on by default. pysyncobj has neither pre-vote nor leader stickiness. Its leader steps down only after 30 s without a majority.

**How to check it in this repo.** Part 1 already has `test_rejoining_follower_with_a_higher_term_disrupts_the_leader`. Add a **partial partition** test: block only the link between one follower and the leader. Count term changes for 30 s. Expect many changes in part 1. In part 3, run the same test four times: with no option, `PreVote` only, `CheckQuorum` only, and both. Only "both" should keep one leader.

## 4. Read consistency

**Problem.** A node's local state can be behind the leader. A leader can be deposed without knowing it yet. So a read from local state can return an old value, even on the leader. In 2014 Jepsen found stale reads in etcd and Consul for this reason ([Jepsen 2014][jepsen-2014]). In 2020 Redis-Raft returned stale reads because a new leader did not commit a no-op entry first ([Jepsen Redis-Raft][jepsen-redisraft]).

| Read mode | How it works | Cost | Safe when |
|---|---|---|---|
| Local (stale) read | Read the local map. | None | Old data is acceptable. |
| Read through the log | Propose the read as a log entry. Answer when it is applied. | One commit and one fsync | Always |
| ReadIndex | The leader records its commit index and confirms leadership with one heartbeat round. Then it applies up to that index and reads. | One round trip, no disk write | Always. The leader must first commit an entry in its own term. |
| Lease read | The leader answers without messages while its lease is valid. | None | Clock drift is bounded and pauses are short. |
| Follower read | A follower asks the leader for a read index, then waits to apply up to it. | One round trip to the leader | Always |

The [thesis][thesis] (section 6.4.1) says that a lease read assumes bounded clock drift. If that assumption fails, the read can return data that is arbitrarily old. Ongaro does not recommend lease reads unless you need the speed.

**In production.**
- etcd uses ReadIndex for linearizable reads by default. Its "serializable" reads are local and can be stale. Jepsen found etcd 3.4.3 key-value operations strict serializable ([Jepsen etcd 3.4.3][jepsen-etcd]).
- Consul has three modes ([Consul consistency][consul-consistency]). `default` can return stale data during a short window after a leader change. `consistent` confirms leadership with a quorum round trip. `stale` lets any server answer, usually within 50 ms of the leader but with no upper limit.
- Consul calls `VerifyLeader()` for each consistent read. After an election, it also waits for a `Barrier()` before it serves consistent reads ([rpc.go][consul-rpc]).
- TiKV uses lease reads with a 9 s lease and a 10 s election timeout. It renews the lease on writes and uses a raw monotonic clock ([TiKV lease read][tikv-lease]).
- CockroachDB stops a node whose clock is off by more than 80% of the maximum offset (default 500 ms) ([CockroachDB clocks][crdb-clock]). This protects its clock assumption.

**How to check it in this repo.** Part 1 has no ReadIndex. Its linearizable read goes through the log. `test_cut_off_follower_serves_stale_reads_but_rejects_linearizable_reads` already shows this on a follower. Add the same test on an **isolated old leader**. It stays leader for up to `leaderFallbackTimeout`, and its local read returns the old value. In part 2, compare a plain FSM read with `VerifyLeader()` plus a `Barrier()` after election. In part 3, compare `ReadOnlySafe` and `ReadOnlyLeaseBased`. Make the old leader's `Tick()` run slower to act as a slow clock, and check the history with Porcupine.

## 5. Write semantics

**Problem.** The leader can commit a write and then crash before it replies. The network can also drop the reply. The client sees a timeout, but the write may be committed. If the client retries, the write applies twice. This breaks operations such as "increment" or "append". The opposite can also happen: etcd says that writes which the old leader had not committed can be lost ([etcd failures][etcd-failures]).

**In production.** The [thesis][thesis] (section 6.3) uses client sessions. Each client has a unique ID and gives each command a serial number. The state machine stores the last serial number and the reply for each client. On a duplicate, it returns the stored reply and does not run the command again. Session expiry must also be deterministic. Compare-and-set (CAS) also helps: Consul KV checks the key's `ModifyIndex` ([Consul KV API][consul-kv-api]), and etcd transactions compare a key's revision ([etcd API][etcd-api]).

| Library | Result that means "unknown" | Result that means "not applied" |
|---|---|---|
| pysyncobj | `SyncObjException('Timeout')`, `LEADER_CHANGED`, `UNKNOWN_OUTCOME` | `QUEUE_FULL`, `MISSING_LEADER`, `DISCARDED`, `NOT_LEADER` (the receiving node was not leader and did not add the command to its log, `syncobj.py` `_checkCommandsToApply`), `REQUEST_DENIED` |
| hashicorp/raft | `ErrLeadershipLost`. The `Apply` docs say it cannot know if the write succeeded. | `ErrNotLeader`, `ErrEnqueueTimeout` |
| etcd/raft | `Propose` gives no result. You match committed entries to a request ID and set your own timeout. | `ErrProposalDropped` |

Two details matter. In pysyncobj, a follower fails all waiting commands with `LEADER_CHANGED` when it sees a new leader, but those commands can still commit ([syncobj.py][pso-syncobj]). In hashicorp/raft, the `Apply` timeout only limits the time to enqueue the command, not the time to commit it ([api.go][hr-api]). The class of `ErrEnqueueTimeout` above follows from that doc and is my reading.

**How to check it in this repo.** Part 1 has `test_write_that_times_out_can_still_commit`. Add a duplicate test: a replicated `increment(client_id, seq)` command, a leader kill during a burst, and a client that retries. Without a dedup table the counter ends too high. With the table it is exact. Run the same test in parts 2 and 3.

## 6. Determinism of the state machine

**Problem.** Every node applies the same log. If apply reads the clock, a random number, an environment variable, a file or the network, the nodes get different states. Raft does not detect this. Iteration order is a trap too. Go randomizes map iteration order. Python randomizes string hashes per process, so iteration order over a `set` of strings can differ between nodes.

**In production.** The hashicorp/raft `FSM` docs say that `Apply` must be deterministic and give the same result on all peers ([fsm.go][hr-fsm]). The [thesis][thesis] puts the leader's timestamp into the log when the state machine needs time, for example to expire sessions. pysyncobj's own `ReplLockManager` follows this rule. The caller reads `time.time()` and passes it to the replicated `acquire` command as an argument ([batteries.py][pso-batteries]). etcd can compare state hashes between members to find corruption ([etcd data corruption][etcd-corruption]).

Rules for apply:
- No clock, no random number, no UUID generation, no I/O.
- Only ordered data structures, or sort before you iterate.
- Each command carries a type tag and a version number (see topic 10).

**How to check it in this repo.** Part 1 has `test_same_commands_in_same_order_give_same_state_on_every_replica`. Add a negative test with a bad command that calls `time.time()` inside apply. Then compare a hash of the state on all three nodes and show the mismatch. In parts 2 and 3, log a state hash at fixed log indexes on each node and compare the hashes in tests.

## 7. Durability

**Problem.** Raft's safety proof assumes that each node keeps `currentTerm`, `votedFor` and its log on stable storage before it answers a message ([Raft paper][raft-paper], Figure 2). If a node acknowledges an entry and then loses it, a quorum may no longer hold a committed entry. If a node forgets its vote, it can vote twice in one term, and two leaders can exist in that term. A node with a wiped disk that rejoins under its old identity has "amnesia". It can help elect a leader that does not have committed entries.

**In production.**
- etcd fsyncs its write-ahead log. The `--unsafe-no-fsync` flag exists, and the docs say it causes data loss ([etcd flags][etcd-config]).
- Consul's Raft log store syncs to disk on every append ([Consul performance][consul-perf]).
- Jepsen tested NATS JetStream 2.12.1 in December 2025 ([Jepsen NATS][jepsen-nats]). NATS called fsync only every two minutes but acknowledged writes at once. Simulated power failures lost 131,418 of 930,005 acknowledged messages.
- Before pysyncobj 0.3.16, pysyncobj did not store `currentTerm` and `votedFor`. After a restart of all nodes, a stale node could win an election and overwrite committed entries ([pysyncobj PR #202][pso-202]).

etcd gives steps for a member with a lost or corrupt disk ([etcd data corruption][etcd-corruption]). Remove the member, add it again, and start it with an empty data directory. The re-added member gets a new member ID. etcd refuses to start with the data directory of a removed member ([etcd runtime reconfiguration][etcd-reconfig]).

pysyncobj facts from [journal.py][pso-journal]:
- With the default `journalFile=None`, the log lives only in memory. A restarted node comes back empty, with term 0 and no vote.
- With a journal file, entries go into a memory-mapped file. The write path never calls `fsync` or `msync`.
- The term and vote file is written with a Python `flush()` and a rename, without `fsync`.
- So the data should survive a process crash, because the OS page cache keeps it. It may not survive an OS crash or a power loss. This is my reading of the code and is **not verified** by a test.

**How to check it in this repo.** Part 1 has `test_full_cluster_restart_keeps_all_committed_data`. Add an **amnesia test** for all parts:
1. Start nodes A, B and C.
2. Cut off C. Write `x=1`. A and B commit it.
3. Stop B and wipe its data.
4. Cut off A. Heal C.
5. Start B again with the same address or ID.
6. B and C now form a quorum without `x=1`. Show that `x=1` is lost.

For power loss in parts 2 and 3, write a `LogStore` wrapper that drops writes made after the last sync when the test "crashes" the node. Jepsen used LazyFS for the same purpose in its NATS tests.

## 8. Log compaction and snapshots

**Problem.** Without compaction the log grows forever. Memory and disk fill up, and a restart replays the whole log. A follower that is far behind may need entries that the leader already deleted. Then the leader must send a full snapshot, which costs disk, network and CPU on both sides.

**In production.** The [thesis][thesis] (section 5.1.2) suggests a snapshot when the log is much bigger than the last snapshot.

| Library | When it snapshots | Kept for slow followers | Notes |
|---|---|---|---|
| pysyncobj | 5000 entries (`logCompactionMinEntries`) or 300 s (`logCompactionMinTime`) | not verified | Uses `fork()` for copy-on-write when it writes to disk. Sends snapshots in 64 KiB chunks. `logCompactionSplit` makes nodes compact at different times. ([config.py][pso-config]) |
| hashicorp/raft | 8192 new entries (`SnapshotThreshold`), checked every 120–240 s (`SnapshotInterval`) | 10,240 entries (`TrailingLogs`) | `FSM.Snapshot()` must return fast. `Persist()` runs at the same time as `Apply()`. ([config.go][hr-config], [fsm.go][hr-fsm]) |
| etcd server | 10,000 changes (`--snapshot-count`, v3.6) | not verified | Default storage quota 2 GiB, suggested maximum 8 GiB ([etcd limits][etcd-limits]) |
| etcd/raft | You decide | You decide | You call `Compact` and send `MsgSnap`. You call `ReportSnapshot` after you send it ([etcd/raft README][er-readme]). |

**How to check it in this repo.** Part 1 has `test_lagging_follower_catches_up_from_a_snapshot`. Extend it: write 10,000 keys while a follower is down. Then measure the catch-up time and the leader's peak memory. Python reference counting writes to objects, so a forked child can still copy many pages. The real growth is **not verified**. In part 2, set `TrailingLogs` and `SnapshotThreshold` low and watch `raft.rpc.installSnapshot`. In part 3, write the snapshot logic yourself and test a follower that misses the snapshot transfer.

## 9. Membership changes

**Problem.** If the old and new majorities do not overlap, two leaders can exist at the same time. A change of one server at a time keeps the overlap. In July 2015 Ongaro reported a bug in the single-server method of his thesis ([raft-dev][raft-dev-bug]). The bug needs concurrent changes and a leader change. The fix: a new leader must commit an entry in its own term before it starts a membership change. Joint consensus (thesis section 4.3) allows any change in one step, at the cost of more code. A new, empty voter can also slow commits while it catches up.

**In production.**
- etcd recommends that you add new members as **learners**. A learner gets the log but does not vote. It can be promoted only after it catches up ([etcd learner][etcd-learner]). The v3.4 design allows one learner per cluster.
- etcd says to remove a failed member first and then add the replacement ([etcd FAQ][etcd-faq]). `--strict-reconfig-check` (default on) rejects changes that would leave too few started members ([etcd runtime reconfiguration][etcd-reconfig]).
- Consul Autopilot removes dead servers. It lets a new server vote only after 10 s of good health (`ServerStabilizationTime`) ([Consul autopilot][consul-autopilot]).
- hashicorp/raft has `AddNonvoter`, `AddVoter` and `RemoveServer`. Their `prevIndex` argument works as a compare-and-set on the configuration ([api.go][hr-api]).
- etcd/raft supports simple changes, joint consensus (`ConfChangeV2`) and learners. A change takes effect when it is applied. Only one change can be pending ([etcd/raft README][er-readme]).
- pysyncobj has `addNodeToCluster` and `removeNodeFromCluster` when `dynamicMembershipChange=True`. It allows one change at a time. It waits until the leader's no-op entry is applied, which is the 2015 fix ([syncobj.py][pso-syncobj]). I found no API that promotes a read-only node to a voter (**not verified** that none exists).

Steps to replace a dead voter:
1. Check that the cluster still has a quorum.
2. Remove the dead member from the configuration.
3. Give the new server an empty data directory and a new ID.
4. Add it as a non-voter or learner.
5. Wait until its log is close to the leader's commit index.
6. Promote it to voter.

Why remove first? In a 3-node cluster with one dead node, "add first" gives 4 voters and a quorum of 3. Only 3 nodes are alive, so one more failure stops the cluster.

**How to check it in this repo.** All parts. Grow 3 → 5 → 3, one server at a time, under write load. Then run a replace test both ways ("add first" and "remove first") and stop one more node during the change. In part 3, replace all three voters in one joint-consensus change.

## 10. Rolling upgrades and versioned commands

**Problem.** During a rolling upgrade, old and new code run in the same cluster. The log and the snapshots hold commands written by old code. New code must still read them. Old code must never get a command that it cannot read, or it crashes or diverges.

**In production.**
- etcd upgrades one member at a time. A mixed cluster uses the protocol of the lowest version. The cluster counts as upgraded only when all members run the new version. Take a snapshot backup first ([etcd upgrade][etcd-upgrade]).
- Consul takes a snapshot first and restarts the followers first and the leader last. Before it moves to the next server, it checks that `commit_index` and `last_log_index` match ([Consul upgrade][consul-upgrade]).
- pysyncobj offers `@replicated(ver=N)` and `setCodeVersion(N)`. You deploy new code to all nodes, and then you switch the cluster to the new version through the log. A node with older code raises `SyncObjExceptionWrongVer` ([syncobj.py][pso-syncobj]).

**pysyncobj gotcha.** The log stores a **number** for each replicated method, not its name. pysyncobj gives these numbers by sorting `(ver, consumer position, method name)` ([syncobj.py][pso-syncobj]). So if you add, rename or remove a `ver=0` method, other methods can get new numbers. Then old log entries and old nodes call the wrong method. Give each new method a higher `ver`, never rename a method, and keep the order of consumers fixed. Commands and snapshots also use `pickle`, so class changes can break the loading of old snapshots.

Good practice for parts 2 and 3: encode each command with an explicit type tag and a version field (JSON or protobuf). Keep a handler for each old version. Deploy in two steps: first ship code that can read the new format, then switch on the new format with a command through the log.

**How to check it in this repo.** Part 1: add a new `ver=0` method whose name sorts before `put`. Run a mixed cluster and show that a node applies the wrong method. Then repeat with `ver=1` and `setCodeVersion(1)`. Parts 2 and 3: run a v1 and a v2 binary together, upgrade the leader last, and compare state hashes.

## 11. Disaster recovery

**Problem.** If a majority of voters is lost for good, Raft cannot commit or elect a leader. No automatic fix is safe. The operator must choose: wait for the nodes, or force a new configuration from the survivors and accept that some acknowledged writes may be lost. If old members are still alive when you force it, you can get two clusters.

**In production.**

| System | Tool | What it does | Warnings |
|---|---|---|---|
| etcd | `etcdctl snapshot save`, `etcdutl snapshot restore` | Restores all members from **the same** snapshot into new data directories, with new member and cluster IDs | Use `--bump-revision` and `--mark-compacted` to protect watchers ([etcd recovery][etcd-recovery]) |
| etcd | `--force-new-cluster` | Makes a one-member cluster from existing data | Strongly discouraged. It panics if old members are still alive. |
| Consul | `peers.json` in the Raft data directory | Lists the surviving servers (`id`, `address`, `non_voter`). Start them with the file in place. | Recovery commits all outstanding log entries. It most likely loses data. A fresh cluster plus a backup is often easier ([Consul outage recovery][consul-outage]). |
| hashicorp/raft | `RecoverCluster()` | Writes a new configuration on every server while they are stopped | Same risks as `peers.json` ([api.go][hr-api]) |
| etcd/raft, pysyncobj | none built in | You write it | My reading: in pysyncobj you would restart a survivor with a shorter peer list. That is unsafe if old members come back. |

A backup that you never restored is not a backup. Run restore drills on a schedule.

**How to check it in this repo.** Part 1 has `test_quorum_loss.py`. Add a "forced recovery" test. Stop 2 of 3 nodes and restart the survivor as a one-node cluster. Then start an old node with its old peer list, and show that two leaders can exist. In part 2, use `RecoverCluster` and list the acknowledged writes that were lost. In all parts, run a backup → wipe → restore → compare-hash drill.

## 12. Observability

**Problem.** Raft problems often look like "slow" before they look like "down". Without metrics, you cannot tell a slow disk from a slow network or an overloaded leader.

| Signal | etcd metric ([etcd metrics][etcd-metrics]) | hashicorp/raft metric ([raft.go][hr-raftgo]) | What it tells you |
|---|---|---|---|
| Leader exists | `etcd_server_has_leader` | `raft.state.leader` (counter) | Without a leader, no writes happen. |
| Leader changes | `etcd_server_leader_changes_seen_total` | `raft.state.candidate`, `raft.state.leader` | Frequent changes point to low timeouts, slow disks or network faults. |
| Step-down reason | not verified | `raft.transition.heartbeat_timeout`, `raft.transition.leader_lease_timeout` | Followers lost the leader, or the leader lost the quorum. |
| Commit latency | not verified | `raft.commitTime` | Time to commit an entry on the leader. |
| Commit-to-apply lag | `etcd_server_proposals_committed_total` minus `etcd_server_proposals_applied_total` | `CommitIndex()` minus `AppliedIndex()` | A growing gap means apply is too slow. |
| Proposal failures | `etcd_server_proposals_failed_total`, `etcd_server_proposals_pending` | count your `ErrLeadershipLost` results | Failed or queued writes. |
| Disk latency | `etcd_disk_wal_fsync_duration_seconds`, `etcd_disk_backend_commit_duration_seconds` | `raft.rpc.appendEntries.storeLogs` | Slow fsync causes false elections. |
| Follower contact | `etcd_network_peer_round_trip_time_seconds` | `raft.leader.lastContact` | Slow or lost followers. Consul Autopilot marks a server unhealthy after 200 ms by default. |
| Snapshots | `etcd_debugging_snap_save_total_duration_seconds` (a debugging metric that can change) | `raft.rpc.installSnapshot` | Slow snapshots point to disk trouble. |

Also log each term change and each leader change with the node ID and the reason. etcd logs a warning when the average apply time is above 100 ms ([etcd FAQ][etcd-faq]).

pysyncobj has no metrics export. Its `getStatus()` returns a dictionary with `state`, `leader`, `raft_term`, `commit_idx`, `last_applied`, `leader_commit_idx`, `log_len`, `has_quorum` and more. The `onStateChanged` callback fires on each state change.

**How to check it in this repo.** Part 1 has a status endpoint. Export these values as Prometheus metrics and alert on the term-change rate. In each fault test, also check that the matching metric moved. For example, the slow-disk test must raise the fsync histogram, and the partition test must raise leader changes.

## 13. Security

**Problem.** Raft peers trust each other's messages fully. Anyone who can send Raft messages can vote, change membership or change the log. Anyone who can reach the client API can read and change all data. Many systems ship with no TLS and no authentication by default.

**In production.** etcd turns on no transport security and no access control by default ([etcd security][etcd-security]). It has flags for peer TLS (`--peer-cert-file`, `--peer-client-cert-auth`, `--peer-cert-allowed-cn`) and client TLS (`--client-cert-auth`). Auto-TLS certificates are self-signed and do not prove identity. etcd has role-based access control for the client API.

| Library | Peer transport security |
|---|---|
| pysyncobj | Peer messages are zlib-compressed Python pickles ([tcp_connection.py][pso-tcp]). The Python docs warn that unpickling untrusted data can run any code ([pickle][py-pickle]). With `password` set, pysyncobj uses Fernet (AES plus HMAC) with a PBKDF2 key and a fixed salt. It checks the token before it unpickles. |
| hashicorp/raft | `NewTCPTransport` uses plain TCP. You can write a `StreamLayer` with mutual TLS ([net_transport.go][hr-nettransport]). |
| etcd/raft | The library has no transport. You add mutual TLS in your own gRPC or HTTP transport. |

My conclusion from the pysyncobj code: without `password`, any host that reaches the Raft port can run code in the node process. The admin commands (status, add node, remove node, set version, transfer leadership) use the same port. So set `password`, bind to a private interface and use a firewall.

**How to check it in this repo.** Part 1: start a node without `password`. Send a crafted pickle to its Raft port that creates a marker file in a temporary directory. Then set `password` and show that the node drops the connection. Parts 2 and 3: use mutual TLS between peers. Show that a peer with a certificate from another CA is rejected.

## 14. Testing

**Problem.** Raft bugs need rare timing, such as a crash between two disk writes or a partition during a membership change. Happy-path unit tests do not find them. You need fault injection, a record of what clients saw, and a checker that decides if that record is correct.

**In production.** Jepsen runs clients, records each operation's start, end and result, injects faults, and then checks the history. Its etcd 3.4.3 tests used process pauses, crashes, clock skew, partitions and membership changes ([Jepsen etcd 3.4.3][jepsen-etcd]). Knossos is Jepsen's linearizability checker ([Knossos][knossos]). Porcupine is a Go checker that is 1,000 to 10,000 times faster on Jepsen data ([Porcupine][porcupine]). etcd's robustness tests use Porcupine ([etcd robustness tests][etcd-robustness]). FoundationDB runs a whole cluster in one single-threaded process with simulated machines, disks and network, so each test run can be repeated exactly ([FoundationDB testing][fdb-testing]).

| Fault | How to inject it | What it finds |
|---|---|---|
| Crash | `SIGKILL` | Durability and recovery bugs |
| Pause | `SIGSTOP` and `SIGCONT` (`docker pause`) | Stale leaders, lease bugs |
| Full or partial partition | A fault-injecting transport, or `iptables` inside containers | Election liveness, stale reads |
| Clock skew | `libfaketime`, or a slower `Tick()` in etcd/raft | Lease reads, TTLs |
| Slow disk | A sleep in the log store's sync | False elections |
| Lost unsynced writes | A store that drops unsynced data on a simulated crash, or LazyFS | Missing fsync |

Library support for tests:
- pysyncobj accepts a custom `transport` or `transportClass`. Part 1 already uses this in `tests/cluster/faults.py`.
- hashicorp/raft has `NewInmemTransport` and `NewInmemStore` for tests only.
- etcd/raft has no clock and no I/O. You call `Tick()` and pass the messages yourself. So you can run many nodes in one goroutine with a seeded random scheduler. The repo also has data-driven interaction tests (`rafttest/`) and a TLA+ spec (`tla/`).

**How to check it in this repo.** All parts. Record each client operation (client ID, operation, start time, end time, result) to a JSON file. Check it with Porcupine. Part 1 needs a small Go program for this, because Porcupine is a Go library. Mark operations with an unknown result as "maybe", not as "failed". In part 3, build the seeded deterministic simulator and print the seed when a test fails.

## 15. Common anti-patterns

| Anti-pattern | What goes wrong | Do this instead |
|---|---|---|
| Even number of voters | Bigger quorum, same fault tolerance | 3 or 5 voters |
| Too many voters (more than 7) | Slower writes, more work for the leader ([etcd FAQ][etcd-faq]) | 5 voters plus non-voters. For more data, use many small Raft groups. |
| Big values in the log | Slow fsync, slow replication, late heartbeats, huge snapshots | Respect limits: etcd allows 1.5 MiB per request by default ([etcd limits][etcd-limits]). Consul KV allows 512 KB per value ([Consul KV][consul-kv]). Put large blobs elsewhere and store a pointer. |
| One Raft cluster over a WAN without tuning | False elections and slow writes | etcd: election timeout at least 10 times the round-trip time. Consul runs one Raft cluster per datacenter and does not replicate between them by default ([Consul control plane][consul-control-plane]). |
| Leader or lock without fencing tokens | A paused old holder writes after a new holder exists | Give each lock holder or leader an increasing token (the Raft term or a revision). The resource rejects lower tokens ([Kleppmann][kleppmann]). Jepsen found that etcd locks alone do not give mutual exclusion ([Jepsen etcd 3.4.3][jepsen-etcd]). pysyncobj's `ReplLockManager` uses a time-based auto-unlock and gives no token. |
| Local reads where the latest value matters | Stale data | ReadIndex or a read through the log (topic 4) |
| fsync off for speed | Lost acknowledged writes after power loss ([Jepsen NATS][jepsen-nats]) | fsync before you acknowledge |
| Wiped node rejoins with its old ID | Amnesia and lost committed writes | Remove the member, then add it back with a new ID (topic 7) |
| Blind retries | Duplicate writes | Client sessions or idempotency keys (topic 5) |

---

## Sources

Raft papers and posts
1. Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 2014. <https://raft.github.io/raft.pdf>
2. Ongaro, "Consensus: Bridging Theory and Practice", PhD thesis, Stanford, 2014. <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf>
3. Ongaro, "bug in single-server membership changes", raft-dev, July 2015. <https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E>
4. Howard and Abraham, "Raft does not Guarantee Liveness in the face of Network Faults", December 2020. <https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/>
5. Cloudflare, "A Byzantine failure in the real world", November 2020. <https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/>
6. Kleppmann, "How to do distributed locking", 2016. <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>

etcd
7. FAQ. <https://etcd.io/docs/v3.6/faq/>
8. Tuning. <https://etcd.io/docs/v3.6/tuning/>
9. Hardware recommendations. <https://etcd.io/docs/v3.6/op-guide/hardware/>
10. Configuration flags. <https://etcd.io/docs/v3.6/op-guide/configuration/>
11. Failure modes. <https://etcd.io/docs/v3.6/op-guide/failures/>
12. Disaster recovery. <https://etcd.io/docs/v3.6/op-guide/recovery/>
13. Runtime reconfiguration. <https://etcd.io/docs/v3.6/op-guide/runtime-configuration/>
14. Learner design. <https://etcd.io/docs/v3.6/learning/design-learner/>
15. Metrics. <https://etcd.io/docs/v3.6/metrics/>
16. Security. <https://etcd.io/docs/v3.6/op-guide/security/>
17. Data corruption. <https://etcd.io/docs/v3.6/op-guide/data_corruption/>
18. System limits. <https://etcd.io/docs/v3.6/dev-guide/limit/>
19. Upgrade to v3.6. <https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/>
20. API guarantees and transactions. <https://etcd.io/docs/v3.6/learning/api/>
21. etcd server Raft settings (`bootstrap.go`). <https://github.com/etcd-io/etcd/blob/main/server/etcdserver/bootstrap.go>
22. Robustness tests. <https://github.com/etcd-io/etcd/tree/main/tests/robustness>
23. etcd/raft README. <https://github.com/etcd-io/raft>
24. etcd/raft `Config` (`raft.go`). <https://github.com/etcd-io/raft/blob/main/raft.go>

HashiCorp
25. hashicorp/raft `config.go`. <https://github.com/hashicorp/raft/blob/main/config.go>
26. hashicorp/raft `api.go`. <https://github.com/hashicorp/raft/blob/main/api.go>
27. hashicorp/raft `raft.go`. <https://github.com/hashicorp/raft/blob/main/raft.go>
28. hashicorp/raft `fsm.go`. <https://github.com/hashicorp/raft/blob/main/fsm.go>
29. hashicorp/raft `net_transport.go` and `replication.go`. <https://github.com/hashicorp/raft/blob/main/net_transport.go>, <https://github.com/hashicorp/raft/blob/main/replication.go>
30. hashicorp/raft CHANGELOG. <https://github.com/hashicorp/raft/blob/main/CHANGELOG.md>
31. Consul consensus protocol. <https://developer.hashicorp.com/consul/docs/architecture/consensus>
32. Consul server performance. <https://developer.hashicorp.com/consul/docs/install/performance>
33. Consul consistency modes. <https://developer.hashicorp.com/consul/api-docs/features/consistency>
34. Consul outage recovery. <https://developer.hashicorp.com/consul/tutorials/operate-consul/recovery-outage>
35. Consul Autopilot. <https://developer.hashicorp.com/consul/docs/manage/scale/autopilot>
36. Consul reference architecture. <https://developer.hashicorp.com/consul/tutorials/production-vms/reference-architecture>
37. Consul reliability. <https://developer.hashicorp.com/consul/docs/concept/reliability>
38. Consul general upgrade process. <https://developer.hashicorp.com/consul/docs/upgrade/instructions/general>
39. Consul KV HTTP API. <https://developer.hashicorp.com/consul/api-docs/kv>
40. Consul KV store. <https://developer.hashicorp.com/consul/docs/automate/kv>
41. Consul control plane. <https://developer.hashicorp.com/consul/docs/architecture/control-plane>
42. Consul `rpc.go` (consistent reads). <https://github.com/hashicorp/consul/blob/main/agent/consul/rpc.go>

pysyncobj
43. Repository and README. <https://github.com/bakwc/PySyncObj>
44. `config.py`. <https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/config.py>
45. `syncobj.py`. <https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/syncobj.py>
46. `journal.py`. <https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/journal.py>
47. `tcp_connection.py`. <https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/tcp_connection.py>
48. `batteries.py`. <https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/batteries.py>
49. PR #202, persist `currentTerm` and `votedFor`. <https://github.com/bakwc/PySyncObj/pull/202>
50. Python `pickle` security warning. <https://docs.python.org/3/library/pickle.html>

Other engineering sources
51. Tang, "How TiKV Uses Lease Read", 2020. <https://tikv.org/blog/lease-read/>
52. Darnell, "Scaling Raft", Cockroach Labs, 2015. <https://www.cockroachlabs.com/blog/scaling-raft/>
53. TiKV, "Multi-Raft" deep dive. <https://tikv.org/deep-dive/scalability/multi-raft/>
54. Cockroach Labs, "Clock management in CockroachDB", 2025. <https://www.cockroachlabs.com/blog/clock-management-cockroachdb/>
55. FoundationDB, "Simulation and Testing". <https://apple.github.io/foundationdb/testing.html>
56. Porcupine. <https://github.com/anishathalye/porcupine>
57. Knossos. <https://github.com/jepsen-io/knossos>

Jepsen
58. Kingsbury, "Call me maybe: etcd and Consul", 2014. <https://aphyr.com/posts/316-call-me-maybe-etcd-and-consul>
59. Jepsen, etcd 3.4.3, 2020. <https://jepsen.io/analyses/etcd-3.4.3>
60. Jepsen, Redis-Raft 1b3fbf6, 2020. <https://jepsen.io/analyses/redis-raft-1b3fbf6>
61. Jepsen, NATS 2.12.1, 2025. <https://jepsen.io/analyses/nats-2.12.1>

[raft-paper]: https://raft.github.io/raft.pdf
[thesis]: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
[raft-dev-bug]: https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E
[howard]: https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/
[cloudflare]: https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/
[kleppmann]: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
[etcd-faq]: https://etcd.io/docs/v3.6/faq/
[etcd-tuning]: https://etcd.io/docs/v3.6/tuning/
[etcd-config]: https://etcd.io/docs/v3.6/op-guide/configuration/
[etcd-failures]: https://etcd.io/docs/v3.6/op-guide/failures/
[etcd-recovery]: https://etcd.io/docs/v3.6/op-guide/recovery/
[etcd-reconfig]: https://etcd.io/docs/v3.6/op-guide/runtime-configuration/
[etcd-learner]: https://etcd.io/docs/v3.6/learning/design-learner/
[etcd-metrics]: https://etcd.io/docs/v3.6/metrics/
[etcd-security]: https://etcd.io/docs/v3.6/op-guide/security/
[etcd-corruption]: https://etcd.io/docs/v3.6/op-guide/data_corruption/
[etcd-limits]: https://etcd.io/docs/v3.6/dev-guide/limit/
[etcd-upgrade]: https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/
[etcd-api]: https://etcd.io/docs/v3.6/learning/api/
[etcd-bootstrap]: https://github.com/etcd-io/etcd/blob/main/server/etcdserver/bootstrap.go
[etcd-robustness]: https://github.com/etcd-io/etcd/tree/main/tests/robustness
[er-readme]: https://github.com/etcd-io/raft
[er-raftgo]: https://github.com/etcd-io/raft/blob/main/raft.go
[hr-config]: https://github.com/hashicorp/raft/blob/main/config.go
[hr-api]: https://github.com/hashicorp/raft/blob/main/api.go
[hr-raftgo]: https://github.com/hashicorp/raft/blob/main/raft.go
[hr-fsm]: https://github.com/hashicorp/raft/blob/main/fsm.go
[hr-nettransport]: https://github.com/hashicorp/raft/blob/main/net_transport.go
[hr-replication]: https://github.com/hashicorp/raft/blob/main/replication.go
[hr-changelog]: https://github.com/hashicorp/raft/blob/main/CHANGELOG.md
[consul-consensus]: https://developer.hashicorp.com/consul/docs/architecture/consensus
[consul-perf]: https://developer.hashicorp.com/consul/docs/install/performance
[consul-consistency]: https://developer.hashicorp.com/consul/api-docs/features/consistency
[consul-outage]: https://developer.hashicorp.com/consul/tutorials/operate-consul/recovery-outage
[consul-autopilot]: https://developer.hashicorp.com/consul/docs/manage/scale/autopilot
[consul-refarch]: https://developer.hashicorp.com/consul/tutorials/production-vms/reference-architecture
[consul-reliability]: https://developer.hashicorp.com/consul/docs/concept/reliability
[consul-upgrade]: https://developer.hashicorp.com/consul/docs/upgrade/instructions/general
[consul-kv-api]: https://developer.hashicorp.com/consul/api-docs/kv
[consul-kv]: https://developer.hashicorp.com/consul/docs/automate/kv
[consul-control-plane]: https://developer.hashicorp.com/consul/docs/architecture/control-plane
[consul-rpc]: https://github.com/hashicorp/consul/blob/main/agent/consul/rpc.go
[pso-config]: https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/config.py
[pso-syncobj]: https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/syncobj.py
[pso-journal]: https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/journal.py
[pso-tcp]: https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/tcp_connection.py
[pso-batteries]: https://github.com/bakwc/PySyncObj/blob/master/pysyncobj/batteries.py
[pso-202]: https://github.com/bakwc/PySyncObj/pull/202
[py-pickle]: https://docs.python.org/3/library/pickle.html
[tikv-lease]: https://tikv.org/blog/lease-read/
[crdb-scaling]: https://www.cockroachlabs.com/blog/scaling-raft/
[tikv-multiraft]: https://tikv.org/deep-dive/scalability/multi-raft/
[crdb-clock]: https://www.cockroachlabs.com/blog/clock-management-cockroachdb/
[fdb-testing]: https://apple.github.io/foundationdb/testing.html
[porcupine]: https://github.com/anishathalye/porcupine
[knossos]: https://github.com/jepsen-io/knossos
[jepsen-2014]: https://aphyr.com/posts/316-call-me-maybe-etcd-and-consul
[jepsen-etcd]: https://jepsen.io/analyses/etcd-3.4.3
[jepsen-redisraft]: https://jepsen.io/analyses/redis-raft-1b3fbf6
[jepsen-nats]: https://jepsen.io/analyses/nats-2.12.1
