# Part 1: Raft key-value store on pysyncobj

This is part 1 of 3 of a practice project. It is a 3-node replicated key-value store.
It uses Python 3.12, pysyncobj 0.3.17, FastAPI, uv and Docker Compose.
The goal is to learn how to run Raft correctly in production, not only how the algorithm works.
The tests inject real faults (crash, pause, partition) and show what happens.

Run every command on this page from the `part1-pysyncobj/` directory.

## Architecture

```
HTTP client --> FastAPI (api.py) --> KeyValueNode (node.py) --> pysyncobj SyncObj --TCP--> 2 peers
                                          |
                                          +--> ReplicatedKeyValueStore (store.py)
                                                  +--> KeyValueState (state.py, pure and deterministic)
```

- Any node accepts writes. pysyncobj forwards a command from a follower to the leader.
- Writes and linearizable reads go through the Raft log. The default read is linearizable.
- A stale read (`?consistency=stale`) reads local memory. It can be old.
- One process and one uvicorn worker per node. Each worker would start its own Raft node.
- Each node keeps its journal and snapshot on its own Docker volume (`/data`).
- The Raft port 4321 stays inside the Docker network. Only the HTTP port is published.

## Quick start

Start the cluster. It builds the image and waits until all 3 nodes are healthy.

```bash
make up
```

Show the Raft state of each node.

```bash
make status
```

Write a value. You can send the write to any node.

```bash
curl -X PUT http://127.0.0.1:8001/kv/greeting -H 'Content-Type: application/json' -d '{"value": "hello"}'
```

Read it with the default (linearizable) read.

```bash
curl http://127.0.0.1:8002/kv/greeting
```

Read it from local memory (stale read).

```bash
curl 'http://127.0.0.1:8002/kv/greeting?consistency=stale'
```

Compare-and-set. It succeeds only if the current value equals `expected_value`.
Use `"expected_value": null` to mean "the key must not exist".

```bash
curl -X POST http://127.0.0.1:8001/kv/greeting/cas -H 'Content-Type: application/json' -d '{"expected_value": "hello", "new_value": "world"}'
```

Delete a key.

```bash
curl -X DELETE http://127.0.0.1:8001/kv/greeting
```

Show the status of one node, and check if it is ready.

```bash
curl http://127.0.0.1:8001/status
```

```bash
curl -i http://127.0.0.1:8001/health/ready
```

Stop the cluster and delete its data volumes.

```bash
make down
```

## API

Node 1 listens on port 8001, node 2 on 8002 and node 3 on 8003. The API is the same on every node.

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| PUT | `/kv/{key}` | `{"value": str}` | 200 `{"key", "value"}` | 503, 504 |
| GET | `/kv/{key}?consistency=linearizable\|stale` | none | 200 `{"key", "value", "consistency"}` | 404, 503, 504 |
| DELETE | `/kv/{key}` | none | 200 `{"key", "deleted": bool}` | 503, 504 |
| POST | `/kv/{key}/cas` | `{"expected_value": str \| null, "new_value": str}` | 200 `{"key", "succeeded": bool}` | 503, 504 |
| GET | `/status` | none | 200 node status JSON | none |
| GET | `/health/live` | none | 200 `{"status": "ok"}` | none |
| GET | `/health/ready` | none | 200 `{"status": "ready", "leader"}` or 503 `{"status": "not_ready", "leader"}` | 503 |

- A stale read does not use the log, so it never returns 503 or 504.
- A GET for a missing key returns 404.
- `/health/ready` is 200 only when a leader is known, the node has connections to a majority, and the node has applied every write committed before it started (see [lessons](docs/lessons-from-tests.md)).
- `/status` shows `ready` from a cached flag. Only `/health/ready` runs the check. So `ready` can be `false` in `/status` until someone calls `/health/ready` on that node.

### 503 and 504

| Code | Body | Meaning | What the client does |
|---|---|---|---|
| 503 | `{"error": "command_rejected", "reason": ..., "retry_safe": true}` and header `Retry-After: 1` | The command is known NOT to be applied. | Retry. |
| 504 | `{"error": "outcome_unknown", "detail": ..., "retry_safe": false}` | The command may or may not be committed. | Do not retry blindly. Read the key first, or use CAS. |

The 503 reasons are `no_quorum`, `no_leader`, `not_leader`, `queue_full`, `discarded` and `request_denied`.
A 504 comes from a timeout, from `leader_changed` or from `unknown_outcome`.
A timeout is not a failure. The write can still commit later (test `test_write_that_times_out_can_still_commit`).

## Configuration

`NodeConfig.from_env()` reads these environment variables. `docker-compose.yml` sets the first four and `KV_RAFT_PASSWORD`.

| Variable | Default | Why it matters |
|---|---|---|
| `KV_SELF_ADDRESS` | required, for example `node1:4321` | The address that the peers use to reach this node. It is the node identity in the cluster. |
| `KV_PEERS` | empty | Comma list of all nodes. The node removes its own address from the list. |
| `KV_BIND_ADDRESS` | same as `KV_SELF_ADDRESS` | The address to listen on. In Docker it is `0.0.0.0:4321`. |
| `KV_DATA_DIR` | empty (memory only) | Folder for the journal and snapshot. Without it, a restart loses all data. |
| `KV_RAFT_PASSWORD` | empty (no encryption). Compose requires it and has no default. `make up` writes a random one to `.env` (git-ignored). | pysyncobj sends peer messages as pickles. Without a password, anyone who reaches port 4321 can run code on the node. With a password, pysyncobj encrypts and signs every peer message (Fernet). All nodes need the same value. To run `docker compose` without `make`, export the variable or create `.env` first. |
| `KV_HTTP_HOST` | `0.0.0.0` | The HTTP listen address. |
| `KV_HTTP_PORT` | `8000` | The HTTP listen port. |
| `KV_COMMAND_TIMEOUT_SECONDS` | `3.0` | How long a command waits for a commit before it returns 504. It turns "hang forever" into an error. |

The Raft timing values are in `RaftTimings` in `src/kvstore/config.py`. They are not environment variables.

| Field (pysyncobj option) | Default | `for_tests()` | Why it matters |
|---|---|---|---|
| `election_timeout_min` (`raftMinTimeout`) | 0.4 s | 0.3 s | Lower bound of the random election timeout. It sets how fast a failover starts. |
| `election_timeout_max` (`raftMaxTimeout`) | 1.4 s | 0.8 s | Upper bound. The random range stops two nodes from campaigning at the same time. |
| `heartbeat_interval` (`appendEntriesPeriod`) | 0.1 s | 0.05 s | The leader sends entries on this tick. A write takes about 2 ticks. |
| `connection_timeout` (`connectionTimeout`) | 3.5 s | 1.0 s | How long a silent TCP link stays open. It sets when a node sees a dead peer. |
| `connection_retry_interval` (`connectionRetryTime`) | 1.0 s | 0.2 s | How often a node reconnects to a dead peer. The library default is 5 s. |
| `leader_fallback_timeout` (`leaderFallbackTimeout`) | 5.0 s | 1.5 s | Check-quorum. A leader without a majority steps down after this time. The library default is 30 s. |

Other `NodeConfig` fields: `log_compaction_min_entries` (default 5000), `log_compaction_min_seconds` (default 300).
The node also sets `commandsWaitLeader=False` (fail fast with `no_leader`) and `useFork=False` (see [lessons](docs/lessons-from-tests.md)).

## Tests

There are three levels.

| Level | Folder | What it needs |
|---|---|---|
| Unit | `tests/unit/` | nothing |
| Cluster | `tests/cluster/` | real Raft nodes in one process on `127.0.0.1`, with fault injection |
| E2E | `tests/e2e/` | Docker. Marked `docker`. |

Install the dependencies.

```bash
make install
```

Run the unit and cluster tests. The `docker` tests are skipped.

```bash
make test
```

Run the e2e tests. They start their own cluster (project `kvstore-part1-e2e`, ports 18001-18003) and delete it at the end.

```bash
make e2e
```

Run one test file.

```bash
uv run pytest tests/cluster/test_partitions.py
```

Run one test.

```bash
uv run pytest tests/cluster/test_partitions.py::test_write_that_times_out_can_still_commit
```

Run ruff.

```bash
make lint
```

## Chaos commands

These `make` targets inject faults into the running Docker cluster (after `make up`).

| Target | What it does |
|---|---|
| `make kill-leader` | Finds the leader and sends it SIGKILL. |
| `make start NODE=node1` | Starts a stopped or killed node. |
| `make pause NODE=node2` | Freezes a node (`docker pause`). It looks like a long GC pause. |
| `make unpause NODE=node2` | Unfreezes the node. |
| `make partition NODE=node1` | Disconnects the node from the Raft network. |
| `make heal NODE=node1` | Connects the node again with its static IP and its DNS alias. |
| `make logs NODE=node1` | Follows the logs of one node (all nodes if `NODE` is empty). |
| `make status` | Prints one line per node. Not a fault, but you need it to see the effect. |

`NODE` is `node1`, `node2` or `node3`.

Example session: kill the leader, watch the failover, start it again.

```bash
make up
```

```bash
make status
```

Output (the leader differs between runs):

```
node1: state=follower leader=node3:4321 term=1 commit=10 applied=10 ready=True quorum=True
node2: state=follower leader=node3:4321 term=1 commit=10 applied=10 ready=False quorum=True
node3: state=leader leader=node3:4321 term=1 commit=10 applied=10 ready=False quorum=True
```

```bash
make kill-leader
```

Wait a few seconds, then look again.

```bash
make status
```

Output. The survivors elected node2 in a higher term. Node3 is unreachable.

```
node1: state=follower leader=node2:4321 term=3 commit=11 applied=11 ready=True quorum=True
node2: state=leader leader=node2:4321 term=3 commit=11 applied=11 ready=False quorum=True
node3: unreachable (<urlopen error [Errno 61] Connection refused>)
```

A write through node1 still works (2 of 3 nodes are a majority).

```bash
curl -X PUT http://127.0.0.1:8001/kv/a -H 'Content-Type: application/json' -d '{"value": "1"}'
```

Start the old leader. It comes back as a follower and catches up.

```bash
make start NODE=node3
```

```bash
make status
```

```bash
make down
```

Try the same with `make partition NODE=node1`, then `make status`, then `make heal NODE=node1`.
In one run the cut-off node raised its term, and after `make heal` it won the next election at term 7 (it was term 3 before).
This is the missing pre-vote. See [lessons](docs/lessons-from-tests.md).

## Failure scenarios and tests

Cluster tests are in `tests/cluster/`. E2E tests are in `tests/e2e/`. Each failure test has a docstring with Scenario, Expected and Raft lesson.

| Failure scenario | Test | What it shows |
|---|---|---|
| 3 healthy nodes start | `tests/cluster/test_smoke.py::test_three_nodes_elect_one_leader_and_replicate_a_write` | Any node accepts a write and forwards it to the leader. |
| Leader cut off, then healed | `tests/cluster/test_smoke.py::test_partition_and_heal_moves_leadership` | A majority makes progress without the old leader. |
| Normal start | `tests/cluster/test_election.py::test_three_nodes_agree_on_one_leader_and_one_term` | One leader per term. Export the term as a metric. |
| Leader crash | `tests/cluster/test_election.py::test_leader_crash_survivors_elect_new_leader_in_higher_term` | The new leader has every committed write. Failover takes about one election timeout. |
| Write sent to a follower | `tests/cluster/test_replication.py::test_write_through_follower_is_forwarded_to_leader_and_applied_everywhere` | Only the leader appends to the log. The caller reads its own write. |
| One follower down | `tests/cluster/test_replication.py::test_writes_commit_with_one_follower_down` | 2 of 3 is a majority. The cluster has no spare node now. |
| Follower restart | `tests/cluster/test_replication.py::test_restarted_follower_catches_up_on_missed_writes` | The leader sends the missing entries. Wait for ready before you read. |
| Follower misses many writes | `tests/cluster/test_durability.py::test_lagging_follower_catches_up_from_a_snapshot` | Log compaction. The follower gets a full snapshot. |
| All nodes restart | `tests/cluster/test_durability.py::test_full_cluster_restart_keeps_all_committed_data` | Journal and snapshot keep the data. Clean stop only. |
| All nodes restart, read at once | `tests/cluster/test_durability.py::test_ready_after_full_restart_means_all_committed_data_is_applied` | pysyncobj `isReady()` is too early. A linearizable read is the barrier. |
| 2 of 3 nodes down | `tests/cluster/test_quorum_loss.py::test_majority_down_fails_closed_within_the_command_timeout` | Fail closed and fast. A stale read still works. |
| Leader cut off from both followers | `tests/cluster/test_quorum_loss.py::test_isolated_leader_cannot_commit_and_steps_down_after_check_quorum` | Two leaders for a short time. Check-quorum ends it. |
| Old leader takes a write, then heals | `tests/cluster/test_partitions.py::test_heal_discards_the_old_leaders_uncommitted_write` | A 504 can mean "lost". The uncommitted entry is overwritten. |
| Follower cut off | `tests/cluster/test_partitions.py::test_cut_off_follower_serves_stale_reads_but_rejects_linearizable_reads` | A stale read can be old. A linearizable read fails. |
| One-way partition (acks lost) | `tests/cluster/test_partitions.py::test_write_that_times_out_can_still_commit` | A timeout is not a failure. The write commits after heal. |
| Isolated follower rejoins | `tests/cluster/test_partitions.py::test_rejoining_follower_with_a_higher_term_disrupts_the_leader` | No pre-vote. A rejoining node forces a new election. |
| 3 nodes share a password | `tests/cluster/test_security.py::test_cluster_with_password_elects_leader_and_replicates` | Authenticated peer traffic works like normal traffic. |
| One node has a wrong password | `tests/cluster/test_security.py::test_node_with_wrong_password_cannot_join` | The bad node never sees a leader and never gets the value. It logs nothing. |
| 6 threads increment one counter | `tests/cluster/test_concurrency.py::test_concurrent_cas_increments_lose_no_updates` | Use CAS for read-modify-write. |
| Replicate across containers | `tests/e2e/test_replication.py::test_write_on_one_node_is_readable_on_the_other_two` | Linearizable and stale reads on the real cluster. |
| Leader killed (SIGKILL) | `tests/e2e/test_failover.py::test_killed_leader_is_replaced_and_catches_up_after_restart` | The cluster survives one crash. The old leader catches up. |
| Follower frozen (`docker pause`) | `tests/e2e/test_failover.py::test_paused_follower_does_not_block_writes_and_catches_up` | A frozen node is a slow follower. |
| Leader disconnected from the network | `tests/e2e/test_partition.py::test_disconnected_leader_is_replaced_and_rejoins_as_follower` | The old leader sees a higher term and steps down. |
| 2 of 3 containers stopped | `tests/e2e/test_quorum_loss.py::test_without_majority_writes_fail_fast_and_stale_reads_still_work` | 503 or 504 in under 5 s. Stale reads still work. |
| All containers stopped and started | `tests/e2e/test_durability.py::test_full_cluster_restart_keeps_the_data` | Data survives a restart on the volumes. No fsync, so not a power loss. |

## What pysyncobj does not give you

Details, evidence and what this repo does about each point are in [docs/lessons-from-tests.md](docs/lessons-from-tests.md).
The library API and options are in [docs/pysyncobj-reference.md](docs/pysyncobj-reference.md).
The general rules are in the study guide [../docs/raft-in-production.md](../docs/raft-in-production.md).

- Peer traffic is pickle. Without `KV_RAFT_PASSWORD`, anyone who reaches port 4321 can run code. The password gives encryption and signing, not mutual TLS.
- Replicated methods are numbered by sorted (version, name). A new command or a rename can break a mixed-version cluster.
- A wrong password gives no log line, only a cluster that cannot join.
- An exception inside a replicated method stops the log on every node for good. `KeyValueNode` checks the input first.
- No pre-vote. A rejoining node with a high term forces the healthy leader to step down.
- No ReadIndex or lease reads. Each linearizable read is a log entry.
- No fsync. The journal is a memory-mapped file. A power loss can lose recent entries.
- The commit index is saved once per second. `isReady()` can be true too early after a full restart.
- No client sessions or idempotency. A 504 write needs a CAS or a read to resolve.
- `LEADER_CHANGED` is documented like `NOT_LEADER`, but the outcome is unknown.
- Check-quorum is on, but the default is 30 s. This repo uses 5 s.
- A cut-off follower keeps the dead leader's address. This repo adds a `no_quorum` fast reject.
- The leader compacts its log without checking the followers. A slow follower often needs a full snapshot.
- DNS results are cached for 600 s. Docker needs static IPs.
- A snapshot with `useFork=False` blocks the Raft thread. A fork with threads can deadlock.
- `getStatus()` can raise `RuntimeError` during a leader change.
