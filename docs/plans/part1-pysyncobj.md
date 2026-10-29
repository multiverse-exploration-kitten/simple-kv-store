# Part 1 plan: KV store on pysyncobj

## Status

- All phases done (1-7). Phase 7 added peer encryption (KV_RAFT_PASSWORD), not in the original list.
- Tests: 56 unit + cluster (`uv run pytest`), 6 Docker e2e (`uv run pytest -m docker`). All pass.
- Code review done. Skipped on purpose: cached readiness, dev default peer password (both in README).
- Docs: README, docs/lessons-from-tests.md, docs/pysyncobj-reference.md, ../docs/raft-in-production.md.
- Next: part 2 (Go + hashicorp/raft) needs its own plan.

## Goal

A 3-node replicated key-value store. Python 3.12, pysyncobj, FastAPI, uv, ruff, pytest, Docker Compose.
Tests demonstrate the Raft failure scenarios. The user's top priority: learn how to use Raft correctly in production.
Read `AGENTS.md` (repo root) first. Its Raft rules and testing rules are part of this plan.

## Architecture

```
HTTP client ──> FastAPI (api.py) ──> KeyValueNode (node.py) ──> pysyncobj SyncObj ──TCP──> 2 peers
                                         │
                                         └─> ReplicatedKeyValueStore (store.py, SyncObjConsumer)
                                                └─> KeyValueState (state.py, pure, deterministic)
```

- Any node accepts writes. pysyncobj forwards the command from a follower to the leader.
- Writes and linearizable reads go through the Raft log. A stale read reads local memory.
- One process per node. One uvicorn worker. Sync FastAPI endpoints (they run in the threadpool).

## Module contracts (wave 1 writes these; later waves use them, do not change them)

`src/kvstore/state.py` — `KeyValueState`: `put`, `delete -> bool`, `compare_and_set(key, expected_value: str | None, new_value) -> bool`, `get -> str | None`.

`src/kvstore/config.py`
- `RaftTimings` (frozen dataclass): Raft timing values. `RaftTimings.for_tests()` returns fast values.
- `NodeConfig` (frozen dataclass): `self_address`, `peer_addresses: tuple[str, ...]`, `bind_address`, `data_dir: Path | None`, `http_host`, `http_port`, `command_timeout_seconds`, `log_compaction_min_entries`, `timings`.
- `NodeConfig.from_env(environ)` reads `KV_SELF_ADDRESS`, `KV_PEERS` (comma list), `KV_BIND_ADDRESS`, `KV_DATA_DIR`, `KV_HTTP_HOST`, `KV_HTTP_PORT`, `KV_COMMAND_TIMEOUT_SECONDS`.

`src/kvstore/errors.py`
- `KeyValueStoreError` (base).
- `CommandRejectedError(reason)`: the command is known NOT to be applied. Safe to retry. HTTP 503.
- `CommandOutcomeUnknownError`: the command may or may not commit (timeout, leader changed). HTTP 504.

`src/kvstore/node.py`
- `ReadConsistency` (str enum): `LINEARIZABLE = "linearizable"`, `STALE = "stale"`.
- `KeyValueNode(config, transport_class=TCPTransport)`:
  - `put(key, value) -> None`, `delete(key) -> bool`, `compare_and_set(key, expected_value, new_value) -> bool`
  - `get(key, consistency=ReadConsistency.LINEARIZABLE) -> str | None`
  - `status() -> dict` (selected keys of `SyncObj.getStatus()`, JSON-safe), `is_leader() -> bool`, `leader_address() -> str | None`, `is_ready() -> bool`
  - `close() -> None` (destroys the SyncObj and frees the port)
  - Raises `CommandRejectedError` or `CommandOutcomeUnknownError`.

## API contract (phase 4 builds it)

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| PUT | `/kv/{key}` | `{"value": str}` | 200 `{"key", "value"}` | 503, 504 |
| GET | `/kv/{key}?consistency=linearizable\|stale` | — | 200 `{"key", "value", "consistency"}` | 404, 503, 504 |
| DELETE | `/kv/{key}` | — | 200 `{"key", "deleted": bool}` | 503, 504 |
| POST | `/kv/{key}/cas` | `{"expected_value": str \| null, "new_value": str}` | 200 `{"key", "succeeded": bool}` | 503, 504 |
| GET | `/status` | — | 200 node status JSON | — |
| GET | `/health/live` | — | 200 `{"status": "ok"}` | — |
| GET | `/health/ready` | — | 200 if a leader is known and the node is ready, else 503 | — |

- 503 body: `{"error": "command_rejected", "reason": str, "retry_safe": true}` plus header `Retry-After: 1`.
- 504 body: `{"error": "outcome_unknown", "detail": "the command may or may not be committed", "retry_safe": false}`.
- Default read consistency: `linearizable`.

## Phases and parallel map

| Phase | Wave | Tier | Owner | Files owned |
|---|---|---|---|---|
| 1 Core | 1 | Complex | main session | `src/kvstore/{state,config,errors,store,node}.py`, `tests/unit/test_state.py`, `tests/unit/test_config.py`, `tests/cluster/{faults,harness,conftest,test_smoke}.py` |
| 2 Cluster tests A | 2 | Complex | phase-builder | `tests/cluster/test_election.py`, `tests/cluster/test_replication.py`, `tests/cluster/test_durability.py` |
| 3 Cluster tests B | 2 | Complex | phase-builder | `tests/cluster/test_partitions.py`, `tests/cluster/test_quorum_loss.py`, `tests/cluster/test_concurrency.py` |
| 4 HTTP API | 2 | Simple | phase-builder | `src/kvstore/api.py`, `src/kvstore/__main__.py`, `tests/unit/test_api.py` |
| 5 Docker + e2e | 3 | Complex | phase-builder | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `Makefile`, `tests/e2e/**` |
| 6 README | 3 | Trivial+ | phase-builder | `part1-pysyncobj/README.md` |

All paths are relative to `part1-pysyncobj/` unless they start with `docs/` at the repo root.
A phase must not edit files that it does not own. If it needs a change in another file, it reports it.

### Phase 1: Core

- [x] Modules in the contract above. Unit test for `config.py`.
- [x] `tests/cluster/faults.py`: `NetworkFaults` (a shared set of blocked `(source, destination)` links) and a `TCPTransport` subclass that drops messages on blocked links, on send and on receive.
- [x] `tests/cluster/harness.py`: `Cluster` (start N nodes on free ports, own data dir each, fast timings) and `wait_until(probe, description, timeout)`.
  `Cluster` methods: `nodes`, `node(address)`, `addresses`, `start(address)`, `stop(node)`, `restart(node)`, `close()`, `wait_for_leader(among=None)`, `followers(leader)`, `partition(group_a, group_b)`, `isolate(node) -> others`, `block(source, destination)` (one-way), `heal()`, `wait_for_value_everywhere(key, value)`.
- [x] `tests/cluster/conftest.py`: fixtures `cluster` (3 nodes) and `make_cluster(**options)` (options: `size`, `command_timeout_seconds`, `log_compaction_min_entries`, `timings`).
- [x] `tests/cluster/test_smoke.py`: 3 nodes elect one leader, a write replicates to all. Partition and heal.

### Phase 2: Cluster tests A (election, replication, durability)

Each test has a docstring: **Scenario**, **Expected**, **Raft lesson**. Use the `Cluster` harness. No fixed sleeps.

- [ ] Exactly one leader in a 3-node cluster. All nodes agree on the leader and the term.
- [ ] Leader crash: the 2 survivors elect a new leader in a higher term. Committed data survives. Writes continue.
- [ ] A write sent to a follower is forwarded to the leader and is visible on every node.
- [ ] One follower down: writes still commit (2 of 3 is a majority).
- [ ] A stopped follower restarts and catches up on the writes that it missed.
- [ ] Log compaction: with a small `log_compaction_min_entries`, a follower that missed many writes catches up from a snapshot. Assert the log stays short.
- [ ] Full cluster restart: all 3 nodes stop and start again from their data dirs. All committed data is still there.

### Phase 3: Cluster tests B (partitions, quorum loss, ambiguity, concurrency)

- [ ] Majority down (2 of 3 stopped): writes fail with `CommandRejectedError` or `CommandOutcomeUnknownError`. They do not hang longer than the command timeout. A linearizable read fails. A stale read still returns the old value.
- [ ] Leader isolated in a minority: the majority side elects a new leader and commits writes. The old leader cannot commit. After `leaderFallbackTimeout` the old leader steps down (check-quorum).
- [ ] Heal after partition: the old leader becomes a follower. Its uncommitted write is discarded. All nodes converge on the majority's data.
- [ ] Stale read: a partitioned follower returns an old value with `STALE`. A `LINEARIZABLE` read on it fails.
- [ ] Ambiguous write: block only follower-to-leader messages (one-way). A write times out with `CommandOutcomeUnknownError`. After heal, the write is committed anyway. This is why a timeout is not a failure.
- [ ] CAS under concurrency: many threads increment one counter with compare-and-set retries through different nodes. The final value equals the number of successful CAS calls (no lost updates).
- [ ] Disruptive rejoin: an isolated follower keeps starting elections and raises its term. After heal, its higher term forces the healthy leader to step down. First verify by experiment whether pysyncobj has pre-vote. Write the test for the behavior you observe, and say in the docstring that etcd/raft fixes it with PreVote.

### Phase 4: HTTP API

- [ ] `api.py`: `create_app(node: KeyValueNodeProtocol) -> FastAPI`. A `typing.Protocol` with the `KeyValueNode` methods that the API uses, so tests can pass a fake.
- [ ] Error mapping as in the API contract.
- [ ] `__main__.py`: `main()` reads `NodeConfig.from_env()`, builds the node and the app, runs uvicorn with 1 worker, closes the node on shutdown.
- [ ] `tests/unit/test_api.py`: FastAPI `TestClient` with a fake node. Test every route and every error mapping.

### Phase 5: Docker + e2e

- [ ] `Dockerfile`: `python:3.12-slim`, uv from `ghcr.io/astral-sh/uv`, `uv sync --locked --no-dev`, non-root user, `CMD ["kvstore"]`.
- [ ] `docker-compose.yml`: `node1`, `node2`, `node3` on network `raft`. Raft port 4321. HTTP 8000 published as 8001, 8002, 8003. One named volume per node at `/data`. Healthcheck on `/health/live` with Python `urllib` (no curl in slim).
- [ ] `Makefile`: `install`, `lint`, `format`, `test`, `e2e`, `up`, `down`, `logs`, `status`, `kill-leader`, `partition NODE=`, `heal NODE=`, `pause NODE=`, `unpause NODE=`.
- [ ] `tests/e2e/`: marked `docker`. Fixture starts the cluster (`docker compose up -d --build --wait`) and deletes it at the end (`down -v`). Tests: replicate across nodes, kill the leader then restart it, pause a follower, disconnect the leader from the network then reconnect, stop 2 nodes then expect 503/504.

### Phase 6: README

- [ ] `part1-pysyncobj/README.md`: what it is, architecture diagram, API, run, test, chaos commands, a table "failure scenario → test → Raft lesson", and "What pysyncobj does not give you in production" (link to `docs/raft-in-production.md`).
