# AGENTS.md

Instructions for coding agents (Claude Code, Codex, others) that work in this repository.

## 1. Purpose

This repository is a practice project. It builds the same distributed key-value (KV) store three times, with three Raft libraries. The main goal is to learn how to use Raft correctly **in production**, not only how the algorithm works.

| Part | Directory | Stack | Status |
|---|---|---|---|
| 1 | `part1-pysyncobj/` | Python 3.12, pysyncobj, FastAPI, uv, ruff, pytest, Docker | built |
| 2 | `part2-hashicorp-raft/` | Go, hashicorp/raft, Docker | not started |
| 3 | `part3-etcd-raft/` | Go, go.etcd.io/raft/v3 | not started, needs a plan first |

Shared study material:

- `docs/raft-in-production.md` — the study guide. Each topic says which part demonstrates it.
- `docs/plans/` — one plan file per part. Read the plan of a part before you change that part.

## 2. Repository rules

1. Each part is self-contained. Do not share code, dependencies or Docker files between parts.
2. Each part has its own `README.md` with run, test and chaos commands.
3. Do not commit `goal.md`. It is in `.gitignore`. Do not remove it from `.gitignore`.
4. Do not commit Raft data (journals, snapshots, `raft-data/`).
5. Use Conventional Commits: `feat(part1): ...`, `test(part1): ...`, `docs: ...`, `chore: ...`.
6. Do not build a part that the user did not ask for.

## 3. Code style

1. Use descriptive names. Do not use one-letter names, except `i` in a short loop.
2. Follow SOLID where it removes real coupling. Do not add layers, factories or interfaces that have only one use and no test need.
3. Python: type hints on every public function. `ruff check` and `ruff format --check` must pass.
4. Go (parts 2, 3): `gofmt`, `go vet` and `golangci-lint` (if configured) must pass.
5. Keep comments short. Write a comment only when the code cannot say why.

## 4. Raft rules (production correctness)

Every part must follow these rules. Tests must prove the rules that are testable.

1. **Deterministic apply.** The state machine apply function must give the same result on every node. Do not read the clock, random numbers, environment or files inside apply. Pass such values as command arguments.
2. **Version the commands.** A command format change must not break nodes that run the old version during a rolling upgrade.
3. **A timeout is not a failure.** A write that times out may still commit. The API must report "outcome unknown" (HTTP 504), not "failed".
4. **Make the read consistency explicit.** The API must say which reads are linearizable and which reads can be stale. The default read is the safe one.
5. **Use compare-and-set (CAS) for read-modify-write.** Do not tell clients to read, change and write back.
6. **Fail closed without quorum.** Without a majority, writes and linearizable reads must fail fast with HTTP 503. They must not hang forever.
7. **Persist before you acknowledge.** Each node keeps its Raft log and snapshots on its own disk (Docker volume). Know and document the fsync behavior of the library.
8. **One process per node.** Do not run more than one web worker per Raft node. Each worker would start its own Raft node.
9. **Odd cluster sizes.** Use 3 nodes in Docker. Document why.

## 5. Testing rules

1. Each part has three test levels:
   - `tests/unit/` — pure logic. No network. Runs in milliseconds.
   - `tests/cluster/` — real Raft nodes in one process on `127.0.0.1`, with fault injection (kill, restart, partition, one-way partition). These tests demonstrate the failure scenarios.
   - `tests/e2e/` — the Docker cluster. Uses `docker kill`, `docker pause` and `docker network disconnect`. Marked `docker`. Not part of the default test run.
2. Each failure-scenario test has a docstring with three parts: **Scenario**, **Expected**, **Raft lesson**.
3. Do not use fixed `sleep` to wait for the cluster. Poll a condition with a deadline (`wait_until`). A failed wait must say what it waited for.
4. Use free ports and `tmp_path` for data, so tests can run in parallel and leave nothing behind.
5. Each test must stop every node it starts, also when the test fails (use fixtures).
6. A flaky test is a bug. Fix the cause. Do not add retries to hide it.

## 6. Part 1 commands

Run all commands from `part1-pysyncobj/`.

```bash
uv sync                                  # install dependencies into .venv
uv run ruff check . && uv run ruff format --check .
uv run pytest                            # unit + cluster tests
uv run pytest -m docker                  # e2e tests against the Docker cluster
docker compose up -d --build             # start 3 nodes on ports 8001-8003
docker compose down -v                   # stop the cluster and delete its data
```

The part 1 README has the full list, including chaos commands.

## 7. Workflow for agents

1. Read this file, the part README and the part plan in `docs/plans/` first.
2. Write or update the plan before a big change. Keep its `## Status` block current.
3. Write the test first for a new failure scenario. See it fail. Then fix the code.
4. Before you say "done": run ruff, run the tests, and (for Docker changes) start the cluster and run the e2e tests. Report the real output.
5. Record what you learn about a Raft library (gotchas, bugs, defaults) in the part `docs/` folder, not only in chat.
