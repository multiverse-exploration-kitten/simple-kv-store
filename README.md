# simple-kv-store

This is a practice project to learn Raft.
It builds the same key-value store three times, with three different Raft libraries.
The main goal is to learn how to run Raft correctly in production, not only how the algorithm works.

Each part is a 3-node cluster. Each part has tests that cause failures (crash, pause, network partition) and show what Raft does.

## Parts

| Part | Directory | Stack | Status |
|---|---|---|---|
| 1 | [part1-pysyncobj/](part1-pysyncobj/) | Python 3.12, [pysyncobj](https://github.com/bakwc/pysyncobj), FastAPI, uv, ruff, Docker | Done |
| 2 | [part2-hashicorp-raft/](part2-hashicorp-raft/) | Go 1.25, [hashicorp/raft](https://github.com/hashicorp/raft), raft-boltdb, net/http, Docker | Done |
| 3 | `part3-etcd-raft/` | Go, [etcd-io/raft](https://github.com/etcd-io/raft) | Planned |

- **Part 1** is a small KV store on pysyncobj. It shows the basic Raft behavior and the gaps of a small library.
- **Part 2** uses hashicorp/raft. This library runs in Consul, Nomad and Vault. The part copies their patterns, for example write forwarding, three read modes, CAS and idempotency keys. It also adds mutual TLS between peers and a linearizability check with Porcupine.
- **Part 3** uses etcd/raft. This library gives only the core algorithm. You write the storage, the network and the log apply yourself. This part studies Raft in depth.

## Repository layout

```
.
├── README.md                  # this file
├── AGENTS.md                  # rules for coding agents (and for you): Raft rules, test rules
├── docs/
│   ├── raft-in-production.md  # study guide: how to run Raft in production
│   └── plans/                 # one plan file for each part
├── part1-pysyncobj/           # part 1: code, tests, Docker, docs
└── part2-hashicorp-raft/      # part 2: code, tests, Docker, docs
```

## How to use this repository

### 1. Read in this order

1. [docs/raft-in-production.md](docs/raft-in-production.md). It gives the production rules and the reasons for them.
2. The README of a part, for example [part1-pysyncobj/README.md](part1-pysyncobj/README.md).
3. The lessons of a part, for example [part1-pysyncobj/docs/lessons-from-tests.md](part1-pysyncobj/docs/lessons-from-tests.md). It lists what the tests found.
4. The tests. Each failure test has a docstring with three parts: **Scenario**, **Expected** and **Raft lesson**.

### 2. Install the tools

You need these tools:

- [uv](https://docs.astral.sh/uv/) for part 1 (it installs Python 3.12 and the packages)
- [Go 1.25](https://go.dev/dl/) for part 2
- Docker with Docker Compose
- make

### 3. Run part 1

Go to the part directory:

```bash
cd part1-pysyncobj
```

Install the packages:

```bash
make install
```

Run the unit tests and the cluster tests (no Docker):

```bash
make test
```

Start the 3-node cluster in Docker (HTTP on ports 8001, 8002 and 8003):

```bash
make up
```

Write a value:

```bash
curl -X PUT localhost:8001/kv/color -H 'content-type: application/json' -d '{"value": "blue"}'
```

Read the value from a different node:

```bash
curl localhost:8002/kv/color
```

Kill the leader:

```bash
make kill-leader
```

Wait about 3 seconds, so the other two nodes can elect a new leader. Then show the state of each node:

```bash
make status
```

Stop the cluster and delete its data:

```bash
make down
```

Run the end-to-end tests against a separate Docker cluster:

```bash
make e2e
```

Run `make` alone to see all commands, including the chaos commands (`partition`, `heal`, `pause`, `unpause`, `start`).

### 4. Run part 2

Go to the part directory:

```bash
cd part2-hashicorp-raft
```

Run the unit tests and the cluster tests (no Docker):

```bash
make test
```

Start the 3-node cluster in Docker (HTTP on ports 8101, 8102 and 8103). A first container makes the TLS certificates for the peers:

```bash
make up
```

Write a value. Any node accepts the write. A follower forwards it to the leader:

```bash
curl -X PUT localhost:8101/kv/color -H 'content-type: application/json' -d '{"value": "blue"}'
```

Read the value from a different node:

```bash
curl localhost:8102/kv/color
```

Kill the leader, wait about 3 seconds, then show the state of each node:

```bash
make kill-leader
```

```bash
make status
```

Stop the cluster and delete its data:

```bash
make down
```

Run the end-to-end tests against a separate Docker cluster:

```bash
make e2e
```

Run `make` alone to see all commands. The part 2 README explains the read modes, CAS and idempotency keys.

## Notes

- Part 1: `make up` writes a random Raft peer password to `part1-pysyncobj/.env`. Git ignores this file.
- Part 2: the certificates live in Docker volumes. `make down` deletes them.
- Part 2: `make up` writes a random admin token to `part2-hashicorp-raft/.env` (git-ignored). The membership and snapshot calls need it.
- Part 1 and part 2 use different ports and subnets, so both clusters can run at the same time.
- Each part is self-contained. Parts do not share code.
