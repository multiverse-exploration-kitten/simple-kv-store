# pysyncobj reference (version 0.3.17)

This file is a checked reference for the Python library
[pysyncobj](https://github.com/bakwc/PySyncObj). It answers 17 questions that
matter when you build a 3-node key-value store with pysyncobj, FastAPI and Docker.

## How this was checked

- Library version: **pysyncobj 0.3.17**. The PyPI package and the GitHub
  `master` branch (commit `6962777`, 2026-08-30) have identical source.
- Python: **3.12.9** and **3.13.2** (Homebrew), on macOS (Darwin 25.6).
- Date of the checks: 2026-10-28 and 2026-10-29.
- Each fact has a tag:
  - **[tested]**: a script ran it and showed the result. Unless a note says
    otherwise, it ran on both Python versions.
  - **[code]**: read in the library source. The file and function are given.
  - **[not verified]**: the fact was not checked. Do not trust it without a test.
- Line numbers refer to `pysyncobj/` in version 0.3.17.
- The study scripts are in the scratchpad folder of the research session:
  `/private/tmp/claude-501/-Users-william-workspace-simple-kv-store/2ee5234f-11ad-496c-90b3-250d84527b4f/scratchpad/pysyncobj-study/scripts/`.
  That folder is temporary. The important code is copied into this file.

## The 12 most important facts

1. **A replicated method that raises an exception stops the whole cluster.**
   All nodes stop applying entries and stop processing network messages. No
   leader is elected again. [tested] Never raise inside a `@replicated` method.
2. **A timeout does not cancel a write.** A `sync=True` call that raises
   `SyncObjException('Timeout')` can still be applied later. [tested]
3. **No fsync anywhere.** The journal is an mmap file. Data survives
   `kill -9` of the process, but power loss or a kernel crash can lose
   acknowledged writes. [tested for kill -9] [code for no fsync]
4. **`journalFile` without `fullDumpFile` loses all data** after the first log
   compaction and a full cluster restart. The nodes never become ready. [tested]
5. **`fullDumpFile` without `journalFile` loses all writes after the last
   snapshot** on a full cluster restart. [tested]
6. **An isolated leader stays leader for `leaderFallbackTimeout` (default 30 s).**
   Its local reads are stale. Writes sent to it are later `DISCARDED`. [tested]
7. **No pre-vote.** A node that is cut off from the leader, but still has an
   open TCP connection, starts elections and raises its term. After the heal,
   that higher term forces the healthy leader to step down. [tested]
8. **No ReadIndex and no lease read.** Plain method calls read local state and
   can be stale on any node. A `@replicated` method that returns a value is a
   linearizable read, but it costs one log entry. [tested] [code]
9. **Method IDs come from the sort order `(ver, consumer number, method name)`.**
   If you add a new `@replicated` method without a higher `ver`, the journal
   replays into the wrong method. [tested]
10. **Default write latency is one heartbeat.** With defaults a sync write takes
    about 100 ms through the leader and 200 ms through a follower.
    With `appendEntriesUseBatch=False` it was about 1 ms (max 3 ms) on
    localhost. [tested]
11. **The node name string must match exactly.** `localhost:4321` and
    `127.0.0.1:4321` are two different nodes for pysyncobj. [tested]
12. **Without `password`, any host that can reach the Raft port can run code**
    on the node, because messages are unpickled. [code] (issue #205)

---

## Q1. Latest version, release date, maintenance

- Latest version on PyPI: **0.3.17**, uploaded **2026-08-30**. Only an sdist
  (`pysyncobj-0.3.17.tar.gz`), no wheel. It is pure Python with no required
  dependencies. [tested: PyPI JSON API]
- Recent releases: 0.3.14 (2025-02-19), 0.3.15 (2026-03-17), 0.3.16
  (2026-08-14), 0.3.17 (2026-08-30). [tested: PyPI JSON API]
- `setup.py` has no `python_requires`. Its classifiers name only Python 2.7,
  3.4 and 3.5. They are old and do not say anything about 3.12 or 3.13. [code]
- CI: GitHub Actions runs `pytest test_syncobj.py` on `ubuntu-latest` with the
  default `python3` of the runner (`.github/workflows/tests.yaml`). [code]
- Maintenance: **maintained, but slowly, by one main person** (Filipp Ozinov,
  GitHub `bakwc`, 337 of the commits). Activity comes in bursts. In 2026 the
  maintainer merged important safety fixes (PRs #197, #199, #202). But:
  - 34 issues are open.
  - Issue #84 (journal is not synced to disk) is open since 2018.
  - Security issue #205 (2026-09-28) has no answer.
  - PR #206 (fsync of snapshots, 2026-10-22) is not merged.
  [tested: `gh issue list`, `gh pr list`, `git log`]
- Use **0.3.17 or later**. Older versions have the data-divergence bug from
  issue #201 (term and vote were not saved).

## Q2. Python 3.12 and 3.13 compatibility

**Result: works on both. [tested]**

What ran on both 3.12.9 and 3.13.2, in one process on `127.0.0.1`:

| Check | 3.12 | 3.13 |
|---|---|---|
| 3 nodes start, elect a leader, all `isReady()` | 0.36 s | 0.41 s |
| Sync write through a follower | OK | OK |
| Leader isolated, new leader elected (10 runs per version) | 10/10 | 10/10 |
| Journal + dump restart, snapshot install, membership change | OK | OK |
| 32 threads x 25 sync writes, no errors | OK | OK |
| `password` (with `cryptography` 50.0.2) | OK | OK |

Warnings that you will see on both versions [tested]:

- `DeprecationWarning: This process ... is multi-threaded, use of fork() may
  lead to deadlocks in the child.` It comes from `serializer.py:79`
  (`Serializer.serialize`) during log compaction, when `fullDumpFile` is set and
  `useFork=True` (default). Python shows it only when warnings are enabled.
- `ResourceWarning: unclosed file ... .journal.meta` from `journal.py:155`
  (`MetaStorer.getMeta` opens the file without `close`). Harmless.

Not done: the library's own `test_syncobj.py` suite was **not run**. A local
hook in this environment blocks pytest commands outside a project. [not verified]
Free-threaded Python (3.13t, no GIL) was not tested. [not verified]

## Q3. SyncObj constructor and the bind address

Signature (`syncobj.py:96`, `SyncObj.__init__`) [code]:

```python
SyncObj(
    selfNode,
    otherNodes,
    conf=None,
    consumers=None,
    nodeClass=TCPNode,
    transport=None,
    transportClass=TCPTransport,
)
```

| Argument | Meaning |
|---|---|
| `selfNode` | `'host:port'` string or a `Node`. `None` makes a read-only node (it gets the log but does not vote). |
| `otherNodes` | Iterable of `'host:port'` strings or `Node` objects. If it contains `selfNode`, that entry is skipped (line 148). |
| `conf` | `SyncObjConf`. `None` means defaults. `conf.validate()` runs at once (line 121). |
| `consumers` | List of `SyncObjConsumer` objects (for example `ReplDict`). Their `@replicated` methods are replicated too. |
| `nodeClass` | Class used to turn strings into nodes. Default `TCPNode`. |
| `transport` | A ready transport object. If `None`, `transportClass(self, selfNode, otherNodes)` is built (line 210). |
| `transportClass` | Default `TCPTransport`. A subclass is the clean way to inject faults (see Q13). |

### Bind address versus advertised address

- **By default pysyncobj binds to all interfaces.** If `conf.bindAddress` is
  `None`, `TCPTransport._createServer` (`transport.py:258`) takes the port from
  `selfNode` and binds to `0.0.0.0` (or `::` if the host contains `:`).
  [code] [tested: `selfNode='localhost:P'` listens on `*:P` (lsof)]
- `conf.bindAddress='host:port'` overrides this. [tested:
  `bindAddress='127.0.0.1:P'` listens on `127.0.0.1:P` only]
- `selfNode` is the **advertised name**. On each new connection the node sends
  `selfNode.address` as its ID (`_sendSelfAddress`, `transport.py:437`). The
  other side looks the ID up in its own partner list
  (`_onIncomingMessageReceived`, `transport.py:353`). An unknown ID is
  disconnected. [code]

So, for Docker:

```python
# container kv1 (no bindAddress needed: it binds 0.0.0.0:4321 by default)
SyncObj("kv1:4321", ["kv2:4321", "kv3:4321"], conf=conf, consumers=[state])
# Same thing, explicit (add your other options):
conf = SyncObjConf(bindAddress="0.0.0.0:4321")
```

**Gotcha: use exactly the same string for a node everywhere.** Only the node
with the larger address string opens the connection to the other node
(`_shouldConnect`, `transport.py:401`, `self._selfNode.address > node.address`).
[code] Test result: node 0 called itself `127.0.0.1:P0`. Its partners called it
`localhost:P0`. Node 0 was cut off. It never saw a leader and all writes through
it timed out. [tested] In another order, the same mismatch happened to work.
So the bug depends on the names. Use the Docker service names (`kv1:4321`) in
all three places.

**Gotcha: DNS cache.** Every connect attempt resolves the host through a
process-global cache (`dns_resolver.py:21`, `DnsCachingResolver.resolve`). A
good answer is kept for `dnsCacheTime` (600 s). A failed lookup is kept for
`dnsFailCacheTime` (30 s). [code] In Docker Compose a peer name may not resolve
until that container runs. A container that restarts may get a new IP. Set
`dnsCacheTime` and `dnsFailCacheTime` low (for example 5 s and 1 s).
[not verified in Docker]

## Q4. SyncObjConf options

Source: `config.py:18`, `SyncObjConf.__init__`. Every option is a keyword
argument and also an attribute. `validate()` (`config.py:171`) checks these
rules [code]:

- `raftMinTimeout > 3 * appendEntriesPeriod`
- `raftMaxTimeout > raftMinTimeout`
- `connectionTimeout >= raftMaxTimeout`
- `leaderFallbackTimeout > appendEntriesPeriod`
- `logCompactionMinEntries >= 2`
- `serializer` and `deserializer` are both set or both `None`. A `serializer`
  needs `fullDumpFile`.
- `tcp_keepalive` is `None` or a tuple of 3 positive ints.

"Prod" = this option matters for production.

| Option | Default | Prod | Meaning |
|---|---|---|---|
| `password` | `None` | **yes** | Encrypt and authenticate traffic with Fernet. Needs the `cryptography` package, else `ImportError`. Nodes with a wrong password cannot connect. [tested] |
| `autoTick` | `True` | yes | `True`: a background thread runs the Raft loop. `False`: you call `doTick()`. |
| `autoTickPeriod` | `0.05` | | Max wait of one loop step for socket events (seconds). |
| `commandsQueueSize` | `100000` | yes | Max commands waiting in the local queue. Over the limit: `QUEUE_FULL`. `0` means no limit (fixed in 0.3.17, issue #203). [tested] |
| `raftMinTimeout` | `0.4` | **yes** | Lower bound of the random election timeout (s). |
| `raftMaxTimeout` | `1.4` | **yes** | Upper bound of the random election timeout (s). |
| `appendEntriesPeriod` | `0.1` | **yes** | Heartbeat and batch period of the leader (s). |
| `connectionTimeout` | `3.5` | **yes** | No bytes received for this long: the TCP connection is closed. |
| `connectionRetryTime` | `5.0` | **yes** | Wait between reconnect attempts to an offline node (s). |
| `leaderFallbackTimeout` | `30.0` | **yes** | Leader without answers from a majority for this long steps down (check-quorum). See Q8. |
| `appendEntriesUseBatch` | `True` | **yes** | `True`: entries wait for the next heartbeat (high throughput, about +1 heartbeat latency). `False`: send at once. See latency numbers below. |
| `appendEntriesBatchSizeBytes` | `65536` | | Max bytes of entries in one append_entries message. |
| `bindAddress` | `None` | yes | `'host:port'` to bind. `None` = `0.0.0.0` + port of `selfNode`. |
| `preferredAddrType` | `'ipv4'` | | `'ipv4'`, `'ipv6'` or `None` for DNS answers. Note: `'ipv6'` maps to `AF_INET` by mistake (`dns_resolver.py`, `setPreferredAddrFamily`). [code] |
| `sendBufferSize` | `65536` | | `SO_SNDBUF` of sockets. |
| `recvBufferSize` | `65536` | | `SO_RCVBUF` of sockets. |
| `dnsCacheTime` | `600.0` | yes (Docker) | Seconds to keep a DNS answer. |
| `dnsFailCacheTime` | `30.0` | yes (Docker) | Seconds to keep a failed DNS lookup. |
| `logCompactionMinEntries` | `5000` | **yes** | Compact when the log has more entries than this. |
| `logCompactionMinTime` | `300` | **yes** | Also compact when this many seconds passed since the last compaction. |
| `logCompactionSplit` | `False` | | `True`: each node compacts only in its own time slot, so not all nodes fork at once. |
| `logCompactionBatchSize` | `65536` | | Chunk size (bytes) when the leader sends a snapshot. |
| `commandsWaitLeader` | `True` | **yes** | `True`: with no known leader, commands wait in the queue. `False`: they fail at once with `MISSING_LEADER`. See Q6. |
| `fullDumpFile` | `None` | **yes** | Snapshot file. Written at each log compaction. `None` = snapshot in memory only. See Q9. |
| `journalFile` | `None` | **yes** | Raft log file. `None` = log in memory only. See Q9. |
| `bindRetryTime` | `1.0` | | Seconds between bind attempts. |
| `maxBindRetries` | `0` | yes | Max bind attempts. `0` = try forever. Set it to fail fast if the port is busy. |
| `onReady` | `None` | yes | `onReady()` is called once, from the tick thread, when the node is first "ready" (Q12). |
| `onStateChanged` | `None` | yes | `onStateChanged(oldState, newState)`, from the tick thread. States: 0 follower, 1 candidate, 2 leader. |
| `dynamicMembershipChange` | `False` | yes | Allow `addNodeToCluster` / `removeNodeFromCluster` (Q15). |
| `pollerType` | `'auto'` | | `'auto'`, `'select'` or `'poll'`. |
| `useFork` | `True` | yes | Write the snapshot file in a forked child (copy-on-write). `False`: write it in the tick thread (blocks Raft while it writes). |
| `serializer` | `None` | | Custom `serializer(fileName, data)`. `data` is internal Raft data that you must store too. Needs `fullDumpFile` and `deserializer`. |
| `serializeChecker` | `None` | | For an async custom serializer. Returns a `SERIALIZER_STATE` value. |
| `deserializer` | `None` | | Custom `deserializer(fileName)`. Must return the `data` given to `serializer`. With a custom serializer pysyncobj does not restore your state for you. [code: `__loadDumpFile`, `syncobj.py:1472`] |
| `onCodeVersionChanged` | `None` | | `onCodeVersionChanged(oldVer, newVer)` after `setCodeVersion` is applied (Q5). |
| `tcp_keepalive` | `(16, 3, 5)` | | TCP keepalive (idle s, interval s, probes). `None` disables it. On macOS only the interval is set. |

Measured write latency, 3 nodes on localhost, 50 sequential sync writes
[tested]:

| Config | Through leader (median) | Through follower (median) |
|---|---|---|
| Defaults (`appendEntriesUseBatch=True`, period 0.1 s) | 102 ms | 205 ms |
| `appendEntriesUseBatch=False` | 0.5-0.8 ms | 0.6-1.1 ms |

With `appendEntriesUseBatch=False`, 32 threads did 800 sync writes in 0.18 s.
With batching and a 0.05 s period, the same load took 3.2 s. [tested]

## Q5. `@replicated`, `@replicated_sync`, `FAIL_REASON`, `ver`

Source: `replicated` (`syncobj.py:1549`), `replicated_sync` (`syncobj.py:1626`).

### Call options

`callback`, `sync` and `timeout` are reserved keyword arguments. Your
replicated method cannot use these names. [code]

| Call | Behaviour |
|---|---|
| `obj.set_value(k, v)` | Async. Returns `None` at once. No way to know the result. [tested] |
| `obj.set_value(k, v, callback=cb)` | Async. Later `cb(result, error)` runs **in the tick thread**. `error` is a `FAIL_REASON` int. `result` is the method's return value on `SUCCESS`, else `None`. If `callback` is given, `sync` is ignored. [tested] |
| `obj.set_value(k, v, sync=True, timeout=2.0)` | Blocks the caller. Returns the method's return value. Raises `SyncObjException` on failure or timeout. [tested] |
| `obj.set_value(k, v, sync=True)` | Blocks **forever** if the command is never resolved. [tested: still blocked after 2.5 s with no leader] |

`@replicated_sync` makes `sync=True` the default. **Bug: `@replicated_sync(timeout=5)`
ignores its timeout.** The inner function `replicated_sync_impl(func, timeout=None)`
never reads the decorator argument. [code: `syncobj.py:1627`] [tested: a
`@replicated_sync(timeout=0.5)` call with no leader was still blocked after
2.5 s] Always pass `timeout=` at call time.

There is no timeout for async calls (open issue #75). A callback can wait
forever. [code]

### What a sync call raises

| Exception | When |
|---|---|
| `SyncObjException`, `errorCode == 'Timeout'` (a string) | No result within `timeout`. **The command is not cancelled.** It can still commit and apply later. [tested: a write that raised `Timeout` during a partition was applied on all nodes after the heal] |
| `SyncObjException`, `errorCode == <FAIL_REASON int>` | The command failed with that reason (`syncobj.py:1605`). |
| `KeyError` | You called a method that exists only with `ver=N`, and the cluster has not switched to version N yet. [tested] |
| `AttributeError` | A consumer method was called before the consumer was passed to a `SyncObj` (`self._syncObj` is `None`). [code] |

Your own exception inside the method is **not** sent back to the caller. See
"Exceptions inside replicated methods" below.

### FAIL_REASON values

Source: `config.py:2`, `class FAIL_REASON`. [code]

| Value | Name | When it happens (source) |
|---|---|---|
| 0 | `SUCCESS` | The entry was committed and applied on the node where you called it (`__applyLogEntries`, `syncobj.py:721`). |
| 1 | `QUEUE_FULL` | Local command queue full (`_applyCommand`, `syncobj.py:500`). [tested] |
| 2 | `MISSING_LEADER` | No leader known and `commandsWaitLeader=False` (`_checkCommandsToApply`, `syncobj.py:574`). [tested: raised after 0.1 s] |
| 3 | `DISCARDED` | The command got a log index, but a different entry (other term) was committed at that index. Typical: the write went to an old leader that lost its leadership (`syncobj.py:723`). [tested] |
| 4 | `NOT_LEADER` | A follower forwarded the command to a node that is no longer leader but knows another leader. That node does not forward again (`syncobj.py:567-572`). Also: `transferLeadership()` called on a non-leader. [code] |
| 5 | `LEADER_CHANGED` | A follower forwarded the command and was waiting for the leader's reply, then saw a new leader or started an election (`__onLeaderChanged`, `syncobj.py:1246`). [code] |
| 6 | `REQUEST_DENIED` | Membership change refused (the leader's no-op of its term is not applied yet, the node already exists, or the leader removes itself). Also `transferLeadership` to a node that is not caught up. [tested: leader removes itself] |
| 7 | `UNKNOWN_OUTCOME` | A stale reply: the leader's answer names a log index that this node already applied (`syncobj.py:1053-1059`). The command may or may not be applied. |

For the API [code]:

| Result | Meaning for the client | HTTP |
|---|---|---|
| `Timeout`, `LEADER_CHANGED`, `UNKNOWN_OUTCOME` | **Outcome unknown.** The command may already be in the leader's log. | 504 |
| `MISSING_LEADER`, `QUEUE_FULL`, `NOT_LEADER` | Not accepted. No node appended it. Safe to retry. | 503 |
| `DISCARDED` | Not applied. Another entry won that log index. | 503 (or 409) |

### Exceptions inside replicated methods

**If a `@replicated` method raises, the whole cluster goes down.** [tested]

What the source does: `__applyLogEntries` (`syncobj.py:708`) catches only
`SyncObjExceptionWrongVer`. Any other exception leaves `_onTick` before
`self._poller.poll()`. So the node stops reading the network. The next tick
tries the same entry again and fails again. The `_autoTickThread` logs the
error and loops (`syncobj.py:592-597`). Every node applies the same entry, so
every node breaks.

Test result (`del self.data[key]` with a missing key): after 1 s, no node
applied any later entry, and two nodes said they were leader. After 2 s, all
nodes were `CANDIDATE`, the term kept rising, and most connections were
closed. The tick threads were still alive.
With a journal, a restart replays the same entry. So it fails again.
[tested for the live cluster] [code for the restart]

Rules:

- Return an error value instead of raising (`dict.pop(key, None)`, not `del`).
- Validate input **before** you call the replicated method.
- Do not use `pysyncobj.batteries.ReplSet.remove` with a missing item. It
  raises `KeyError` inside a replicated method (`batteries.py:254`). [code]
- Do not read the clock, random numbers or files inside a replicated method.
  Every node must compute the same result.

### `ver` and rolling upgrades

How it works [code]:

- `@replicated(ver=N)` stores the version. The decorator also adds a copy named
  `<name>_v<N>` to the class (`syncobj.py:1614-1617`).
- `SyncObj.__init__` collects all `<name>_v<N>` methods of the object and its
  consumers. It sorts them by `(ver, consumer number, method name)` and gives
  IDs 0, 1, 2, ... in that order (`syncobj.py:233-255`). **The journal stores
  this ID, not the method name.**
- `selfCodeVersion` = the highest `ver` in this node's code.
  `enabledCodeVersion` = the version the cluster switched to (starts at 0).
- A call uses the highest version of that method that is `<= enabledCodeVersion`
  (`__onSetCodeVersion`, `syncobj.py:459`).
- `setCodeVersion(N)` replicates a VERSION entry. When a node applies it, all
  nodes switch to version N at the same log index. It raises if `N` is higher
  than this node's `selfCodeVersion` (`syncobj.py:353`).
- `syncobj_admin -conn host:port -set_version N` does the same remotely.

Upgrade procedure (from the wiki page "Zero-downtime deploy"):

1. Keep the old method. Add the changed method with `@replicated(ver=N)`.
   Add new methods with `ver=N` too.
2. Deploy the new code to every node, one node at a time.
3. When **all** nodes run the new code, call `setCodeVersion(N)`.
4. Remove the old version only after the cluster runs version N.

Test results [tested]:

| Case | Result |
|---|---|
| Old code wrote `increment(5)`. New code added `add_twice` with default `ver=0` and replayed the journal. | `add_twice` sorts first and took ID 0. The journal entry ran `add_twice(5)`. **Total became 10, not 5.** |
| Same, but `add_twice` has `ver=1`. | IDs of old methods did not change. Total 5. Correct. |
| Call a `ver=1`-only method before `setCodeVersion(1)`. | `KeyError`. |
| `increment` has v0 and v1. Before the switch. | All nodes used v0. |
| `setCodeVersion(1)` on a node with old code. | Raises `wrong version, current version is 0, requested version is 1`. |
| `setCodeVersion(1)` while one node still runs old code. | New nodes switched. The old node stopped applying entries (`last_applied` 3, `commit_idx` 5, others at 8). |

Rules for this project:

- Never rename, delete or reorder `@replicated` methods without a version step.
- A new replicated method needs `ver = current max + 1`.
- The order of `consumers=[...]` is part of the ID. Keep it fixed.
- Upgrade all nodes before you call `setCodeVersion`.

## Q6. Does a follower forward writes to the leader?

**Yes. [tested]** A sync write through a follower returned the new value, and
the follower already had the value at that moment.

The exact path [code]:

1. **Caller thread.** The decorator's `newFunc` pickles `(funcID, args, kwargs)`
   and calls `SyncObj._applyCommand` (`syncobj.py:492`). It puts
   `(command, callback)` into the thread-safe `FastQueue`. For `sync=True`, the
   callback sets a `threading.Event`, and the caller waits on it
   (`syncobj.py:1594-1607`).
2. **Tick thread, follower.** `_onTick` calls `_checkCommandsToApply`
   (`syncobj.py:503`). If no leader is known and `commandsWaitLeader=True`, it
   stops and leaves the command in the queue. Else, on a follower that knows
   the leader, it stores the callback in `__commandsWaitingReply[request_id]`
   and sends `{'type': 'apply_command', 'command': ..., 'request_id': ...}` to
   the leader (`syncobj.py:554-566`). The return value of `transport.send` is
   ignored. A lost message is not detected here.
3. **Leader.** `__onMessageReceived` gets `apply_command` and calls
   `_applyCommand(command, (node, request_id))` (`syncobj.py:1037-1041`). The
   leader's next tick appends the entry to its log and answers
   `{'type': 'apply_command_response', 'request_id', 'log_idx', 'log_term'}`
   (`syncobj.py:528-539`).
4. **Follower.** The reply moves the callback to
   `__commandsWaitingCommit[log_idx]` with the term (`syncobj.py:1043-1061`).
5. When the follower applies that index (after the leader's commit index
   arrives in a heartbeat), `__applyLogEntries` calls `callback(result, SUCCESS)`
   if the term matches, else `callback(None, DISCARDED)` (`syncobj.py:708-723`).
   The result comes from the follower's **own** apply.

If no leader is known [code] [tested]:

- `commandsWaitLeader=True` (default): the command waits in the queue. It is
  sent when a leader appears. A sync call times out, but the command still
  runs later.
- `commandsWaitLeader=False`: `MISSING_LEADER` at once (0.1 s in the test).

A node elects itself only if it has at least one open connection
(`__connectedToAnyone`, `syncobj.py:1339`). A node alone with no partners
online stays follower. It never gets a leader. [code] [tested]

## Q7. Read semantics

- A plain (not replicated) method reads the local Python object. **It can be
  stale on any node.** [tested]
  - Isolated old leader: it returned the old value after the majority had
    committed a new one.
  - Healthy follower: right after a sync write on the leader returned, the
    other follower still returned the old value until the next heartbeat.
- **No ReadIndex and no lease read** exist in the code. A search for
  `readindex`, `lease` and `prevote` finds nothing in `pysyncobj/`. [code]
- `_isLeader()` and `getStatus()['leader']` are local views. They can be wrong.
  The source says "there could be multiple leaders at the same time"
  (`syncobj.py:1164-1182`). [code]
- **A `@replicated` method that returns a value is a linearizable read.** The
  read is a log entry. It runs at its position in the log, after every write
  that committed before it. [code] [tested: it returned the latest value
  through a follower]
  - The cost: one log entry, replicated and written to the journal on every node.
  - On an isolated old leader it does **not** return stale data. It raised
    `Timeout`, while the local value there was stale. [tested]
- Read-your-writes on the same node: after a sync write returns on node X, node
  X has applied it (step 5 above). Another node may not have it yet. [tested]

Practical options for the API:

| Read mode | How | Linearizable |
|---|---|---|
| Default (safe) | Call a `@replicated` read method with `sync=True, timeout=...` | Yes |
| Fast (stale) | Read the local dict | No. State it in the API. |

## Q8. Leader step-down (check-quorum) and pre-vote

- **Check-quorum exists. The option is `leaderFallbackTimeout` (default 30 s).**
  Each tick the leader counts partners whose last `next_node_idx` reply is newer
  than `now - leaderFallbackTimeout`. If that is not a majority, it becomes
  follower and forgets the leader (`_onTick`, `syncobj.py:679-686`). [code]
- Test results [tested]:

  | `leaderFallbackTimeout` | New leader on majority side | Old leader stepped down |
  |---|---|---|
  | 1.0 s | 0.33-1.15 s | 0.94-1.20 s |
  | 2.0 s | 0.62-0.74 s | 2.04-2.05 s |
  | 30.0 s (default) | 0.42 s | 30.02 s (Python 3.12 only, 1 run) |

  With the default, **two nodes think they are leader for about 30 s.** The old
  one cannot commit. Its local reads are stale. Writes sent to it come back as
  `DISCARDED` after the partition heals.
- `hasQuorum` is not a check-quorum. It counts open TCP connections
  (`syncobj.py:800-817`). With the message-drop test transport the isolated
  leader showed `has_quorum=True` for the whole partition. With the
  connection-refusing transport it showed `False` at once. [tested]
- **No pre-vote.** A node votes for any higher term, and a leader that sees a
  higher term in `request_vote` steps down (`syncobj.py:918-923`). [code]
- A node starts an election only if it has at least one open connection
  (`__connectedToAnyone`, `syncobj.py:635` and `1339`). [code] So the damage
  depends on the kind of partition [tested, both transports from Q13]:

  | Partition type | Isolated node | After the heal |
  |---|---|---|
  | Messages dropped, TCP stays open (`PartitionableTransport`) | Campaigned again and again, term 3-4 | Its higher term forced the healthy leader (term 2) to step down. New election at term 5-6. |
  | Connections refused too (`DisconnectingPartitionTransport`) | Isolated old leader: did not campaign. Isolated follower: campaigned once (term +1). | Old leader case: no new election, terms stayed at 2. Follower case: its +1 term can force one extra election [code]. |

  A real partition is between the two rows. A connection closes only when the
  node sends on it and has read nothing for `connectionTimeout`
  (`__trySendBuffer` and `__processConnectionTimeout`, `tcp_connection.py:227-234`).
  So an isolated follower can campaign a few times in the first
  `connectionTimeout` seconds, then stop. After the heal, expect one extra
  election. [code] [not verified in Docker]
- Leadership transfer exists since 0.3.16: `transferLeadership(node=None,
  callback=None)` on the leader (`syncobj.py:415`). The target must be fully
  caught up. Use it before you stop the leader in a rolling restart.
  [code] [not verified by a test here]

## Q9. Persistence

### What each file stores [code]

| File | Content | Written when |
|---|---|---|
| `journalFile` | Every Raft log entry: `(command bytes, index, term)`. Binary format: header (name, version, format, last record offset) + records `[size][index, term, command][size]` (`journal.py:181-225`). | On every `add`, into an mmap (`FileJournal.add`, `journal.py:218`). |
| `journalFile + '.meta'` | Pickled dict: `raftCommitIndex`, `currentTerm`, `votedForNodeId`. | Term and vote: at once, in `setCurrentTerm` (`journal.py:271`), since 0.3.16. Commit index: once per second (`onOneSecondTimer`, `journal.py:285`). |
| `fullDumpFile` | gzip + pickle of `(state, last applied entry, entry before it, set of cluster nodes)` (`__tryLogCompaction`, `syncobj.py:1460-1470`). | At each log compaction. Written to `<file>.tmp`, then `os.rename` (`serializer.py:84-93`). |
| `fullDumpFile + '.1.tmp'` | Snapshot that a follower receives from the leader, before rename. | During snapshot install (`serializer.py:176`). |

### fsync

**pysyncobj never calls `os.fsync`.** [code: no `fsync` in `pysyncobj/`]

- Journal records go into an mmap. `mmap.flush()` (msync) runs only in
  `_destroy` (`journal.py:139`). `FileJournal.flush()` exists but nothing
  calls it.
- The meta file uses `f.flush()` (Python buffer only) and `shutil.move`
  (`journal.py:160-164`).
- The snapshot uses `os.rename` without fsync of the file or the folder
  (`atomic_replace.py:33`). Open PR #206 fixes this.
- A follower answers "success" to the leader after the mmap write, without
  fsync. So "committed" means "in the page cache of a majority".

What survives [tested for kill -9] [not verified for power loss]:

- **Process crash (`kill -9`, `docker kill`)**: yes. The page cache belongs to
  the kernel. Test: 200 acknowledged writes, `SIGKILL` right after the last ack,
  restart: all 200 keys, `last_applied=203`.
- **Power loss, kernel crash, VM reset**: acknowledged writes can be lost. The
  journal, meta and snapshot can disagree. Issue #84 (open since 2018) and PR
  #197 describe real cases.

### What is pickled from your object [tested]

- `SyncObj` subclass: attributes that exist at the end of `SyncObj.__init__`
  are skipped (`self.__properies`, `syncobj.py:274-276`). Attributes you set
  **after** `super().__init__()` are dumped. Attributes you set **before** it
  are not dumped. The dump also contains `_SyncObj__enabledCodeVersion`.
  Test: dumped names were `['_SyncObj__enabledCodeVersion', '_data',
  'set_after_super']`.
- `SyncObjConsumer`: the same rule with `SyncObjConsumer.__init__`
  (`syncobj.py:75-90`).
- A non-picklable attribute (for example `threading.Lock`) set after
  `super().__init__()` breaks every snapshot. The log then grows forever. Only a
  log warning `Failed to store full dump` shows it. [tested: no dump file, log
  not compacted] Set locks **before** `super().__init__()`. [tested: works]
- **Race in a `SyncObj` subclass:** the tick thread starts inside
  `SyncObj.__init__`. It sleeps 0.1 s, then loads the dump into `self.__dict__`
  (`_autoTickThread`, `syncobj.py:584`). If your `__init__` sets
  `self._data = {}` later than that, it erases the loaded state. [tested: with
  0.3 s of work after `super().__init__()`, the restarted node had `_data = {}`]
  **Use the consumer pattern**: build the `SyncObjConsumer` first, then pass it
  in `consumers=[...]`.

### Restart with an existing journal [code] [tested]

1. `FileJournal.__init__` reads all records into a Python list
   (`journal.py:189-204`). Term, vote and commit index come from `.meta`.
2. Before the port is bound, ticks apply entries up to the saved commit index
   (`_onTick`, `syncobj.py:619-622`).
3. On the first tick with a bound port, the dump file is loaded if it exists
   (`syncobj.py:624-627`). `raftLastApplied` becomes the dump's last index. If
   the journal does not match the dump, the journal is replaced by the 2 dump
   entries (`__loadDumpFile`, `syncobj.py:1489-1495`).
4. Entries after the commit index are applied after a leader confirms them.

Full cluster restart after log compaction, 3 nodes, 5 keys before compaction
and 1 key after [tested]:

| Mode | Result after restart of all 3 nodes |
|---|---|
| `journalFile` + `fullDumpFile` | All 6 keys. All nodes ready. **Use this.** |
| `journalFile` only | **No keys. Nodes never ready** (`last_applied=1`, `commit_idx=9`). The compacted entries exist nowhere. |
| `fullDumpFile` only | The 5 old keys. **The key written after the snapshot is lost.** In an earlier variant (one node restarted alone first, Python 3.13), even the key written before the snapshot was lost, and a node logged `CRITICAL truncating already-applied log entries ... state machine may have diverged`. A follower can take its snapshot before it has applied the newest entries. |
| Neither | No keys. |

One node restart alone with journal + dump: it had all keys before any partner
connected. [tested]

### Known corruption risks

- No fsync (above). Issues #84, #197, PR #206.
- `journalFile` without `fullDumpFile` (above). [tested]
- `FileJournal.deleteEntriesTo` rewrites the journal in place: `clear()`, then
  `add()` for each kept entry (`journal.py:252-256`). A crash during this
  rewrite can lose the journal tail. [code] [not verified]
- `MetaStorer.getMeta` swallows every read error and returns `{}`
  (`journal.py:152-158`). With a broken `.meta` file the node forgets its vote,
  takes the term of its last log entry (`syncobj.py:174`) and starts with commit
  index 1. A forgotten vote can allow a second vote in the same term.
  [code] [not verified]
- The journal file grows by doubling. It never shrinks (`ResizableFile.write`,
  `journal.py:117`). A full disk stops snapshots and the journal keeps growing
  (issue #164). [code]
- Old bugs fixed in 0.3.16: crash in `ResizableFile.write` (#196/#199), startup
  deadlock when the meta commit index is past the journal end (#197).

## Q10. Log compaction and snapshots

Trigger (`__tryLogCompaction`, `syncobj.py:1421`) [code]: each tick, if no
snapshot is in progress and one of these is true:

- the log has more than `logCompactionMinEntries` entries (5000), or
- more than `logCompactionMinTime` seconds (300) passed since the last one, or
- you called `forceLogCompaction()`.

With `logCompactionSplit=True`, a node compacts only in its own slot of the
`logCompactionMinTime` window.

Steps [code]:

1. Take the state dict (and each consumer's dict), the last applied entry, the
   entry before it and the cluster member set.
2. Serialize: in memory (no `fullDumpFile`), or to a file in a forked child
   (`useFork=True`), or to a file in the tick thread (`useFork=False`).
3. On success, delete log entries before `lastApplied - 1`. The log keeps 2
   entries (`log_len = 2`). [tested]

How a lagging node gets a snapshot [code] [tested]:

- In `__sendAppendEntries` (`syncobj.py:1309-1329`), if the follower's next
  index is not in the leader's log any more, the leader sends `append_entries`
  with a `serialized` chunk instead of entries. Chunks are
  `logCompactionBatchSize` bytes, read from the leader's dump file (or its
  in-memory snapshot). While the leader writes a new snapshot, it waits.
- The follower writes the chunks to `fullDumpFile + '.1.tmp'`, renames it, and
  calls `__loadDumpFile(clearJournal=True)` (`syncobj.py:1021-1025`,
  `serializer.py:157`).
- Then the leader sends the normal entries after the snapshot.
- Test: one node was stopped. 50 keys were written. The other two compacted to
  `log_len=2`. The node restarted with empty files. It got all 50 keys, its
  `log_len` was 2, and it had its own dump file.

## Q11. `getStatus()`

Source: `getStatus`, `syncobj.py:819`. It returns a new dict. [code] [tested]

| Key | Meaning |
|---|---|
| `version` | Library version string (`'0.3.17'`). |
| `revision` | Always `'deprecated'`. |
| `self` | This node, a `TCPNode` object (not a string). Use `.address`. |
| `state` | `0` follower, `1` candidate, `2` leader. |
| `leader` | Last known leader as a `TCPNode`, or `None`. A local view. Can be stale. |
| `has_quorum` | `True` if this node plus its **connected** partners are a majority. It counts TCP connections, not recent answers. |
| `partner_nodes_count` | Number of other voting nodes in this node's config. |
| `partner_node_status_server_<host:port>` | `2` = connected, `0` = not connected. One key per partner. |
| `readonly_nodes_count` | Number of connected read-only nodes. |
| `readonly_node_status_server_<id>` | `2` / `0` for each read-only node. |
| `log_len` | Entries in the in-memory log (after compaction it starts again at 2). |
| `last_applied` | Last log index applied to the state machine. |
| `commit_idx` | Last log index this node knows is committed. |
| `raft_term` | Current term. |
| `next_node_idx_count`, `next_node_idx_server_<host:port>` | Leader's next index per partner. Only correct on the current leader. Not cleared on step-down. [code] |
| `match_idx_count`, `match_idx_server_<host:port>` | Leader's match index per partner. Only correct on the current leader. |
| `leader_commit_idx` | Commit index that the leader last sent (on the leader: its own). `None` before contact. |
| `uptime` | Whole seconds since this `SyncObj` was created. |
| `self_code_version` | Highest `ver` in this node's code. |
| `enabled_code_version` | Version the cluster switched to. |

Direct properties exist too: `raftCurrentTerm`, `raftCommitIndex`,
`raftLastApplied`, `hasQuorum`, `selfNode`, `otherNodes`, `isNodeConnected(node)`.
[code: `syncobj.py:757-817`]

Lag of a follower = `commit_idx - last_applied` on that node, or
`commit_idx(leader) - match_idx_server_<follower>` on the leader.

`getStatus()` iterates internal dicts that the tick thread changes. A call
from another thread during a membership change could raise
`RuntimeError: dictionary changed size`. [code] [not verified]

## Q12. `isReady()` and `onReady`

Source: `_onTick`, `syncobj.py:694-697`, and `isReady`, `syncobj.py:1184`. [code]

- "Ready" means: **at least once**, `raftLastApplied == leaderCommitIndex`.
  `leaderCommitIndex` is the commit index from the last leader heartbeat (on
  the leader, its own commit index).
- It is a **one-time latch**. It never goes back to `False`.
  [tested: an isolated node still returned `isReady() == True`]
- `onReady()` is called once, from the tick thread, at that moment.
- It does **not** mean: connected now, has quorum now, or up to date now.
- After a restart, a node with a journal applies its saved entries, but it is
  ready only after it hears from a leader (or becomes leader).

For a health check: use `isReady()` for "started". For "can serve safe reads
and writes", run a `@replicated` no-op or read with a short timeout.

## Q13. Fault injection for tests

### What pysyncobj's own tests do [code: `test_syncobj.py`]

- They use `autoTick=False` and call `o._onTick(interval)` for each node in its
  own thread (`doTicks`, `test_syncobj.py:188`).
- **Node pause** = stop ticking a node (`test_syncThreeObjectsLeaderFail`,
  lines 332-397: only `newObjs` are ticked).
- **Node kill and restart** = `o._destroy()`, then a new object with the same
  journal file (`test_randomTest1`, `test_simultaneousRestartDataDivergence`).
- **Network delay** = `obj._SyncObj__transport._send_random_sleep_duration = 0.03`
  (a private hook in `TCPTransport.send`, `transport.py:570`).
- **Force an election** = `follower._SyncObj__raftElectionDeadline = 0`.
- They do not simulate a network partition.

### A partition with a custom `transportClass` (works) [tested]

`TCPTransport` has two clean points [code]:

- `send(self, node, message)` (`transport.py:556`): every Raft message out goes
  through it.
- `_onMessageReceived(self, node, message)` (`transport.py:106`): every Raft
  message in goes through it, after the handshake. The connection callbacks are
  built as `functools.partial(self._onMessageReceived, node)`
  (`transport.py:369`, `531`), so an override in a subclass is used.

You do not need to override `connect` for this variant. The TCP connections
stay open, because the idle check in `TcpConnection` runs only on socket events
and real sends (`__processConnection`, `__trySendBuffer`, `tcp_connection.py:176`
and `232`). [code] [tested: the isolated node showed 2 of 2 partners connected
for 6 s] So this variant models "the link looks up, but every message is lost".
`has_quorum` and `partner_node_status_*` stay "connected".

```python
import threading

from pysyncobj import SyncObj, SyncObjConf, SyncObjConsumer, replicated
from pysyncobj.transport import TCPTransport


class PartitionableTransport(TCPTransport):
    """TCPTransport that drops Raft messages on blocked links.

    The blocked-link table is shared by all nodes in this process.
    A link is a (sender_address, receiver_address) pair.
    """

    _blocked_links = set()
    _blocked_links_lock = threading.Lock()

    @classmethod
    def block_link(cls, sender_address, receiver_address):
        with cls._blocked_links_lock:
            cls._blocked_links.add((sender_address, receiver_address))

    @classmethod
    def isolate(cls, isolated_address, all_addresses):
        """Drop all traffic in both directions between one node and all others."""
        for other_address in all_addresses:
            if other_address != isolated_address:
                cls.block_link(isolated_address, other_address)
                cls.block_link(other_address, isolated_address)

    @classmethod
    def heal_all(cls):
        with cls._blocked_links_lock:
            cls._blocked_links.clear()

    @classmethod
    def _is_blocked(cls, sender_address, receiver_address):
        with cls._blocked_links_lock:
            return (sender_address, receiver_address) in cls._blocked_links

    def send(self, node, message):
        receiver_address = getattr(node, "address", None)
        if self._selfNode is not None and self._is_blocked(
            self._selfNode.address, receiver_address
        ):
            return False  # drop outgoing message
        return super().send(node, message)

    def _onMessageReceived(self, node, message):
        sender_address = getattr(node, "address", None)
        if self._selfNode is not None and self._is_blocked(sender_address, self._selfNode.address):
            return  # drop incoming message
        super()._onMessageReceived(node, message)


class KeyValueState(SyncObjConsumer):
    """Replicated key-value state. Built before SyncObj, passed as a consumer."""

    def __init__(self):
        super().__init__()
        self._data = {}

    @replicated
    def set_value(self, key, value):
        self._data[key] = value
        return value

    @replicated
    def delete_value(self, key):
        return self._data.pop(key, None)  # never raise inside a replicated method

    @replicated
    def read_value_through_log(self, key):
        return self._data.get(key)

    def get_local(self, key):
        return self._data.get(key)
```

Start the nodes with `transportClass=PartitionableTransport`:

```python
node = SyncObj(
    self_address,
    other_addresses,
    conf=conf,
    consumers=[state],
    transportClass=PartitionableTransport,
)
```

The partition proof (shortened from `test_partition_proof.py`; `wait_until`
polls a condition with a deadline):

```python
PartitionableTransport.isolate(old_leader, addresses)
new_leader = wait_until(new_leader_elected_on_majority_side, 15)
states[new_leader_index].set_value("k", "during", sync=True, timeout=5)
assert states[old_leader_index].get_local("k") == "before"  # stale read
wait_until(lambda: nodes[old_leader_index].getStatus()["state"] != 2, 12)
PartitionableTransport.heal_all()
wait_until(lambda: len({s.get_local("k") for s in states}) == 1, 10)  # all nodes equal
```

(`new_leader_elected_on_majority_side` returns the leader address that both
majority nodes agree on, if it is not the old leader.)

Result, 10 runs per Python version, all passed [tested]:

- New leader on the majority side after 0.33-1.15 s (one run had a split vote).
- Old leader stepped down after about `leaderFallbackTimeout` (1.0 s).
- A write sent to the old leader during the partition: `DISCARDED` after the heal.
- A sync write on the isolated node with no leader: `Timeout`, then applied
  after the heal.
- After the heal: one leader, same data on all nodes, term bumped (no pre-vote).
- The isolated old leader showed `has_quorum=True` during the whole cut,
  because its TCP connections stayed open.

A one-way partition: call `block_link(a, b)` only. Messages from `a` to `b`
are dropped, and `b` to `a` still flows.

One-way test with `PartitionableTransport`: `block_link(leader, follower_a)`
for 6 s, writes in a loop with `timeout=0.5` [tested, 3 runs per version]:

- Follower A never got heartbeats. It started election after election. Its
  `request_vote` reached the leader, so the leader stepped down each time.
  The term went from 1 to 5-15 in 6 s (no pre-vote).
- Writes through the leader or the other follower all succeeded (89-92
  writes, 0 failures) when the writer was not A.
- Writes through A: 0 of 13 succeeded in one run. In three other runs the
  leadership moved to a node that can reach A, and 78-92 % succeeded.
- After the heal, all nodes had the same value.

### Variant: also refuse the TCP connection (works) [tested]

With `PartitionableTransport`, TCP connections stay open during the
partition. So `has_quorum` and `partner_node_status_*` still show "connected".
Use this subclass when the test needs a clean cut (like `docker network
disconnect`). It overrides two more methods:
`_connectIfNecessarySingle(node)` (outgoing connect, `transport.py:411`) and
`_onIncomingMessageReceived(conn, message)` (incoming handshake,
`transport.py:331`). All socket work stays in the tick thread.

```python
from pysyncobj.tcp_connection import CONNECTION_STATE


class DisconnectingPartitionTransport(PartitionableTransport):
    """Like PartitionableTransport, but a blocked link also has no TCP connection."""

    def _link_blocked_either_way(self, other_address):
        self_address = self._selfNode.address
        return self._is_blocked(self_address, other_address) or self._is_blocked(
            other_address, self_address
        )

    def _drop_connection(self, node):
        connection = self._connections.get(node)
        if connection is not None and connection.state != CONNECTION_STATE.DISCONNECTED:
            connection.disconnect()

    def send(self, node, message):
        receiver_address = getattr(node, "address", None)
        if self._selfNode is not None and self._link_blocked_either_way(receiver_address):
            self._drop_connection(node)
            return False
        return super().send(node, message)

    def _onMessageReceived(self, node, message):
        sender_address = getattr(node, "address", None)
        if self._selfNode is not None and self._link_blocked_either_way(sender_address):
            self._drop_connection(node)
            return
        super()._onMessageReceived(node, message)

    def _connectIfNecessarySingle(self, node):
        if self._selfNode is not None and self._link_blocked_either_way(
            getattr(node, "address", None)
        ):
            return False  # no outgoing connect while blocked
        return super()._connectIfNecessarySingle(node)

    def _onIncomingMessageReceived(self, conn, message):
        if (
            self._selfNode is not None
            and isinstance(message, str)
            and message in self._nodeAddrToNode
            and self._link_blocked_either_way(message)
        ):
            conn.disconnect()  # refuse the handshake of a blocked node
            self._unknownConnections.discard(conn)
            return
        super()._onIncomingMessageReceived(conn, message)
```

Note: these hooks use private names (`_connections`, `_nodeAddrToNode`,
`_unknownConnections`). A new pysyncobj version can break them. Pin the version.

Result, 3 runs per Python version [tested]: new leader after 0.36-0.41 s. The
isolated old leader showed `has_quorum=False` at once, stepped down after
0.96-1.03 s, and did not campaign. After the heal no new election was needed.


### Timing values that worked

Fast set, used for all in-process tests above (10/10 partition runs) [tested]:

```python
FAST_TEST_TIMING = dict(
    appendEntriesPeriod=0.05,
    raftMinTimeout=0.3,
    raftMaxTimeout=0.6,
    connectionTimeout=1.0,
    connectionRetryTime=0.5,
    leaderFallbackTimeout=1.0,
    autoTickPeriod=0.02,
)
```

Slower set for CI or a busy machine (the repo's own test values plus the rest)
[tested, 1 run per version]:

```python
CI_TEST_TIMING = dict(
    appendEntriesPeriod=0.1,
    raftMinTimeout=0.5,
    raftMaxTimeout=1.0,
    connectionTimeout=2.0,
    connectionRetryTime=0.5,
    leaderFallbackTimeout=2.0,
    autoTickPeriod=0.05,
)
```

Wait deadlines: give elections at least 5 s and heals at least 10 s. Add
`appendEntriesUseBatch=False` if a test does many sync writes.

### Freeze a node with `autoTick=False` (works) [tested]

```python
nodes = [
    SyncObj(
        address,
        [a for a in addresses if a != address],
        conf=SyncObjConf(autoTick=False, **FAST_TEST_TIMING),
        consumers=[state],
    )
    for address, state in zip(addresses, states)
]


def tick_for(seconds, running_nodes, stop_condition):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for node in running_nodes:
            node.doTick(0.0)  # one non-blocking tick
        if stop_condition():
            return True
        time.sleep(0.005)
    return stop_condition()


running_nodes = [n for n in nodes if n.selfNode.address != leader]  # leader is frozen
tick_for(10, running_nodes, lambda: agreed_leader(running_nodes) not in (None, leader))
```

Result: new leader 0.34-0.36 s after the freeze. After unfreeze, all nodes
agreed on the new leader. [tested]

With `autoTick=False`, use only async calls with `callback=` in the ticking
thread. A `sync=True` call there blocks the only thread that ticks.

## Q14. Stopping a node and port reuse

Source: `destroy` (`syncobj.py:296`), `destroy_synchronous` (`syncobj.py:313`),
`_doDestroy` (`syncobj.py:344`), `TCPTransport.destroy` (`transport.py:577`).
[code]

- `autoTick=True`: `destroy()` only sets a flag. The tick thread does the work
  on its next loop: it closes all connections, unbinds the server socket, and
  flushes and closes the journal mmap.
- `destroy_synchronous()` = `destroy()` + `join()` of the tick thread. When it
  returns, the port is free.
- `autoTick=False`: `destroy()` does the work at once.
  **`destroy_synchronous()` raises `AttributeError: 'NoneType' object has no
  attribute 'join'`** with `autoTick=False`. [tested]
- The server socket uses `SO_REUSEADDR` (`tcp_server.py:39`). So `TIME_WAIT`
  does not block a new bind.

Test results [tested]:

| Action | Can a new socket bind the same port? |
|---|---|
| Right after `destroy()` (no join) | No: `Address already in use` |
| 0.3 s after `destroy()` | Yes |
| Right after `destroy_synchronous()` (took 0.021 s) | Yes. A new `SyncObj` bound in 0.001 s and caught up from the leader. |

Other facts about the tick thread [code] [tested]:

- The tick thread is not a daemon thread.
- **It stops when the thread that created the `SyncObj` ends**
  (`_autoTickThread`, `syncobj.py:587`). It then exits without cleanup.
  [tested: `tick_thread_alive()` was `False` after the creator thread ended]
  In FastAPI, create the node in the lifespan (main thread), not in a request
  or a thread-pool task.
- `tick_thread_alive()` tells you if the tick thread still runs.
- `waitBinded()` raises `SyncObjException('BindError')` after `maxBindRetries`
  failed binds. With `maxBindRetries=0` it can wait forever.

## Q15. Membership changes

Source: `addNodeToCluster` (`syncobj.py:370`), `removeNodeFromCluster`
(`syncobj.py:386`), `__changeCluster` (`syncobj.py:1356`), `__doChangeCluster`
(`syncobj.py:1374`). Needs `dynamicMembershipChange=True`, else
`Exception('dynamicMembershipChange is disabled')`. [tested]

How it works [code]:

- The change is a log entry (single-server change). Each node uses the new
  member list as soon as the entry is in its log, not when it commits. A
  follower undoes it if the entry is truncated (`syncobj.py:991-997`).
- The leader refuses a change (`REQUEST_DENIED`) until the no-op of its own
  term is applied (`syncobj.py:1357`).
- The leader cannot remove itself: `REQUEST_DENIED`. [tested] Transfer the
  leadership first.
- The call can be made on any node. A follower forwards it to the leader.
  [tested]

Procedure (wiki page "Dynamic membership change"):

- Add: call `addNodeToCluster('kv4:4321', callback=...)`. Wait for `SUCCESS`.
  Then start the new node with the full list of the other nodes.
- Remove: stop the node first. Then call `removeNodeFromCluster(...)`. Wait for
  `SUCCESS`.
- Change one node at a time.

Test results [tested]:

| Step | Result |
|---|---|
| Add node 4 through a follower | `SUCCESS`, partner counts 3, node 4 caught up |
| Remove node 4 | `SUCCESS`, partner counts back to 2 |
| Two adds back-to-back, without waiting | **Both `SUCCESS`**, partner counts 4 |
| Leader removes itself | `REQUEST_DENIED` |

Known bugs and limits:

- **The "one change at a time" guard does not work.** `__changeClusterIDx` is
  checked but never set to an index, since the first commit in 2016
  (`syncobj.py:1361-1367`). [code] [tested: two changes in flight were both
  accepted] Raft single-server changes are safe only one at a time. You must
  wait for the callback before the next change.
- The member list is stored in the dump file and in the log. On restart it can
  differ from the `otherNodes` you pass (issue #133, closed). [code]
- Race on port binding for new nodes (issue #89, open).
- Without `password`, anyone who can reach the port can send `add` and `remove`
  utility commands (`syncobj_admin`). [code: `_onUtilityMessage`,
  `transport.py:375`]

For a fixed 3-node Docker cluster, keep `dynamicMembershipChange=False`.

## Q16. Known issues from GitHub

Open issues and PRs that matter in production:

| Item | Topic | Status |
|---|---|---|
| [#84](https://github.com/bakwc/PySyncObj/issues/84) | Journal is not synced to disk (no msync/fsync). The vote part was fixed by #202. | Open since 2018 |
| [PR #98](https://github.com/bakwc/PySyncObj/pull/98) | `flushJournal` option. With flush on, the author measured 40 writes/s instead of 14,000. | Open, stale since 2018 |
| [PR #206](https://github.com/bakwc/PySyncObj/pull/206) | Snapshot is not fsynced before journal compaction. A crash can leave an empty snapshot next to a compacted journal. | Open (2026-10-22) |
| [#205](https://github.com/bakwc/PySyncObj/issues/205) | Security: network frames are unpickled before authentication. Password is off by default. | Open, no answer |
| [#174](https://github.com/bakwc/PySyncObj/issues/174) | Unauthenticated memory DoS: the read buffer has no size limit. | Open |
| [#164](https://github.com/bakwc/PySyncObj/issues/164) | Disk full on the leader: `Failed to store full dump`, 1 GB journal, no leader change. | Open |
| [#89](https://github.com/bakwc/PySyncObj/issues/89) | Dynamic membership race on port binding. | Open |
| [#75](https://github.com/bakwc/PySyncObj/issues/75) | No timeout for async calls. | Open |
| [#147](https://github.com/bakwc/PySyncObj/issues/147) | Snapshot fails with non-picklable attributes (`RLock`). | Open |
| [#110](https://github.com/bakwc/PySyncObj/issues/110) | Commit of entries from older terms. The code now has the term check from #170. | Open, but fixed in code |
| [#85](https://github.com/bakwc/PySyncObj/issues/85), [#86](https://github.com/bakwc/PySyncObj/issues/86), [#117](https://github.com/bakwc/PySyncObj/issues/117) | Flaky library tests (EOFError on dump load, big storage, port bind). | Open |

Fixed bugs (they show why you need 0.3.17 or later):

| Item | Topic | Fixed in |
|---|---|---|
| [#201](https://github.com/bakwc/PySyncObj/issues/201) / [PR #202](https://github.com/bakwc/PySyncObj/pull/202) | `currentTerm` and `votedFor` were not saved. A full cluster restart could overwrite committed entries and make state machines differ (Patroni). | 0.3.16 |
| [PR #197](https://github.com/bakwc/PySyncObj/pull/197) | Startup deadlock when the meta commit index was past the journal end after a crash. Now clamped with a warning (the lost entries stay lost). | 0.3.16 |
| [#196](https://github.com/bakwc/PySyncObj/issues/196) / [PR #199](https://github.com/bakwc/PySyncObj/pull/199) | `IndexError` in `ResizableFile.write` on a big write. | 0.3.16 |
| [#189](https://github.com/bakwc/PySyncObj/issues/189) / [PR #190](https://github.com/bakwc/PySyncObj/pull/190), [PR #194](https://github.com/bakwc/PySyncObj/pull/194) | An `AssertionError` killed the tick thread. Now exceptions are logged and the loop goes on. The stale reply case returns `UNKNOWN_OUTCOME`. | 0.3.14, 0.3.15 |
| [#203](https://github.com/bakwc/PySyncObj/issues/203), [#204](https://github.com/bakwc/PySyncObj/issues/204) | Wrong queue capacity checks (`commandsQueueSize`, `ReplQueue`). | 0.3.17 |
| [#169](https://github.com/bakwc/PySyncObj/issues/169), [#166](https://github.com/bakwc/PySyncObj/issues/166), [#167](https://github.com/bakwc/PySyncObj/issues/167) | Raft safety: commit of older-term entries, commit index and match index not monotonic. | 0.3.12 |
| [#139](https://github.com/bakwc/PySyncObj/issues/139) | Journal was not applied after restart until other nodes connected. | 2020 |
| [#119](https://github.com/bakwc/PySyncObj/issues/119) | DNS resolve failed after reboot. | 2020 |
| [#116](https://github.com/bakwc/PySyncObj/issues/116), [#102](https://github.com/bakwc/PySyncObj/issues/102) | "Set/dict changed size during iteration" in `destroy` and `ReplLockManager`. | 2020, 2019 |
| [#101](https://github.com/bakwc/PySyncObj/issues/101) | "Multiple leaders": the answer is that Raft allows two leaders in different terms at the same time. | Closed (by design) |

New problems found in this study (not reported on GitHub as far as the issue
list shows):

1. An exception in a replicated method stops the whole cluster (Q5). [tested]
2. `@replicated_sync(timeout=...)` ignores the timeout (Q5). [tested]
3. The membership "one change at a time" guard is never active (Q15). [tested]
4. `destroy_synchronous()` fails with `autoTick=False` (Q14). [tested]
5. `journalFile` without `fullDumpFile` loses all data after compaction (Q9).
   [tested]

## Q17. Thread safety

**Yes, many threads can call replicated methods with `sync=True` at the same
time while `autoTick=True`. No lock is needed for the calls.** [tested]

- Test: 32 threads x 25 sync writes, spread over all 3 nodes. 0 failures. All
  800 keys on all nodes. 3.2 s with batching (period 0.05 s). 0.18 s with
  `appendEntriesUseBatch=False`.
- Why it is safe [code]: the caller thread only does `FastQueue.put_nowait`
  (with a lock, `fast_queue.py:16`) and waits on its own `threading.Event`.
  Only the tick thread reads the queue, changes Raft state and runs your
  replicated methods. The maintainer says the same in issue #74.

What still needs care:

- **Your replicated methods run in the tick thread. Your reads run in the HTTP
  threads.** Under the GIL a single `dict.get` is safe. Iteration over the dict
  in Python code (`for key in data`) can raise `RuntimeError: dictionary changed
  size during iteration`. Use a `threading.Lock` in both the replicated method
  and the reader, or copy with `dict(data)`. Set the lock attribute **before**
  `super().__init__()` so it is not pickled. [tested: lock in consumer, dump OK]
- **Never call `sync=True` from the tick thread** (inside a replicated method,
  `onReady`, `onStateChanged` or a `callback`). The tick thread waits for
  itself. [tested: `sync=True` inside `onReady` raised `Timeout` after 1 s,
  and the write was applied after that]
- Callbacks run in the tick thread. Keep them short. A slow callback delays
  heartbeats and can cause elections.
- FastAPI: a `sync=True` call blocks the calling thread. Do not call it in an
  `async def` endpoint (it blocks the event loop). Use a plain `def` endpoint
  (FastAPI runs it in a thread pool) or `run_in_threadpool`.
  [not verified with FastAPI here]
- **One process per node.** `uvicorn --workers 3` would start 3 `SyncObj`
  objects for one port in 3 processes. Create the node once, in the lifespan of
  a single worker. [not verified with uvicorn here] Do not create it before a
  fork: threads do not survive `fork()` (a general POSIX fact).

---

## Recommended setup for this project

```python
import os

from pysyncobj import SyncObj, SyncObjConf

state = KeyValueState()  # SyncObjConsumer, built BEFORE SyncObj

conf = SyncObjConf(
    journalFile="/data/raft.journal",  # always together with fullDumpFile
    fullDumpFile="/data/raft.dump",
    appendEntriesPeriod=0.1,  # default
    raftMinTimeout=0.4,  # default
    raftMaxTimeout=1.4,  # default
    connectionTimeout=3.5,  # default, must be >= raftMaxTimeout
    connectionRetryTime=1.0,  # default 5.0 reconnects slowly after a peer restart
    leaderFallbackTimeout=3.0,  # default 30.0: shorter two-leader window
    appendEntriesUseBatch=False,  # about 1 ms instead of 100-200 ms per write
    commandsWaitLeader=False,  # fail fast (HTTP 503) with no leader
    dnsCacheTime=5.0,  # Docker: peers can change IP
    dnsFailCacheTime=1.0,  # Docker: peers may start later
    maxBindRetries=10,  # fail fast if the port is taken
    password=os.environ.get("RAFT_PASSWORD"),  # needs `cryptography`; None = no auth
    dynamicMembershipChange=False,  # fixed 3-node cluster
)
node = SyncObj("kv1:4321", ["kv2:4321", "kv3:4321"], conf=conf, consumers=[state])
```

Notes on the choices:

- `leaderFallbackTimeout=3.0`, `connectionRetryTime=1.0`, `dnsCacheTime=5.0`
  and `dnsFailCacheTime=1.0` are recommendations from the source code and the
  in-process tests. They were **not verified in Docker**.
- `commandsWaitLeader=False`: a write on a node that knows no leader fails at
  once with `MISSING_LEADER`. Test: an isolated follower answered in 0.00-0.02 s
  with both test transports. [tested] This does not help on an isolated **old
  leader**: it still accepts writes until `leaderFallbackTimeout`, and those
  writes time out. [tested] If you keep the default `True`, every timed-out
  write stays queued and may run later. Report it as "outcome unknown".
- Always pass `timeout=` on every `sync=True` call.
- Call `node.waitBinded()` right after the constructor. A busy port does not
  raise in the constructor. `waitBinded()` raised `SyncObjException('BindError')`
  after `maxBindRetries` attempts (0.4 s with 3 tries of 0.2 s). [tested]
- Call `node.destroy_synchronous()` at shutdown.
- 3 nodes tolerate 1 failure. 4 nodes still tolerate only 1 (the majority is 3).
  An odd size gives the most fault tolerance per node.
