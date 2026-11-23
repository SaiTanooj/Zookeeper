# ZooKeeper-Style Coordination Service in Python

## Day 1: Architecture and Implementation Guide

Distributed Systems, Monsoon 2026 · Project 34

**Day 1 scope:** the znode data tree, Fast Leader Election, failure detection, the client library and tools

---

[TOC]

---

# Chapter 1: Introduction

## 1.1 What we are building

ZooKeeper is a small, replicated service that a group of machines uses to agree on basic facts: who the leader is, which workers are alive, and what the current configuration is. It stores its data in a tree of small nodes called **znodes** and gives clients a few simple operations (create, delete, read, write, list children). Applications build locks, leader election and group membership out of those operations.

We are building a simplified ZooKeeper in **Python**. ZooKeeper's two protocols are **Fast Leader Election (FLE)**, which picks a leader, and **ZAB (ZooKeeper Atomic Broadcast)**, which replicates every write. We use both instead of Raft, so the system follows the real ZooKeeper design.

## 1.2 The 4-day plan and where Day 1 fits

| Day | Theme | Main result |
|---|---|---|
| **1** | **Foundations** | **Data tree, leader election, failure detection, client skeleton** |
| 2 | Replication | ZAB broadcast (PROPOSAL / ACK / COMMIT), transaction log, snapshots |
| 3 | ZooKeeper semantics | ZAB recovery (DIFF/TRUNC/SNAP), sessions, ephemerals, watches, sync, recipes |
| 4 | Proof | Fault injection, benchmarks, Docker, demo, report |

## 1.3 Day 1 goals

By the end of Day 1 we have:

1. A **complete in-memory znode tree** with every ZooKeeper operation, version checks, sequential nodes and ephemeral bookkeeping. It is tested on its own, without any networking.
2. A **3- or 5-server cluster** that always agrees on exactly one leader, using Fast Leader Election.
3. **Failure detection**: when the leader dies, the others notice and elect a new leader in under one second. When a server loses the majority, it stops leading.
4. A **client library** that connects to any live server and offers the full API, with Python exceptions for errors.
5. **Tools** to start a cluster, watch every server's role live, and run commands.
6. **32 automated tests**, all passing.

Day 1 deliberately has **no replication**. Writes are applied only on the leader's own tree. Day 2 adds that path; nothing built on Day 1 has to be thrown away.

## 1.4 Team split for Day 1

| Member | Owns | Files |
|---|---|---|
| A | Election and liveness | `vote.py`, `election.py`, `liveness.py`, `peer_client.py`, `peer_service.py` |
| B | Data tree | `znode.py`, `datatree.py`, `common/paths.py`, `common/stat.py`, `common/errors.py` |
| C | Client, tools, tests | `client/client.py`, `tools/*`, `tests/*`, `server.py` wiring |
| All | Shared contract | `zk/proto/zk.proto`, `common/config.py`, `common/zxid.py` |

Writing the `.proto` file together first is the single most important step. Once the messages are fixed, the three people can work in parallel without blocking each other.

---

# Chapter 2: Core Concepts

This chapter explains the vocabulary used in the rest of the document.

## 2.1 Znodes and the data tree

A znode is a node in a tree, addressed by a path like a file system: `/`, `/app`, `/app/config`. Each znode holds a small amount of **data** (bytes) and a **Stat** record of metadata. Unlike a file system, every znode can hold data *and* have children.

There are three kinds of znodes:

| Kind | Behaviour | Used for |
|---|---|---|
| Regular | Stays until someone deletes it | Configuration, structure |
| Ephemeral | Deleted automatically when the session that created it ends; cannot have children | "I am alive" markers, group membership, locks |
| Sequential | ZooKeeper appends a 10-digit, ever-increasing counter to the name | Queues, fair locks, leader election recipes |

A node can be both ephemeral and sequential, which is exactly what the lock and leader-election recipes need (Day 3).

## 2.2 Versions and conditional updates

Every znode has a `version` that goes up by one on each `setData`. Clients can say "only change this if the version is still 4". If someone else changed it in the meantime, the call fails with `BadVersionError`. This **compare-and-set** behaviour lets clients update shared data safely without locks. Passing `-1` means "any version".

## 2.3 The Stat record

| Field | Meaning |
|---|---|
| `czxid` | zxid of the transaction that created the node |
| `mzxid` | zxid of the last `setData` |
| `pzxid` | zxid of the last child create/delete |
| `ctime`, `mtime` | creation and modification time (ms) |
| `version` | number of `setData` calls |
| `cversion` | number of child creates + deletes (also the sequential counter) |
| `ephemeral_owner` | session id of the owner, or 0 for regular nodes |
| `data_length`, `num_children` | derived sizes |

## 2.4 zxid: the transaction id

Every write in ZooKeeper gets a unique **zxid**, a 64-bit number split into two halves:

```
 63                    32 31                     0
+------------------------+------------------------+
|        epoch           |        counter         |
+------------------------+------------------------+
```

* **epoch**: increases by one every time a new leader takes over (set by ZAB on Day 2).
* **counter**: increases by one for every write that leader makes.

Because the epoch occupies the high bits, comparing two zxids as plain integers orders them first by epoch, then by counter. The bigger zxid is always the newer write. This one number lets us say "server A has seen more history than server B", which is exactly what leader election needs. Code: `zk/common/zxid.py`.

## 2.5 Quorum

A **quorum** is any majority of the servers: 2 of 3, or 3 of 5. Any two majorities always share at least one server. That shared server is what makes elections and replication safe: a decision made by one majority can never be missed by a later majority. A cluster of `2f + 1` servers keeps working while `f` servers are down.

## 2.6 Server states

| State | Meaning |
|---|---|
| `LOOKING` | No leader known; running an election |
| `FOLLOWING` | Knows the leader and follows it |
| `LEADING` | This server is the leader |

---

# Chapter 3: System Architecture

## 3.1 The big picture

```
                 +-----------+      +-----------+
                 | ZKClient  |      |   cli /   |
                 | (library) |      |  status   |
                 +-----+-----+      +-----+-----+
                       |   gRPC: ZooKeeper service (client_port 218x)
        +--------------+--------+---------+-------------------+
        |                       |                             |
+-------v--------+     +--------v-------+            +--------v-------+
|   Server S1    |     |   Server S2    |            |   Server S3    |
|   FOLLOWING    |<--->|   FOLLOWING    |<---------->|    LEADING     |
+----------------+     +----------------+            +----------------+
        ^        gRPC: Peer service (peer_port 288x)          ^
        +-----------------------------------------------------+
              election notifications + liveness pings
```

Every server runs the **same code**. Which role it plays is decided at run time by the election. Each server opens two ports:

* **client port** (2181, 2182, ...) for the `ZooKeeper` gRPC service that clients call;
* **peer port** (2881, 2882, ...) for the `Peer` gRPC service that other servers call.

Using separate ports keeps client traffic and server-to-server traffic apart. ZooKeeper does the same.

## 3.2 Inside one server

```
+------------------------------- ZKServer --------------------------------+
|                                                                          |
|   ClientService  <-- client RPCs        PeerService <-- peer RPCs        |
|        |                                     |                           |
|        v                                     v                           |
|    DataTree                       FastLeaderElection ----> PeerClient ---+--> other
|   (znodes)                         (state, vote, round)    (outgoing)    |    servers
|                                            ^                            |
|   StateStore (epoch.json)                  |                            |
|                                main loop:  elect -> lead/follow -> elect |
|                                            (liveness.py)                 |
+--------------------------------------------------------------------------+
```

| Component | File | Job |
|---|---|---|
| `ZKServer` | `server/server.py` | Creates every part, opens ports, runs the main loop |
| `ClientService` | `server/client_service.py` | Turns client RPCs into data-tree calls |
| `DataTree` | `server/datatree.py` | The znodes; knows nothing about networking |
| `FastLeaderElection` | `server/election.py` | Decides who leads; knows nothing about gRPC |
| `PeerClient` | `server/peer_client.py` | Sends notifications and pings to other servers |
| `PeerService` | `server/peer_service.py` | Receives notifications and pings |
| `liveness` | `server/liveness.py` | Detects a dead leader or a lost majority |
| `StateStore` | `server/state_store.py` | Saves the epochs on disk |

## 3.3 Design principles

1. **One job per file.** Every module can be read and tested on its own.
2. **Logic separate from networking.** `DataTree` and `FastLeaderElection` never import gRPC. Election talks to the network only through a small `transport` object with `send()` and `broadcast()`, and the data tree is plain Python. This makes them easy to unit-test and easy to reason about.
3. **One event loop per server process, no threads.** Every request, timer and message is handled by a single asyncio loop. Only one piece of code runs at a time, so the data tree and the election state need no locks. Python has no race detector like Go's `-race`, so avoiding shared-memory threads is our main defence against concurrency bugs.
4. **Shared code in `common/`.** The client never imports server modules. Both use `common/errors.py`, `common/stat.py`, and so on.

## 3.4 Project layout

```
zookeeper-py/
  cluster.json, cluster5.json   cluster configs (3 and 5 servers)
  requirements.txt, pytest.ini
  zk/
    proto/zk.proto              the contract: every message
    proto/zk_pb2*.py            generated by grpc_tools.protoc
    common/                     errors.py  zxid.py  paths.py  stat.py  config.py
    server/                     znode.py  datatree.py  vote.py  election.py
                                liveness.py  peer_client.py  peer_service.py
                                convert.py  client_service.py  state_store.py
                                server.py  __main__.py
    client/client.py            client library
    tools/                      cli.py  status.py  start_cluster.py
  tests/                        helpers.py + 4 test files
  docs/                         this document + build_pdf.py
```

---

# Chapter 4: The Communication Layer (`zk.proto`)

## 4.1 Why gRPC and Protocol Buffers

* The `.proto` file is a **single, language-neutral contract** that all three team members code against.
* gRPC gives us typed stubs, timeouts, and an **asyncio API** (`grpc.aio`) that fits our single-event-loop design.
* On Day 2 we need **FIFO, ordered delivery** between leader and followers for ZAB. A gRPC **bidirectional stream** provides that for free, and it runs on the same library.

## 4.2 The two services

**`ZooKeeper` service (clients → server):**

| RPC | Request | Response |
|---|---|---|
| `Create` | path, data, ephemeral, sequential, session_id | err, actual path |
| `Delete` | path, version | err |
| `Exists` | path | err, exists, stat |
| `GetData` | path | err, data, stat |
| `SetData` | path, data, version | err, stat |
| `GetChildren` | path | err, children |
| `GetStatus` | (empty) | server id, state, leader, zxid, epoch, round, node count |

**`Peer` service (server → server):**

| RPC | Purpose |
|---|---|
| `SendNotification(Notification)` | One FLE message: sender, state, vote, round |
| `Ping(PingRequest)` | Day 1 liveness check; replies with the receiver's state and leader |

## 4.3 Errors are returned in the response, not raised as gRPC errors

Every response carries an `ErrorCode err` field (`OK`, `NO_NODE`, `NODE_EXISTS`, `BAD_VERSION`, `NOT_EMPTY`, `NO_CHILDREN_FOR_EPHEMERALS`, `BAD_ARGUMENTS`, `NOT_LEADER`). The flow is:

```
DataTree raises NodeExistsError
  -> ClientService catches ZKError, puts err=NODE_EXISTS in the response
    -> ZKClient sees err != OK, raises NodeExistsError for the application
```

This keeps "the operation failed for a normal reason" (a ZooKeeper error) separate from "the network failed" (a gRPC error, which the client turns into `ConnectionLossError`). The same exception classes in `common/errors.py` are used on both sides, and a test checks that their numbers match the proto enum.

## 4.4 Converting between protobuf and Python

The election code works with plain frozen dataclasses (`Vote`, `Notification`). `server/convert.py` is the only place that translates them to and from protobuf. If the proto changes, only that file changes.

## 4.5 Sending to peers: fire-and-forget

`PeerClient.send()` starts a background task that makes the RPC with a 0.5 s timeout and **ignores failures**. FLE is built for this: if a notification is lost, the election simply re-sends its vote on the next timeout. Nothing ever waits on a single message.

`PeerClient` also lowers gRPC's **reconnect backoff** from about 1 s to 100–500 ms. With the default, a channel that once failed to reach a peer (for example because the peer had not started yet) would stay silent for longer than a whole election, and the results became unpredictable (see Chapter 12).

---

# Chapter 5: The Data Tree

Files: `server/znode.py`, `server/datatree.py`, `common/paths.py`, `common/stat.py`.

## 5.1 Storage layout

```python
self.nodes = {                     # flat dict: full path -> ZNode
    "/":            ZNode(children={"app"}),
    "/app":         ZNode(data=b"hello", children={"config"}),
    "/app/config":  ZNode(data=b"v=1"),
}
self.ephemerals = {111: {"/group/w1"}}   # session id -> its ephemeral paths
```

We use a **flat dictionary keyed by full path** instead of nested objects. Every lookup is a single O(1) dictionary access, without walking the tree. Each node also keeps the **set of its children's names**, so `getChildren` is cheap. The `ephemerals` index means deleting all of an expired session's nodes doesn't require scanning the whole tree.

## 5.2 Every write takes a zxid

```python
tree.create(path, data, zxid, ephemeral_owner=0, sequential=False)
tree.delete(path, version, zxid)
tree.set_data(path, data, version, zxid)
```

The tree never makes up zxids; the caller passes them in. On Day 1 the leader's `ClientService` supplies them. On Day 2 the very same calls are made when a ZAB `COMMIT` arrives, with the zxid taken from the transaction. Every server therefore applies the same operations with the same zxids in the same order and ends up with an identical tree. **The data tree is ZAB's state machine, and it is already finished.**

## 5.3 The operations

**create(path, data, zxid, ephemeral_owner, sequential)**

1. Validate the path (`/` prefix, no trailing `/`, no empty, `.` or `..` parts). For a sequential node we validate as if a digit were already appended, so `/locks/` is allowed.
2. The parent must exist (`NoNodeError`) and must not be ephemeral (`NoChildrenForEphemeralsError`).
3. If sequential, append the parent's `cversion` as 10 digits: `/locks/lock-` becomes `/locks/lock-0000000003`.
4. The final path must not exist yet (`NodeExistsError`).
5. Create the node with `czxid = mzxid = pzxid = zxid`, then update the parent: add the child name, `cversion += 1`, `pzxid = zxid`.
6. If ephemeral, record it in the `ephemerals` index.

Because the sequential counter comes from `cversion`, and `cversion` also goes up on deletes, **the counter never goes backwards**, even after nodes are deleted. The tests check this.

**delete(path, version, zxid)**: the node must exist, the version must match (or be −1), and it must have no children (`NotEmptyError`). The root cannot be deleted. The parent's `cversion` and `pzxid` are updated, and the ephemeral index is cleaned up.

**set_data(path, data, version, zxid)**: version check, then replace the data, `version += 1`, `mzxid = zxid`, and return the new Stat.

**exists / get_data / get_children**: reads, no side effects. `exists` returns `None` instead of raising, matching ZooKeeper.

**delete_ephemerals(session_id, zxid)**: deletes every ephemeral node of a session. On Day 3 this runs when a replicated `closeSession` transaction is applied.

## 5.4 Safety detail: callers only get copies

`ZNode.snapshot_stat()` returns a **new** Stat each time, with `data_length` and `num_children` filled in. Code that receives a Stat can change it freely without affecting the tree. A test checks this.

---

# Chapter 6: Fast Leader Election (FLE)

File: `server/election.py` (algorithm) and `server/vote.py` (data types). This is the heart of Day 1.

## 6.1 What a vote is

```python
Vote(leader=3, zxid=0x100000005, epoch=1)
```

A vote says "I think server 3 should lead, and here is how much history server 3 has". Each server starts by voting for itself, with its own last zxid and current epoch.

## 6.2 The ordering rule: which vote is better

```python
def key(self):  return (self.epoch, self.zxid, self.leader)
def is_better_than(self, other):  return self.key() > other.key()
```

A vote for A beats a vote for B when A has:

1. a **higher epoch**, or
2. the same epoch and a **higher last zxid**, or
3. the same epoch and zxid and a **higher server id** (only a tie-breaker).

Rules 1 and 2 guarantee that **the winner always has the most up-to-date history** among the servers that voted. That matters for ZAB on Day 2: a committed write is always stored on a majority, and any electing majority overlaps it, so the winner already has every committed write. The new leader never has to fetch missing history from followers; it only has to push its own history to them. This is the simplification real ZooKeeper relies on.

## 6.3 Election rounds (the logical clock)

Each server has a counter `round` that goes up by one every time it starts an election. Notifications carry the sender's round:

* **Newer round than mine:** I'm behind. Jump to that round, forget the votes I collected, re-pick the better of (their vote, my own vote), and broadcast it.
* **Older round than mine:** the sender is behind. Send it my vote directly so it catches up, and ignore the old message.
* **Same round:** a normal message (next section).

Rounds stop old messages from a previous election from confusing the current one.

## 6.4 The algorithm step by step

```
look_for_leader():
    state = LOOKING; round += 1
    vote  = (me, my_last_zxid, my_epoch)
    votes = {me: vote}              # latest vote of each LOOKING server this round
    established = {}                # servers already FOLLOWING / LEADING
    broadcast(vote)

    loop:
        n = next notification (wait up to `wait`)
        if none arrived:  broadcast(vote) again; wait = min(2*wait, max)   # back off
        elif n.state == LOOKING:
            handle the round rules (6.3)
            if n.vote is better than mine:  vote = n.vote; broadcast(vote)
            elif n.vote differs from mine:  send my vote to n.sender     # it may have missed it
            votes[n.sender] = n.vote
            if a majority in `votes` holds my vote:
                wait finalize_wait (200 ms) for anything better
                if nothing better arrived: DECIDE(vote)
        else:  (n.sender is FOLLOWING or LEADING)
            established[n.sender] = n.vote
            if a majority of `established` names leader L
               and L itself reported LEADING:  DECIDE(L)       # join running cluster

DECIDE(v): state = LEADING if v.leader == me else FOLLOWING
```

## 6.5 Why the finalize wait

When a majority first agrees, a better vote may still be on its way, for example from a server that just started. Waiting 200 ms for anything better makes it very likely the best server wins. If a better vote shows up during the wait, it is put back in the queue and the main loop handles it normally.

## 6.6 Joining a cluster that already has a leader

When a server restarts, the others are already FOLLOWING or LEADING. They don't start a new election. When a FOLLOWING or LEADING server receives a notification from a LOOKING server, it **replies with its own vote and state** (`FastLeaderElection.receive`). The newcomer collects these replies in `established`. Once a majority names the same leader L, **and L itself has said LEADING**, the newcomer follows L right away.

The second condition (`_leader_confirmed`) is important. Right after a leader dies, its old followers may not have noticed yet and still say "I follow L". Without the check, a newcomer could decide to follow a dead leader.

## 6.7 Worked example: three servers start together

All servers have zxid 0 and epoch 0, so only the server id decides.

| Time | S1 | S2 | S3 |
|---|---|---|---|
| t0 | votes S1, broadcasts | votes S2, broadcasts | votes S3, broadcasts |
| t1 | gets S2's vote: better, switches to S2 | gets S1's vote: worse, replies with S2 | gets S1 and S2's votes: worse, replies with S3 |
| t2 | gets S3's vote: better, switches to S3, broadcasts | gets S3's vote: better, switches to S3, broadcasts | votes = {S3, S3 from S1, S3 from S2}: majority |
| t3 | majority on S3; finalize wait | majority on S3; finalize wait | finalize wait (200 ms) |
| t4 | **FOLLOWING S3** | **FOLLOWING S3** | **LEADING** |

Measured from the logs of a real 3-process run: all three servers decided **about 215 ms** after starting, in a single round.

## 6.8 Worked example: the newest history wins

If S1 has zxid 50 and S2 and S3 have zxid 10, every server eventually switches to the vote `(epoch 0, zxid 50, S1)` because it beats the others on zxid, even though S1 has the lowest id. The test `test_server_with_newest_history_wins` checks this.

## 6.9 What FLE does *not* promise

FLE only promises that **a majority agrees on one leader with the newest history among the voters**. If two servers agree and finish before a slow third server's vote arrives, the winner may not have the highest id. That's correct and harmless; the slow server joins as a follower. Our test for equal histories therefore only checks that exactly one leader exists, not which one.

---

# Chapter 7: Failure Detection (Day 1 Liveness)

File: `server/liveness.py`. Without a way to notice failures, an elected cluster would follow a dead leader forever. Day 1 adds a simple heartbeat. On Day 2 it is replaced by ZAB's own `PING` messages, which travel on the leader–follower stream.

## 7.1 Follower side

```
every ping_interval (100 ms):
    ask the leader "who leads?"  (Peer.Ping)
    "I am LEADING"              -> leader is fine, reset the timer
    "I follow / lead someone else" -> return at once -> new election
    "I am LOOKING"              -> leader may still be finishing its own
                                   election; tolerate it until the timer runs out
    no answer                   -> count against the timer
    if no good answer for sync_limit (500 ms) -> return -> new election
```

## 7.2 Leader side

The leader records the time of every ping it receives. Every 100 ms it counts itself plus the followers that pinged in the last 500 ms. If that number is **below a majority**, it **steps down** and goes back to LOOKING.

This matters under network partitions: a leader cut off from the majority must stop acting as leader, or there would be two leaders. The test `test_minority_cannot_lead` kills both followers and checks that the old leader goes back to LOOKING.

## 7.3 The main loop that ties it together

```python
while running:
    vote = await election.look_for_leader()
    if election.state == LEADING:  reason = await lead()     # leader_loop
    else:                          reason = await follow()   # follower_loop
    log("leaving", state, reason)                            # then elect again
```

A server is always in exactly one of these three activities, and every way out leads back to a new election.

## 7.4 Failover timeline (measured)

From a real run with three separate processes, killing leader S3:

| Time | Event |
|---|---|
| 25.531 | S3 process killed |
| 26.052 | S1 and S2: "leader stopped answering" (≈ sync_limit later), go LOOKING, round 2 |
| 26.267 | S2 decides LEADING (after the 200 ms finalize wait) |
| 26.275 | S1 follows S2 |

**Total: about 0.74 s from crash to new leader**, within the project's 1-second goal. Most of the time is the 500 ms detection timeout. Lowering `sync_limit_ms` in `cluster.json` makes failover faster, at the risk of false alarms on a slow network.

## 7.5 Timing settings

| Setting in cluster.json | Default | Effect |
|---|---|---|
| `finalize_wait_ms` | 200 | Extra wait after a majority agrees |
| `max_notification_interval_ms` | 800 | Longest gap between re-sending a vote when nobody answers |
| `ping_interval_ms` | 100 | How often followers ping the leader |
| `sync_limit_ms` | 500 | Silence after which the leader or the majority counts as gone |

---

# Chapter 8: Wiring the Server Together

File: `server/server.py`, `server/__main__.py`, `server/state_store.py`, `server/client_service.py`.

## 8.1 Start-up sequence

1. `python -m zk.server --id 2` parses arguments and loads `cluster.json`.
2. `ZKServer.__init__` creates the StateStore, DataTree, PeerClient and FastLeaderElection. Nothing touches the network yet.
3. `asyncio.run(server.serve_forever())` starts the event loop. Inside it, `start()`:
   * opens the gRPC channels to the peers (this must happen inside the running loop; see Chapter 12),
   * starts two gRPC servers (client port and peer port),
   * starts the main loop task (election → lead/follow → election ...).

## 8.2 The epoch store

`StateStore` keeps `current_epoch` and `accepted_epoch` in `data/server<N>/epoch.json`. It writes the file atomically: write a temporary file, `fsync` it, then `os.replace` it over the old one, so a crash never leaves a half-written file. On Day 1 the epochs are only read (default 0) and fed into votes. On Day 2, ZAB's Discovery phase sets `accepted_epoch` and its Synchronization phase sets `current_epoch`.

## 8.3 How client requests are handled (Day 1)

* **Reads** are served from the server's **own** tree, like real ZooKeeper (fast, but possibly a little stale).
* **Writes** are accepted **only by the leader**. A follower answers `NOT_LEADER`.
* The leader computes the next zxid (same epoch, counter + 1), applies the write to its tree, and **only if it succeeds** moves `last_zxid` forward. A failed write, such as a bad version, does not use up a zxid.

## 8.4 Day 1 limitations

Because there is no replication yet:

* followers' trees stay empty, and reading from a follower doesn't show the leader's writes;
* if the leader dies, its writes are lost.

This is expected on Day 1, and it shows exactly why Day 2's ZAB is needed. The `GetStatus` RPC (and `zk.tools.status`) shows each server's node count, so this is visible in demos.

---

# Chapter 9: The Client Library

File: `client/client.py`.

## 9.1 Usage

```python
from zk.client import ZKClient

async with ZKClient(["127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"]) as zk:
    await zk.create("/app", b"hello")
    path = await zk.create("/app/job-", b"", sequential=True)   # /app/job-0000000000
    data, stat = await zk.get_data("/app")
    await zk.set_data("/app", b"world", version=stat.version)
    print(await zk.get_children("/app"))
```

## 9.2 Connecting

`connect()` tries the servers **in random order** (which spreads clients across the cluster) and keeps the first one that answers a `GetStatus` call within the timeout. If none answers, it raises `ConnectionLossError`.

## 9.3 Errors

Every response's `err` field is checked. A non-OK code becomes the matching exception (`NoNodeError`, `BadVersionError`, ...). A gRPC failure (server down, timeout) becomes `ConnectionLossError`.

## 9.4 Write redirect (temporary)

If a write hits a follower and gets `NOT_LEADER`, the client asks that server for the leader's address (`GetStatus.leader_address`), reconnects there, and retries once. On Day 2 followers will forward writes to the leader themselves, and this code will be deleted.

## 9.5 Coming later

| Feature | Day |
|---|---|
| Sessions, session id, heartbeats from idle clients | 2 |
| FIFO request order with per-request `xid` | 2 |
| Watches (one-shot notifications) | 3 |
| Failover that keeps the session, never going back to an older zxid | 3 |
| Recipes: config, group membership, leader election, lock | 3 |

---

# Chapter 10: How to Run Everything

All commands are for **Windows PowerShell** and are run from the project folder. On Linux or macOS, replace `.venv\Scripts\python.exe` with `.venv/bin/python`.

## 10.1 One-time setup

```
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

If pip fails with `No module named pip._internal...` on Windows, the folder path is too long. Create the venv at a short path (`python -m venv C:\venvs\zkpy`) and use that `python.exe` instead. Optionally run `.venv\Scripts\Activate.ps1` so that `python` points to the venv; the commands below assume that.

## 10.2 Regenerating the gRPC code (only after editing zk.proto)

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. zk/proto/zk.proto
```

Running from the project root with `-I .` makes the generated files import each other as `zk.proto.zk_pb2`, so they work as part of the package.

## 10.3 Running the tests

```
python -m pytest -v
```

Expected result: **32 passed** in about 10–15 seconds.

## 10.4 Starting a cluster

Option A, one terminal per server (best for the kill-the-leader demo):

```
python -m zk.server --id 1
python -m zk.server --id 2
python -m zk.server --id 3
```

Option B, all servers from one terminal (Ctrl+C stops all):

```
python -m zk.tools.start_cluster
```

For five servers, add `--config cluster5.json` to every command. Each server keeps its files in `data/server<N>/` (change with `--data-dir`). Add `--verbose` for debug logs.

## 10.5 Watching and using the cluster

```
python -m zk.tools.status --watch
```

```
server  address           state      leader  round  last zxid     znodes
S1      127.0.0.1:2181    FOLLOWING  S3      1      0x0:00000000  1
S2      127.0.0.1:2182    FOLLOWING  S3      1      0x0:00000000  1
S3      127.0.0.1:2183    LEADING    S3      1      0x0:00000003  4
```

Command-line client:

```
python -m zk.tools.cli create /app hello
python -m zk.tools.cli create /app/task- job --sequential
python -m zk.tools.cli get /app
python -m zk.tools.cli set /app world --version 0
python -m zk.tools.cli ls /app
python -m zk.tools.cli stat /app
python -m zk.tools.cli delete /app/task-0000000000
python -m zk.tools.cli status
python -m zk.tools.cli --server 127.0.0.1:2182 get /app
```

Note: run these in PowerShell or cmd. Git Bash on Windows rewrites arguments that start with `/` (e.g. `/app` becomes `C:/Program Files/Git/app`).

## 10.6 Demo script for Day 1

1. Start three servers in three terminals and `status --watch` in a fourth.
2. Show one `LEADING` and two `FOLLOWING` servers.
3. Create a few nodes with the CLI; the leader's `last zxid` and `znodes` go up.
4. Press **Ctrl+C on the leader**. Within about 1 second the table shows a new leader.
5. Restart the killed server. It joins as `FOLLOWING` without disturbing the leader.
6. Stop two of the three servers. The remaining one drops to `LOOKING`: no majority, no leader.

---

# Chapter 11: Testing

## 11.1 Strategy

* **Unit tests** for pure logic (data tree, vote ordering, zxids, paths, config), with no network.
* **Integration tests** that start real gRPC servers on free localhost ports **inside the test process** (`tests/helpers.py`). This uses the real network code but stays fast and needs no extra terminals.
* `pytest-asyncio` runs the async tests; `asyncio_mode = auto` is set in `pytest.ini`.

## 11.2 What each test proves

| File | Tests | What they prove |
|---|---|---|
| `test_basics.py` | 5 | zxid halves; vote order (epoch > zxid > id); quorum sizes 1/3/5; error codes match the proto |
| `test_datatree.py` | 15 | create/read, missing parent, duplicates, 6 kinds of bad paths, version checks, delete rules, parent counters, sequential naming that never goes back, ephemerals and cleanup, stat copies |
| `test_election.py` | 7 | exactly one leader with 1/3/5 servers; newest history wins; new leader within 1.5 s after a crash; a restarted server rejoins under the existing leader; a minority cannot keep a leader |
| `test_client.py` | 5 | full API through gRPC; failed writes don't use a zxid; write on a follower is redirected; status; dead servers are skipped; no server → ConnectionLossError |

## 11.3 Results

All **32 tests pass**, and they passed on every one of several repeated full runs during development. The failover test also measures the time to a new leader and fails if it takes 1.5 s or more.

---

# Chapter 12: Problems We Hit and How We Fixed Them

These are real bugs found while building Day 1.

**1. gRPC channels bound to the wrong event loop.** The server object (and its peer channels) was created *before* `asyncio.run()` started the event loop. Every RPC then failed with "attached to a different loop", and the servers stayed in LOOKING forever. The in-process tests did not catch it, because there the object was created inside the loop. **Fix:** `PeerClient.open()` creates the channels inside `start()`, which always runs inside the loop. **Lesson:** create gRPC aio objects inside the running loop.

**2. One-server cluster never decided.** With no peers, no notification ever arrives, so the "do we have a majority?" check never ran. **Fix:** also check right after the first broadcast (one server is its own majority).

**3. Unpredictable winners at start-up.** gRPC waits about 1 s before retrying a peer that was unreachable. A server started a little early tried its peers, failed, and then stayed silent for longer than an election takes. **Fix:** shorter reconnect backoff (100–500 ms) in `PeerClient`.

**4. A missed vote was never re-sent.** If a server with a worse vote started after others had broadcast, nobody told it the better vote. The others kept receiving its messages, so their own resend timer never fired. **Fix:** when a LOOKING server gets a worse vote in the same round, it sends its own vote straight back to the sender.

**5. Extra election round at start-up.** A follower finished its election a few milliseconds before the leader did, pinged it, got "LOOKING", and gave up at once. **Fix:** a follower tolerates a leader that is LOOKING until `sync_limit` and gives up immediately only if the leader names a different leader. Start-up now finishes in one round.

**6. Failed writes used up zxids.** A `setData` with the wrong version still moved `last_zxid` forward, leaving gaps. **Fix:** compute the next zxid, apply the write, and only then store it as `last_zxid`.

---

# Chapter 13: Design Decisions and Trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| FLE + ZAB instead of Raft | Matches real ZooKeeper; the newest-history-wins rule keeps ZAB recovery simple | Less textbook pseudocode than Raft |
| asyncio, one loop per server | No locks, no data races; fits gRPC aio | The GIL limits throughput (benchmark trends still hold) |
| Unary RPCs for FLE, streams later | FLE tolerates loss and reordering; simplest possible code | Day 2 needs a bidirectional stream for ordered ZAB messages |
| Separate client and peer ports | Isolates traffic, like ZooKeeper | Two ports per server |
| Errors inside responses | Clear split between ZooKeeper errors and network errors | Every response type has an `err` field |
| Flat dict for the tree | O(1) lookups, simple code | Path strings repeated in keys (fine at our scale) |
| Day 1 ping-based liveness | Needed to demo failover before ZAB exists | Thrown away on Day 2 (small file, easy to replace) |

---

# Chapter 14: Hand-off to Day 2

What Day 2 adds, and where it plugs in:

| Day 2 piece | Builds on | Change |
|---|---|---|
| Leader–follower **bidirectional stream** | `zk.proto` Peer service | Add `rpc Sync(stream ZabMessage) returns (stream ZabMessage)` |
| **ZAB Discovery** (FOLLOWERINFO / NEWEPOCH / ACKEPOCH) | `StateStore` | New epoch = max(accepted epochs) + 1; save `accepted_epoch` |
| **ZAB Synchronization** (DIFF / TRUNC / SNAP, NEWLEADER, UPTODATE) | FLE's newest-history-wins guarantee | Leader pushes its history to a majority before serving |
| **ZAB Broadcast** (PROPOSAL / ACK / COMMIT) | `DataTree` write methods with zxid | Apply each committed transaction on every server |
| **Transaction log + snapshots** | `last_zxid` | `last_zxid` read from the log instead of `initial_zxid` |
| **Write forwarding** | `ClientService._run` | Followers forward writes; remove the client's NOT_LEADER redirect |
| **ZAB PING** | `liveness.py` | Replace the Day 1 ping loop |
| **Sessions and heartbeats** | `CreateRequest.session_id`, `delete_ephemerals` | Real session ids and expiry |

---

# Appendix A: Configuration File Reference

```json
{
  "servers": {
    "1": {"host": "127.0.0.1", "client_port": 2181, "peer_port": 2881},
    "2": {"host": "127.0.0.1", "client_port": 2182, "peer_port": 2882},
    "3": {"host": "127.0.0.1", "client_port": 2183, "peer_port": 2883}
  },
  "timing": {
    "finalize_wait_ms": 200,
    "max_notification_interval_ms": 800,
    "ping_interval_ms": 100,
    "sync_limit_ms": 500
  }
}
```

The `timing` block is optional; the values shown are the defaults. For a multi-machine demo, replace `127.0.0.1` with each machine's IP address and copy the same file to every machine.

# Appendix B: Glossary

| Term | Meaning |
|---|---|
| Znode | A node in ZooKeeper's data tree |
| Ephemeral node | Deleted automatically when its session ends |
| Sequential node | Gets an increasing 10-digit suffix |
| zxid | 64-bit transaction id: epoch (high 32 bits) + counter (low 32 bits) |
| Epoch | Number of the current leader's reign; increases on every new leader |
| Quorum | Any majority of servers |
| FLE | Fast Leader Election: ZooKeeper's leader election algorithm |
| ZAB | ZooKeeper Atomic Broadcast: ZooKeeper's replication protocol |
| Round / logical clock | Election counter; newer rounds override older ones |
| Finalize wait | Extra wait after a majority agrees, in case a better vote arrives |
| sync limit | Silence after which a leader or majority counts as gone |

# Appendix C: References

1. P. Hunt, M. Konar, F. P. Junqueira, B. Reed. *ZooKeeper: Wait-free coordination for Internet-scale systems.* USENIX ATC 2010.
2. F. P. Junqueira, B. C. Reed, M. Serafini. *Zab: High-performance broadcast for primary-backup systems.* IEEE/IFIP DSN 2011.
3. A. Medeiros. *ZooKeeper's atomic broadcast protocol: Theory and practice.* Aalto University, 2012.
4. Apache ZooKeeper source: `FastLeaderElection.java` (the reference for the election algorithm).
