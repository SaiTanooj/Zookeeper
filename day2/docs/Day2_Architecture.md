# ZooKeeper-Style Coordination Service in Python

## Day 2: Replication, Storage and Sessions

Distributed Systems, Monsoon 2026 · Project 34

**Day 2 scope:** ZAB replication and recovery, the transaction log and snapshots, write forwarding, sessions and heartbeats

---

[TOC]

---

# Chapter 1: What Day 2 Adds

## 1.1 The problem with Day 1

On Day 1 the cluster could elect a leader, but only the leader stored data. Followers had empty trees, and if the leader died, every write was lost. Nothing was saved to disk either.

## 1.2 What works now

| Feature | Day 1 | Day 2 |
|---|---|---|
| Where writes are stored | Leader's memory only | Every server, in the same order |
| Leader crash | Data lost | Data kept; new leader continues |
| Server restart | Starts empty | Recovers from disk, then syncs |
| Write to a follower | Refused, client redirected | Forwarded to the leader automatically |
| Sessions | None | Connect, heartbeat, close |
| Ephemeral nodes | Stored, never removed | Removed when the session closes |
| Request order | Not defined | FIFO per client (xid numbers) |
| Failure detection | Separate ping loop | Built into ZAB (PING on the stream) |

## 1.3 The plan, and where we are

| Day | Theme | Status |
|---|---|---|
| 1 | Data tree, leader election, client | Done |
| **2** | **ZAB replication, storage, sessions** | **Done (this document)** |
| 3 | Session expiry, watches, sync, client failover, recipes | Next |
| 4 | Fault injection, benchmarks, Docker, demo | Later |

## 1.4 Where the code is

Day 2 lives in its own folder, `day2/`. It started as a copy of Day 1 and was then extended, so Day 1 (in the project root) still runs as it was. Run every Day 2 command from inside `day2/`.

---

# Chapter 2: Key Ideas in Plain Words

**Transaction (txn).** A write that the leader has already checked and fully decided. For example: "create `/locks/lock-0000000007` with data X at zxid 0x2:00000005". Every server applies the same txn and gets the same result.

**zxid.** The txn's id: the leader's epoch in the top 32 bits, and a counter in the bottom 32 bits. A bigger zxid is always a newer write.

**Epoch.** Goes up by one every time a new leader takes over. Messages from an older epoch are ignored, so an old leader that wakes up cannot cause damage.

**Logged vs applied.** A server *logs* a txn when it accepts a proposal: it writes the txn to disk and waits for the fsync. It *applies* the txn to its tree when the leader says COMMIT. Reads see only applied txns.

**Quorum (majority).** 2 of 3 servers, or 3 of 5. A write is committed once a majority has logged it. Any two majorities share at least one server, so a committed write can never be lost by a later leader.

**Serving.** A server answers clients only when it is synced with an active leader. Otherwise it answers NOT_READY.

---

# Chapter 3: Architecture

## 3.1 The big picture

```
   client ----write----> Follower S1 ---ForwardWrite---> Leader S3
                                                            |
                         +------- PROPOSAL (txn) ----------+---------+
                         v                                           v
                   Follower S1                                 Follower S2
                   log + fsync                                 log + fsync
                         |                                           |
                         +--------- ACK ----------> Leader <--- ACK -+
                                                     |
                                majority ACKed -> apply to tree
                                                     |
                         +-------- COMMIT ----------+---------+
                         v                                    v
                    S1 applies                           S2 applies
                         |
   client <---reply-----+   (S1 replies once the write is applied on S1)
```

## 3.2 Inside one server

```
+------------------------------ ZKServer -------------------------------+
|  ClientService (client port)            PeerService (peer port)        |
|     reads -> db.tree                     - election notifications      |
|     writes -> submit_write()             - ZAB stream (Follow)         |
|                                          - ForwardWrite                |
|                 +---------------------------------+                    |
|                 |          ZKDatabase             |                    |
|                 |  tree + sessions (memory)       |                    |
|                 |  txnlog.jsonl + snapshot.json   |                    |
|                 +---------------------------------+                    |
|                        ^                  ^                            |
|          Leader (zab_leader.py)   Follower (zab_follower.py)           |
|                        ^                  ^                            |
|   main loop:   elect (election.py) -> lead or follow -> elect again    |
+------------------------------------------------------------------------+
```

## 3.3 New and changed files

| File | New? | What it does |
|---|---|---|
| `server/txn.py` | new | The Txn record and its JSON form |
| `server/txnlog.py` | new | Write-ahead log on disk (append + fsync) |
| `server/snapshot.py` | new | Tree + sessions as JSON, on disk and on the wire |
| `server/database.py` | new | Tree, sessions, log and snapshots together; recovery |
| `server/write_op.py` | new | A client write on its way to the leader, and its result |
| `server/processor.py` | new | Leader: turns a write into a Txn (checks + decisions) |
| `server/zab_messages.py` | new | Helpers to build ZAB messages |
| `server/zab_leader.py` | new | Leader side of ZAB: discovery, sync, broadcast |
| `server/zab_follower.py` | new | Follower side of ZAB |
| `server/session_tracker.py` | new | When each session was last heard from |
| `server/liveness.py` | removed | Replaced by ZAB PINGs |
| `server/server.py` | changed | Main loop now runs Leader/Follower; `submit_write()` |
| `server/client_service.py` | changed | Sessions; writes go through `submit_write()` |
| `server/peer_service.py` | changed | Adds the ZAB stream and ForwardWrite |
| `server/election.py` | fixed | Election bug fix (Chapter 13) |
| `client/client.py` | changed | Sessions, heartbeats, FIFO, NOT_READY retries |
| `tools/cli.py` | changed | New interactive `shell` command |
| `tools/status.py` | changed | Shows serving, epoch, applied zxid, sessions |
| `proto/zk.proto` | changed | New RPCs and messages (Chapter 4) |

---

# Chapter 4: Protocol Changes

## 4.1 Client API

| RPC | New? | Purpose |
|---|---|---|
| `Connect(timeout_ms)` | new | Open a session; returns the session id |
| `Ping(session_id)` | new | Heartbeat while idle |
| `CloseSession(session_id)` | new | End the session; its ephemeral nodes are deleted |
| `Create / Delete / SetData` | changed | Now carry `session_id` and `xid` |
| `Exists / GetData / GetChildren` | changed | Now carry `session_id` and `xid` |
| `GetStatus` | changed | Adds serving, epoch, logged/applied zxid, sessions |

Every response now carries the request's `xid` (so the client can match them) and the `zxid` the server had applied when it answered.

## 4.2 Error codes

| Code | Meaning | Safe to retry? |
|---|---|---|
| `NOT_READY` | No active leader right now (election running) | Yes: nothing was changed |
| `SESSION_EXPIRED` | The session does not exist | No: open a new session |
| `CONNECTION_LOSS` | The leader changed in the middle of a write | Not blindly: the write may or may not have happened |

The difference between NOT_READY and CONNECTION_LOSS matters. If the client simply retried after CONNECTION_LOSS, a sequential create could happen twice.

## 4.3 Server-to-server

| RPC | Purpose |
|---|---|
| `SendNotification` | Leader election (unchanged) |
| `Follow(stream ZabMessage) ↔ stream ZabMessage` | One long-lived ZAB stream per follower |
| `ForwardWrite(WriteRequest)` | A follower hands a client write to the leader |

The ZAB stream is a **bidirectional gRPC stream**. Messages on one stream always arrive in the order they were sent. ZAB needs exactly this: proposals must reach each follower in zxid order.

---

# Chapter 5: Transactions

File: `server/txn.py`.

## 5.1 A txn is a decided write

A client asks: "create `/locks/lock-` as sequential". The leader decides everything up front and records the outcome:

```json
{"zxid": 8589934597, "type": "create", "path": "/locks/lock-0000000007",
 "data": "", "ephemeral_owner": 0, "session_id": 0, "timeout_ms": 0}
```

There are five types: `create`, `delete`, `set_data`, `create_session`, `close_session`.

## 5.2 Why decide everything on the leader

If each server worked out the sequential number itself, two servers could pick different numbers, and the trees would drift apart. Because the txn already carries the final name, applying it is purely mechanical and gives the same result everywhere.

## 5.3 Applying a txn twice is harmless

The database remembers `last_applied`. A txn whose zxid is not newer than that is skipped. So replaying the log after a crash, or receiving the same commit again, never changes anything twice. This is what "idempotent transactions" means in our project.

---

# Chapter 6: Storage

Files: `server/txnlog.py`, `server/snapshot.py`, `server/database.py`.

## 6.1 Files on disk

Each server has a folder `data/server<N>/`:

| File | Contents |
|---|---|
| `epoch.json` | `accepted_epoch` and `current_epoch` (from Day 1) |
| `txnlog.jsonl` | One txn per line, oldest first |
| `snapshot.json` | The whole tree + sessions at one zxid |

Everything is plain JSON, so you can open the files and read them.

## 6.2 The transaction log

* `append(txn)` writes one line, then calls **fsync**. When a follower ACKs a proposal, the txn is really on disk and survives a crash or power cut.
* If the server crashes halfway through writing a line, the broken last line is ignored on the next start.

## 6.3 Snapshots

Replaying a long log on every start would be slow. Every `snapshot_every` committed txns (default 100), the server:

1. writes the whole tree and the sessions to `snapshot.json` (temporary file, fsync, rename);
2. removes from the log every txn the snapshot already contains.

The same snapshot format is sent over the network in the SNAP message (Chapter 8).

## 6.4 The database and recovery

`ZKDatabase` keeps two counters:

```
last_logged   newest txn in the log       -> used in election votes
last_applied  newest txn in the tree      -> what reads see
```

and a `pending` list of txns that are logged but not yet committed.

```
           log_txn(txn)                commit(zxid)
PROPOSAL  -------------->  pending  ------------------>  tree
           (fsync, ACK)               (apply in order)
```

**Recovery on start-up:** load `snapshot.json`, then apply every txn in the log. This may include txns that were never committed. That's fine: the server doesn't serve clients until it syncs with a leader, and the sync replaces anything the leader does not have.

---

# Chapter 7: The Leader's Request Processor

File: `server/processor.py`.

## 7.1 The problem

The leader can have several writes in flight at once (proposed, not yet committed). Suppose client A creates `/a` and client B immediately creates `/a/b`. When B's write is checked, `/a` is not in the real tree yet, but it will be. B's check must see the tree *as it will be*.

## 7.2 The solution: a shadow tree

When a server becomes leader, the processor makes a private copy of the committed tree and sessions (the "shadow"). For every write:

1. Run the operation on the shadow tree with the next zxid. This performs every check (parent exists, version matches, session exists) and every decision (sequential number, new session id).
2. If a check fails, the client gets the error at once and **no zxid is used up**.
3. If it passes, build the Txn from the outcome.

The real tree changes only on commit. If the leader steps down, the shadow is simply thrown away.

## 7.3 zxids in a new epoch

The processor numbers txns `(epoch, 1)`, `(epoch, 2)`, ... Every new leader starts counting again at 1 in its new epoch, and since the epoch is in the top bits, the new zxids are still bigger than all the old ones.

---

# Chapter 8: ZAB, Step by Step

Files: `server/zab_leader.py`, `server/zab_follower.py`.

After the election (Day 1), the leader and its followers go through three phases on the ZAB stream.

## 8.1 Message types

| Message | Direction | Meaning |
|---|---|---|
| FOLLOWERINFO | F → L | "I am S1, my accepted epoch is 2, my last zxid is Z" |
| NEWEPOCH | L → F | "The new epoch is 3" |
| ACKEPOCH | F → L | "I accept epoch 3" |
| SNAP | L → F | Full snapshot of the leader's committed data |
| NEWLEADER | L → F | "You now have my history, please confirm" |
| ACK_NEWLEADER | F → L | Confirmation |
| UPTODATE | L → F | "Start serving clients" |
| PROPOSAL | L → F | One txn to log |
| ACK | F → L | "I logged txn Z" |
| COMMIT | L → F | "Apply everything up to Z" |
| PING | both | Heartbeat |

## 8.2 Phase 1: Discovery (agree on a new epoch)

```
Follower                               Leader
   | --- FOLLOWERINFO(accepted=2) ---->  |   waits for a majority (counting itself)
   |                                     |   new epoch = max(accepted epochs) + 1 = 3
   | <---------- NEWEPOCH(3) ----------  |
   |  save accepted_epoch = 3            |
   | ----------- ACKEPOCH ------------>  |
```

A follower refuses a NEWEPOCH that is older than an epoch it has already accepted. That stops a stale leader.

## 8.3 Phase 2: Synchronization (give everyone the same history)

```
   | <------- SNAP(leader's data) -----  |
   |  replace my data, clear my log      |
   | <-- PROPOSALs still in flight ----  |   (only for late joiners)
   | <--------- NEWLEADER(3) ----------  |
   |  save current_epoch = 3             |
   | -------- ACK_NEWLEADER ---------->  |   majority acked -> leader is "established"
   | <---------- UPTODATE -------------  |
   |  start serving clients              |
```

Two details make this simple and safe:

* **The leader never needs anyone else's data.** Fast Leader Election always picks the server with the newest history (highest epoch, then highest zxid). So the leader only pushes its own history out.
* **Nothing slips through the gap.** The leader sends the snapshot, any in-flight proposals and NEWLEADER, and adds the follower to its broadcast list, all in one step with no `await` in between. No proposal or commit can happen in the middle.

We always sync with a full snapshot. Real ZooKeeper can also send just the missing txns (DIFF) or tell a follower to drop extra txns (TRUNC). Those are speed-ups for large data sets; a snapshot gives the same result.

## 8.4 Phase 3: Broadcast (normal operation)

```
Leader:   txn = processor.prepare(write)      (checks + decisions)
          log txn (fsync)                      (the leader's own ACK)
          send PROPOSAL(txn) to all followers
Follower: log txn (fsync), send ACK(zxid)
Leader:   when a majority has ACKed the OLDEST pending proposal:
              apply it, send COMMIT(zxid), answer the client
          repeat while the next oldest also has a majority
Follower: on COMMIT(zxid): apply every pending txn up to zxid
```

Commits always happen **in zxid order**, even if the ACKs for a later proposal happen to arrive first. Every server therefore applies exactly the same txns in exactly the same order.

## 8.5 Heartbeats and stepping down

* The leader sends PING to every follower every 100 ms, and followers answer PING.
* A follower that hears nothing for `sync_limit` (500 ms) leaves and starts an election.
* A leader that cannot hear from a majority for `sync_limit` steps down. Pending writes fail with CONNECTION_LOSS.
* A new leader that cannot get a majority through discovery and sync within `init_limit` (2 s) gives up and a new election starts.

---

# Chapter 9: The Life of a Request

## 9.1 A write sent to a follower

1. The client sends `Create("/app")` to S1, a follower.
2. S1's `submit_write()` forwards it to the leader with `ForwardWrite`.
3. The leader's processor checks it and builds the txn.
4. The leader logs the txn and sends PROPOSAL; followers log it and ACK.
5. With a majority of ACKs, the leader applies it, sends COMMIT and answers S1 with the zxid.
6. S1 waits until **it** has applied that zxid, then answers the client.

Step 6 means a client always reads its own writes on the server it is connected to.

## 9.2 A write sent to the leader

The same, without the forwarding step.

## 9.3 A read

The server answers from its own tree, without asking the leader. Reads are fast, but a follower may be a few milliseconds behind the leader. This is ZooKeeper's normal behaviour. The `sync` call on Day 3 lets a client ask for the latest data when it needs it.

---

# Chapter 10: Sessions and the Client

Files: `client/client.py`, `server/session_tracker.py`, `server/client_service.py`.

## 10.1 Sessions are replicated

* `connect()` sends `Connect`. The server turns it into a `create_session` txn, which goes through ZAB like any write. Every server now knows the session and its timeout.
* `close()` sends `CloseSession`. The `close_session` txn deletes all of the session's ephemeral nodes on every server.
* Creating an ephemeral node with a session that does not exist fails with SESSION_EXPIRED.

## 10.2 Heartbeats

When the client has not sent anything for timeout/3, it sends `Ping`. Every request, Ping included, marks the session as "seen just now" in that server's SessionTracker. On Day 3 the leader will use these times to expire sessions whose clients disappeared.

## 10.3 FIFO order and xids

The client sends one request at a time, in the order the application made the calls. Each request gets the next `xid` (1, 2, 3, ...). So if an application fires 20 `set_data` calls at once, the server receives and applies them in exactly that order. A test checks this.

The client also remembers the highest zxid it has seen (`last_zxid`). On Day 3 this stops the client from moving to a server that is further behind than what it has already seen.

## 10.4 Retries

While the cluster is electing a leader, servers answer NOT_READY. The client waits 0.2 s and retries, for up to about 3 s. CONNECTION_LOSS is never retried automatically.

---

# Chapter 11: What Happens When Things Fail

| Failure | What happens |
|---|---|
| Leader crashes | Followers notice within 0.5 s, elect the server with the newest log, which starts a new epoch and syncs the others. Committed data is kept. |
| Follower crashes | The leader keeps going while it still has a majority. |
| Crashed server comes back | Recovers from its own disk, joins the leader, gets a snapshot, catches up. |
| All servers restart | Each recovers from disk; the election picks the newest; everyone syncs to it. |
| Only a minority is up | No leader can be established; servers answer NOT_READY; no writes are accepted. |
| Leader dies during a write | The client gets CONNECTION_LOSS: the write may or may not have been committed. |
| A server crashes mid-write to the log | The half-written last line is ignored on restart. |

**Why committed data is never lost:** a txn is committed only after a majority has written it to disk. Any later election needs a majority too, so at least one voter has that txn. Votes favour the newest history, so the winner has it. The winner then syncs everyone else to its history.

---

# Chapter 12: How to Run

Run every command from inside the `day2` folder. The project's virtual environment is one level up.

## 12.1 Setup

```
..\.venv\Scripts\Activate.ps1
```

If the venv does not exist yet:

```
python -m venv ..\.venv
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 12.2 Tests

```
python -m pytest -v
```

Expected: **56 passed** in about 25 seconds.

## 12.3 Start a cluster

```
python -m zk.server --id 1
python -m zk.server --id 2
python -m zk.server --id 3
```

Run each in its own terminal, or use `python -m zk.tools.start_cluster`. For five servers, add `--config cluster5.json`.

## 12.4 Watch it

```
python -m zk.tools.status --watch
```

```
server  state      serving  leader  epoch  applied zxid  znodes  sessions
S1      FOLLOWING  yes      S2      2      0x1:0000000e  3       0
S2      LEADING    yes      S2      2      0x1:0000000e  3       0
S3      DOWN
```

## 12.5 Use it

```
python -m zk.tools.cli --server 127.0.0.1:2181 create /app hello
python -m zk.tools.cli --server 127.0.0.1:2182 get /app
python -m zk.tools.cli create /app/task- job --sequential
python -m zk.tools.cli shell
```

In the shell, `create /app/worker busy --ephemeral`, then `quit`. The node disappears on every server.

## 12.6 Demo results (real processes)

From a run with three separate server processes on one machine:

1. Start-up: S3 led epoch 1 and all three servers were serving.
2. A write through S1 (`/app`) was readable through S2 and S3.
3. An ephemeral node created in the shell was visible during the session and gone on S3 right after `quit`.
4. Killing leader S3: S2 became leader of **epoch 2**, still had all the data, and accepted new writes (`mzxid = 0x2:00000004`).
5. Stopping and restarting all three: they came back in **epoch 3** with all data, recovered from disk.

---

# Chapter 13: Problems We Hit and How We Fixed Them

**1. An election bug left over from Day 1.** S1 and S2 had decided "S3 leads", but their last LOOKING messages never reached S3, because S3 had started a few milliseconds later and gRPC was still retrying the connection. S3 only heard "we follow S3" and refused to accept that, so it waited about 2 seconds for a timeout and a second election round. Real ZooKeeper accepts this when the message comes from the same election round, and so do we now. Start-up dropped from about 2.5 s to under half a second, and the fix was also applied to Day 1.

**2. Shutdown sometimes hung.** Stopping a gRPC server while a ZAB stream was still open sometimes hung forever in the test suite. **Fix:** a 0.5 s grace period so streams end cleanly, waiting for cancelled calls before closing channels, and a time limit on every shutdown step so `stop()` can never hang.

**3. Tests that assumed instant replication.** A test checked every server right after a write, but followers apply the COMMIT a few milliseconds after the leader. **Fix:** those tests now wait until the condition is true (with a time limit).

**4. Windows added an invisible character.** When text is piped into the CLI shell from PowerShell, the first line starts with a byte-order mark, so the first command was rejected. **Fix:** the shell strips it.

---

# Chapter 14: Tests

| File | Tests | What they check |
|---|---|---|
| `test_basics.py` | 5 | zxid, vote order, quorum sizes, error codes match the proto |
| `test_datatree.py` | 15 | The znode tree (from Day 1) |
| `test_storage.py` | 9 | Txn JSON, log append/drop/crash-safety, snapshots, commit, idempotency, recovery, install snapshot |
| `test_processor.py` | 5 | zxid numbering, in-flight writes see each other, sequential names, failed writes use no zxid, sessions |
| `test_election.py` | 8 | One leader with 1/3/5 servers, newest history wins, new epoch per leader, failover < 2 s, rejoin, minority |
| `test_replication.py` | 7 | Writes reach every server, forwarding, data survives leader crash, follower catch-up, full restart (with snapshots), no writes without majority, 15 concurrent creates in order |
| `test_client.py` | 7 | Full API, replicated sessions, ephemeral cleanup, xid/zxid, FIFO order, heartbeats, no server reachable |

All **56 tests pass**, and they passed six full runs in a row after the fixes in Chapter 13.

---

# Chapter 15: Limits and Day 3

## 15.1 Simplifications in Day 2

| Simplification | Why it is acceptable |
|---|---|
| Sync is always a full snapshot (no DIFF/TRUNC) | Same result; only slower for big data |
| A session only ends with an explicit close | Expiry of silent clients is Day 3 |
| One fsync per txn | Simple and safe; batching would be faster |
| The client stays on one server | Moving to another server is Day 3 |

## 15.2 Day 3 builds on this

| Day 3 feature | Uses from Day 2 |
|---|---|
| Session expiry | SessionTracker times + `close_session` txn |
| Watches | Fire when `db.commit()` applies a change |
| `sync` | Follower waits until it has applied the leader's latest zxid (`wait_until_applied`) |
| Client failover | `session_id` and `last_zxid` kept in the client |
| Recipes (lock, election, group, config) | Ephemeral + sequential nodes, all replicated now |

---

# Appendix A: Configuration

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
    "sync_limit_ms": 500,
    "init_limit_ms": 2000,
    "snapshot_every": 100
  }
}
```

| Setting | Meaning |
|---|---|
| `ping_interval_ms` | How often the leader pings followers |
| `sync_limit_ms` | Silence after which the leader or the majority counts as gone |
| `init_limit_ms` | Time a new leader has to get a majority synced |
| `snapshot_every` | Committed txns between snapshots |

# Appendix B: Glossary

| Term | Meaning |
|---|---|
| ZAB | ZooKeeper Atomic Broadcast: the replication protocol |
| Txn | A checked, fully decided write |
| Proposal | A txn the leader has sent out but not committed yet |
| Commit | The point where a txn is applied to the tree |
| Snapshot | The whole tree + sessions at one zxid |
| Write-ahead log | Txns are written to disk before they are acknowledged |
| Established | A leader that a majority has synced with |
| Serving | A server that answers clients |
| xid | A client's request number (FIFO order) |

# Appendix C: References

1. P. Hunt, M. Konar, F. P. Junqueira, B. Reed. *ZooKeeper: Wait-free coordination for Internet-scale systems.* USENIX ATC 2010.
2. F. P. Junqueira, B. C. Reed, M. Serafini. *Zab: High-performance broadcast for primary-backup systems.* IEEE/IFIP DSN 2011.
3. A. Medeiros. *ZooKeeper's atomic broadcast protocol: Theory and practice.* Aalto University, 2012.
