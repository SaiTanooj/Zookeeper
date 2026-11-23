# Zookeeper

This project deals with the ZooKeeper architecture: we are implementing a
ZooKeeper-style coordination service (see `ZooKeeper_Go_Project_Writeup_v2.pdf`
for the original proposal; we are building it in Python with FLE + ZAB).

## ZooKeeper-style coordination service in Python: Day 1

A simplified ZooKeeper built in Python with asyncio and gRPC. Day 1 covers:

* the **znode data tree** (create, delete, exists, getData, setData, getChildren,
  version checks, sequential and ephemeral nodes)
* **Fast Leader Election (FLE)**, the election algorithm ZooKeeper uses, across 3 or 5 servers
* a simple **liveness check**, so a dead leader is replaced in under a second
* a **client library** plus command-line tools

Replication with ZAB is not part of Day 1; it comes on Day 2. See
`docs/Day1_Architecture.pdf` for the full design.

## 1. Setup (once)

You need Python 3.10 or newer.

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

> On Windows, if `pip` fails with `No module named pip._internal...`, the folder
> path is too long. Create the venv at a short path instead, e.g.
> `python -m venv C:\venvs\zkpy`, and use `C:\venvs\zkpy\Scripts\python.exe`
> in place of `.venv\Scripts\python.exe` below.

Optional: activate the venv so that `python` means the venv's Python:
`.venv\Scripts\Activate.ps1`. If PowerShell blocks scripts, skip this step
and use the full path to `python.exe` instead.

The generated gRPC files (`zk/proto/zk_pb2*.py`) are already included.
After editing `zk/proto/zk.proto`, regenerate them:

```powershell
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. zk/proto/zk.proto
```

## 2. Run the tests

```powershell
python -m pytest -v
```

There are 32 tests: the data tree, election with 1/3/5 servers, leader crash,
restart, minority partition, and the client API.

## 3. Run a cluster

Run all commands from the project root. Open **one terminal per server**:

```powershell
python -m zk.server --id 1
```
```powershell
python -m zk.server --id 2
```
```powershell
python -m zk.server --id 3
```

Or start all three from one terminal (Ctrl+C stops them all):

```powershell
python -m zk.tools.start_cluster
```

For 5 servers, use `--config cluster5.json` on every command.

## 4. Watch the cluster and use it

```powershell
python -m zk.tools.status --watch
```

```powershell
python -m zk.tools.cli create /app hello
python -m zk.tools.cli create /app/task- job --sequential
python -m zk.tools.cli get /app
python -m zk.tools.cli set /app world --version 0
python -m zk.tools.cli ls /app
python -m zk.tools.cli delete /app/task-0000000000
python -m zk.tools.cli status
python -m zk.tools.cli --server 127.0.0.1:2182 get /app
```

## 5. Demo: kill the leader

1. Start 3 servers in 3 terminals and run `python -m zk.tools.status --watch` in a fourth.
2. Find the server that shows `LEADING` (normally S3) and press Ctrl+C in its terminal.
3. Within about 1 second the table shows a new leader. Restart the old one
   (`python -m zk.server --id 3`) and it rejoins as a `FOLLOWING` server.

**Day 1 limitation:** writes are applied only on the leader (followers answer
NOT_LEADER and the client redirects to the leader). Followers do not have the
data yet, and data is lost when the leader dies. Day 2 adds ZAB replication.

## Layout

```
zk/proto/zk.proto        all gRPC messages (client API + server-to-server)
zk/common/               code shared by server and client: errors, zxid, paths, stat, config
zk/server/datatree.py    the znode tree
zk/server/znode.py       one node
zk/server/vote.py        Vote / Notification / ServerState + vote ordering
zk/server/election.py    Fast Leader Election
zk/server/liveness.py    Day 1 leader/follower heartbeat checks
zk/server/peer_client.py outgoing server-to-server RPCs
zk/server/peer_service.py incoming server-to-server RPCs
zk/server/client_service.py the client-facing API
zk/server/state_store.py epochs saved on disk
zk/server/server.py      wires everything together + main loop
zk/client/client.py      client library
zk/tools/                cli, status table, start_cluster
tests/                   pytest suite
docs/                    architecture document (Markdown + PDF) and its build script
```
