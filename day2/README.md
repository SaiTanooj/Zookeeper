# Day 2: Replication with ZAB, durable storage, sessions

Day 2 builds on Day 1 (the project root) and adds:

* **ZAB replication**: every write goes through the leader as PROPOSAL → ACK → COMMIT
  and reaches every server, in the same order.
* **ZAB recovery**: a new leader starts a new epoch and syncs every follower with a snapshot.
* **Durable storage**: a write-ahead transaction log (fsync on every write) plus snapshots,
  so servers recover after a crash or a full restart.
* **Write forwarding**: followers pass writes to the leader. Clients can use any server.
* **Sessions**: connect/close, heartbeats, ephemeral nodes deleted when the session closes,
  and FIFO request order with request numbers (xid).

The full design is in `docs/Day2_Architecture.pdf`.

## Run it

All commands are run **from this `day2` folder**. The virtual environment one level up
(`..\.venv`) already has everything installed. To set one up from scratch:

```powershell
python -m venv ..\.venv
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Activate it so that `python` uses it (or use `..\.venv\Scripts\python.exe` everywhere):

```powershell
..\.venv\Scripts\Activate.ps1
```

### Tests (56)

```powershell
python -m pytest -v
```

### A 3-server cluster

One terminal per server:

```powershell
python -m zk.server --id 1
```
```powershell
python -m zk.server --id 2
```
```powershell
python -m zk.server --id 3
```

Or all of them in one terminal: `python -m zk.tools.start_cluster`.
Each server keeps its files in `data\server<N>\` (`epoch.json`, `txnlog.jsonl`, `snapshot.json`).

### Watch and use it

```powershell
python -m zk.tools.status --watch
```

```powershell
python -m zk.tools.cli --server 127.0.0.1:2181 create /app hello
python -m zk.tools.cli --server 127.0.0.1:2182 get /app
python -m zk.tools.cli create /app/task- job --sequential
python -m zk.tools.cli set /app world --version 0
python -m zk.tools.cli ls /app
```

Interactive shell (keeps one session open, which you need for ephemeral nodes):

```powershell
python -m zk.tools.cli shell
```
```
zk> create /app/worker busy --ephemeral
zk> ls /app
zk> quit
```

After `quit` the session closes and `/app/worker` disappears on every server.

## Demo script

1. Start 3 servers and `status --watch`. One server shows LEADING, the others FOLLOWING,
   and all show `serving = yes` and `epoch = 1`.
2. Write through S1 and read through S2: the data is on every server.
3. Kill the leader (Ctrl+C). A new leader appears with **epoch 2** and all the data.
   Writes keep working.
4. Restart the killed server. It is synced and catches up.
5. Stop all three servers and start them again. Everything is still there (recovered from
   the snapshot and log on disk).
6. Leave only one server running. It shows `serving = no` and writes fail with
   NotReadyError (no majority).
