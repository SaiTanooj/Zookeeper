"""Helpers to run a whole cluster inside one test process."""

import asyncio
import os
import socket

from zk.common.config import ClusterConfig, ServerInfo, Timing
from zk.common.zxid import make_zxid
from zk.server import txn as T
from zk.server.server import ZKServer
from zk.server.txn import Txn
from zk.server.txnlog import TxnLog
from zk.server.vote import ServerState


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_config(n, **timing):
    servers = {
        sid: ServerInfo(id=sid, host="127.0.0.1", client_port=free_port(), peer_port=free_port())
        for sid in range(1, n + 1)
    }
    return ClusterConfig(servers=servers, timing=Timing(**timing))


def seed_history(data_dir, sid, count, epoch=0):
    """Write `count` create txns into server `sid`'s log before it starts."""
    log = TxnLog(os.path.join(str(data_dir), f"server{sid}"))
    for i in range(1, count + 1):
        log.append(Txn(zxid=make_zxid(epoch, i), type=T.CREATE, path=f"/seed{i}"))
    log.close()


async def start_cluster(config, data_dir):
    servers = {sid: ZKServer(sid, config, str(data_dir)) for sid in config.servers}
    for s in servers.values():
        await s.start()
    return servers


async def restart(servers, sid, config, data_dir):
    """Stop server `sid` (if running) and start a fresh process-like copy."""
    if sid in servers:
        await servers[sid].stop()
    servers[sid] = ZKServer(sid, config, str(data_dir))
    await servers[sid].start()
    return servers[sid]


async def stop_all(servers):
    for s in servers.values():
        await s.stop()


def leaders(servers):
    return [s for s in servers.values() if s.election.state == ServerState.LEADING]


async def wait_for_stable_leader(servers, timeout=8.0):
    """Wait until one server leads, every other one follows it, and all serve."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        lead = leaders(servers)
        if len(lead) == 1:
            leader_id = lead[0].my_id
            ok = all(
                s.serving and (s.my_id == leader_id or s.election.vote.leader == leader_id)
                for s in servers.values()
            )
            if ok:
                return lead[0]
        await asyncio.sleep(0.05)
    states = {sid: (s.election.state.name, s.election.vote.leader, s.serving)
              for sid, s in servers.items()}
    raise AssertionError(f"no stable leader after {timeout}s: {states}")


async def wait_until(condition, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not reached in time")


def addresses(config, *sids):
    sids = sids or tuple(config.servers)
    return [config.servers[sid].client_address for sid in sids]
