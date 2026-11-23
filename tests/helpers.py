"""Helpers to run a whole cluster inside one test process."""

import asyncio
import socket

from zk.common.config import ClusterConfig, ServerInfo, Timing
from zk.server.server import ZKServer
from zk.server.vote import ServerState


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_config(n):
    servers = {
        sid: ServerInfo(id=sid, host="127.0.0.1", client_port=free_port(), peer_port=free_port())
        for sid in range(1, n + 1)
    }
    return ClusterConfig(servers=servers, timing=Timing())


async def start_cluster(config, data_dir, zxids=None):
    """Start every server; `zxids` optionally maps server id -> initial zxid."""
    zxids = zxids or {}
    servers = {
        sid: ZKServer(sid, config, str(data_dir), initial_zxid=zxids.get(sid, 0))
        for sid in config.servers
    }
    for s in servers.values():
        await s.start()
    return servers


async def stop_all(servers):
    for s in servers.values():
        await s.stop()


def leaders(servers):
    return [s for s in servers.values() if s.election.state == ServerState.LEADING]


async def wait_for_stable_leader(servers, timeout=5.0):
    """Wait until exactly one leader exists and every other server follows it."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        lead = leaders(servers)
        if len(lead) == 1:
            leader_id = lead[0].my_id
            followers_ok = all(
                s.election.state == ServerState.FOLLOWING and s.election.vote.leader == leader_id
                for s in servers.values() if s.my_id != leader_id
            )
            if followers_ok:
                return lead[0]
        await asyncio.sleep(0.05)
    states = {sid: (s.election.state.name, s.election.vote.leader) for sid, s in servers.items()}
    raise AssertionError(f"no stable leader after {timeout}s: {states}")
