"""The client library: API, sessions, heartbeats and FIFO order."""

import asyncio

import pytest

from tests.helpers import (
    addresses, make_config, start_cluster, stop_all, wait_for_stable_leader, wait_until,
)
from zk.client import ConnectionLossError, ZKClient
from zk.common.errors import BadVersionError, NodeExistsError, NoNodeError


@pytest.fixture
async def cluster(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    leader = await wait_for_stable_leader(servers)
    yield config, servers, leader
    await stop_all(servers)


async def test_full_api(cluster):
    config, _, _ = cluster
    async with ZKClient(addresses(config)) as zk:
        assert await zk.create("/app", b"v0") == "/app"
        assert await zk.exists("/nope") is None
        data, stat = await zk.get_data("/app")
        assert data == b"v0" and stat.version == 0

        stat = await zk.set_data("/app", b"v1", version=0)
        assert stat.version == 1
        with pytest.raises(BadVersionError):
            await zk.set_data("/app", b"v2", version=0)

        seq = await zk.create("/app/job-", sequential=True)
        assert seq == "/app/job-0000000000"
        with pytest.raises(NodeExistsError):
            await zk.create("/app")
        with pytest.raises(NoNodeError):
            await zk.get_data("/missing")
        await zk.delete(seq)
        await zk.delete("/app", version=1)
        assert await zk.exists("/app") is None


async def test_session_is_replicated(cluster):
    config, servers, _ = cluster
    async with ZKClient(addresses(config)) as zk:
        assert zk.session_id != 0
        # Committed on our server at once; the others apply the COMMIT a moment later.
        await wait_until(lambda: all(zk.session_id in s.db.sessions for s in servers.values()))
    await wait_until(lambda: all(s.db.sessions == {} for s in servers.values()))


async def test_ephemeral_node_disappears_when_session_closes(cluster):
    config, servers, _ = cluster
    async with ZKClient(addresses(config)) as watcher:
        await watcher.create("/workers")
        async with ZKClient(addresses(config)) as worker:
            await worker.create("/workers/w1", b"busy", ephemeral=True)
            assert await watcher.get_children("/workers") == ["w1"]
            stat = await watcher.exists("/workers/w1")
            assert stat.ephemeral_owner == worker.session_id
        assert await watcher.get_children("/workers") == []
    await wait_until(lambda: all(s.db.tree.get_children("/workers") == []
                                 for s in servers.values()))


async def test_xids_increase_and_zxid_is_tracked(cluster):
    config, _, _ = cluster
    async with ZKClient(addresses(config)) as zk:
        first = zk._xid
        await zk.create("/a")
        z1 = zk.last_zxid
        await zk.set_data("/a", b"x")
        assert zk._xid == first + 2
        assert zk.last_zxid > z1


async def test_requests_run_in_the_order_they_were_made(cluster):
    config, _, _ = cluster
    async with ZKClient(addresses(config)) as zk:
        await zk.create("/order")
        # Fire 20 writes at once: FIFO means they are applied in call order.
        await asyncio.gather(*(zk.set_data("/order", str(i).encode()) for i in range(20)))
        data, stat = await zk.get_data("/order")
        assert data == b"19" and stat.version == 20


async def test_heartbeats_are_sent_while_idle(cluster):
    config, servers, _ = cluster
    async with ZKClient(addresses(config), session_timeout_ms=600) as zk:
        server = next(s for s in servers.values()
                      if s.info.client_address == zk.address)
        await asyncio.sleep(0.7)  # idle: about 3 heartbeats (every 200 ms)
        assert server.session_tracker.idle_for(zk.session_id) < 0.4


async def test_no_server_reachable():
    with pytest.raises(ConnectionLossError):
        await ZKClient(["127.0.0.1:1"]).connect()
