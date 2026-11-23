"""The client library against a real 3-server cluster."""

import pytest

from tests.helpers import make_config, start_cluster, stop_all, wait_for_stable_leader
from zk.client import ConnectionLossError, ZKClient
from zk.common.errors import BadVersionError, NodeExistsError, NoNodeError


@pytest.fixture
async def cluster(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    leader = await wait_for_stable_leader(servers)
    yield config, servers, leader
    await stop_all(servers)


async def test_full_api_on_leader(cluster):
    config, _, leader = cluster
    async with ZKClient([config.servers[leader.my_id].client_address]) as zk:
        assert await zk.create("/app", b"v0") == "/app"
        assert await zk.exists("/nope") is None

        data, stat = await zk.get_data("/app")
        assert data == b"v0" and stat.version == 0

        stat = await zk.set_data("/app", b"v1", version=0)
        assert stat.version == 1
        with pytest.raises(BadVersionError):
            await zk.set_data("/app", b"v2", version=0)
        # The failed write must not use up a zxid.
        assert (await zk.set_data("/app", b"v2")).mzxid == stat.mzxid + 1
        stat = await zk.set_data("/app", b"v1", version=2)

        seq = await zk.create("/app/job-", b"", sequential=True)
        assert seq == "/app/job-0000000000"
        assert await zk.get_children("/app") == ["job-0000000000"]

        with pytest.raises(NodeExistsError):
            await zk.create("/app")
        with pytest.raises(NoNodeError):
            await zk.get_data("/missing")

        await zk.delete(seq)
        await zk.delete("/app", version=3)
        assert await zk.exists("/app") is None


async def test_write_on_follower_is_redirected_to_leader(cluster):
    config, _, leader = cluster
    follower_id = next(sid for sid in config.servers if sid != leader.my_id)
    async with ZKClient([config.servers[follower_id].client_address]) as zk:
        await zk.create("/redirected", b"x")
        assert zk.address == config.servers[leader.my_id].client_address
        assert (await zk.get_data("/redirected"))[0] == b"x"


async def test_status_reports_roles(cluster):
    config, _, leader = cluster
    async with ZKClient([config.servers[leader.my_id].client_address]) as zk:
        info = await zk.status()
        assert info["state"] == "LEADING" and info["leader_id"] == leader.my_id


async def test_connect_skips_dead_servers(cluster):
    config, _, leader = cluster
    dead = "127.0.0.1:1"  # nothing listens here
    async with ZKClient([dead, config.servers[leader.my_id].client_address], timeout=0.5) as zk:
        assert zk.address != dead


async def test_no_server_reachable():
    with pytest.raises(ConnectionLossError):
        await ZKClient(["127.0.0.1:1"], timeout=0.3).connect()
