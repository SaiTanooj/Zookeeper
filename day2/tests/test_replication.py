"""ZAB replication end to end: writes reach every server and survive failures."""

import asyncio

import pytest

from tests.helpers import (
    addresses, make_config, restart, start_cluster, stop_all, wait_for_stable_leader, wait_until,
)
from zk.client import ZKClient
from zk.common.errors import NotReadyError
from zk.common.zxid import epoch_of


def follower_ids(servers, leader):
    return [sid for sid in servers if sid != leader.my_id]


async def test_write_on_leader_reaches_every_server(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        async with ZKClient(addresses(config, leader.my_id)) as zk:
            await zk.create("/app", b"hello")
            zxid = zk.last_zxid
        await wait_until(lambda: all(s.db.last_applied >= zxid for s in servers.values()))
        for s in servers.values():
            assert s.db.tree.get_data("/app")[0] == b"hello"
    finally:
        await stop_all(servers)


async def test_write_on_follower_is_forwarded_and_readable_there(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        fid = follower_ids(servers, leader)[0]
        async with ZKClient(addresses(config, fid)) as zk:
            await zk.create("/via-follower", b"x")
            # Read your own write on the same follower, straight away.
            assert (await zk.get_data("/via-follower"))[0] == b"x"
            assert zk.address == config.servers[fid].client_address  # never redirected
        assert leader.db.tree.exists("/via-follower") is not None
    finally:
        await stop_all(servers)


async def test_data_survives_leader_crash(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        async with ZKClient(addresses(config, leader.my_id)) as zk:
            for i in range(10):
                await zk.create(f"/item{i}", str(i).encode())
        await leader.stop()
        del servers[leader.my_id]

        new_leader = await wait_for_stable_leader(servers)
        async with ZKClient(addresses(config, *servers)) as zk:
            assert len(await zk.get_children("/")) == 10
            await zk.create("/after-crash")
        # New writes carry the new epoch.
        assert epoch_of(new_leader.db.last_applied) == new_leader.state_store.current_epoch
    finally:
        await stop_all(servers)


async def test_restarted_follower_catches_up(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        fid = follower_ids(servers, leader)[0]
        await servers.pop(fid).stop()

        async with ZKClient(addresses(config, leader.my_id)) as zk:
            await zk.create("/written-while-down", b"!")

        await restart(servers, fid, config, tmp_path)
        await wait_for_stable_leader(servers)
        assert servers[fid].db.tree.get_data("/written-while-down")[0] == b"!"
    finally:
        await stop_all(servers)


async def test_whole_cluster_restart_keeps_data(tmp_path):
    config = make_config(3, snapshot_every=5)  # also exercises snapshots
    servers = await start_cluster(config, tmp_path)
    try:
        await wait_for_stable_leader(servers)
        async with ZKClient(addresses(config)) as zk:
            for i in range(12):
                await zk.create(f"/n{i}")
            await zk.set_data("/n0", b"final")
        await stop_all(servers)

        servers = await start_cluster(config, tmp_path)
        await wait_for_stable_leader(servers)
        async with ZKClient(addresses(config)) as zk:
            assert len(await zk.get_children("/")) == 12
            data, stat = await zk.get_data("/n0")
            assert data == b"final" and stat.version == 1
    finally:
        await stop_all(servers)


async def test_no_writes_without_a_majority(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        survivor = follower_ids(servers, leader)[0]
        async with ZKClient(addresses(config, survivor)) as zk:
            for sid in [sid for sid in servers if sid != survivor]:
                await servers.pop(sid).stop()
            await asyncio.sleep(1.0)
            with pytest.raises(NotReadyError):
                await zk.create("/should-fail")
    finally:
        await stop_all(servers)


async def test_many_concurrent_writes_commit_in_order(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        clients = [ZKClient(addresses(config, sid)) for sid in servers]
        for c in clients:
            await c.connect()
        await clients[0].create("/q")
        results = await asyncio.gather(*(
            c.create("/q/job-", sequential=True) for c in clients for _ in range(5)
        ))
        assert len(set(results)) == 15  # every create got its own number
        for c in clients:
            await c.close()
        await wait_until(lambda: all(len(s.db.tree.get_children("/q")) == 15
                                     for s in servers.values()))
        trees = [s.db.tree.get_children("/q") for s in servers.values()]
        assert trees[0] == trees[1] == trees[2]
    finally:
        await stop_all(servers)
