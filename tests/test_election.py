"""Fast Leader Election with real gRPC servers on localhost."""

import asyncio

import pytest

from tests.helpers import (
    leaders, make_config, start_cluster, stop_all, wait_for_stable_leader,
)
from zk.server.server import ZKServer
from zk.server.vote import ServerState


@pytest.mark.parametrize("n", [1, 3, 5])
async def test_cluster_elects_exactly_one_leader(tmp_path, n):
    servers = await start_cluster(make_config(n), tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        # Usually the highest id wins (the tie-breaker), but FLE only promises
        # that a majority agrees: if a quorum decides before a slow server's
        # vote arrives, a lower id may win. Both are correct.
        assert leader.my_id in servers
        assert len(leaders(servers)) == 1
    finally:
        await stop_all(servers)


async def test_server_with_newest_history_wins(tmp_path):
    # Server 1 has seen more writes than 2 and 3, so it must lead.
    servers = await start_cluster(make_config(3), tmp_path, zxids={1: 50, 2: 10, 3: 10})
    try:
        assert (await wait_for_stable_leader(servers)).my_id == 1
    finally:
        await stop_all(servers)


async def test_new_leader_after_leader_crash(tmp_path):
    servers = await start_cluster(make_config(3), tmp_path)
    try:
        old = await wait_for_stable_leader(servers)
        await old.stop()
        del servers[old.my_id]

        loop = asyncio.get_running_loop()
        t0 = loop.time()
        new = await wait_for_stable_leader(servers, timeout=3)
        took = loop.time() - t0
        assert new.my_id != old.my_id
        assert took < 1.5, f"failover took {took:.2f}s"
    finally:
        await stop_all(servers)


async def test_restarted_server_rejoins_as_follower(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        follower_id = next(sid for sid in servers if sid != leader.my_id)
        await servers[follower_id].stop()

        servers[follower_id] = ZKServer(follower_id, config, str(tmp_path))
        await servers[follower_id].start()

        again = await wait_for_stable_leader(servers)
        assert again.my_id == leader.my_id  # the existing leader is kept
    finally:
        await stop_all(servers)


async def test_minority_cannot_lead(tmp_path):
    servers = await start_cluster(make_config(3), tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        # Kill both followers: the leader alone is not a majority.
        for sid in [sid for sid in servers if sid != leader.my_id]:
            await servers.pop(sid).stop()
        await asyncio.sleep(1.5)
        assert leader.election.state == ServerState.LOOKING
        assert leaders(servers) == []
    finally:
        await stop_all(servers)
