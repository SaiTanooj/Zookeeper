"""Leader election + ZAB start-up with real gRPC servers on localhost."""

import asyncio

import pytest

from tests.helpers import (
    leaders, make_config, restart, seed_history, start_cluster, stop_all,
    wait_for_stable_leader,
)
from zk.server.vote import ServerState


@pytest.mark.parametrize("n", [1, 3, 5])
async def test_cluster_elects_one_leader_and_all_serve(tmp_path, n):
    servers = await start_cluster(make_config(n), tmp_path)
    try:
        await wait_for_stable_leader(servers)
        assert len(leaders(servers)) == 1
    finally:
        await stop_all(servers)


async def test_server_with_newest_history_wins(tmp_path):
    config = make_config(3)
    seed_history(tmp_path, 1, count=5)   # server 1 has seen 5 writes
    seed_history(tmp_path, 2, count=2)
    seed_history(tmp_path, 3, count=2)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        assert leader.my_id == 1
        # Followers were synced to the leader's history.
        for s in servers.values():
            assert s.db.tree.exists("/seed5") is not None
    finally:
        await stop_all(servers)


async def test_every_new_leader_starts_a_new_epoch(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        first = await wait_for_stable_leader(servers)
        epoch1 = first.state_store.current_epoch
        assert epoch1 >= 1
        await first.stop()
        del servers[first.my_id]
        second = await wait_for_stable_leader(servers)
        assert second.state_store.current_epoch > epoch1
        assert all(s.state_store.current_epoch == second.state_store.current_epoch
                   for s in servers.values())
    finally:
        await stop_all(servers)


async def test_new_leader_quickly_after_crash(tmp_path):
    servers = await start_cluster(make_config(3), tmp_path)
    try:
        old = await wait_for_stable_leader(servers)
        await old.stop()
        del servers[old.my_id]
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        new = await wait_for_stable_leader(servers)
        took = loop.time() - t0
        assert new.my_id != old.my_id
        assert took < 2.0, f"failover took {took:.2f}s"
    finally:
        await stop_all(servers)


async def test_restarted_server_rejoins(tmp_path):
    config = make_config(3)
    servers = await start_cluster(config, tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        follower_id = next(sid for sid in servers if sid != leader.my_id)
        await restart(servers, follower_id, config, tmp_path)
        again = await wait_for_stable_leader(servers)
        assert again.my_id == leader.my_id
    finally:
        await stop_all(servers)


async def test_minority_cannot_lead(tmp_path):
    servers = await start_cluster(make_config(3), tmp_path)
    try:
        leader = await wait_for_stable_leader(servers)
        for sid in [sid for sid in servers if sid != leader.my_id]:
            await servers.pop(sid).stop()
        await asyncio.sleep(1.5)
        assert not leader.serving
        assert leader.election.state in (ServerState.LOOKING, ServerState.LEADING)
        assert leader.leader is None or not leader.leader.established
    finally:
        await stop_all(servers)
