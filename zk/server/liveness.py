"""Day 1 liveness checks: notice when the leader (or the quorum) is gone.

After an election, each server runs one of these until it returns, then the
server goes back to LOOKING and runs a new election.

* follower_loop - ping the leader every `ping_interval`. Return when the
                  leader has not confirmed "I am LEADING" for `sync_limit`,
                  or at once if it says somebody else is the leader.
* leader_loop   - count which followers pinged recently. Return (step
                  down) when fewer than a majority, counting itself, have
                  pinged within `sync_limit`.

On Day 2 this module is replaced by ZAB's own PING messages, which travel
over the leader <-> follower stream together with proposals.
"""

import asyncio
import time

from zk.server.vote import ServerState


async def follower_loop(peers, leader_id, ping_interval, sync_limit, is_running):
    last_ok = time.monotonic()
    while is_running():
        await asyncio.sleep(ping_interval)
        answer = await peers.ping(leader_id)
        if answer is not None:
            state, their_leader = answer
            if state == ServerState.LEADING and their_leader == leader_id:
                last_ok = time.monotonic()
            elif state != ServerState.LOOKING:
                return f"S{leader_id} now says the leader is S{their_leader}"
            # LOOKING: the leader may still be finishing its own election
            # (it decides a moment after us), so give it until sync_limit.
        if time.monotonic() - last_ok > sync_limit:
            return "leader stopped answering (or is not leading)"
    return "shutting down"


async def leader_loop(last_ping_times, quorum, ping_interval, sync_limit, is_running):
    """last_ping_times: dict follower id -> time.monotonic() of its last ping."""
    started = time.monotonic()
    while is_running():
        await asyncio.sleep(ping_interval)
        now = time.monotonic()
        if now - started < sync_limit:
            continue  # give followers time to start pinging
        alive = 1 + sum(1 for t in last_ping_times.values() if now - t <= sync_limit)
        if alive < quorum:
            return f"only {alive} server(s) reachable, need {quorum}"
    return "shutting down"
