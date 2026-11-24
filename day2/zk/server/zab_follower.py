"""The follower side of ZAB.

Follower.run() opens one stream to the leader and reacts to its messages:

    send FOLLOWERINFO(my accepted epoch, my last zxid)
    NEWEPOCH(e)   -> refuse if e is older than an epoch I already accepted,
                     else save accepted_epoch = e, reply ACKEPOCH
    SNAP          -> replace my data with the leader's snapshot
    PROPOSAL(txn) -> write txn to my log (fsync), reply ACK
    NEWLEADER(e)  -> save current_epoch = e, reply ACK_NEWLEADER
    UPTODATE      -> start serving clients
    COMMIT(zxid)  -> apply logged txns up to zxid to my tree
    PING          -> reply PING

If the leader is silent for `sync_limit` (or the stream breaks), run()
returns and the server starts a new election.
"""

import asyncio
import logging
import time

import grpc

from zk.proto import zk_pb2_grpc
from zk.server import zab_messages as zm
from zk.server.peer_client import CHANNEL_OPTIONS
from zk.server.txn import Txn

log = logging.getLogger(__name__)


class Follower:
    def __init__(self, server, leader_id):
        self.server = server
        self.db = server.db
        self.store = server.state_store
        self.my_id = server.my_id
        self.leader_id = leader_id
        self.leader_address = server.config.servers[leader_id].peer_address
        self.timing = server.timing
        self.uptodate = False

    async def run(self):
        """Follow the leader until something goes wrong; return the reason."""
        log.info("S%d following S%d", self.my_id, self.leader_id)
        deadline = time.monotonic() + self.timing.init_limit_ms / 1000
        try:
            # The leader may still be finishing its election: retry for a while.
            while time.monotonic() < deadline:
                reason = await self._follow_once()
                if reason:
                    return reason
                await asyncio.sleep(0.1)
            return f"could not connect to leader S{self.leader_id}"
        finally:
            self.server.serving = False

    async def _follow_once(self):
        """One stream to the leader. Returns None if it never got going."""
        started = False
        channel = grpc.aio.insecure_channel(self.leader_address, options=CHANNEL_OPTIONS)
        call = zk_pb2_grpc.PeerStub(channel).Follow()
        try:
            await call.write(zm.make(zm.FOLLOWERINFO, self.my_id,
                                     self.store.accepted_epoch, self.db.last_logged))
            while True:
                limit = self.timing.sync_limit_ms if self.uptodate else self.timing.init_limit_ms
                msg = await asyncio.wait_for(call.read(), limit / 1000)
                if msg is grpc.aio.EOF:
                    return "leader closed the connection" if started else None
                started = True
                reply, stop_reason = self._handle(msg)
                if stop_reason:
                    return stop_reason
                if reply is not None:
                    await call.write(reply)
        except asyncio.TimeoutError:
            return "leader went silent"
        except grpc.aio.AioRpcError:
            return "connection to leader lost" if started else None
        finally:
            call.cancel()
            await channel.close()

    def _handle(self, msg):
        """Return (reply message or None, reason to stop or None)."""
        t = msg.type
        if t == zm.NEWEPOCH:
            if msg.epoch < self.store.accepted_epoch:
                return None, f"leader offered old epoch {msg.epoch}"
            self.store.set_accepted_epoch(msg.epoch)
            return zm.make(zm.ACKEPOCH, self.my_id, self.store.current_epoch, self.db.last_logged), None

        if t == zm.SNAP:
            self.db.install_snapshot(msg.payload)
            return None, None

        if t == zm.PROPOSAL:
            txn = Txn.from_json(msg.payload)
            self.db.log_txn(txn)
            return zm.make(zm.ACK, self.my_id, msg.epoch, txn.zxid), None

        if t == zm.NEWLEADER:
            self.store.set_current_epoch(msg.epoch)
            return zm.make(zm.ACK_NEWLEADER, self.my_id, msg.epoch), None

        if t == zm.UPTODATE:
            self.uptodate = True
            self.server.serving = True
            log.info("S%d synced with leader S%d, serving clients", self.my_id, self.leader_id)
            return None, None

        if t == zm.COMMIT:
            self.db.commit(msg.zxid)
            return None, None

        if t == zm.PING:
            return zm.make(zm.PING, self.my_id, msg.epoch), None

        log.warning("S%d ignoring unexpected %s", self.my_id, zm.name(msg))
        return None, None
