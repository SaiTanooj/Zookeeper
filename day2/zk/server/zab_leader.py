"""The leader side of ZAB (ZooKeeper Atomic Broadcast).

After Fast Leader Election picks this server, Leader.run() goes through
the three ZAB phases:

1. Discovery
   Every follower opens a stream and sends FOLLOWERINFO with its accepted
   epoch. Once a majority (counting the leader) has reported, the new epoch
   is max(accepted epochs) + 1. Each follower gets NEWEPOCH and answers
   ACKEPOCH. A new epoch makes every old leader's messages easy to reject.

2. Synchronization
   The leader sends each follower a SNAP (a full copy of its committed
   data), any proposals still in flight, then NEWLEADER. The follower
   answers ACK_NEWLEADER. When a majority has done this, the leader is
   "established": it sends UPTODATE and starts accepting client writes.
   Fast Leader Election always picks the server with the newest history,
   so the leader never has to fetch anything from followers.

3. Broadcast
   For every write: turn it into a Txn, log it, send PROPOSAL to all
   followers. Each follower logs it and replies ACK. When a majority
   (counting the leader) has ACKed, the leader applies it and sends COMMIT.
   Commits always happen in zxid order.
   The leader also PINGs followers; if it cannot hear from a majority for
   `sync_limit`, it steps down and a new election starts.
"""

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import grpc

from zk.common.errors import ConnectionLossError, NotReadyError
from zk.common.zxid import zxid_str
from zk.server import txn as T
from zk.server import zab_messages as zm
from zk.server.processor import RequestProcessor
from zk.server.write_op import WriteResult

log = logging.getLogger(__name__)


@dataclass
class Proposal:
    txn: object
    acks: set
    future: asyncio.Future = field(repr=False)


class FollowerHandler:
    """The leader's end of the stream to one follower.

    Messages to the follower go through a queue, so the leader can "send"
    without waiting. A reader task passes every incoming message to the
    leader. When either side stops, the stream is closed.
    """

    def __init__(self, leader, context):
        self.leader = leader
        self.context = context
        self.sid = None
        self.queue = asyncio.Queue()
        self.synced = False  # True after ACK_NEWLEADER
        self.last_heard = time.monotonic()

    def send(self, msg):
        self.queue.put_nowait(msg)

    def close(self):
        self.queue.put_nowait(None)

    async def run(self):
        reader = asyncio.create_task(self._read_loop())
        try:
            while True:
                msg = await self.queue.get()
                if msg is None:
                    break
                await self.context.write(msg)
        finally:
            reader.cancel()
            self.leader.remove_follower(self)

    async def _read_loop(self):
        try:
            while True:
                msg = await self.context.read()
                if msg is grpc.aio.EOF:
                    break
                self.last_heard = time.monotonic()
                await self.leader.on_message(self, msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # the follower went away
            log.debug("stream from S%s ended: %s", self.sid, e)
        finally:
            self.close()


class Leader:
    def __init__(self, server):
        self.server = server
        self.db = server.db
        self.store = server.state_store
        self.my_id = server.my_id
        self.quorum = server.config.quorum_size
        self.timing = server.timing

        self.handlers = set()      # every connected follower
        self.forwarding = set()    # followers that receive proposals and commits

        # Discovery
        self.follower_epochs = {self.my_id: self.store.accepted_epoch}
        self.epoch = None
        self.epoch_ready = asyncio.Event()
        # Synchronization
        self.newleader_acks = {self.my_id}
        self.established = False
        self.established_event = asyncio.Event()
        # Broadcast
        self.processor = None
        self.outstanding = OrderedDict()  # zxid -> Proposal, in zxid order
        self.closed = False

    # ------------------------------------------------------------ main

    async def run(self):
        """Lead until something goes wrong; return the reason."""
        log.info("S%d *** LEADING: waiting for a majority of followers ***", self.my_id)
        self.db.commit_all_pending()  # our whole log is now committed history
        self._maybe_decide_epoch()
        init_limit = self.timing.init_limit_ms / 1000
        try:
            try:
                await asyncio.wait_for(self.epoch_ready.wait(), init_limit)
            except asyncio.TimeoutError:
                return "discovery: a majority of followers did not join in time"
            self._maybe_establish()
            try:
                await asyncio.wait_for(self.established_event.wait(), init_limit)
            except asyncio.TimeoutError:
                return "sync: a majority of followers did not sync in time"
            log.info("S%d established as leader of epoch %d", self.my_id, self.epoch)
            return await self._heartbeat_loop()
        finally:
            self._shutdown()

    async def _heartbeat_loop(self):
        ping_interval = self.timing.ping_interval_ms / 1000
        sync_limit = self.timing.sync_limit_ms / 1000
        while not self.closed:
            await asyncio.sleep(ping_interval)
            for h in self.forwarding:
                h.send(zm.make(zm.PING, self.my_id, self.epoch))
            now = time.monotonic()
            alive = 1 + sum(1 for h in self.forwarding
                            if h.synced and now - h.last_heard <= sync_limit)
            if alive < self.quorum:
                return f"lost the majority ({alive} of {self.quorum} needed)"
        return "closed"

    def _shutdown(self):
        self.closed = True
        self.server.serving = False
        for p in self.outstanding.values():
            if not p.future.done():
                p.future.set_exception(ConnectionLossError("leader stepped down during the write"))
        self.outstanding.clear()
        for h in list(self.handlers):
            h.close()

    # ------------------------------------------------------------ followers

    def add_follower(self, handler):
        self.handlers.add(handler)

    def remove_follower(self, handler):
        self.handlers.discard(handler)
        self.forwarding.discard(handler)

    async def on_message(self, h, msg):
        """Handle one message from a follower."""
        if msg.type == zm.FOLLOWERINFO:
            h.sid = msg.sid
            self.follower_epochs[msg.sid] = msg.epoch
            self._maybe_decide_epoch()
            await self.epoch_ready.wait()
            h.send(zm.make(zm.NEWEPOCH, self.my_id, self.epoch))

        elif msg.type == zm.ACKEPOCH:
            self._start_sync(h)

        elif msg.type == zm.ACK_NEWLEADER:
            h.synced = True
            self.newleader_acks.add(h.sid)
            if self.established:
                h.send(zm.make(zm.UPTODATE, self.my_id, self.epoch))
            else:
                self._maybe_establish()

        elif msg.type == zm.ACK:
            proposal = self.outstanding.get(msg.zxid)
            if proposal:
                proposal.acks.add(h.sid)
                self._commit_ready_proposals()

        # PING replies need no handling: reading any message updates last_heard.

    def _maybe_decide_epoch(self):
        if self.epoch is None and len(self.follower_epochs) >= self.quorum:
            self.epoch = max(self.follower_epochs.values()) + 1
            self.store.set_accepted_epoch(self.epoch)
            self.store.set_current_epoch(self.epoch)
            self.processor = RequestProcessor(self.db.tree, self.db.sessions, self.epoch)
            log.info("S%d new epoch %d", self.my_id, self.epoch)
            self.epoch_ready.set()

    def _start_sync(self, h):
        """Send SNAP + in-flight proposals + NEWLEADER, then forward new traffic.

        This runs without any `await`, so no proposal or commit can slip in
        between the snapshot and the moment the follower joins `forwarding`.
        """
        h.send(zm.make(zm.SNAP, self.my_id, self.epoch, self.db.last_applied,
                       self.db.snapshot_bytes()))
        for p in self.outstanding.values():
            h.send(zm.make(zm.PROPOSAL, self.my_id, self.epoch, p.txn.zxid, p.txn.to_json()))
        h.send(zm.make(zm.NEWLEADER, self.my_id, self.epoch))
        self.forwarding.add(h)
        log.info("S%d syncing S%s with a snapshot at %s", self.my_id, h.sid,
                 zxid_str(self.db.last_applied))

    def _maybe_establish(self):
        if not self.established and self.epoch is not None and len(self.newleader_acks) >= self.quorum:
            self.established = True
            self.server.serving = True
            for h in self.forwarding:
                if h.synced:
                    h.send(zm.make(zm.UPTODATE, self.my_id, self.epoch))
            self.established_event.set()

    # ------------------------------------------------------------ writes

    async def propose(self, op):
        """Replicate one client write; return its WriteResult once committed."""
        if self.closed or not self.established:
            raise NotReadyError("leader is not ready")
        txn = self.processor.prepare(op)  # raises ZKError if the write is invalid
        self.db.log_txn(txn)              # the leader logs it too (its own ACK)
        future = asyncio.get_running_loop().create_future()
        self.outstanding[txn.zxid] = Proposal(txn, {self.my_id}, future)
        payload = txn.to_json()
        for h in self.forwarding:
            h.send(zm.make(zm.PROPOSAL, self.my_id, self.epoch, txn.zxid, payload))
        self._commit_ready_proposals()  # a 1-server cluster commits at once
        return await future

    def _commit_ready_proposals(self):
        """Commit proposals from the oldest one, while each has a majority."""
        while self.outstanding:
            zxid, proposal = next(iter(self.outstanding.items()))
            if len(proposal.acks) < self.quorum:
                break
            del self.outstanding[zxid]
            result = self.db.commit(zxid)
            for h in self.forwarding:
                h.send(zm.make(zm.COMMIT, self.my_id, self.epoch, zxid))
            if not proposal.future.done():
                proposal.future.set_result(_make_result(proposal.txn, result))


def _make_result(txn, result):
    if txn.type == T.CREATE:
        return WriteResult(zxid=txn.zxid, path=result)
    if txn.type == T.SET_DATA:
        return WriteResult(zxid=txn.zxid, stat=result)
    if txn.type == T.CREATE_SESSION:
        return WriteResult(zxid=txn.zxid, session_id=result)
    return WriteResult(zxid=txn.zxid)
