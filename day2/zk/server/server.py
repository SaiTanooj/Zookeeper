"""ZKServer: wires all the server parts together (Day 2).

    +------------------------------ ZKServer -------------------------------+
    |  ClientService (client_port)              PeerService (peer_port)      |
    |        |  reads: db.tree                     |  notifications, ZAB      |
    |        |  writes: submit_write()             |  stream, forwarded writes|
    |        v                                     v                          |
    |    ZKDatabase  <---- Leader (if LEADING) / Follower (if FOLLOWING)      |
    |  tree + sessions          ^                                             |
    |  txn log + snapshots      |    main loop: elect -> lead/follow -> elect |
    |                    FastLeaderElection ----> PeerClient ----> other servers
    +------------------------------------------------------------------------+

Everything runs on one asyncio event loop: no threads, no locks.
"""

import asyncio
import logging
import os

import grpc

from zk.common.errors import ConnectionLossError, NotReadyError
from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server.client_service import ClientService
from zk.server.database import ZKDatabase
from zk.server.election import FastLeaderElection
from zk.server.peer_client import PeerClient
from zk.server.peer_service import PeerService
from zk.server.session_tracker import SessionTracker
from zk.server.state_store import StateStore
from zk.server.vote import ServerState
from zk.server.zab_follower import Follower
from zk.server.zab_leader import Leader

log = logging.getLogger(__name__)

APPLY_WAIT = 3.0  # seconds a follower waits for a forwarded write's COMMIT


class ZKServer:
    def __init__(self, my_id, config, data_dir):
        self.my_id = my_id
        self.config = config
        self.info = config.servers[my_id]
        self.timing = config.timing

        my_dir = os.path.join(data_dir, f"server{my_id}")
        self.state_store = StateStore(my_dir)
        self.db = ZKDatabase(my_dir, snapshot_every=self.timing.snapshot_every)
        self.db.recover()
        self.session_tracker = SessionTracker()

        self.peers = PeerClient(my_id, config.peers_of(my_id))
        self.election = FastLeaderElection(
            my_id=my_id,
            server_ids=config.servers.keys(),
            transport=self.peers,
            get_last_zxid=lambda: self.db.last_logged,
            get_epoch=lambda: self.state_store.current_epoch,
            finalize_wait=self.timing.finalize_wait_ms / 1000,
            max_notification_interval=self.timing.max_notification_interval_ms / 1000,
        )
        self.leader = None     # the Leader object while LEADING
        self.serving = False   # True once synced with an active leader

        self._running = False
        self._grpc_servers = []
        self._main_task = None

    # ------------------------------------------------------------ start / stop

    async def start(self):
        self._running = True
        self.peers.open()
        self._grpc_servers = [
            self._make_grpc_server(
                self.info.client_port,
                lambda s: zk_pb2_grpc.add_ZooKeeperServicer_to_server(ClientService(self), s),
            ),
            self._make_grpc_server(
                self.info.peer_port,
                lambda s: zk_pb2_grpc.add_PeerServicer_to_server(PeerService(self), s),
            ),
        ]
        for s in self._grpc_servers:
            await s.start()
        log.info("S%d listening: clients on %s, peers on %s",
                 self.my_id, self.info.client_address, self.info.peer_address)
        self._main_task = asyncio.create_task(self._main_loop())

    async def stop(self):
        self._running = False
        if self._main_task:
            self._main_task.cancel()
            await self._step("main loop", asyncio.gather(self._main_task, return_exceptions=True))
        # A short grace period lets open streams (ZAB) finish cleanly.
        for s in self._grpc_servers:
            await self._step("grpc server", s.stop(grace=0.5))
        await self._step("peer channels", self.peers.close())
        self.db.close()
        log.info("S%d stopped", self.my_id)

    async def _step(self, what, awaitable, limit=3.0):
        """Run one shutdown step, but never let it hang the whole stop()."""
        try:
            await asyncio.wait_for(awaitable, limit)
        except asyncio.TimeoutError:
            log.warning("S%d: stopping %s took longer than %.0fs, moving on", self.my_id, what, limit)

    async def serve_forever(self):
        await self.start()
        try:
            await self._main_task
        finally:
            await self.stop()

    def _make_grpc_server(self, port, register):
        server = grpc.aio.server()
        register(server)
        server.add_insecure_port(f"{self.info.host}:{port}")
        return server

    # ------------------------------------------------------------ main loop

    async def _main_loop(self):
        """elect -> lead or follow until something breaks -> elect again."""
        while self._running:
            vote = await self.election.look_for_leader()
            if self.election.state == ServerState.LEADING:
                self.leader = Leader(self)
                try:
                    reason = await self.leader.run()
                finally:
                    self.leader = None
            else:
                reason = await Follower(self, vote.leader).run()
            self.serving = False
            log.warning("S%d leaving %s: %s", self.my_id, self.election.state.name, reason)

    # ------------------------------------------------------------ writes

    async def submit_write(self, op):
        """Replicate a client write and return its WriteResult.

        Leader: propose it with ZAB.
        Follower: forward it to the leader, then wait until the COMMIT has
        reached *this* server too, so the client's next read here sees it.
        """
        if self.leader is not None:
            return await self.leader.propose(op)
        if self.election.state == ServerState.FOLLOWING and self.serving:
            result = await self.peers.forward_write(self.election.vote.leader, op)
            try:
                await self.db.wait_until_applied(result.zxid, APPLY_WAIT)
            except asyncio.TimeoutError:
                raise ConnectionLossError("write committed but not yet seen here")
            return result
        raise NotReadyError("no active leader")

    # ------------------------------------------------------------ status

    def status(self):
        state = self.election.state
        leader_id = self.election.vote.leader if state != ServerState.LOOKING else 0
        return zk_pb2.StatusResponse(
            server_id=self.my_id,
            state=state.name,
            serving=self.serving,
            leader_id=leader_id,
            current_epoch=self.state_store.current_epoch,
            last_logged_zxid=self.db.last_logged,
            last_applied_zxid=self.db.last_applied,
            election_round=self.election.round,
            node_count=len(self.db.tree),
            session_count=len(self.db.sessions),
        )
