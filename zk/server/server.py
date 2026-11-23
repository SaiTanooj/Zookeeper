"""ZKServer: wires all the server parts together.

    +----------------------------- ZKServer -----------------------------+
    |  ClientService (gRPC, client_port)    PeerService (gRPC, peer_port) |
    |        |                                     |                      |
    |        v                                     v                      |
    |    DataTree            FastLeaderElection <--+   PeerClient ------> other servers
    |                              ^                                      |
    |   StateStore (epochs)        |  main loop: elect -> lead/follow -> elect ...
    +--------------------------------------------------------------------+

Everything runs on one asyncio event loop, so only one piece of code touches
the tree or the election state at a time - no locks needed.
"""

import asyncio
import logging
import os
import time

import grpc

from zk.common.zxid import make_zxid, counter_of, epoch_of
from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server import liveness
from zk.server.client_service import ClientService
from zk.server.datatree import DataTree
from zk.server.election import FastLeaderElection
from zk.server.peer_client import PeerClient
from zk.server.peer_service import PeerService
from zk.server.state_store import StateStore
from zk.server.vote import ServerState

log = logging.getLogger(__name__)


class ZKServer:
    def __init__(self, my_id, config, data_dir, initial_zxid=0):
        """initial_zxid: Day 1 stand-in for "last zxid in my transaction log".

        On Day 2 the real value is read from the log on disk. Tests use this
        to check that the server with the newest history wins the election.
        """
        self.my_id = my_id
        self.config = config
        self.info = config.servers[my_id]
        self.timing = config.timing

        self.state_store = StateStore(os.path.join(data_dir, f"server{my_id}"))
        self.tree = DataTree()
        self.last_zxid = initial_zxid

        self.peers = PeerClient(my_id, config.peers_of(my_id))
        self.election = FastLeaderElection(
            my_id=my_id,
            server_ids=config.servers.keys(),
            transport=self.peers,
            get_last_zxid=lambda: self.last_zxid,
            get_epoch=lambda: self.state_store.current_epoch,
            finalize_wait=self.timing.finalize_wait_ms / 1000,
            max_notification_interval=self.timing.max_notification_interval_ms / 1000,
        )
        self.last_ping_times = {}  # used while LEADING: follower id -> time

        self._running = False
        self._grpc_servers = []
        self._main_task = None

    # ------------------------------------------------------------ start / stop

    async def start(self):
        """Open both ports and start the election loop in the background."""
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
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        for s in self._grpc_servers:
            await s.stop(grace=None)
        await self.peers.close()
        log.info("S%d stopped", self.my_id)

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
                reason = await self._lead()
            else:
                reason = await self._follow(vote.leader)
            log.warning("S%d leaving %s: %s", self.my_id, self.election.state.name, reason)

    async def _lead(self):
        log.info("S%d *** I am the LEADER ***", self.my_id)
        self.last_ping_times = {}
        return await liveness.leader_loop(
            self.last_ping_times, self.config.quorum_size,
            self.timing.ping_interval_ms / 1000, self.timing.sync_limit_ms / 1000,
            lambda: self._running,
        )

    async def _follow(self, leader_id):
        log.info("S%d following leader S%d", self.my_id, leader_id)
        return await liveness.follower_loop(
            self.peers, leader_id,
            self.timing.ping_interval_ms / 1000, self.timing.sync_limit_ms / 1000,
            lambda: self._running,
        )

    # ------------------------------------------------------------ called by services

    def on_ping(self, sender):
        """A follower pinged us. Remember it (if we lead) and say who leads."""
        if self.election.state == ServerState.LEADING:
            self.last_ping_times[sender] = time.monotonic()
        leader = self.election.vote.leader if self.election.state != ServerState.LOOKING else 0
        return self.election.state, leader

    def next_zxid(self):
        """zxid the next write will use: same epoch, counter + 1.

        It does not change last_zxid; the caller does that once the write
        has succeeded.
        """
        epoch = self.state_store.current_epoch
        counter = counter_of(self.last_zxid) + 1 if epoch_of(self.last_zxid) == epoch else 1
        return make_zxid(epoch, counter)

    def status(self):
        state = self.election.state
        leader_id = self.election.vote.leader if state != ServerState.LOOKING else 0
        leader_addr = self.config.servers[leader_id].client_address if leader_id else ""
        return zk_pb2.StatusResponse(
            server_id=self.my_id,
            state=state.name,
            leader_id=leader_id,
            leader_address=leader_addr,
            last_zxid=self.last_zxid,
            current_epoch=self.state_store.current_epoch,
            election_round=self.election.round,
            node_count=len(self.tree),
        )
