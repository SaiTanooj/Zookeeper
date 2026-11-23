"""PeerClient: the *outgoing* side of server-to-server traffic.

It keeps one gRPC channel per peer and offers:
* send / broadcast  - fire-and-forget election notifications
* ping              - Day 1 liveness check, returns None if the peer is down

Fire-and-forget is fine for FLE: a lost notification is simply re-sent on
the next timeout, so the election never depends on one message arriving.
"""

import asyncio
import logging

import grpc

from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server.convert import notification_to_proto
from zk.server.vote import ServerState

log = logging.getLogger(__name__)

RPC_TIMEOUT = 0.5  # seconds; a peer that is slower than this counts as down

# By default gRPC waits ~1 s before retrying a peer that was down. That is
# longer than a whole election, so we retry much sooner.
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


class PeerClient:
    def __init__(self, my_id, peers):
        """peers: dict of server id -> ServerInfo (not including me)."""
        self.my_id = my_id
        self.peers = peers
        self.channels = {}
        self.stubs = {}
        self._tasks = set()  # keep references so tasks are not garbage collected

    def open(self):
        """Create the channels. Must run inside the event loop that uses them
        (gRPC channels belong to the loop they were created in)."""
        for sid, info in self.peers.items():
            channel = grpc.aio.insecure_channel(info.peer_address, options=CHANNEL_OPTIONS)
            self.channels[sid] = channel
            self.stubs[sid] = zk_pb2_grpc.PeerStub(channel)

    # ------------------------------------------------------------ election

    def send(self, peer_id, notification):
        """Send one notification in the background."""
        if peer_id == self.my_id or peer_id not in self.stubs:
            return
        task = asyncio.create_task(self._send(peer_id, notification))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def broadcast(self, notification):
        for peer_id in self.stubs:
            self.send(peer_id, notification)

    async def _send(self, peer_id, notification):
        try:
            await self.stubs[peer_id].SendNotification(
                notification_to_proto(notification), timeout=RPC_TIMEOUT
            )
        except grpc.aio.AioRpcError:
            pass  # peer is down; FLE will try again later

    # ------------------------------------------------------------ liveness

    async def ping(self, peer_id):
        """Return (state, leader_id) of the peer, or None if it did not answer."""
        try:
            resp = await self.stubs[peer_id].Ping(
                zk_pb2.PingRequest(sender=self.my_id), timeout=RPC_TIMEOUT
            )
            return ServerState(resp.state), resp.leader
        except grpc.aio.AioRpcError:
            return None

    async def close(self):
        for task in list(self._tasks):
            task.cancel()
        for channel in self.channels.values():
            await channel.close()
        self.channels, self.stubs = {}, {}
