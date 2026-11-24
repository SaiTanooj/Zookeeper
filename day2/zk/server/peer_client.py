"""PeerClient: the *outgoing* side of server-to-server calls.

* send / broadcast - fire-and-forget election notifications
* forward_write    - a follower hands a client write to the leader

(The ZAB stream to the leader is opened by zab_follower.py.)
"""

import asyncio
import logging

import grpc

from zk.common.errors import ConnectionLossError, ErrorCode, error_for_code
from zk.proto import zk_pb2_grpc
from zk.server.convert import notification_to_proto
from zk.server.write_op import WriteResult

log = logging.getLogger(__name__)

RPC_TIMEOUT = 0.5      # election notifications
WRITE_TIMEOUT = 3.0    # forwarded writes wait for a commit

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
        """Create the channels. Must run inside the event loop that uses them."""
        for sid, info in self.peers.items():
            channel = grpc.aio.insecure_channel(info.peer_address, options=CHANNEL_OPTIONS)
            self.channels[sid] = channel
            self.stubs[sid] = zk_pb2_grpc.PeerStub(channel)

    # ------------------------------------------------------------ election

    def send(self, peer_id, notification):
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

    # ------------------------------------------------------------ writes

    async def forward_write(self, leader_id, op):
        """Send a WriteOp to the leader; return its WriteResult or raise ZKError."""
        try:
            resp = await self.stubs[leader_id].ForwardWrite(op.to_proto(), timeout=WRITE_TIMEOUT)
        except grpc.aio.AioRpcError as e:
            # We cannot tell whether the leader applied the write.
            raise ConnectionLossError(f"forwarding to S{leader_id} failed: {e.code().name}") from e
        if resp.err != ErrorCode.OK:
            raise error_for_code(resp.err)
        return WriteResult.from_proto(resp)

    async def close(self):
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)  # let them finish first
        for channel in self.channels.values():
            await channel.close()
        self.channels, self.stubs = {}, {}
