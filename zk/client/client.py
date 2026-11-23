"""ZKClient: the library applications use to talk to the cluster.

Usage:

    client = ZKClient(["127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"])
    await client.connect()
    await client.create("/app", b"hello")
    data, stat = await client.get_data("/app")
    await client.close()

Day 1 features:
* connects to any live server from the list (tried in random order)
* the full read/write API with version checks
* errors come back as Python exceptions (NoNodeError, BadVersionError, ...)
* if a write reaches a follower (NOT_LEADER), the client asks that server
  where the leader is, reconnects there and retries once

Coming later: sessions + heartbeats (Day 2), watches and failover that keeps
the session (Day 3). On Day 2 followers forward writes, so the NOT_LEADER
redirect below can be deleted.
"""

import random

import grpc

from zk.common.errors import ErrorCode, NotLeaderError, ZKError, error_for_code
from zk.common.stat import Stat
from zk.proto import zk_pb2, zk_pb2_grpc

ANY_VERSION = -1


class ConnectionLossError(ZKError):
    """No server in the list could be reached."""


class ZKClient:
    def __init__(self, servers, timeout=2.0):
        self.servers = list(servers)  # ["host:port", ...]
        self.timeout = timeout
        self.address = None           # server we are connected to
        self._channel = None
        self._stub = None

    # ------------------------------------------------------------ connection

    async def connect(self):
        """Connect to the first server (in random order) that answers."""
        for address in random.sample(self.servers, len(self.servers)):
            if await self._connect_to(address):
                return address
        raise ConnectionLossError(f"no server reachable in {self.servers}")

    async def _connect_to(self, address):
        await self.close()
        channel = grpc.aio.insecure_channel(address)
        stub = zk_pb2_grpc.ZooKeeperStub(channel)
        try:
            await stub.GetStatus(zk_pb2.Empty(), timeout=self.timeout)
        except grpc.aio.AioRpcError:
            await channel.close()
            return False
        self._channel, self._stub, self.address = channel, stub, address
        return True

    async def close(self):
        if self._channel is not None:
            await self._channel.close()
        self._channel = self._stub = self.address = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    # ------------------------------------------------------------ API: writes

    async def create(self, path, data=b"", ephemeral=False, sequential=False):
        """Create a znode; returns the real path (with counter if sequential)."""
        req = zk_pb2.CreateRequest(path=path, data=data, ephemeral=ephemeral, sequential=sequential)
        resp = await self._write("Create", req)
        return resp.path

    async def delete(self, path, version=ANY_VERSION):
        await self._write("Delete", zk_pb2.DeleteRequest(path=path, version=version))

    async def set_data(self, path, data, version=ANY_VERSION):
        """Replace a node's data if `version` matches; returns the new Stat."""
        req = zk_pb2.SetDataRequest(path=path, data=data, version=version)
        resp = await self._write("SetData", req)
        return Stat.from_proto(resp.stat)

    # ------------------------------------------------------------ API: reads

    async def exists(self, path):
        """Return the node's Stat, or None if it does not exist."""
        resp = await self._call("Exists", zk_pb2.PathRequest(path=path))
        return Stat.from_proto(resp.stat) if resp.exists else None

    async def get_data(self, path):
        """Return (data, Stat)."""
        resp = await self._call("GetData", zk_pb2.PathRequest(path=path))
        return resp.data, Stat.from_proto(resp.stat)

    async def get_children(self, path):
        resp = await self._call("GetChildren", zk_pb2.PathRequest(path=path))
        return list(resp.children)

    async def status(self):
        """Role and leader of the server we are connected to (handy for demos)."""
        resp = await self._stub_call("GetStatus", zk_pb2.Empty())
        return {
            "server_id": resp.server_id, "state": resp.state,
            "leader_id": resp.leader_id, "leader_address": resp.leader_address,
            "last_zxid": resp.last_zxid, "current_epoch": resp.current_epoch,
            "election_round": resp.election_round, "node_count": resp.node_count,
        }

    # ------------------------------------------------------------ plumbing

    async def _write(self, method, request):
        """Send a write; on NOT_LEADER move to the leader and retry once."""
        try:
            return await self._call(method, request)
        except NotLeaderError:
            info = await self.status()
            if not info["leader_address"] or not await self._connect_to(info["leader_address"]):
                raise
            return await self._call(method, request)

    async def _call(self, method, request):
        """Call an RPC and raise the matching exception if err != OK."""
        resp = await self._stub_call(method, request)
        if resp.err != ErrorCode.OK:
            raise error_for_code(resp.err, f"{method} {getattr(request, 'path', '')}")
        return resp

    async def _stub_call(self, method, request):
        if self._stub is None:
            raise ConnectionLossError("not connected; call connect() first")
        try:
            return await getattr(self._stub, method)(request, timeout=self.timeout)
        except grpc.aio.AioRpcError as e:
            raise ConnectionLossError(f"{self.address}: {e.code().name}") from e
