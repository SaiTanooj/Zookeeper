"""ZKClient: the library applications use to talk to the cluster (Day 2).

    async with ZKClient(["127.0.0.1:2181", "127.0.0.1:2182"]) as zk:
        await zk.create("/app", b"hello")
        await zk.create("/workers/w1", b"", ephemeral=True)  # gone when we close
        data, stat = await zk.get_data("/app")

What it does:
* Session: connect() opens a session on the cluster (a replicated
  create_session txn) and gets a session id. close() ends it, which
  deletes every ephemeral node the session created.
* Heartbeats: while the client is idle, it pings the server every
  timeout/3 so the session stays alive.
* FIFO order: requests are sent one at a time, in the order the
  application made them, each with an increasing request number (xid).
* NOT_READY: while the cluster is electing a leader, calls are retried
  for a few seconds. This is safe: NOT_READY means nothing was changed.

Coming on Day 3: moving to another server (keeping the session) when the
current server dies, and watches.
"""

import asyncio
import logging
import random
import time

import grpc

from zk.common.errors import ConnectionLossError, ErrorCode, error_for_code
from zk.common.stat import Stat
from zk.proto import zk_pb2, zk_pb2_grpc

log = logging.getLogger(__name__)

ANY_VERSION = -1
NOT_READY_RETRIES = 15     # x 0.2 s = wait up to 3 s for a new leader
NOT_READY_DELAY = 0.2


class ZKClient:
    def __init__(self, servers, session_timeout_ms=6000, request_timeout=3.0):
        self.servers = list(servers)          # ["host:port", ...]
        self.session_timeout_ms = session_timeout_ms
        self.request_timeout = request_timeout
        self.address = None
        self.session_id = 0
        self.last_zxid = 0                    # newest zxid this client has seen
        self._xid = 0
        self._channel = None
        self._stub = None
        self._lock = asyncio.Lock()           # one request at a time = FIFO order
        self._last_sent = time.monotonic()
        self._heartbeat_task = None

    # ------------------------------------------------------------ connection

    async def connect(self):
        """Connect to a live server and open a session."""
        for address in random.sample(self.servers, len(self.servers)):
            if await self._open(address):
                break
        else:
            raise ConnectionLossError(f"no server reachable in {self.servers}")
        resp = await self._call("Connect", zk_pb2.ConnectRequest(timeout_ms=self.session_timeout_ms))
        self.session_id = resp.session_id
        self.session_timeout_ms = resp.timeout_ms
        self._heartbeat_task = asyncio.create_task(self._heartbeat())
        return self.address

    async def close(self):
        """End the session (its ephemeral nodes are deleted) and disconnect."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        if self.session_id and self._stub:
            try:
                await self._call("CloseSession", zk_pb2.SessionRequest())
            except Exception as e:
                log.debug("close session failed: %s", e)
        self.session_id = 0
        if self._channel is not None:
            await self._channel.close()
        self._channel = self._stub = self.address = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _open(self, address):
        channel = grpc.aio.insecure_channel(address)
        try:
            await zk_pb2_grpc.ZooKeeperStub(channel).GetStatus(zk_pb2.Empty(), timeout=1.0)
        except grpc.aio.AioRpcError:
            await channel.close()
            return False
        self._channel, self.address = channel, address
        self._stub = zk_pb2_grpc.ZooKeeperStub(channel)
        return True

    async def _heartbeat(self):
        interval = self.session_timeout_ms / 3000
        while True:
            await asyncio.sleep(interval)
            if time.monotonic() - self._last_sent >= interval:
                try:
                    await self._call("Ping", zk_pb2.SessionRequest())
                except Exception as e:
                    log.warning("heartbeat failed: %s", e)

    # ------------------------------------------------------------ API: writes

    async def create(self, path, data=b"", ephemeral=False, sequential=False):
        """Create a znode; returns the real path (with counter if sequential)."""
        req = zk_pb2.CreateRequest(path=path, data=data, ephemeral=ephemeral, sequential=sequential)
        return (await self._call("Create", req)).path

    async def delete(self, path, version=ANY_VERSION):
        await self._call("Delete", zk_pb2.DeleteRequest(path=path, version=version))

    async def set_data(self, path, data, version=ANY_VERSION):
        req = zk_pb2.SetDataRequest(path=path, data=data, version=version)
        return Stat.from_proto((await self._call("SetData", req)).stat)

    # ------------------------------------------------------------ API: reads

    async def exists(self, path):
        resp = await self._call("Exists", zk_pb2.PathRequest(path=path))
        return Stat.from_proto(resp.stat) if resp.exists else None

    async def get_data(self, path):
        resp = await self._call("GetData", zk_pb2.PathRequest(path=path))
        return resp.data, Stat.from_proto(resp.stat)

    async def get_children(self, path):
        return list((await self._call("GetChildren", zk_pb2.PathRequest(path=path))).children)

    async def status(self):
        """Role of the server we are connected to (handy for demos)."""
        resp = await self._stub.GetStatus(zk_pb2.Empty(), timeout=self.request_timeout)
        return {f.name: getattr(resp, f.name) for f in resp.DESCRIPTOR.fields}

    # ------------------------------------------------------------ plumbing

    async def _call(self, method, request):
        """Send one request (in FIFO order), retry NOT_READY, raise on errors."""
        if self._stub is None:
            raise ConnectionLossError("not connected; call connect() first")
        fields = request.DESCRIPTOR.fields_by_name
        async with self._lock:
            for attempt in range(NOT_READY_RETRIES):
                if "session_id" in fields:
                    request.session_id = self.session_id
                if "xid" in fields:
                    self._xid += 1
                    request.xid = self._xid
                try:
                    resp = await getattr(self._stub, method)(request, timeout=self.request_timeout)
                except grpc.aio.AioRpcError as e:
                    raise ConnectionLossError(f"{self.address}: {e.code().name}") from e
                self._last_sent = time.monotonic()
                if resp.err == ErrorCode.NOT_READY and attempt < NOT_READY_RETRIES - 1:
                    await asyncio.sleep(NOT_READY_DELAY)
                    continue
                break
        if hasattr(resp, "zxid"):
            self.last_zxid = max(self.last_zxid, resp.zxid)
        if resp.err != ErrorCode.OK:
            raise error_for_code(resp.err, f"{method} {getattr(request, 'path', '')}")
        return resp
