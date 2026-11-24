"""ClientService: the gRPC API that ZooKeeper clients call.

* Reads are answered from this server's own tree (fast; may be slightly
  behind the leader, exactly like real ZooKeeper).
* Writes - including creating and closing sessions - go through
  server.submit_write(): on the leader they are replicated with ZAB, on a
  follower they are forwarded to the leader first.
* Nothing is served until this server is synced with an active leader:
  until then every call answers NOT_READY.
* Every request touches the client's session (heartbeat), and every
  response echoes the request's xid and the zxid this server has applied.
"""

from zk.common.errors import NotReadyError, SessionExpiredError, ZKError
from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server import write_op as W
from zk.server.write_op import WriteOp


class ClientService(zk_pb2_grpc.ZooKeeperServicer):
    def __init__(self, server):
        self.server = server

    # ------------------------------------------------------------ sessions

    async def Connect(self, req, context):
        async def op():
            result = await self.server.submit_write(
                WriteOp(W.CREATE_SESSION, timeout_ms=req.timeout_ms))
            self.server.session_tracker.touch(result.session_id)
            timeout = self.server.db.sessions.get(result.session_id, req.timeout_ms)
            return zk_pb2.ConnectResponse(session_id=result.session_id, timeout_ms=timeout,
                                          zxid=result.zxid)
        return await self._run(None, op, zk_pb2.ConnectResponse)

    async def Ping(self, req, context):
        async def op():
            self._check_serving()
            if req.session_id not in self.server.db.sessions:
                raise SessionExpiredError()
            return zk_pb2.SessionResponse()
        return await self._run(req, op, zk_pb2.SessionResponse)

    async def CloseSession(self, req, context):
        async def op():
            result = await self.server.submit_write(
                WriteOp(W.CLOSE_SESSION, session_id=req.session_id))
            self.server.session_tracker.forget(req.session_id)
            return zk_pb2.SessionResponse(zxid=result.zxid)
        return await self._run(req, op, zk_pb2.SessionResponse)

    # ------------------------------------------------------------ writes

    async def Create(self, req, context):
        async def op():
            result = await self.server.submit_write(WriteOp(
                W.CREATE, path=req.path, data=req.data, ephemeral=req.ephemeral,
                sequential=req.sequential, session_id=req.session_id))
            return zk_pb2.CreateResponse(path=result.path, zxid=result.zxid)
        return await self._run(req, op, zk_pb2.CreateResponse)

    async def Delete(self, req, context):
        async def op():
            result = await self.server.submit_write(
                WriteOp(W.DELETE, path=req.path, version=req.version))
            return zk_pb2.DeleteResponse(zxid=result.zxid)
        return await self._run(req, op, zk_pb2.DeleteResponse)

    async def SetData(self, req, context):
        async def op():
            result = await self.server.submit_write(
                WriteOp(W.SET_DATA, path=req.path, data=req.data, version=req.version))
            return zk_pb2.SetDataResponse(stat=result.stat.to_proto(zk_pb2.Stat()), zxid=result.zxid)
        return await self._run(req, op, zk_pb2.SetDataResponse)

    # ------------------------------------------------------------ reads

    async def Exists(self, req, context):
        async def op():
            self._check_serving()
            stat = self.server.db.tree.exists(req.path)
            if stat is None:
                return zk_pb2.ExistsResponse(exists=False)
            return zk_pb2.ExistsResponse(exists=True, stat=stat.to_proto(zk_pb2.Stat()))
        return await self._run(req, op, zk_pb2.ExistsResponse)

    async def GetData(self, req, context):
        async def op():
            self._check_serving()
            data, stat = self.server.db.tree.get_data(req.path)
            return zk_pb2.GetDataResponse(data=data, stat=stat.to_proto(zk_pb2.Stat()))
        return await self._run(req, op, zk_pb2.GetDataResponse)

    async def GetChildren(self, req, context):
        async def op():
            self._check_serving()
            return zk_pb2.GetChildrenResponse(children=self.server.db.tree.get_children(req.path))
        return await self._run(req, op, zk_pb2.GetChildrenResponse)

    async def GetStatus(self, req, context):
        return self.server.status()

    # ------------------------------------------------------------ helpers

    def _check_serving(self):
        if not self.server.serving:
            raise NotReadyError("server is not synced with a leader")

    async def _run(self, req, op, response_type):
        """Run `op`, turn ZKErrors into error codes, fill in xid and zxid."""
        if req is not None:
            self.server.session_tracker.touch(req.session_id)
        try:
            response = await op()
        except ZKError as e:
            response = response_type(err=int(e.code))
        if req is not None and hasattr(response, "xid"):
            response.xid = req.xid
        if not response.zxid:
            response.zxid = self.server.db.last_applied
        return response
