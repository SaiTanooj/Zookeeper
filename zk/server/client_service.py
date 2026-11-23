"""ClientService: the gRPC API that ZooKeeper clients call.

Day 1 behaviour:
* Reads (exists, getData, getChildren) are answered from this server's own
  data tree, exactly like real ZooKeeper.
* Writes (create, delete, setData) are only accepted by the leader, which
  applies them straight to its own tree. Followers answer NOT_LEADER.
  There is no replication yet, so followers do not see the leader's data.

Day 2 changes only the write path: the leader turns each write into a
transaction, replicates it with ZAB, and every server applies it to its
tree on COMMIT. Followers will forward writes instead of refusing them.
"""

from zk.common.errors import NotLeaderError, ZKError
from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server.vote import ServerState


class ClientService(zk_pb2_grpc.ZooKeeperServicer):
    def __init__(self, server):
        self.server = server
        self.tree = server.tree

    # ------------------------------------------------------------ writes

    async def Create(self, req, context):
        def op(zxid):
            path = self.tree.create(
                req.path, req.data, zxid,
                ephemeral_owner=req.session_id if req.ephemeral else 0,
                sequential=req.sequential,
            )
            return zk_pb2.CreateResponse(path=path)
        return self._run(op, zk_pb2.CreateResponse, write=True)

    async def Delete(self, req, context):
        def op(zxid):
            self.tree.delete(req.path, req.version, zxid)
            return zk_pb2.DeleteResponse()
        return self._run(op, zk_pb2.DeleteResponse, write=True)

    async def SetData(self, req, context):
        def op(zxid):
            stat = self.tree.set_data(req.path, req.data, req.version, zxid)
            return zk_pb2.SetDataResponse(stat=stat.to_proto(zk_pb2.Stat()))
        return self._run(op, zk_pb2.SetDataResponse, write=True)

    # ------------------------------------------------------------ reads

    async def Exists(self, req, context):
        def op():
            stat = self.tree.exists(req.path)
            if stat is None:
                return zk_pb2.ExistsResponse(exists=False)
            return zk_pb2.ExistsResponse(exists=True, stat=stat.to_proto(zk_pb2.Stat()))
        return self._run(op, zk_pb2.ExistsResponse)

    async def GetData(self, req, context):
        def op():
            data, stat = self.tree.get_data(req.path)
            return zk_pb2.GetDataResponse(data=data, stat=stat.to_proto(zk_pb2.Stat()))
        return self._run(op, zk_pb2.GetDataResponse)

    async def GetChildren(self, req, context):
        def op():
            return zk_pb2.GetChildrenResponse(children=self.tree.get_children(req.path))
        return self._run(op, zk_pb2.GetChildrenResponse)

    async def GetStatus(self, req, context):
        return self.server.status()

    # ------------------------------------------------------------ helpers

    def _run(self, op, response_type, write=False):
        """Run `op`; turn any ZKError into an error code in the response.

        A write op gets the zxid it should use. last_zxid only moves forward
        when the write succeeds, so failed writes do not use up a zxid.
        """
        try:
            if not write:
                return op()
            if self.server.election.state != ServerState.LEADING:
                raise NotLeaderError("writes must go to the leader (Day 1)")
            zxid = self.server.next_zxid()
            response = op(zxid)
            self.server.last_zxid = zxid
            return response
        except ZKError as e:
            return response_type(err=int(e.code))
