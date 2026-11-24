"""PeerService: the *incoming* side of server-to-server calls."""

from zk.common.errors import NotReadyError, ZKError
from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server.convert import notification_from_proto
from zk.server.write_op import WriteOp
from zk.server.zab_leader import FollowerHandler


class PeerService(zk_pb2_grpc.PeerServicer):
    def __init__(self, server):
        self.server = server

    async def SendNotification(self, request, context):
        self.server.election.receive(notification_from_proto(request))
        return zk_pb2.Empty()

    async def Follow(self, request_iterator, context):
        """A follower opened its ZAB stream to us."""
        leader = self.server.leader
        if leader is None or leader.closed:
            return  # we are not (yet) leading: end the stream, the follower retries
        handler = FollowerHandler(leader, context)
        leader.add_follower(handler)
        await handler.run()

    async def ForwardWrite(self, request, context):
        """A follower passes us a client write to replicate."""
        try:
            leader = self.server.leader
            if leader is None:
                raise NotReadyError("not the leader")
            result = await leader.propose(WriteOp.from_proto(request))
            return result.to_proto()
        except ZKError as e:
            return zk_pb2.WriteResponse(err=int(e.code))
