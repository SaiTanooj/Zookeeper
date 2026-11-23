"""PeerService: the *incoming* side of server-to-server traffic.

It only translates protobuf messages and hands them to the server object;
all decisions are made elsewhere.
"""

from zk.proto import zk_pb2, zk_pb2_grpc
from zk.server.convert import notification_from_proto


class PeerService(zk_pb2_grpc.PeerServicer):
    def __init__(self, server):
        self.server = server  # the ZKServer that owns this service

    async def SendNotification(self, request, context):
        self.server.election.receive(notification_from_proto(request))
        return zk_pb2.Empty()

    async def Ping(self, request, context):
        state, leader = self.server.on_ping(request.sender)
        return zk_pb2.PingResponse(state=int(state), leader=leader)
