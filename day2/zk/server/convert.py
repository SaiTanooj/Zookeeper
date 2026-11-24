"""Convert election objects between Python dataclasses and protobuf messages.

Keeping this in one place means the election code never touches protobuf.
"""

from zk.proto import zk_pb2
from zk.server.vote import Notification, ServerState, Vote


def vote_to_proto(v):
    return zk_pb2.Vote(leader=v.leader, zxid=v.zxid, epoch=v.epoch)


def vote_from_proto(m):
    return Vote(leader=m.leader, zxid=m.zxid, epoch=m.epoch)


def notification_to_proto(n):
    return zk_pb2.Notification(
        sender=n.sender, state=int(n.state), vote=vote_to_proto(n.vote), round=n.round
    )


def notification_from_proto(m):
    return Notification(
        sender=m.sender, state=ServerState(m.state), vote=vote_from_proto(m.vote), round=m.round
    )
