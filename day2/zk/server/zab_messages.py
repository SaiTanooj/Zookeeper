"""Small helpers to build and name ZAB messages."""

from zk.proto import zk_pb2

# Message types, re-exported so other modules can write zm.PROPOSAL etc.
FOLLOWERINFO = zk_pb2.FOLLOWERINFO
NEWEPOCH = zk_pb2.NEWEPOCH
ACKEPOCH = zk_pb2.ACKEPOCH
SNAP = zk_pb2.SNAP
NEWLEADER = zk_pb2.NEWLEADER
ACK_NEWLEADER = zk_pb2.ACK_NEWLEADER
UPTODATE = zk_pb2.UPTODATE
PROPOSAL = zk_pb2.PROPOSAL
ACK = zk_pb2.ACK
COMMIT = zk_pb2.COMMIT
PING = zk_pb2.PING


def make(type_, sid, epoch=0, zxid=0, payload=b""):
    return zk_pb2.ZabMessage(type=type_, sid=sid, epoch=epoch, zxid=zxid, payload=payload)


def name(msg):
    return zk_pb2.ZabType.Name(msg.type)
