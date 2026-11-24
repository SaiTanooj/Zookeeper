"""WriteOp and WriteResult: a client write on its way to the leader, and its answer.

A WriteOp is still just a *request* ("create /locks/lock- as sequential").
Only the leader turns it into a Txn (see processor.py). Followers send the
WriteOp to the leader with the ForwardWrite RPC, so it needs to convert
to and from protobuf.
"""

from dataclasses import dataclass, field

from zk.common.stat import Stat
from zk.proto import zk_pb2

CREATE = "create"
DELETE = "delete"
SET_DATA = "set_data"
CREATE_SESSION = "create_session"
CLOSE_SESSION = "close_session"


@dataclass
class WriteOp:
    op: str
    path: str = ""
    data: bytes = b""
    version: int = -1
    ephemeral: bool = False
    sequential: bool = False
    session_id: int = 0
    timeout_ms: int = 0

    def to_proto(self):
        return zk_pb2.WriteRequest(**self.__dict__)

    @classmethod
    def from_proto(cls, m):
        return cls(**{name: getattr(m, name) for name in cls.__dataclass_fields__})


@dataclass
class WriteResult:
    zxid: int                 # zxid of the committed txn
    path: str = ""            # create: the real path
    stat: Stat = field(default_factory=Stat)  # set_data: the new Stat
    session_id: int = 0       # create_session: the new id

    def to_proto(self):
        return zk_pb2.WriteResponse(
            zxid=self.zxid, path=self.path, session_id=self.session_id,
            stat=self.stat.to_proto(zk_pb2.Stat()),
        )

    @classmethod
    def from_proto(cls, m):
        return cls(zxid=m.zxid, path=m.path, session_id=m.session_id, stat=Stat.from_proto(m.stat))
