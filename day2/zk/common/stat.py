"""Stat: the metadata ZooKeeper keeps for every znode.

Used by the server's data tree and returned to client code, so it lives in
`common` (the client never imports server modules).
"""

from dataclasses import dataclass


@dataclass
class Stat:
    czxid: int = 0            # zxid of the create
    mzxid: int = 0            # zxid of the last setData
    pzxid: int = 0            # zxid of the last child create/delete
    ctime: int = 0            # created at (ms)
    mtime: int = 0            # modified at (ms)
    version: int = 0          # number of setData calls
    cversion: int = 0         # number of child changes
    ephemeral_owner: int = 0  # session id, 0 for regular nodes
    data_length: int = 0
    num_children: int = 0

    def to_proto(self, msg):
        """Copy the fields into a zk_pb2.Stat message."""
        for name in self.__dataclass_fields__:
            setattr(msg, name, getattr(self, name))
        return msg

    @classmethod
    def from_proto(cls, msg):
        return cls(**{name: getattr(msg, name) for name in cls.__dataclass_fields__})
