"""ZNode: one node of the data tree."""

from dataclasses import dataclass, field

from zk.common.stat import Stat


@dataclass
class ZNode:
    data: bytes = b""
    stat: Stat = field(default_factory=Stat)
    children: set = field(default_factory=set)  # child *names*, not full paths

    @property
    def is_ephemeral(self):
        return self.stat.ephemeral_owner != 0

    def snapshot_stat(self):
        """A copy of the stat with the derived fields filled in.

        We hand out copies so callers can never change the tree by accident.
        """
        s = self.stat
        return Stat(
            czxid=s.czxid, mzxid=s.mzxid, pzxid=s.pzxid,
            ctime=s.ctime, mtime=s.mtime,
            version=s.version, cversion=s.cversion,
            ephemeral_owner=s.ephemeral_owner,
            data_length=len(self.data),
            num_children=len(self.children),
        )
