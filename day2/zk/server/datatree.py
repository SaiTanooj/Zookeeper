"""DataTree: the in-memory tree of znodes that every server keeps.

This class knows nothing about networking or replication. It only applies
operations to the tree. Every write takes the `zxid` of the transaction that
caused it, so later (Day 2) the exact same calls can be replayed from the
replicated log on every server and give the same result everywhere.

Nodes are stored in a flat dict keyed by full path ("/app/config"), and each
node also keeps the set of its children's names. That makes lookups O(1)
and getChildren cheap.
"""

import time
from collections import defaultdict

from zk.common.errors import (
    BadArgumentsError,
    BadVersionError,
    NoChildrenForEphemeralsError,
    NodeExistsError,
    NoNodeError,
    NotEmptyError,
)
from zk.common.paths import ROOT, name_of, parent_of, validate_path
from zk.server.znode import ZNode

ANY_VERSION = -1


def _now_ms():
    return int(time.time() * 1000)


class DataTree:
    def __init__(self):
        self.nodes = {ROOT: ZNode()}
        # session id -> set of ephemeral paths it owns (for fast cleanup)
        self.ephemerals = defaultdict(set)

    # ------------------------------------------------------------ reads

    def exists(self, path):
        """Return the node's Stat, or None if it does not exist."""
        validate_path(path)
        node = self.nodes.get(path)
        return node.snapshot_stat() if node else None

    def get_data(self, path):
        """Return (data, stat)."""
        node = self._get(path)
        return node.data, node.snapshot_stat()

    def get_children(self, path):
        """Return the children's names, sorted."""
        return sorted(self._get(path).children)

    # ------------------------------------------------------------ writes

    def create(self, path, data, zxid, ephemeral_owner=0, sequential=False):
        """Create a node and return its final path.

        sequential=True appends a 10-digit counter taken from the parent's
        cversion, e.g. "/locks/lock-" -> "/locks/lock-0000000003".
        """
        # For a sequential node the counter is appended later, so check the
        # path as if a digit were already there ("/locks/" is fine then).
        validate_path(path + "0" if sequential else path)
        if path == ROOT:
            raise NodeExistsError(ROOT)
        parent_path = parent_of(path)
        parent = self.nodes.get(parent_path)
        if parent is None:
            raise NoNodeError(f"parent does not exist: {parent_path}")
        if parent.is_ephemeral:
            raise NoChildrenForEphemeralsError(parent_path)

        if sequential:
            path = f"{path}{parent.stat.cversion:010d}"
        if path in self.nodes:
            raise NodeExistsError(path)

        now = _now_ms()
        node = ZNode(data=data)
        node.stat.czxid = node.stat.mzxid = node.stat.pzxid = zxid
        node.stat.ctime = node.stat.mtime = now
        node.stat.ephemeral_owner = ephemeral_owner
        self.nodes[path] = node

        parent.children.add(name_of(path))
        parent.stat.cversion += 1
        parent.stat.pzxid = zxid

        if ephemeral_owner:
            self.ephemerals[ephemeral_owner].add(path)
        return path

    def delete(self, path, version, zxid):
        validate_path(path)
        if path == ROOT:
            raise BadArgumentsError("cannot delete the root")
        node = self._get(path)
        if version != ANY_VERSION and version != node.stat.version:
            raise BadVersionError(f"{path}: expected v{version}, have v{node.stat.version}")
        if node.children:
            raise NotEmptyError(path)

        del self.nodes[path]
        parent = self.nodes[parent_of(path)]
        parent.children.discard(name_of(path))
        parent.stat.cversion += 1
        parent.stat.pzxid = zxid

        if node.is_ephemeral:
            owned = self.ephemerals[node.stat.ephemeral_owner]
            owned.discard(path)
            if not owned:
                del self.ephemerals[node.stat.ephemeral_owner]

    def set_data(self, path, data, version, zxid):
        """Replace the data if the version matches; return the new Stat."""
        node = self._get(path)
        if version != ANY_VERSION and version != node.stat.version:
            raise BadVersionError(f"{path}: expected v{version}, have v{node.stat.version}")
        node.data = data
        node.stat.version += 1
        node.stat.mzxid = zxid
        node.stat.mtime = _now_ms()
        return node.snapshot_stat()

    def delete_ephemerals(self, session_id, zxid):
        """Delete every ephemeral node of a session (used when it expires).

        Returns the deleted paths. Deepest paths go first, although an
        ephemeral node can never have children anyway.
        """
        paths = sorted(self.ephemerals.get(session_id, ()), reverse=True)
        for path in paths:
            self.delete(path, ANY_VERSION, zxid)
        return paths

    # ------------------------------------------------------------ helpers

    def _get(self, path):
        validate_path(path)
        node = self.nodes.get(path)
        if node is None:
            raise NoNodeError(path)
        return node

    def __len__(self):
        return len(self.nodes)
