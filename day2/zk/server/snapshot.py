"""Snapshots: the whole data tree and session table as one JSON document.

Used in two places:
* on disk  - snapshot.json in the server's data folder, so a restart does
             not have to replay the entire history;
* on the wire - the SNAP message a leader sends to bring a follower up to
                date in one step.

Format:
    {
      "zxid": 4294967301,                     # last txn included
      "sessions": {"123456": 6000},           # session id -> timeout (ms)
      "nodes": {"/app": {"data": "aGVsbG8=", "stat": {...}}, ...}
    }
"""

import base64
import json
import os

from zk.common.paths import ROOT, name_of, parent_of
from zk.common.stat import Stat
from zk.server.datatree import DataTree
from zk.server.znode import ZNode


def to_dict(tree, sessions, zxid):
    nodes = {
        path: {
            "data": base64.b64encode(node.data).decode("ascii"),
            "stat": {k: getattr(node.stat, k) for k in Stat.__dataclass_fields__},
        }
        for path, node in tree.nodes.items()
    }
    return {"zxid": zxid, "sessions": {str(k): v for k, v in sessions.items()}, "nodes": nodes}


def from_dict(d):
    """Return (tree, sessions, zxid)."""
    tree = DataTree()
    # Parents before children, so every child can be attached to its parent.
    for path in sorted(d["nodes"], key=lambda p: (p.count("/"), p)):
        raw = d["nodes"][path]
        node = ZNode(data=base64.b64decode(raw["data"]), stat=Stat(**raw["stat"]))
        node.children = set()
        tree.nodes[path] = node
        if path != ROOT:
            tree.nodes[parent_of(path)].children.add(name_of(path))
        if node.stat.ephemeral_owner:
            tree.ephemerals[node.stat.ephemeral_owner].add(path)
    sessions = {int(k): v for k, v in d["sessions"].items()}
    return tree, sessions, d["zxid"]


def to_bytes(tree, sessions, zxid):
    return json.dumps(to_dict(tree, sessions, zxid)).encode("utf-8")


def from_bytes(raw):
    return from_dict(json.loads(raw))


class SnapshotStore:
    """Keeps the newest snapshot in <data_dir>/snapshot.json."""

    FILE_NAME = "snapshot.json"

    def __init__(self, data_dir):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, self.FILE_NAME)

    def save(self, tree, sessions, zxid):
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(to_bytes(tree, sessions, zxid))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def load(self):
        """Return (tree, sessions, zxid), or None if there is no snapshot."""
        if not os.path.exists(self.path):
            return None
        with open(self.path, "rb") as f:
            return from_bytes(f.read())
