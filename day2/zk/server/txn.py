"""Txn: one transaction - a write that has been checked and fully decided.

The leader turns every client write into a Txn *before* replicating it.
All decisions are already made inside the Txn:

* a sequential create carries the final name ("/locks/lock-0000000007"),
* version checks have already passed on the leader,
* a new session already has its id.

So every server can apply the same Txn and get exactly the same result,
without checking anything again. Each Txn has its own zxid, and a server
applies a Txn only if its zxid is newer than the last one it applied, so
replaying the log after a crash never applies anything twice.

Txns are stored as JSON, both in the log on disk and inside network
messages, so the log file can be read by a human.
"""

import base64
import json
from dataclasses import asdict, dataclass

# Transaction types
CREATE = "create"
DELETE = "delete"
SET_DATA = "set_data"
CREATE_SESSION = "create_session"
CLOSE_SESSION = "close_session"


@dataclass
class Txn:
    zxid: int
    type: str
    path: str = ""
    data: bytes = b""
    ephemeral_owner: int = 0  # CREATE: owning session (0 = regular node)
    session_id: int = 0       # CREATE_SESSION / CLOSE_SESSION
    timeout_ms: int = 0       # CREATE_SESSION

    def to_dict(self):
        d = asdict(self)
        d["data"] = base64.b64encode(self.data).decode("ascii")
        return d

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        d["data"] = base64.b64decode(d.get("data", ""))
        return cls(**d)

    def to_json(self):
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_json(cls, raw):
        return cls.from_dict(json.loads(raw))
