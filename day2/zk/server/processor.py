"""RequestProcessor: the leader's step that turns a WriteOp into a Txn.

The problem: the leader may have several writes "in flight" (proposed but
not committed yet). The second write must be checked against the tree *as
it will be* after the first one, not as it is now.

The simple solution used here: the leader keeps a private copy of the tree
(the "shadow tree") and applies every write to it the moment it is
accepted. Checks and decisions (does the parent exist? which sequential
number? does the version match?) are made against the shadow tree. The real
tree only changes when a txn is committed.

If the check fails, the client gets the error straight away and no zxid is
used up. If it passes, the result is a Txn with everything decided.
"""

import copy
import secrets

from zk.common.errors import BadArgumentsError, SessionExpiredError
from zk.common.zxid import make_zxid
from zk.server import txn as T
from zk.server import write_op as W
from zk.server.txn import Txn

DEFAULT_SESSION_TIMEOUT_MS = 6000


class RequestProcessor:
    def __init__(self, tree, sessions, epoch):
        """Start from a copy of the committed state, at the start of a new epoch."""
        self.tree = copy.deepcopy(tree)
        self.sessions = dict(sessions)
        self.epoch = epoch
        self.counter = 0  # zxids in this epoch: (epoch, 1), (epoch, 2), ...

    def prepare(self, op):
        """Check `op` against the shadow state and return its Txn (or raise ZKError)."""
        zxid = make_zxid(self.epoch, self.counter + 1)
        txn = self._build(op, zxid)
        self.counter += 1  # only successful writes use up a zxid
        return txn

    def _build(self, op, zxid):
        if op.op == W.CREATE:
            owner = 0
            if op.ephemeral:
                owner = op.session_id
                if owner not in self.sessions:
                    raise SessionExpiredError(f"session {owner} does not exist")
            path = self.tree.create(op.path, op.data, zxid, ephemeral_owner=owner,
                                    sequential=op.sequential)
            return Txn(zxid=zxid, type=T.CREATE, path=path, data=op.data, ephemeral_owner=owner)

        if op.op == W.DELETE:
            self.tree.delete(op.path, op.version, zxid)
            return Txn(zxid=zxid, type=T.DELETE, path=op.path)

        if op.op == W.SET_DATA:
            self.tree.set_data(op.path, op.data, op.version, zxid)
            return Txn(zxid=zxid, type=T.SET_DATA, path=op.path, data=op.data)

        if op.op == W.CREATE_SESSION:
            session_id = self._new_session_id()
            timeout = op.timeout_ms or DEFAULT_SESSION_TIMEOUT_MS
            self.sessions[session_id] = timeout
            return Txn(zxid=zxid, type=T.CREATE_SESSION, session_id=session_id, timeout_ms=timeout)

        if op.op == W.CLOSE_SESSION:
            if op.session_id not in self.sessions:
                raise SessionExpiredError(f"session {op.session_id} does not exist")
            self.tree.delete_ephemerals(op.session_id, zxid)
            del self.sessions[op.session_id]
            return Txn(zxid=zxid, type=T.CLOSE_SESSION, session_id=op.session_id)

        raise BadArgumentsError(f"unknown write op {op.op!r}")

    def _new_session_id(self):
        # A random 62-bit number: two sessions getting the same id is
        # practically impossible, and we still double-check.
        while True:
            session_id = secrets.randbits(62) + 1
            if session_id not in self.sessions:
                return session_id
