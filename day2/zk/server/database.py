"""ZKDatabase: the replicated state of one server, in memory and on disk.

It holds:
* tree      - the znodes (DataTree)
* sessions  - session id -> timeout in ms (replicated through txns too)
* log       - every txn this server has accepted (TxnLog, on disk)
* snapshots - an occasional full copy of tree + sessions (on disk)

Two zxids describe how far a server has got:

    last_logged   newest txn written to the log      (used in election votes)
    last_applied  newest txn applied to the tree     (what reads see)

A txn is *logged* when the server accepts a proposal, and *applied* when
it is committed. Txns that are logged but not applied yet wait in `pending`.

                 log(txn)                    commit(zxid)
    PROPOSAL  ------------->  pending  ------------------->  tree
              (fsync, ACK)               (apply in order)
"""

import asyncio
import logging
from collections import OrderedDict

from zk.common.errors import ZKError
from zk.server import snapshot as snap
from zk.server import txn as T
from zk.server.datatree import ANY_VERSION, DataTree
from zk.server.snapshot import SnapshotStore
from zk.server.txnlog import TxnLog

log = logging.getLogger(__name__)


class ZKDatabase:
    def __init__(self, data_dir, snapshot_every=100):
        self.tree = DataTree()
        self.sessions = {}
        self.txnlog = TxnLog(data_dir)
        self.snapshots = SnapshotStore(data_dir)
        self.snapshot_every = snapshot_every

        self.pending = OrderedDict()  # zxid -> Txn, logged but not applied
        self.last_logged = 0
        self.last_applied = 0
        self._applied_since_snapshot = 0
        self._waiters = []  # (zxid, future) for wait_until_applied

    # ------------------------------------------------------------ start-up

    def recover(self):
        """Rebuild memory from disk: load the snapshot, then replay the log.

        Every txn in the log is applied, even ones that were never committed.
        That is fine: a server does not serve clients until it has synced
        with a leader, and syncing replaces anything the leader does not have.
        """
        loaded = self.snapshots.load()
        if loaded:
            self.tree, self.sessions, self.last_applied = loaded
        self.last_logged = self.last_applied
        for txn in self.txnlog.read_all():
            self.last_logged = max(self.last_logged, txn.zxid)
            self._apply(txn)
        log.info("recovered: last zxid %d, %d znodes, %d sessions",
                 self.last_applied, len(self.tree), len(self.sessions))

    # ------------------------------------------------------------ log + commit

    def log_txn(self, txn):
        """Write a proposed txn to the log (fsync) and remember it as pending."""
        self.txnlog.append(txn)
        self.pending[txn.zxid] = txn
        self.last_logged = txn.zxid

    def commit(self, zxid):
        """Apply every pending txn up to `zxid`, in order.

        Returns the result of the txn with exactly this zxid (the new path for
        a create, the new Stat for set_data, ...), or None.
        """
        result = None
        while self.pending and next(iter(self.pending)) <= zxid:
            _, txn = self.pending.popitem(last=False)
            r = self._apply(txn)
            if txn.zxid == zxid:
                result = r
        self._after_apply()
        return result

    def commit_all_pending(self):
        """Used by a new leader: its whole log becomes committed history."""
        if self.pending:
            self.commit(next(reversed(self.pending)))

    async def wait_until_applied(self, zxid, timeout):
        """Wait until this server's tree includes txn `zxid`."""
        if self.last_applied >= zxid:
            return
        future = asyncio.get_running_loop().create_future()
        self._waiters.append((zxid, future))
        await asyncio.wait_for(future, timeout)

    # ------------------------------------------------------------ snapshots

    def snapshot_bytes(self):
        """The committed state, ready to send in a SNAP message."""
        return snap.to_bytes(self.tree, self.sessions, self.last_applied)

    def install_snapshot(self, raw):
        """Replace everything with the leader's snapshot (follower sync).

        Our own log is thrown away: the leader's history is the truth now.
        """
        self.tree, self.sessions, self.last_applied = snap.from_bytes(raw)
        self.last_logged = self.last_applied
        self.pending.clear()
        self.txnlog.clear()
        self.snapshots.save(self.tree, self.sessions, self.last_applied)
        self._after_apply()

    def take_snapshot(self):
        self.snapshots.save(self.tree, self.sessions, self.last_applied)
        self.txnlog.drop_through(self.last_applied)
        self._applied_since_snapshot = 0
        log.info("snapshot written at zxid %d", self.last_applied)

    # ------------------------------------------------------------ applying

    def _apply(self, txn):
        """Apply one txn to the tree. Skips txns that are already applied."""
        if txn.zxid <= self.last_applied:
            return None
        try:
            result = self._apply_to_tree(txn)
        except ZKError as e:
            # Cannot happen if the leader checked the txn correctly.
            log.error("txn %d (%s %s) failed to apply: %s", txn.zxid, txn.type, txn.path, e)
            result = None
        self.last_applied = txn.zxid
        self._applied_since_snapshot += 1
        return result

    def _apply_to_tree(self, txn):
        if txn.type == T.CREATE:
            return self.tree.create(txn.path, txn.data, txn.zxid, ephemeral_owner=txn.ephemeral_owner)
        if txn.type == T.DELETE:
            return self.tree.delete(txn.path, ANY_VERSION, txn.zxid)
        if txn.type == T.SET_DATA:
            return self.tree.set_data(txn.path, txn.data, ANY_VERSION, txn.zxid)
        if txn.type == T.CREATE_SESSION:
            self.sessions[txn.session_id] = txn.timeout_ms
            return txn.session_id
        if txn.type == T.CLOSE_SESSION:
            self.tree.delete_ephemerals(txn.session_id, txn.zxid)
            self.sessions.pop(txn.session_id, None)
            return None
        raise ValueError(f"unknown txn type {txn.type}")

    def _after_apply(self):
        if self._applied_since_snapshot >= self.snapshot_every:
            self.take_snapshot()
        # Wake up everybody waiting for a zxid we have now reached.
        still_waiting = []
        for zxid, future in self._waiters:
            if future.done():
                continue
            if self.last_applied >= zxid:
                future.set_result(None)
            else:
                still_waiting.append((zxid, future))
        self._waiters = still_waiting

    def close(self):
        self.txnlog.close()
