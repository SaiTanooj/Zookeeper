"""Txn log, snapshots and the database - no network."""

from zk.server import snapshot as snap
from zk.server import txn as T
from zk.server.database import ZKDatabase
from zk.server.datatree import DataTree
from zk.server.txn import Txn
from zk.server.txnlog import TxnLog


def test_txn_json_round_trip():
    t = Txn(zxid=7, type=T.CREATE, path="/a", data=b"\x00\xffhi", ephemeral_owner=5)
    assert Txn.from_json(t.to_json()) == t


def test_txnlog_append_read_and_drop(tmp_path):
    log = TxnLog(tmp_path)
    for z in (1, 2, 3):
        log.append(Txn(zxid=z, type=T.CREATE, path=f"/n{z}"))
    log.close()

    log = TxnLog(tmp_path)  # reopen: still there
    assert [t.zxid for t in log.read_all()] == [1, 2, 3]
    log.drop_through(2)
    assert [t.zxid for t in log.read_all()] == [3]
    log.append(Txn(zxid=4, type=T.DELETE, path="/n3"))
    assert [t.zxid for t in log.read_all()] == [3, 4]
    log.close()


def test_txnlog_ignores_half_written_last_line(tmp_path):
    log = TxnLog(tmp_path)
    log.append(Txn(zxid=1, type=T.CREATE, path="/a"))
    log.close()
    with open(log.path, "a", encoding="utf-8") as f:
        f.write('{"zxid": 2, "ty')  # crash in the middle of a write
    assert [t.zxid for t in TxnLog(tmp_path).read_all()] == [1]


def test_snapshot_round_trip():
    tree = DataTree()
    tree.create("/a", b"x", zxid=1)
    tree.create("/a/b", b"y", zxid=2)
    tree.create("/e", b"", zxid=3, ephemeral_owner=42)
    tree2, sessions, zxid = snap.from_bytes(snap.to_bytes(tree, {42: 5000}, 3))
    assert zxid == 3 and sessions == {42: 5000}
    assert tree2.get_children("/") == ["a", "e"]
    assert tree2.get_data("/a/b")[0] == b"y"
    assert tree2.ephemerals[42] == {"/e"}


def test_log_then_commit(tmp_path):
    db = ZKDatabase(tmp_path)
    db.log_txn(Txn(zxid=1, type=T.CREATE, path="/a", data=b"1"))
    db.log_txn(Txn(zxid=2, type=T.SET_DATA, path="/a", data=b"2"))
    assert db.last_logged == 2 and db.last_applied == 0
    assert db.tree.exists("/a") is None          # logged, not applied yet

    assert db.commit(1) == "/a"
    assert db.tree.get_data("/a")[0] == b"1"
    stat = db.commit(2)
    assert stat.version == 1 and db.last_applied == 2


def test_apply_is_idempotent(tmp_path):
    db = ZKDatabase(tmp_path)
    txn = Txn(zxid=1, type=T.CREATE, path="/a")
    db.log_txn(txn)
    db.commit(1)
    assert db._apply(txn) is None  # already applied: skipped, no error
    assert len(db.tree) == 2


def test_recovery_from_snapshot_and_log(tmp_path):
    db = ZKDatabase(tmp_path, snapshot_every=4)
    for z in range(1, 6):
        db.log_txn(Txn(zxid=z, type=T.CREATE, path=f"/n{z}"))
        db.commit(z)
    db.log_txn(Txn(zxid=6, type=T.CREATE_SESSION, session_id=9, timeout_ms=4000))
    db.commit(6)
    db.close()

    # A snapshot was taken at zxid 4 and the log was shortened.
    assert [t.zxid for t in TxnLog(tmp_path).read_all()] == [5, 6]

    db2 = ZKDatabase(tmp_path, snapshot_every=4)
    db2.recover()
    assert db2.last_applied == 6 and db2.last_logged == 6
    assert db2.tree.get_children("/") == ["n1", "n2", "n3", "n4", "n5"]
    assert db2.sessions == {9: 4000}


def test_close_session_removes_ephemerals(tmp_path):
    db = ZKDatabase(tmp_path)
    for t in [Txn(zxid=1, type=T.CREATE_SESSION, session_id=5, timeout_ms=1000),
              Txn(zxid=2, type=T.CREATE, path="/e", ephemeral_owner=5),
              Txn(zxid=3, type=T.CLOSE_SESSION, session_id=5)]:
        db.log_txn(t)
    db.commit(2)
    assert db.tree.exists("/e") is not None
    db.commit(3)
    assert db.tree.exists("/e") is None and db.sessions == {}


def test_install_snapshot_replaces_everything(tmp_path):
    leader = ZKDatabase(tmp_path / "leader")
    leader.log_txn(Txn(zxid=10, type=T.CREATE, path="/from-leader"))
    leader.commit(10)

    follower = ZKDatabase(tmp_path / "follower")
    follower.log_txn(Txn(zxid=3, type=T.CREATE, path="/stale"))  # never committed
    follower.install_snapshot(leader.snapshot_bytes())
    assert follower.tree.exists("/stale") is None
    assert follower.tree.exists("/from-leader") is not None
    assert follower.last_applied == follower.last_logged == 10
    assert follower.txnlog.read_all() == []
