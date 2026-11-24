"""The leader's RequestProcessor: WriteOp -> fully decided Txn."""

import pytest

from zk.common.errors import BadVersionError, NodeExistsError, NoNodeError, SessionExpiredError
from zk.common.zxid import make_zxid
from zk.server import txn as T
from zk.server import write_op as W
from zk.server.datatree import DataTree
from zk.server.processor import RequestProcessor
from zk.server.write_op import WriteOp


@pytest.fixture
def proc():
    return RequestProcessor(DataTree(), {}, epoch=2)


def test_zxids_use_the_epoch_and_count_up(proc):
    a = proc.prepare(WriteOp(W.CREATE, path="/a"))
    b = proc.prepare(WriteOp(W.CREATE, path="/b"))
    assert (a.zxid, b.zxid) == (make_zxid(2, 1), make_zxid(2, 2))


def test_second_write_sees_the_first(proc):
    # Neither is committed, but the second one must see "/a".
    proc.prepare(WriteOp(W.CREATE, path="/a"))
    txn = proc.prepare(WriteOp(W.CREATE, path="/a/child"))
    assert txn.path == "/a/child"
    with pytest.raises(NodeExistsError):
        proc.prepare(WriteOp(W.CREATE, path="/a"))


def test_sequential_name_is_decided_by_the_leader(proc):
    proc.prepare(WriteOp(W.CREATE, path="/q"))
    t1 = proc.prepare(WriteOp(W.CREATE, path="/q/item-", sequential=True))
    t2 = proc.prepare(WriteOp(W.CREATE, path="/q/item-", sequential=True))
    assert (t1.path, t2.path) == ("/q/item-0000000000", "/q/item-0000000001")
    assert t1.type == T.CREATE  # the txn itself is a plain create


def test_failed_write_uses_no_zxid(proc):
    proc.prepare(WriteOp(W.CREATE, path="/a"))
    with pytest.raises(NoNodeError):
        proc.prepare(WriteOp(W.DELETE, path="/missing"))
    with pytest.raises(BadVersionError):
        proc.prepare(WriteOp(W.SET_DATA, path="/a", data=b"x", version=9))
    assert proc.prepare(WriteOp(W.SET_DATA, path="/a", data=b"x")).zxid == make_zxid(2, 2)


def test_sessions_and_ephemerals(proc):
    with pytest.raises(SessionExpiredError):
        proc.prepare(WriteOp(W.CREATE, path="/e", ephemeral=True, session_id=123))
    s = proc.prepare(WriteOp(W.CREATE_SESSION, timeout_ms=5000))
    assert s.session_id > 0 and s.timeout_ms == 5000
    e = proc.prepare(WriteOp(W.CREATE, path="/e", ephemeral=True, session_id=s.session_id))
    assert e.ephemeral_owner == s.session_id
    proc.prepare(WriteOp(W.CLOSE_SESSION, session_id=s.session_id))
    with pytest.raises(SessionExpiredError):
        proc.prepare(WriteOp(W.CLOSE_SESSION, session_id=s.session_id))
