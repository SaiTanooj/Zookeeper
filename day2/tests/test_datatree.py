"""DataTree on its own: no network, no cluster."""

import pytest

from zk.common.errors import (
    BadArgumentsError, BadVersionError, NoChildrenForEphemeralsError,
    NodeExistsError, NoNodeError, NotEmptyError,
)
from zk.server.datatree import DataTree


@pytest.fixture
def tree():
    return DataTree()


def test_create_and_read(tree):
    assert tree.create("/app", b"hello", zxid=1) == "/app"
    data, stat = tree.get_data("/app")
    assert data == b"hello"
    assert stat.czxid == stat.mzxid == 1
    assert stat.version == 0 and stat.data_length == 5
    assert tree.get_children("/") == ["app"]


def test_create_needs_parent(tree):
    with pytest.raises(NoNodeError):
        tree.create("/a/b", b"", zxid=1)


def test_create_twice_fails(tree):
    tree.create("/a", b"", zxid=1)
    with pytest.raises(NodeExistsError):
        tree.create("/a", b"", zxid=2)


@pytest.mark.parametrize("bad", ["a", "/a/", "//a", "/a/./b", "/a/../b", ""])
def test_bad_paths(tree, bad):
    with pytest.raises(BadArgumentsError):
        tree.create(bad, b"", zxid=1)


def test_set_data_with_version_check(tree):
    tree.create("/cfg", b"v0", zxid=1)
    stat = tree.set_data("/cfg", b"v1", version=0, zxid=2)
    assert stat.version == 1 and stat.mzxid == 2
    with pytest.raises(BadVersionError):
        tree.set_data("/cfg", b"v2", version=0, zxid=3)  # stale version
    tree.set_data("/cfg", b"v2", version=-1, zxid=3)     # -1 = any version
    assert tree.get_data("/cfg")[0] == b"v2"


def test_delete(tree):
    tree.create("/a", b"", zxid=1)
    tree.create("/a/b", b"", zxid=2)
    with pytest.raises(NotEmptyError):
        tree.delete("/a", -1, zxid=3)
    with pytest.raises(BadVersionError):
        tree.delete("/a/b", 7, zxid=3)
    tree.delete("/a/b", 0, zxid=3)
    tree.delete("/a", -1, zxid=4)
    assert tree.exists("/a") is None
    with pytest.raises(NoNodeError):
        tree.delete("/a", -1, zxid=5)


def test_parent_counters(tree):
    tree.create("/p", b"", zxid=1)
    tree.create("/p/x", b"", zxid=2)
    tree.delete("/p/x", -1, zxid=3)
    stat = tree.exists("/p")
    assert stat.cversion == 2   # one create + one delete
    assert stat.pzxid == 3      # last child change


def test_sequential_names(tree):
    tree.create("/locks", b"", zxid=1)
    a = tree.create("/locks/lock-", b"", zxid=2, sequential=True)
    b = tree.create("/locks/lock-", b"", zxid=3, sequential=True)
    assert a == "/locks/lock-0000000000"
    assert b == "/locks/lock-0000000001"
    tree.delete(a, -1, zxid=4)
    c = tree.create("/locks/lock-", b"", zxid=5, sequential=True)
    assert c == "/locks/lock-0000000003"  # the counter never goes back


def test_ephemerals(tree):
    tree.create("/group", b"", zxid=1)
    tree.create("/group/w1", b"", zxid=2, ephemeral_owner=111)
    tree.create("/group/w2", b"", zxid=3, ephemeral_owner=111)
    tree.create("/group/w3", b"", zxid=4, ephemeral_owner=222)
    with pytest.raises(NoChildrenForEphemeralsError):
        tree.create("/group/w1/child", b"", zxid=5)
    deleted = tree.delete_ephemerals(111, zxid=6)
    assert sorted(deleted) == ["/group/w1", "/group/w2"]
    assert tree.get_children("/group") == ["w3"]
    assert 111 not in tree.ephemerals


def test_returned_stat_is_a_copy(tree):
    tree.create("/a", b"", zxid=1)
    stat = tree.exists("/a")
    stat.version = 99
    assert tree.exists("/a").version == 0
