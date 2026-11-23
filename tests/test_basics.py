"""Small building blocks: zxids, votes, paths, config, error codes."""

from zk.common.config import ClusterConfig
from zk.common.errors import ErrorCode, NoNodeError, error_for_code
from zk.common.paths import name_of, parent_of
from zk.common.zxid import counter_of, epoch_of, make_zxid
from zk.proto import zk_pb2
from zk.server.vote import Vote


def test_zxid_halves():
    z = make_zxid(3, 17)
    assert epoch_of(z) == 3 and counter_of(z) == 17
    assert make_zxid(2, 999999) < make_zxid(3, 0)  # a newer epoch always wins


def test_vote_order():
    assert Vote(1, zxid=0, epoch=2).is_better_than(Vote(3, zxid=50, epoch=1))  # epoch first
    assert Vote(1, zxid=9, epoch=1).is_better_than(Vote(3, zxid=5, epoch=1))   # then zxid
    assert Vote(3, zxid=5, epoch=1).is_better_than(Vote(2, zxid=5, epoch=1))   # then id
    assert not Vote(2, 5, 1).is_better_than(Vote(2, 5, 1))


def test_paths():
    assert parent_of("/a/b") == "/a"
    assert parent_of("/a") == "/"
    assert name_of("/a/b") == "b"


def test_quorum_sizes():
    for n, q in [(1, 1), (3, 2), (5, 3)]:
        raw = {"servers": {str(i): {"host": "h", "client_port": 1, "peer_port": 2} for i in range(n)}}
        assert ClusterConfig.from_dict(raw).quorum_size == q


def test_error_codes_match_proto():
    for code in ErrorCode:
        assert zk_pb2.ErrorCode.Value(code.name) == code.value
    assert isinstance(error_for_code(1), NoNodeError)
