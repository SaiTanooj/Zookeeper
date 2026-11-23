"""Error codes and exceptions shared by the server and the client.

The server raises these exceptions inside the data tree, turns them into an
error code in the gRPC response, and the client turns the code back into the
same exception. The numbers must match `enum ErrorCode` in zk/proto/zk.proto.
"""

from enum import IntEnum


class ErrorCode(IntEnum):
    OK = 0
    NO_NODE = 1
    NODE_EXISTS = 2
    BAD_VERSION = 3
    NOT_EMPTY = 4
    NO_CHILDREN_FOR_EPHEMERALS = 5
    BAD_ARGUMENTS = 6
    NOT_LEADER = 7


class ZKError(Exception):
    """Base class for every ZooKeeper error."""

    code = None  # set by each subclass

    def __init__(self, message=""):
        super().__init__(message or self.__class__.__name__)


class NoNodeError(ZKError):
    code = ErrorCode.NO_NODE


class NodeExistsError(ZKError):
    code = ErrorCode.NODE_EXISTS


class BadVersionError(ZKError):
    code = ErrorCode.BAD_VERSION


class NotEmptyError(ZKError):
    code = ErrorCode.NOT_EMPTY


class NoChildrenForEphemeralsError(ZKError):
    code = ErrorCode.NO_CHILDREN_FOR_EPHEMERALS


class BadArgumentsError(ZKError):
    code = ErrorCode.BAD_ARGUMENTS


class NotLeaderError(ZKError):
    code = ErrorCode.NOT_LEADER


_BY_CODE = {cls.code: cls for cls in ZKError.__subclasses__()}


def error_for_code(code, message=""):
    """Build the exception that matches an error code from a response."""
    cls = _BY_CODE.get(ErrorCode(code), ZKError)
    return cls(message)
