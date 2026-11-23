"""Helpers for znode paths such as "/app/config".

Rules (same as ZooKeeper):
* must start with "/"
* no trailing "/" (except the root "/" itself)
* no empty parts ("//"), and no "." or ".." parts
"""

from zk.common.errors import BadArgumentsError

ROOT = "/"


def validate_path(path):
    if not isinstance(path, str) or not path.startswith("/"):
        raise BadArgumentsError(f"path must start with '/': {path!r}")
    if path == ROOT:
        return
    if path.endswith("/"):
        raise BadArgumentsError(f"path must not end with '/': {path!r}")
    for part in path[1:].split("/"):
        if part in ("", ".", ".."):
            raise BadArgumentsError(f"invalid path part {part!r} in {path!r}")


def parent_of(path):
    """parent_of("/a/b") -> "/a",  parent_of("/a") -> "/"."""
    if path == ROOT:
        raise BadArgumentsError("the root has no parent")
    parent = path.rsplit("/", 1)[0]
    return parent or ROOT


def name_of(path):
    """name_of("/a/b") -> "b"."""
    return path.rsplit("/", 1)[1]
