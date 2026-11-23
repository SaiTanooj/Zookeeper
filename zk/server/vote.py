"""Votes and notifications used by Fast Leader Election.

The single most important rule of FLE lives here: `Vote.is_better_than`.
A vote for server A beats a vote for server B when A has

    1. a higher epoch, or
    2. the same epoch and a higher last zxid, or
    3. the same epoch and zxid and a higher server id (tie-breaker).

Because of rule 1 and 2, the server with the most up-to-date history always
wins, so a new leader already has every committed write. Rule 3 just makes
sure everybody picks the same server when histories are equal.
"""

from dataclasses import dataclass
from enum import IntEnum


class ServerState(IntEnum):
    # Same numbers as `enum ServerState` in zk.proto.
    LOOKING = 0
    FOLLOWING = 1
    LEADING = 2


@dataclass(frozen=True)
class Vote:
    leader: int  # proposed leader's server id
    zxid: int    # proposed leader's last zxid
    epoch: int   # proposed leader's current epoch

    def key(self):
        return (self.epoch, self.zxid, self.leader)

    def is_better_than(self, other):
        return self.key() > other.key()


def best_vote(a, b):
    return a if a.is_better_than(b) else b


@dataclass(frozen=True)
class Notification:
    sender: int         # who sent it
    state: ServerState  # sender's state
    vote: Vote          # sender's current vote
    round: int          # sender's election round ("logical clock")
