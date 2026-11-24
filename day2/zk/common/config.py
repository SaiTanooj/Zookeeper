"""Cluster configuration, loaded from a JSON file such as cluster.json.

Example file:

    {
      "servers": {
        "1": {"host": "127.0.0.1", "client_port": 2181, "peer_port": 2881},
        "2": {"host": "127.0.0.1", "client_port": 2182, "peer_port": 2882},
        "3": {"host": "127.0.0.1", "client_port": 2183, "peer_port": 2883}
      },
      "timing": {"finalize_wait_ms": 200, "ping_interval_ms": 100, ...}
    }
"""

import json
from dataclasses import dataclass, field


@dataclass
class ServerInfo:
    id: int
    host: str
    client_port: int  # clients talk to this port (ZooKeeper service)
    peer_port: int    # other servers talk to this port (Peer service)

    @property
    def client_address(self):
        return f"{self.host}:{self.client_port}"

    @property
    def peer_address(self):
        return f"{self.host}:{self.peer_port}"


@dataclass
class Timing:
    # FLE: after a quorum agrees, wait this long for a better vote.
    finalize_wait_ms: int = 200
    # FLE: longest gap between re-sending our vote while nobody answers.
    max_notification_interval_ms: int = 800
    # ZAB: the leader pings every follower this often.
    ping_interval_ms: int = 100
    # ZAB: silence for this long = leader (or quorum) is gone.
    sync_limit_ms: int = 500
    # ZAB: a new leader must get a majority synced within this time.
    init_limit_ms: int = 2000
    # Storage: write a snapshot (and shrink the log) every N committed txns.
    snapshot_every: int = 100


@dataclass
class ClusterConfig:
    servers: dict  # server id -> ServerInfo
    timing: Timing = field(default_factory=Timing)

    @property
    def quorum_size(self):
        """Smallest majority: 2 of 3, 3 of 5, ..."""
        return len(self.servers) // 2 + 1

    def peers_of(self, my_id):
        """Every server except me."""
        return {sid: info for sid, info in self.servers.items() if sid != my_id}

    @classmethod
    def from_dict(cls, raw):
        servers = {
            int(sid): ServerInfo(id=int(sid), **info)
            for sid, info in raw["servers"].items()
        }
        timing = Timing(**raw.get("timing", {}))
        return cls(servers=servers, timing=timing)

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))
