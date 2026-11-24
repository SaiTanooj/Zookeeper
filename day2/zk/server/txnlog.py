"""TxnLog: the write-ahead log of transactions, one JSON line per Txn.

    {"zxid": 4294967297, "type": "create", "path": "/app", ...}
    {"zxid": 4294967298, "type": "set_data", "path": "/app", ...}

Rules:
* append() writes the line and calls fsync before returning, so once a
  server has ACKed a proposal the txn survives a crash or power cut.
* When a snapshot is taken at zxid Z, everything up to Z is in the
  snapshot, so drop_through(Z) rewrites the file keeping only newer lines.
  The rewrite goes to a temporary file that then replaces the old one, so
  a crash in the middle never loses the log.
"""

import json
import os

from zk.server.txn import Txn


class TxnLog:
    FILE_NAME = "txnlog.jsonl"

    def __init__(self, data_dir):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, self.FILE_NAME)
        self._file = open(self.path, "a", encoding="utf-8")

    def append(self, txn):
        self._file.write(json.dumps(txn.to_dict()) + "\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def read_all(self):
        """Every txn in the log, oldest first."""
        if not os.path.exists(self.path):
            return []
        txns = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    txns.append(Txn.from_dict(json.loads(line)))
                except ValueError:
                    break  # a half-written last line after a crash: ignore it
        return txns

    def drop_through(self, zxid):
        """Remove every txn with zxid <= `zxid` (they are in a snapshot now)."""
        keep = [t for t in self.read_all() if t.zxid > zxid]
        self._rewrite(keep)

    def clear(self):
        self._rewrite([])

    def _rewrite(self, txns):
        self._file.close()
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for t in txns:
                f.write(json.dumps(t.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        self._file = open(self.path, "a", encoding="utf-8")

    def close(self):
        self._file.close()
