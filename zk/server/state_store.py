"""Small on-disk state every server must not forget across restarts.

ZAB keeps two epochs per server:

* accepted_epoch - the newest epoch this server has *promised* to follow
                   (set during ZAB Discovery, Day 2)
* current_epoch  - the newest epoch whose leader this server has fully
                   synced with (set during ZAB Synchronization, Day 2)

On Day 1 they are only loaded (default 0) and fed into election votes.
The file is replaced atomically (write temp file, fsync, rename) so a crash
can never leave a half-written file behind.
"""

import json
import os


class StateStore:
    FILE_NAME = "epoch.json"

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.path = os.path.join(data_dir, self.FILE_NAME)
        self.current_epoch = 0
        self.accepted_epoch = 0
        os.makedirs(data_dir, exist_ok=True)
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            raw = json.load(f)
        self.current_epoch = raw.get("current_epoch", 0)
        self.accepted_epoch = raw.get("accepted_epoch", 0)

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"current_epoch": self.current_epoch, "accepted_epoch": self.accepted_epoch},
                f,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def set_accepted_epoch(self, epoch):
        self.accepted_epoch = epoch
        self.save()

    def set_current_epoch(self, epoch):
        self.current_epoch = epoch
        self.save()
