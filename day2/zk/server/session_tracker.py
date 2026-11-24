"""SessionTracker: remembers when each session was last heard from.

Every request from a client (including its Ping heartbeat) "touches" its
session on the server it is connected to.

Day 2 only records the times. Day 3 uses them to expire sessions whose
clients went silent for longer than their timeout: the leader then
replicates a close_session txn, which deletes the session's ephemeral nodes.
"""

import time


class SessionTracker:
    def __init__(self):
        self.last_seen = {}  # session id -> time.monotonic()

    def touch(self, session_id):
        if session_id:
            self.last_seen[session_id] = time.monotonic()

    def forget(self, session_id):
        self.last_seen.pop(session_id, None)

    def idle_for(self, session_id):
        """Seconds since the session was last heard from (None if never)."""
        t = self.last_seen.get(session_id)
        return None if t is None else time.monotonic() - t
