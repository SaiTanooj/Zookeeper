"""Fast Leader Election (FLE), the election algorithm ZooKeeper uses.

How it works, in short
----------------------
1. A server that has no leader enters LOOKING, bumps its election round
   ("logical clock") and votes for itself: Vote(me, my_last_zxid, my_epoch).
2. It sends its vote to every other server.
3. Whenever it hears a *better* vote (see Vote.is_better_than) in the same
   round, it switches to that vote and sends the new vote to everybody.
4. It remembers the latest vote of every server in this round. As soon as
   a majority holds the same vote as itself, it waits `finalize_wait` more:
   if no better vote shows up, the election is over.
5. The winner becomes LEADING, everybody else FOLLOWING.

Two extra rules make it work in a real cluster:
* Rounds: a message from a newer round makes us jump to that round and
  start over; a message from an older round is answered with our vote so
  the slow server can catch up.
* Joining a running cluster: a server that is already FOLLOWING/LEADING
  answers a LOOKING server with its own vote and state. If a majority
  says "we follow L" and L itself says "I lead", the newcomer follows L
  without starting a new election.

This class only decides *who* leads. Sending messages is done by a
`transport` object with two methods, `send(peer_id, notification)` and
`broadcast(notification)`, so the algorithm can be tested without a network.
"""

import asyncio
import logging

from zk.server.vote import Notification, ServerState, Vote, best_vote

log = logging.getLogger(__name__)


class FastLeaderElection:
    def __init__(self, my_id, server_ids, transport, get_last_zxid, get_epoch,
                 finalize_wait=0.2, max_notification_interval=0.8):
        self.my_id = my_id
        self.server_ids = set(server_ids)  # everybody, including me
        self.quorum = len(self.server_ids) // 2 + 1
        self.transport = transport
        self.get_last_zxid = get_last_zxid  # callables: always read fresh values
        self.get_epoch = get_epoch
        self.finalize_wait = finalize_wait
        self.max_interval = max_notification_interval

        self.state = ServerState.LOOKING
        self.round = 0
        self.vote = self._my_own_vote()
        self.inbox = asyncio.Queue()  # notifications received while LOOKING

    # ------------------------------------------------------------ incoming

    def receive(self, n):
        """Called by the network layer for every notification we get."""
        if n.sender not in self.server_ids:
            return
        if self.state == ServerState.LOOKING:
            self.inbox.put_nowait(n)
        elif n.state == ServerState.LOOKING:
            # We already know the leader: tell the LOOKING server about it.
            self.transport.send(n.sender, self._my_notification())

    # ------------------------------------------------------------ the election

    async def look_for_leader(self):
        """Run one election and return the winning Vote."""
        self.state = ServerState.LOOKING
        self.round += 1
        self.vote = self._my_own_vote()
        self._drain_inbox()
        log.info("S%d LOOKING round=%d, voting for myself %s", self.my_id, self.round, self.vote)

        votes = {self.my_id: self.vote}  # LOOKING servers' votes in my round
        established = {}                 # sid -> (vote, state) of FOLLOWING/LEADING servers
        self._broadcast()
        wait = self.finalize_wait

        # A one-server "cluster" is its own majority; nobody will ever write to it.
        if self._has_quorum(votes, self.vote) and await self._no_better_vote_arrives():
            return self._decide(self.vote)

        while True:
            n = await self._next_notification(wait)
            if n is None:
                # Nobody talked to us: send our vote again, back off a bit.
                self._broadcast()
                wait = min(wait * 2, self.max_interval)
                continue

            if n.state == ServerState.LOOKING:
                if n.round > self.round:
                    # We are behind: join the newer round and start over.
                    self.round = n.round
                    votes = {}
                    self.vote = best_vote(n.vote, self._my_own_vote())
                    self._broadcast()
                elif n.round < self.round:
                    # The sender is behind: send it our vote so it catches up.
                    self.transport.send(n.sender, self._my_notification())
                    continue
                elif n.vote.is_better_than(self.vote):
                    self.vote = n.vote
                    self._broadcast()
                elif n.vote != self.vote:
                    # The sender holds a worse vote; it may have missed ours
                    # (e.g. it started after we broadcast), so tell it directly.
                    self.transport.send(n.sender, self._my_notification())

                votes[n.sender] = n.vote
                votes[self.my_id] = self.vote

                if self._has_quorum(votes, self.vote) and await self._no_better_vote_arrives():
                    return self._decide(self.vote)

            else:
                # The sender already follows or leads somebody.
                established[n.sender] = (n.vote, n.state)
                same_round = n.round == self.round
                if same_round:
                    votes[n.sender] = n.vote
                    if (self._has_quorum(votes, n.vote)
                            and self._leader_confirmed(n.vote, established, same_round)):
                        return self._decide(n.vote)
                est_votes = {sid: v for sid, (v, _) in established.items()}
                if (self._has_quorum(est_votes, n.vote)
                        and self._leader_confirmed(n.vote, established, same_round)):
                    self.round = n.round
                    return self._decide(n.vote)

    # ------------------------------------------------------------ helpers

    def _my_own_vote(self):
        return Vote(leader=self.my_id, zxid=self.get_last_zxid(), epoch=self.get_epoch())

    def _my_notification(self):
        return Notification(sender=self.my_id, state=self.state, vote=self.vote, round=self.round)

    def _broadcast(self):
        self.transport.broadcast(self._my_notification())

    def _has_quorum(self, votes, vote):
        return sum(1 for v in votes.values() if v == vote) >= self.quorum

    def _leader_confirmed(self, vote, established, same_round):
        """Is it safe to accept `vote.leader` as the leader?

        * Someone else: only if it told us itself that it is LEADING. This
          stops us from following a dead leader just because its old
          followers have not noticed yet.
        * Me: only if the majority decided in *this* election round. This
          happens when the others finished the election before their last
          LOOKING message reached me; without this rule I would never lead.
        """
        if vote.leader == self.my_id:
            return same_round
        entry = established.get(vote.leader)
        return entry is not None and entry[1] == ServerState.LEADING

    async def _no_better_vote_arrives(self):
        """Wait `finalize_wait`; give up early if a better vote shows up."""
        while True:
            n = await self._next_notification(self.finalize_wait)
            if n is None:
                return True
            if (n.state == ServerState.LOOKING and n.round == self.round
                    and n.vote.is_better_than(self.vote)):
                self.inbox.put_nowait(n)  # handle it in the main loop
                return False

    def _decide(self, vote):
        self.vote = vote
        self.state = ServerState.LEADING if vote.leader == self.my_id else ServerState.FOLLOWING
        self._drain_inbox()
        log.info("S%d decided: %s, leader=S%d (round %d)",
                 self.my_id, self.state.name, vote.leader, self.round)
        return vote

    async def _next_notification(self, timeout):
        try:
            return await asyncio.wait_for(self.inbox.get(), timeout)
        except asyncio.TimeoutError:
            return None

    def _drain_inbox(self):
        while not self.inbox.empty():
            self.inbox.get_nowait()
