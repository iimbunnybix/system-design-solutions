import unittest

from solution import (ALIVE, DEAD, SUSPECT, GossipMessage, MembershipTable, MemberState,
                      NaiveMembershipTable, count_status_flips, overrides)

CRASHED_NODE = "cache-c"


def alive(incarnation: int = 0) -> MemberState:
    return MemberState(ALIVE, incarnation)


def suspect(incarnation: int = 0) -> MemberState:
    return MemberState(SUSPECT, incarnation)


def dead(incarnation: int = 0) -> MemberState:
    return MemberState(DEAD, incarnation)


def crash_then_stale_gossip(stale_rounds: int) -> list[GossipMessage]:
    """cache-c is seen alive, then declared dead; peers that missed the news
    keep repeating "alive", interleaved with peers repeating "dead"."""
    messages = [GossipMessage(CRASHED_NODE, alive()), GossipMessage(CRASHED_NODE, dead())]
    for _ in range(stale_rounds):
        messages.append(GossipMessage(CRASHED_NODE, alive()))
        messages.append(GossipMessage(CRASHED_NODE, dead()))
    messages.append(GossipMessage(CRASHED_NODE, alive()))
    return messages


def observed_statuses(table, messages: list[GossipMessage]) -> list[str]:
    statuses = []
    for message in messages:
        table.apply(message)
        statuses.append(table.members[CRASHED_NODE].status)
    return statuses


class ReproduceProblem(unittest.TestCase):
    def test_stale_alive_message_revives_dead_node(self):
        table = NaiveMembershipTable()
        table.apply(GossipMessage(CRASHED_NODE, dead()))
        table.apply(GossipMessage(CRASHED_NODE, alive()))
        self.assertIn(CRASHED_NODE, table.routable_members())

    def test_mixed_gossip_makes_dead_node_flap(self):
        statuses = observed_statuses(NaiveMembershipTable(), crash_then_stale_gossip(10))
        self.assertEqual(count_status_flips(statuses), 22)
        self.assertEqual(statuses[-1], ALIVE)


class FixPrecedenceRules(unittest.TestCase):
    def test_dead_beats_alive_at_same_incarnation(self):
        self.assertTrue(overrides(alive(3), dead(3)))
        self.assertFalse(overrides(dead(3), alive(3)))

    def test_suspect_beats_alive_at_same_incarnation(self):
        self.assertTrue(overrides(alive(3), suspect(3)))

    def test_higher_incarnation_alive_beats_suspect(self):
        self.assertTrue(overrides(suspect(3), alive(4)))

    def test_older_incarnation_never_wins(self):
        self.assertFalse(overrides(alive(4), dead(3)))

    def test_repeated_identical_message_is_not_new(self):
        self.assertFalse(overrides(dead(2), dead(2)))


class FixMembershipTable(unittest.TestCase):
    def test_stale_alive_message_does_not_revive_dead_node(self):
        table = MembershipTable("cache-a")
        table.apply(GossipMessage(CRASHED_NODE, dead()))
        table.apply(GossipMessage(CRASHED_NODE, alive()))
        self.assertNotIn(CRASHED_NODE, table.routable_members())

    def test_mixed_gossip_settles_on_dead_after_one_flip(self):
        statuses = observed_statuses(MembershipTable("cache-a"), crash_then_stale_gossip(10))
        self.assertEqual(count_status_flips(statuses), 1)
        self.assertEqual(statuses[-1], DEAD)

    def test_restarted_node_rejoins_with_higher_incarnation(self):
        table = MembershipTable("cache-a")
        table.apply(GossipMessage(CRASHED_NODE, dead(0)))
        table.apply(GossipMessage(CRASHED_NODE, alive(1)))
        self.assertEqual(table.status_of(CRASHED_NODE), ALIVE)

    def test_unknown_member_is_added(self):
        table = MembershipTable("cache-a")
        table.apply(GossipMessage("cache-new", alive()))
        self.assertEqual(table.routable_members(), ["cache-a", "cache-new"])

    def test_changed_information_is_forwarded_and_duplicates_are_not(self):
        table = MembershipTable("cache-a")
        message = GossipMessage(CRASHED_NODE, dead())
        self.assertEqual(table.apply(message), message)
        self.assertIsNone(table.apply(message))

    def test_suspected_member_still_receives_traffic(self):
        table = MembershipTable("cache-a")
        table.apply(GossipMessage("cache-b", suspect()))
        self.assertIn("cache-b", table.routable_members())


class FixSelfRefutation(unittest.TestCase):
    def test_live_node_refutes_suspicion_with_higher_incarnation(self):
        table = MembershipTable("cache-b")
        reply = table.apply(GossipMessage("cache-b", suspect(0)))
        self.assertEqual(reply, GossipMessage("cache-b", alive(1)))
        self.assertEqual(table.status_of("cache-b"), ALIVE)

    def test_refutation_overrides_suspicion_on_other_nodes(self):
        observer = MembershipTable("cache-a")
        observer.apply(GossipMessage("cache-b", suspect(0)))
        refutation = MembershipTable("cache-b").apply(GossipMessage("cache-b", suspect(0)))
        observer.apply(refutation)
        self.assertEqual(observer.status_of("cache-b"), ALIVE)

    def test_old_rumour_about_self_is_ignored(self):
        table = MembershipTable("cache-b")
        table.apply(GossipMessage("cache-b", suspect(0)))
        self.assertIsNone(table.apply(GossipMessage("cache-b", dead(0))))
        self.assertEqual(table.self_incarnation, 1)

    def test_alive_rumour_about_self_needs_no_reply(self):
        table = MembershipTable("cache-b")
        self.assertIsNone(table.apply(GossipMessage("cache-b", alive(0))))


if __name__ == "__main__":
    unittest.main()
