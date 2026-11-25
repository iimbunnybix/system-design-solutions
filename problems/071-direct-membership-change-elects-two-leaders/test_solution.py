import unittest

from solution import (OLD_CLUSTER, TARGET_CLUSTER, ChangeInProgress, MembershipManager,
                      can_elect_two_leaders, disjoint_winning_splits, elect_on_each_side,
                      grow_cluster, naive_membership_plan, plan_is_safe, single_server_plan,
                      wins_election)


def nodes(names: str) -> frozenset[str]:
    """Build a config from a string of one-letter node names, e.g. "ABC"."""
    return frozenset(names)


class ReproduceProblem(unittest.TestCase):
    def test_split_between_old_and_new_config_elects_two_leaders_in_one_term(self):
        leaders = elect_on_each_side(nodes("AB"), OLD_CLUSTER, nodes("CDE"), TARGET_CLUSTER)
        self.assertEqual(leaders, ["side A", "side B"])

    def test_three_different_splits_give_two_leaders_when_jumping_from_3_to_5(self):
        self.assertEqual(len(disjoint_winning_splits(OLD_CLUSTER, TARGET_CLUSTER)), 3)

    def test_naive_plan_is_unsafe(self):
        self.assertFalse(plan_is_safe(naive_membership_plan(OLD_CLUSTER, TARGET_CLUSTER)))

    def test_replacing_a_node_in_one_step_is_also_unsafe(self):
        self.assertTrue(can_elect_two_leaders(nodes("ABC"), nodes("ABD")))


class FixSingleServerChanges(unittest.TestCase):
    def test_plan_adds_one_server_per_step(self):
        plan = single_server_plan(OLD_CLUSTER, TARGET_CLUSTER)
        self.assertEqual(plan, [nodes("ABC"), nodes("ABCD"), nodes("ABCDE")])

    def test_single_server_plan_can_never_elect_two_leaders(self):
        self.assertTrue(plan_is_safe(single_server_plan(OLD_CLUSTER, TARGET_CLUSTER)))

    def test_replacement_is_done_as_add_then_remove(self):
        plan = single_server_plan(nodes("ABC"), nodes("ABD"))
        self.assertEqual(plan, [nodes("ABC"), nodes("ABCD"), nodes("ABD")])
        self.assertTrue(plan_is_safe(plan))

    def test_shrinking_the_cluster_one_server_at_a_time_is_safe(self):
        self.assertTrue(plan_is_safe(single_server_plan(TARGET_CLUSTER, OLD_CLUSTER)))

    def test_same_config_on_both_sides_never_elects_two_leaders(self):
        self.assertFalse(can_elect_two_leaders(TARGET_CLUSTER, TARGET_CLUSTER))

    def test_votes_from_non_members_do_not_count(self):
        self.assertFalse(wins_election(nodes("ADE"), OLD_CLUSTER))


class FixOneChangeAtATime(unittest.TestCase):
    def test_second_change_before_commit_is_refused(self):
        manager = MembershipManager(OLD_CLUSTER)
        manager.propose(nodes("ABCD"))
        with self.assertRaises(ChangeInProgress):
            manager.propose(nodes("ABCDE"))

    def test_change_of_two_servers_is_refused(self):
        manager = MembershipManager(OLD_CLUSTER)
        with self.assertRaises(ValueError):
            manager.propose(TARGET_CLUSTER)

    def test_configs_in_use_during_a_change_cannot_elect_two_leaders(self):
        manager = MembershipManager(OLD_CLUSTER)
        manager.propose(nodes("ABCD"))
        older, newer = manager.configs_in_use()
        self.assertFalse(can_elect_two_leaders(older, newer))
        self.assertFalse(can_elect_two_leaders(newer, older))

    def test_grow_cluster_reaches_the_target_through_committed_steps(self):
        manager = MembershipManager(OLD_CLUSTER)
        history = grow_cluster(manager, TARGET_CLUSTER)
        self.assertEqual(history[-1], TARGET_CLUSTER)
        self.assertEqual(len(history), 3)
        self.assertIsNone(manager.pending)

    def test_commit_without_a_pending_change_is_an_error(self):
        with self.assertRaises(RuntimeError):
            MembershipManager(OLD_CLUSTER).commit()


if __name__ == "__main__":
    unittest.main()
