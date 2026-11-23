import random
import unittest

from solution import DeltaSyncReplica, MaxTotalReplica, PNCounterReplica, merge_counts

REGIONS = ["eu", "us", "ap"]
LIKES = 3_000
SYNC_EVERY = 50              # a sync round after every 50 likes
REDELIVERY_RATE = 0.2        # share of sync messages delivered twice (lost ack, retry)
SEED = 11


def pick_regions(rng):
    """Which region accepts each like, decided once so all versions see the same traffic."""
    return [rng.choice(REGIONS) for _ in range(LIKES)]


def deliveries(rng):
    """1 normally, 2 when the ack was lost and the sender retried."""
    return 2 if rng.random() < REDELIVERY_RATE else 1


def run_delta_sync(seed=SEED):
    rng = random.Random(seed)
    replicas = {region: DeltaSyncReplica(region) for region in REGIONS}
    for index, region in enumerate(pick_regions(rng), start=1):
        replicas[region].like()
        if index % SYNC_EVERY == 0:
            for sender in replicas.values():
                delta = sender.take_delta()
                for receiver in replicas.values():
                    if receiver is not sender:
                        for _ in range(deliveries(rng)):
                            receiver.receive_delta(delta)
    return replicas


def run_max_total(seed=SEED):
    rng = random.Random(seed)
    replicas = {region: MaxTotalReplica(region) for region in REGIONS}
    for index, region in enumerate(pick_regions(rng), start=1):
        replicas[region].like()
        if index % SYNC_EVERY == 0:
            totals = {name: replica.total for name, replica in replicas.items()}
            for receiver in replicas.values():
                for total in totals.values():
                    receiver.receive_total(total)
    return replicas


def run_pn_counter(seed=SEED, shuffle_messages=False):
    rng = random.Random(seed)
    replicas = {region: PNCounterReplica(region) for region in REGIONS}
    for index, region in enumerate(pick_regions(rng), start=1):
        replicas[region].like()
        if index % SYNC_EVERY == 0:
            messages = []
            for sender in replicas.values():
                for receiver in replicas.values():
                    if receiver is not sender:
                        for _ in range(deliveries(rng)):
                            messages.append((receiver, sender.state()))
            if shuffle_messages:
                rng.shuffle(messages)
            for receiver, state in messages:
                receiver.receive_state(state)
    return replicas


class ReproduceProblem(unittest.TestCase):
    def test_redelivered_delta_is_counted_twice(self):
        eu, us = DeltaSyncReplica("eu"), DeltaSyncReplica("us")
        eu.like()
        delta = eu.take_delta()
        us.receive_delta(delta)
        us.receive_delta(delta)
        self.assertEqual(us.total, 2)

    def test_retries_inflate_every_regions_count(self):
        for replica in run_delta_sync().values():
            self.assertGreater(replica.total, LIKES)

    def test_max_of_totals_loses_concurrent_likes(self):
        eu, us = MaxTotalReplica("eu"), MaxTotalReplica("us")
        eu.like()
        us.like()
        eu.receive_total(us.total)
        self.assertEqual(eu.total, 1)

    def test_max_of_totals_undercounts_the_whole_run(self):
        for replica in run_max_total().values():
            self.assertLess(replica.total, LIKES)


class ReadmeNumbers(unittest.TestCase):
    def test_delta_sync_totals_quoted_in_readme(self):
        totals = sorted(replica.total for replica in run_delta_sync().values())
        self.assertEqual(totals, [3_439, 3_441, 3_480])

    def test_max_total_quoted_in_readme(self):
        totals = {replica.total for replica in run_max_total().values()}
        self.assertEqual(totals, {1_201})


class FixPNCounter(unittest.TestCase):
    def test_duplicate_state_is_merged_once(self):
        eu, us = PNCounterReplica("eu"), PNCounterReplica("us")
        eu.like()
        us.receive_state(eu.state())
        us.receive_state(eu.state())
        self.assertEqual(us.total, 1)

    def test_concurrent_likes_in_two_regions_are_both_kept(self):
        eu, us = PNCounterReplica("eu"), PNCounterReplica("us")
        eu.like()
        us.like()
        eu.receive_state(us.state())
        us.receive_state(eu.state())
        self.assertEqual((eu.total, us.total), (2, 2))

    def test_stale_state_arriving_late_changes_nothing(self):
        eu, us = PNCounterReplica("eu"), PNCounterReplica("us")
        eu.like()
        old_state = eu.state()
        eu.like()
        us.receive_state(eu.state())
        us.receive_state(old_state)
        self.assertEqual(us.total, 2)

    def test_all_regions_converge_to_the_true_total_despite_retries(self):
        for replica in run_pn_counter().values():
            self.assertEqual(replica.total, LIKES)

    def test_message_order_does_not_matter(self):
        for replica in run_pn_counter(shuffle_messages=True).values():
            self.assertEqual(replica.total, LIKES)

    def test_unlikes_are_subtracted_and_survive_merges(self):
        eu, us = PNCounterReplica("eu"), PNCounterReplica("us")
        eu.like()
        eu.like()
        us.unlike()
        eu.receive_state(us.state())
        eu.receive_state(us.state())
        self.assertEqual(eu.total, 1)

    def test_merge_does_not_modify_its_inputs(self):
        mine, theirs = {"eu": 3}, {"eu": 5, "us": 1}
        merged = merge_counts(mine, theirs)
        self.assertEqual(merged, {"eu": 5, "us": 1})
        self.assertEqual(mine, {"eu": 3})


if __name__ == "__main__":
    unittest.main()
