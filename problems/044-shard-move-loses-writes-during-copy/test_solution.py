import unittest

from solution import (CatchUpTooSlow, LiveTraffic, Router, ShardFrozen, ShardServer,
                      move_with_catch_up, naive_move)

KEY_COUNT = 10_000
WRITES_PER_STEP = 20
SEED = 3


def loaded_shard() -> ShardServer:
    """The old server, holding 10,000 user records."""
    source = ShardServer("old-server")
    for index in range(KEY_COUNT):
        source.apply(f"user:{index}", index)
    return source


def setup(writes_per_step: int = WRITES_PER_STEP) -> tuple[Router, ShardServer, LiveTraffic]:
    """Router pointing at a loaded old server, an empty new server, and live traffic."""
    router = Router(loaded_shard())
    target = ShardServer("new-server")
    traffic = LiveTraffic(router, KEY_COUNT, writes_per_step, SEED)
    return router, target, traffic


def lost_writes(server: ShardServer, traffic: LiveTraffic) -> list[str]:
    """Acknowledged writes whose value is not on the server clients now read from."""
    lost = []
    for key, value in traffic.acknowledged.items():
        if server.data.get(key) != value:
            lost.append(key)
    return lost


class ReproduceProblem(unittest.TestCase):
    def test_copy_then_switch_loses_acknowledged_writes(self):
        router, target, traffic = setup()
        naive_move(router, target, traffic)
        self.assertGreater(len(lost_writes(router.owner, traffic)), 0)

    def test_new_keys_created_during_the_copy_are_missing(self):
        router, target, traffic = setup()
        naive_move(router, target, traffic)
        missing = [key for key in traffic.acknowledged if key not in target.data]
        self.assertGreater(len(missing), 0)


class FixWithChangeLogCatchUp(unittest.TestCase):
    def test_no_acknowledged_write_is_lost(self):
        router, target, traffic = setup()
        move_with_catch_up(router, target, traffic)
        self.assertEqual(lost_writes(router.owner, traffic), [])

    def test_router_points_at_new_server_and_is_unfrozen(self):
        router, target, traffic = setup()
        move_with_catch_up(router, target, traffic)
        self.assertIs(router.owner, target)
        self.assertFalse(router.frozen)

    def test_new_server_holds_everything_the_old_one_had(self):
        router, target, traffic = setup()
        source = router.owner
        move_with_catch_up(router, target, traffic)
        self.assertEqual(target.data, source.data)

    def test_only_writes_during_the_short_freeze_are_refused(self):
        router, target, traffic = setup()
        move_with_catch_up(router, target, traffic)
        self.assertEqual(traffic.refused, WRITES_PER_STEP)

    def test_writes_after_the_move_go_to_the_new_server(self):
        router, target, traffic = setup()
        move_with_catch_up(router, target, traffic)
        router.write("user:1", -1)
        self.assertEqual(target.data["user:1"], -1)

    def test_frozen_router_refuses_writes(self):
        router = Router(ShardServer("s"), frozen=True)
        with self.assertRaises(ShardFrozen):
            router.write("user:1", 1)

    def test_move_of_empty_shard_succeeds(self):
        router = Router(ShardServer("empty"))
        target = ShardServer("new")
        traffic = LiveTraffic(router, existing_keys=1, writes_per_step=0, seed=SEED)
        move_with_catch_up(router, target, traffic)
        self.assertIs(router.owner, target)

    def test_move_gives_up_when_writes_outrun_the_replay(self):
        router, target, traffic = setup(writes_per_step=200)
        with self.assertRaises(CatchUpTooSlow):
            move_with_catch_up(router, target, traffic)
        self.assertFalse(router.frozen)


if __name__ == "__main__":
    unittest.main()
