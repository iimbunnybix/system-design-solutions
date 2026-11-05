import random
import unittest

from solution import (Crash, Inventory, Payments, SagaLog, SagaOrchestrator, Shipping,
                      TransientError, naive_place_order, order_steps)

N = 1_000


def scenario(seed):
    rng = random.Random(seed)
    ids = [f"o{i}" for i in range(N)]
    declines = {i for i in ids if rng.random() < 0.08}
    unserviceable = {i for i in ids if rng.random() < 0.02}
    return ids, declines, unserviceable


def check_invariants(tc, ids, inv, pay, ship, start_stock):
    """Every order is either fully placed or fully undone."""
    placed = 0
    for i in ids:
        reserved = i in inv.reservations
        charged = pay.net_charged(i) > 0
        shipped = i in ship.shipments
        tc.assertIn((reserved, charged, shipped), [(True, True, True), (False, False, False)], i)
        placed += reserved
    tc.assertEqual(inv.stock["sku"], start_stock - placed * 1)
    return placed


class ReproduceLeakedState(unittest.TestCase):
    def test_failed_orders_keep_stock_and_money(self):
        ids, declines, unserviceable = scenario(0)
        inv, pay, ship = Inventory({"sku": 10_000}), Payments(declines), Shipping(unserviceable)
        ok = sum(naive_place_order(inv, pay, ship, i, "sku", 1, 500) for i in ids)
        leaked_stock = len(inv.reservations) - ok
        charged_not_shipped = sum(1 for i in pay.charges if i not in ship.shipments)
        self.assertEqual(leaked_stock, 116)
        self.assertEqual(charged_not_shipped, 19)
        self.assertEqual(inv.stock["sku"], 10_000 - len(inv.reservations))


class Saga(unittest.TestCase):
    def test_failures_are_compensated(self):
        ids, declines, unserviceable = scenario(0)
        inv, pay, ship = Inventory({"sku": 10_000}), Payments(declines), Shipping(unserviceable)
        orch = SagaOrchestrator(SagaLog())
        results = [orch.run(i, order_steps(inv, pay, ship, "sku", 1, 500)) for i in ids]
        placed = check_invariants(self, ids, inv, pay, ship, 10_000)
        self.assertEqual(placed, sum(r.status == "completed" for r in results))
        self.assertEqual(placed, 884)
        self.assertEqual({r.status for r in results}, {"completed", "compensated"})
        refunded = [r for r in results if r.error and r.error.startswith("create_shipment")]
        self.assertTrue(all(r.saga_id in pay.refunds for r in refunded))

    def test_compensation_runs_in_reverse_order(self):
        calls = []
        inv, pay, ship = Inventory({"sku": 5}), Payments(), Shipping({"o1"})
        steps = order_steps(inv, pay, ship, "sku", 1, 100)
        steps = [s.__class__(s.name, s.action,
                             (lambda f, n: (lambda x: (calls.append(n), f(x))))(s.compensate, s.name))
                 for s in steps]
        rec = SagaOrchestrator(SagaLog()).run("o1", steps)
        self.assertEqual(rec.status, "compensated")
        self.assertEqual(calls, ["charge_card", "reserve_stock"])

    def test_out_of_stock_needs_no_compensation(self):
        inv, pay, ship = Inventory({"sku": 0}), Payments(), Shipping()
        rec = SagaOrchestrator(SagaLog()).run("o1", order_steps(inv, pay, ship, "sku", 1, 100))
        self.assertEqual((rec.status, rec.compensated), ("compensated", []))
        self.assertEqual(pay.charges, {})

    def test_transient_errors_retried(self):
        inv, pay, ship = Inventory({"sku": 5}), Payments(), Shipping()
        flaky = {"n": 0}
        real = pay.charge

        def charge(saga_id, amount):
            flaky["n"] += 1
            if flaky["n"] <= 2:
                raise TransientError("503")
            real(saga_id, amount)
        pay.charge = charge
        rec = SagaOrchestrator(SagaLog()).run("o1", order_steps(inv, pay, ship, "sku", 1, 100))
        self.assertEqual(rec.status, "completed")
        self.assertEqual(flaky["n"], 3)

    def test_transient_exhausted_compensates_that_step_too(self):
        inv, pay, ship = Inventory({"sku": 5}), Payments(), Shipping()
        real = pay.charge

        def charge(saga_id, amount):
            real(saga_id, amount)            # it DID charge, then timed out
            raise TransientError("timeout")
        pay.charge = charge
        rec = SagaOrchestrator(SagaLog(), max_attempts=2).run(
            "o1", order_steps(inv, pay, ship, "sku", 1, 100))
        self.assertEqual(rec.status, "compensated")
        self.assertEqual(pay.net_charged("o1"), 0)
        self.assertEqual(inv.stock["sku"], 5)


class CrashRecovery(unittest.TestCase):
    def test_crash_at_every_point_then_recover(self):
        points = [(p, s) for s in ("reserve_stock", "charge_card", "create_shipment")
                  for p in ("before", "after", "compensate")]
        for fail_shipping in (False, True):
            for point in points:
                inv = Inventory({"sku": 10})
                pay = Payments()
                ship = Shipping({"o1"} if fail_shipping else set())
                log = SagaLog()

                def hook(saga, phase, step, point=point):
                    if (phase, step) == point:
                        raise Crash()
                steps = lambda _s: order_steps(inv, pay, ship, "sku", 1, 100)
                try:
                    SagaOrchestrator(log, crash_hook=hook).run("o1", steps("o1"))
                except Crash:
                    pass
                SagaOrchestrator(log).recover(steps)       # new process
                self.assertEqual(log.unfinished(), [], point)
                check_invariants(self, ["o1"], inv, pay, ship, 10)
                expected = "compensated" if fail_shipping else "completed"
                self.assertEqual(log.records["o1"].status, expected, point)

    def test_recover_is_noop_when_all_done(self):
        log = SagaLog()
        inv, pay, ship = Inventory({"sku": 1}), Payments(), Shipping()
        SagaOrchestrator(log).run("o1", order_steps(inv, pay, ship, "sku", 1, 100))
        self.assertEqual(SagaOrchestrator(log).recover(lambda s: []), [])


if __name__ == "__main__":
    unittest.main()
