import random
import unittest

from solution import FixedTimeoutDetector, PhiAccrualDetector

SEED = 20
HEARTBEATS = 600                    # ten minutes at one heartbeat per second
GC_PAUSE_EVERY = 20                 # every 20th heartbeat is delayed by a GC pause
GC_PAUSE_SECONDS = 4.0
CHECK_STEP_SECONDS = 0.1            # how often the cluster asks "is it alive?"


def gc_pausing_node() -> list[float]:
    """Heartbeat times of a healthy node: ~1 s apart, with a 4 s gap every 20th beat."""
    rng = random.Random(SEED)
    now, beats = 0.0, []
    for index in range(HEARTBEATS):
        if index % GC_PAUSE_EVERY == GC_PAUSE_EVERY - 1:
            now += GC_PAUSE_SECONDS
        else:
            now += rng.uniform(0.9, 1.1)
        beats.append(now)
    return beats


def steady_node() -> list[float]:
    """Heartbeat times of a node that never pauses."""
    rng = random.Random(SEED)
    now, beats = 0.0, []
    for _ in range(HEARTBEATS):
        now += rng.uniform(0.95, 1.05)
        beats.append(now)
    return beats


def count_false_failovers(detector, beats: list[float]) -> int:
    """Replay heartbeats and count separate periods in which a live node was suspected."""
    episodes, suspected_before, next_beat, now = 0, False, 0, 0.0
    while now < beats[-1]:
        while next_beat < len(beats) and beats[next_beat] <= now:
            detector.heartbeat(beats[next_beat])
            next_beat += 1
        suspected = detector.is_suspected(now)
        if suspected and not suspected_before:
            episodes += 1
        suspected_before = suspected
        now = round(now + CHECK_STEP_SECONDS, 6)
    return episodes


def seconds_until_suspected(detector, beats: list[float]) -> float:
    """Feed all heartbeats, then let the node die and measure how long detection takes."""
    for beat in beats:
        detector.heartbeat(beat)
    now = beats[-1]
    while not detector.is_suspected(now):
        now = round(now + CHECK_STEP_SECONDS, 6)
    return now - beats[-1]


class ReproduceProblem(unittest.TestCase):
    def test_fixed_timeout_fails_over_on_every_gc_pause(self):
        failovers = count_false_failovers(FixedTimeoutDetector(), gc_pausing_node())
        self.assertEqual(failovers, HEARTBEATS // GC_PAUSE_EVERY)


class FixWithPhiAccrual(unittest.TestCase):
    def test_gc_pauses_stop_causing_failovers_once_rhythm_is_learned(self):
        failovers = count_false_failovers(PhiAccrualDetector(), gc_pausing_node())
        # Only the first two pauses, before any pause is in the history, are flagged.
        self.assertEqual(failovers, 2)

    def test_dead_gc_pausing_node_is_still_detected(self):
        detection = seconds_until_suspected(PhiAccrualDetector(), gc_pausing_node())
        self.assertAlmostEqual(detection, 4.9, places=6)

    def test_steady_node_is_detected_faster_than_a_jittery_one(self):
        steady = seconds_until_suspected(PhiAccrualDetector(), steady_node())
        jittery = seconds_until_suspected(PhiAccrualDetector(), gc_pausing_node())
        self.assertAlmostEqual(steady, 3.9, places=6)
        self.assertLess(steady, jittery)

    def test_phi_rises_the_longer_the_node_is_silent(self):
        detector = PhiAccrualDetector()
        for beat in steady_node():
            detector.heartbeat(beat)
        last = detector.last_heartbeat
        readings = [detector.phi(last + gap) for gap in (0.5, 1.5, 2.5, 3.5)]
        self.assertEqual(readings, sorted(readings))
        self.assertLess(readings[0], 1.0)

    def test_phi_stays_finite_after_a_very_long_silence(self):
        detector = PhiAccrualDetector()
        for beat in steady_node():
            detector.heartbeat(beat)
        self.assertLess(detector.phi(detector.last_heartbeat + 10_000), 1000)

    def test_never_heard_from_node_is_not_suspected(self):
        self.assertFalse(PhiAccrualDetector().is_suspected(now=100.0))

    def test_thin_history_falls_back_to_the_fixed_timeout(self):
        detector = PhiAccrualDetector()
        detector.heartbeat(0.0)
        detector.heartbeat(1.0)
        self.assertFalse(detector.is_suspected(3.9))
        self.assertTrue(detector.is_suspected(4.1))


if __name__ == "__main__":
    unittest.main()
