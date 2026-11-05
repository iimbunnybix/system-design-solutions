import random
import unittest

from solution import Job, NaiveScheduler, TimingWheelScheduler

SEED = 18
PENDING_JOBS = 20_000
MAX_DELAY_SECONDS = 2 * 24 * 3600          # reminders up to two days out
OBSERVED_TICKS = 60


def random_jobs(count: int, max_delay: int) -> list[Job]:
    """Deterministic jobs with due times spread over the next `max_delay` seconds."""
    rng = random.Random(SEED)
    jobs = []
    for index in range(count):
        jobs.append(Job(rng.randint(1, max_delay), f"job-{index}"))
    return jobs


def load(scheduler, jobs: list[Job]):
    for job in jobs:
        scheduler.schedule(job)
    return scheduler


def run_ticks(scheduler, ticks: int) -> dict[str, int]:
    """Tick `ticks` times and record the tick at which each job fired."""
    fired_at: dict[str, int] = {}
    for _ in range(ticks):
        for job in scheduler.tick():
            fired_at[job.job_id] = scheduler.current_tick
    return fired_at


class ReproduceProblem(unittest.TestCase):
    def test_naive_scheduler_examines_every_pending_job_each_tick(self):
        scheduler = load(NaiveScheduler(), random_jobs(PENDING_JOBS, MAX_DELAY_SECONDS))
        run_ticks(scheduler, OBSERVED_TICKS)
        self.assertGreater(scheduler.jobs_examined, PENDING_JOBS * (OBSERVED_TICKS - 1))

    def test_naive_work_grows_with_queue_size_not_with_due_jobs(self):
        small = load(NaiveScheduler(), random_jobs(1_000, MAX_DELAY_SECONDS))
        large = load(NaiveScheduler(), random_jobs(10_000, MAX_DELAY_SECONDS))
        run_ticks(small, OBSERVED_TICKS)
        run_ticks(large, OBSERVED_TICKS)
        self.assertGreater(large.jobs_examined, 9 * small.jobs_examined)


class FixWithTimingWheel(unittest.TestCase):
    def test_wheel_examines_far_fewer_jobs(self):
        jobs = random_jobs(PENDING_JOBS, MAX_DELAY_SECONDS)
        naive = load(NaiveScheduler(), jobs)
        wheel = load(TimingWheelScheduler(), jobs)
        run_ticks(naive, OBSERVED_TICKS)
        run_ticks(wheel, OBSERVED_TICKS)
        self.assertLess(wheel.jobs_examined * 1000, naive.jobs_examined)

    def test_wheel_fires_the_same_jobs_at_the_same_ticks_as_naive(self):
        jobs = random_jobs(2_000, 2 * 3600)
        ticks = 2 * 3600
        naive_fired = run_ticks(load(NaiveScheduler(), jobs), ticks)
        wheel_fired = run_ticks(load(TimingWheelScheduler(), jobs), ticks)
        self.assertEqual(wheel_fired, naive_fired)

    def test_every_job_fires_exactly_at_its_due_tick_across_all_levels(self):
        jobs = random_jobs(3_000, MAX_DELAY_SECONDS)
        wheel = load(TimingWheelScheduler(), jobs)
        fired_at = run_ticks(wheel, MAX_DELAY_SECONDS)
        for job in jobs:
            self.assertEqual(fired_at[job.job_id], job.due_tick)
        self.assertEqual(wheel.pending_count(), 0)

    def test_job_scheduled_in_the_past_fires_on_next_tick(self):
        wheel = TimingWheelScheduler()
        run_ticks(wheel, 10)
        wheel.schedule(Job(3, "late"))
        self.assertEqual([job.job_id for job in wheel.tick()], ["late"])

    def test_jobs_on_level_boundaries_fire_on_time(self):
        wheel = TimingWheelScheduler()
        boundary_delays = [59, 60, 61, 3599, 3600, 3601]
        for delay in boundary_delays:
            wheel.schedule(Job(delay, f"d{delay}"))
        fired_at = run_ticks(wheel, 3700)
        for delay in boundary_delays:
            self.assertEqual(fired_at[f"d{delay}"], delay)

    def test_jobs_scheduled_while_running_fire_on_time(self):
        wheel = TimingWheelScheduler()
        run_ticks(wheel, 125)
        wheel.schedule(Job(125 + 61, "mid-minute"))
        fired_at = run_ticks(wheel, 100)
        self.assertEqual(fired_at["mid-minute"], 186)

    def test_many_jobs_due_in_the_same_second_all_fire(self):
        wheel = TimingWheelScheduler()
        for index in range(500):
            wheel.schedule(Job(90, f"same-{index}"))
        fired_at = run_ticks(wheel, 90)
        self.assertEqual(len(fired_at), 500)


if __name__ == "__main__":
    unittest.main()
