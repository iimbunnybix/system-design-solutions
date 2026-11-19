import unittest

from solution import Replica, orphan_replies

REGIONS = ["eu", "us", "apac"]


def make_regions():
    return {name: Replica(name, REGIONS) for name in REGIONS}


def question_and_answer(regions):
    """EU asks, the question reaches the US, the US answers."""
    question = regions["eu"].local_write("q1", "How do I reset my router?")
    regions["us"].receive(question)
    answer = regions["us"].local_write("a1", "Hold the button for 10 s", parent_id="q1")
    return question, answer


class ReproduceProblem(unittest.TestCase):
    def test_answer_arriving_first_is_shown_without_its_question(self):
        regions = make_regions()
        question, answer = question_and_answer(regions)
        regions["apac"].naive_receive(answer)
        self.assertEqual(orphan_replies(regions["apac"]), ["a1"])


class FixCausalDelivery(unittest.TestCase):
    def test_answer_waits_until_its_question_arrives(self):
        regions = make_regions()
        question, answer = question_and_answer(regions)
        apac = regions["apac"]
        apac.receive(answer)
        self.assertNotIn("a1", apac.comments)
        self.assertEqual(len(apac.pending), 1)

    def test_question_arrival_releases_the_waiting_answer(self):
        regions = make_regions()
        question, answer = question_and_answer(regions)
        apac = regions["apac"]
        apac.receive(answer)
        apac.receive(question)
        self.assertEqual(apac.apply_order, ["q1", "a1"])
        self.assertEqual(apac.pending, [])

    def test_no_orphan_is_ever_visible_during_delivery(self):
        regions = make_regions()
        question, answer = question_and_answer(regions)
        apac = regions["apac"]
        for write in (answer, question):
            apac.receive(write)
            self.assertEqual(orphan_replies(apac), [])

    def test_gap_in_one_regions_sequence_is_waited_for(self):
        regions = make_regions()
        first = regions["eu"].local_write("q1", "first")
        second = regions["eu"].local_write("q2", "second")
        us = regions["us"]
        us.receive(second)
        self.assertNotIn("q2", us.comments)
        us.receive(first)
        self.assertEqual(us.apply_order, ["q1", "q2"])

    def test_duplicate_delivery_is_ignored(self):
        regions = make_regions()
        question = regions["eu"].local_write("q1", "question")
        us = regions["us"]
        us.receive(question)
        us.receive(question)
        self.assertEqual(us.apply_order, ["q1"])
        self.assertEqual(us.pending, [])

    def test_unrelated_writes_apply_in_arrival_order(self):
        regions = make_regions()
        eu_post = regions["eu"].local_write("e1", "from eu")
        us_post = regions["us"].local_write("u1", "from us")
        apac = regions["apac"]
        apac.receive(us_post)
        apac.receive(eu_post)
        self.assertEqual(apac.apply_order, ["u1", "e1"])


if __name__ == "__main__":
    unittest.main()
