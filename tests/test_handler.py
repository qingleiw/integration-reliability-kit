import logging
import unittest

from reliability import ReliableHandler, Store
from reliability.testing import FlakyTarget


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def make(script=(), *, honors=True, **kw):
    sleeps = []
    clock = kw.pop("clock", None) or Clock()
    target = FlakyTarget(script, honors_idempotency_key=honors)
    handler = ReliableHandler(
        Store(), target, sleep=sleeps.append, jitter=lambda: 1.0, clock=clock, **kw
    )
    return handler, target, sleeps, clock


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class Delivery(unittest.TestCase):
    def test_first_delivery(self):
        handler, target, sleeps, _ = make()
        out = handler.handle("m1", {"id": "A1"}, business_key="WB-1")
        self.assertEqual(out.status, "done")
        self.assertEqual(out.attempts, 1)
        self.assertEqual(target.written_ids(), ["A1"])
        self.assertEqual(sleeps, [])

    def test_same_key_is_not_delivered_twice(self):
        handler, target, _, _ = make()
        first = handler.handle("m1", {"id": "A1"})
        second = handler.handle("m1", {"id": "A1"})
        self.assertEqual(first.status, "done")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.result, first.result)
        self.assertEqual(target.calls, 1)

    def test_different_keys_are_independent(self):
        handler, target, _, _ = make()
        handler.handle("m1", {"id": "A1"})
        handler.handle("m2", {"id": "A2"})
        self.assertEqual(target.written_ids(), ["A1", "A2"])


class Retries(unittest.TestCase):
    def test_transient_failures_are_retried_with_backoff(self):
        handler, target, sleeps, _ = make(["transient", "transient", "ok"])
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "done")
        self.assertEqual(out.attempts, 3)
        self.assertEqual(sleeps, [0.5, 1.0])
        self.assertEqual(target.written_ids(), ["A1"])

    def test_retries_are_bounded_and_the_message_is_kept(self):
        handler, target, sleeps, _ = make(["transient"] * 4)
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "dead")
        self.assertEqual(out.attempts, 4)
        self.assertEqual(target.calls, 4)
        self.assertEqual(sleeps, [0.5, 1.0, 2.0])
        dead = handler.store.dead_letters()
        self.assertEqual([r["key"] for r in dead], ["m1"])
        self.assertIn("TransientError", dead[0]["last_error"])

    def test_permanent_error_is_not_retried(self):
        handler, target, sleeps, _ = make(["permanent"])
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "dead")
        self.assertEqual(target.calls, 1)
        self.assertEqual(sleeps, [])

    def test_unknown_exception_is_treated_as_permanent(self):
        def broken(payload, key):
            raise ValueError("bad field")

        handler = ReliableHandler(Store(), broken, sleep=lambda s: None)
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "dead")
        self.assertEqual(out.attempts, 1)

    def test_dead_message_is_not_retried_by_a_new_arrival(self):
        handler, target, _, _ = make(["permanent"])
        handler.handle("m1", {"id": "A1"})
        again = handler.handle("m1", {"id": "A1"})
        self.assertEqual(again.status, "dead")
        self.assertEqual(target.calls, 1)


class Replay(unittest.TestCase):
    def test_replay_delivers_once_and_only_once(self):
        handler, target, _, _ = make(["transient"] * 4)  # then the target recovers
        self.assertEqual(handler.handle("m1", {"id": "A1"}).status, "dead")

        replayed = handler.replay("m1")
        self.assertEqual(replayed.status, "done")
        self.assertEqual(target.written_ids(), ["A1"])

        again = handler.replay("m1")
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(target.written_ids(), ["A1"])

    def test_replay_all(self):
        handler, target, _, _ = make(["permanent", "permanent"])
        handler.handle("m1", {"id": "A1"})
        handler.handle("m2", {"id": "A2"})
        outcomes = handler.replay_all()
        self.assertEqual([o.status for o in outcomes], ["done", "done"])
        self.assertEqual(sorted(target.written_ids()), ["A1", "A2"])
        self.assertEqual(handler.store.dead_letters(), [])

    def test_replay_of_unknown_key_raises(self):
        handler, _, _, _ = make()
        with self.assertRaises(KeyError):
            handler.replay("nope")


class LostAnswer(unittest.TestCase):
    """The write reached the target but the answer never came back."""

    def test_idempotency_key_prevents_a_double_write(self):
        handler, target, _, _ = make(["timeout_after_write"])
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "done")
        self.assertEqual(out.attempts, 2)
        self.assertEqual(target.written_ids(), ["A1"])

    def test_without_it_the_retry_writes_twice(self):
        # Documents the failure the key exists to prevent. A target that ignores the key
        # cannot be retried safely, and then reconciliation is what catches the damage.
        handler, target, _, _ = make(["timeout_after_write"], honors=False)
        handler.handle("m1", {"id": "A1"})
        self.assertEqual(target.written_ids(), ["A1", "A1"])


class Workers(unittest.TestCase):
    def test_a_live_claim_blocks_a_second_worker(self):
        handler, target, _, clock = make()
        handler.store.claim("m1", {"id": "A1"}, None, "other", clock(), 60)
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "in_progress")
        self.assertEqual(target.calls, 0)

    def test_an_expired_claim_is_taken_over(self):
        handler, target, _, clock = make()
        handler.store.claim("m1", {"id": "A1"}, None, "other", clock(), 60)
        clock.advance(61)
        out = handler.handle("m1", {"id": "A1"})
        self.assertEqual(out.status, "done")
        self.assertEqual(target.written_ids(), ["A1"])


class Tracing(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("reliability")
        self.logger.setLevel(logging.INFO)
        self.sink = ListHandler()
        self.logger.addHandler(self.sink)
        self.addCleanup(self.logger.removeHandler, self.sink)

    def test_every_line_carries_the_correlation_id_and_business_key(self):
        handler, _, _, _ = make(["transient", "ok"])
        handler.handle("m1", {"id": "A1"}, business_key="WB-1001", correlation_id="abc123")
        self.assertGreaterEqual(len(self.sink.records), 3)
        for record in self.sink.records:
            self.assertEqual(record.correlation_id, "abc123")
            self.assertEqual(record.business_key, "WB-1001")
        events = [r.getMessage() for r in self.sink.records]
        self.assertEqual(events, ["received", "attempt_failed_will_retry", "delivered"])

    def test_an_id_is_minted_when_none_is_given(self):
        handler, _, _, _ = make()
        handler.handle("m1", {"id": "A1"})
        ids = {r.correlation_id for r in self.sink.records}
        self.assertEqual(len(ids), 1)
        self.assertTrue(ids.pop())

    def test_replay_keeps_the_original_correlation_id(self):
        handler, _, _, _ = make(["permanent"])
        handler.handle("m1", {"id": "A1"}, correlation_id="orig-77")
        self.sink.records.clear()
        handler.replay("m1")
        self.assertTrue(self.sink.records)
        for record in self.sink.records:
            self.assertEqual(record.correlation_id, "orig-77")


if __name__ == "__main__":
    unittest.main()
