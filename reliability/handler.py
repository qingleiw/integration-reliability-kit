"""A message handler that is safe to run twice.

handle() takes a stable message key and a payload, calls the target system, and guarantees:

  * the same key is never delivered twice, however often it arrives
  * transient failures are retried a bounded number of times, with backoff
  * a message that cannot be delivered is kept, with its error, not dropped
  * a dead message can be replayed later through exactly the same path
  * every step is logged under the message's correlation ID
"""

import json
import random
import time
from dataclasses import dataclass
from typing import Any, Optional

from .tracing import get_logger, log_event, new_correlation_id, use_correlation_id


class TransientError(Exception):
    """The target could not do it right now. Trying again may work."""


class PermanentError(Exception):
    """The target will never accept this message as it is. Retrying is pointless."""


def is_transient(exc):
    return isinstance(exc, (TransientError, TimeoutError, ConnectionError))


@dataclass
class Outcome:
    status: str                      # done | duplicate | dead | in_progress
    key: str
    result: Any = None
    attempts: int = 0
    error: Optional[str] = None


class ReliableHandler:
    def __init__(
        self,
        store,
        target,
        *,
        max_attempts=4,
        base_delay=0.5,
        max_delay=8.0,
        lease_seconds=60.0,
        sleep=time.sleep,
        jitter=random.random,
        clock=time.time,
        logger=None,
    ):
        """target(payload, idempotency_key) -> result.

        The idempotency key is passed down so the target can de-duplicate on its side too.
        That is what makes a timeout after a successful write safe to retry.
        """
        self.store = store
        self.target = target
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.lease_seconds = lease_seconds
        self._sleep = sleep
        self._jitter = jitter
        self._clock = clock
        self.log = logger or get_logger()

    # -- public -----------------------------------------------------------------------

    def handle(self, key, payload, *, business_key=None, correlation_id=None):
        cid = correlation_id or new_correlation_id()
        with use_correlation_id(cid, business_key):
            state, row = self.store.claim(
                key, payload, business_key, cid, self._clock(), self.lease_seconds
            )
            log_event(self.log, "received", key=key, state=state)
            if state == "duplicate":
                return Outcome("duplicate", key, json.loads(row["result"]), row["attempts"])
            if state == "dead":
                return Outcome("dead", key, attempts=row["attempts"], error=row["last_error"])
            if state == "in_progress":
                return Outcome("in_progress", key, attempts=row["attempts"])
            return self._deliver(key, payload, row["attempts"])

    def replay(self, key):
        """Try a dead message again. A message already delivered is left alone."""
        row = self.store.get(key)
        if row is None:
            raise KeyError(key)
        with use_correlation_id(row["correlation_id"], row["business_key"]):
            if row["status"] == "done":
                log_event(self.log, "replay_skipped_already_done", key=key)
                return Outcome("duplicate", key, json.loads(row["result"]), row["attempts"])
            if row["status"] == "dead":
                self.store.reopen(key, self._clock())
            elif self._clock() - row["updated_at"] <= self.lease_seconds:
                return Outcome("in_progress", key, attempts=row["attempts"])
            log_event(self.log, "replay", key=key)
            return self._deliver(key, json.loads(row["payload"]), row["attempts"])

    def replay_all(self):
        return [self.replay(r["key"]) for r in self.store.dead_letters()]

    # -- internals --------------------------------------------------------------------

    def _delay(self, run_attempt):
        raw = min(self.max_delay, self.base_delay * (2 ** (run_attempt - 1)))
        return raw * (0.5 + self._jitter() / 2.0)

    def _deliver(self, key, payload, total_attempts):
        run_attempt = 0
        while True:
            run_attempt += 1
            total_attempts += 1
            self.store.record_attempt(key, total_attempts, self._clock())
            try:
                result = self.target(payload, key)
            except Exception as exc:  # noqa: BLE001 - classified below
                error = "%s: %s" % (type(exc).__name__, exc)
                if not is_transient(exc):
                    return self._give_up(key, total_attempts, error, "permanent_error")
                if run_attempt >= self.max_attempts:
                    return self._give_up(key, total_attempts, error, "retries_exhausted")
                delay = self._delay(run_attempt)
                log_event(
                    self.log, "attempt_failed_will_retry",
                    key=key, attempt=run_attempt, error=error, retry_in=round(delay, 3),
                )
                self._sleep(delay)
                continue
            self.store.mark_done(key, result, self._clock())
            log_event(self.log, "delivered", key=key, attempts=total_attempts)
            return Outcome("done", key, result, total_attempts)

    def _give_up(self, key, attempts, error, reason):
        self.store.mark_dead(key, error, self._clock())
        log_event(self.log, "dead_lettered", key=key, reason=reason, attempts=attempts, error=error)
        return Outcome("dead", key, attempts=attempts, error=error)
