"""Correlation IDs and structured logs.

A correlation ID is minted where a message first enters the system and is carried through
every log line that handles it, so one grep follows a message from receipt to delivery,
retry, dead letter and replay. The business key (an order number, a waybill number) is
logged beside it, because the people on the other side search by that, not by our ID.
"""

import contextvars
import json
import logging
import time
import uuid
from contextlib import contextmanager

_correlation_id = contextvars.ContextVar("correlation_id", default=None)
_business_key = contextvars.ContextVar("business_key", default=None)


def new_correlation_id():
    return uuid.uuid4().hex[:16]


def current():
    """Return (correlation_id, business_key) for the message being handled, or (None, None)."""
    return _correlation_id.get(), _business_key.get()


@contextmanager
def use_correlation_id(correlation_id, business_key=None):
    t1 = _correlation_id.set(correlation_id)
    t2 = _business_key.set(business_key)
    try:
        yield correlation_id
    finally:
        _correlation_id.reset(t1)
        _business_key.reset(t2)


class ContextFilter(logging.Filter):
    """Copies the current correlation ID and business key onto every log record."""

    def filter(self, record):
        record.correlation_id, record.business_key = current()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": round(getattr(record, "created", time.time()), 3),
            "level": record.levelname,
            "event": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", None),
            "business_key": getattr(record, "business_key", None),
        }
        entry.update(getattr(record, "fields", {}) or {})
        return json.dumps(entry, ensure_ascii=False, sort_keys=False)


def get_logger(name="reliability"):
    logger = logging.getLogger(name)
    if not any(isinstance(f, ContextFilter) for f in logger.filters):
        logger.addFilter(ContextFilter())
    return logger


def log_event(logger, event, level=logging.INFO, **fields):
    logger.log(level, event, extra={"fields": fields})
