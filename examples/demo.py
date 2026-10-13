"""Run:  python examples/demo.py

Five messages go through a target that misbehaves in different ways. The log lines are JSON
and carry the correlation ID, so any one message can be followed with a single grep.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reliability import JsonFormatter, ReliableHandler, Store, get_logger, reconcile  # noqa: E402
from reliability.testing import FlakyTarget  # noqa: E402


def main():
    log = get_logger()
    log.setLevel(logging.INFO)
    sink = logging.StreamHandler(sys.stdout)
    sink.setFormatter(JsonFormatter())
    log.addHandler(sink)

    # What the target does on each call, in order across all messages.
    target = FlakyTarget(
        [
            "ok",                   # WB-1  delivered first time
            "transient",            # WB-2  busy, then fine
            "ok",
            "timeout_after_write",  # WB-3  written, but the answer is lost
            "ok",                   # WB-3  retry: the key makes it a no-op
            "permanent",            # WB-4  rejected for good
        ]
    )
    handler = ReliableHandler(Store(), target, base_delay=0.05, max_delay=0.2)

    messages = [
        ("msg-1", {"id": "WB-1"}),
        ("msg-2", {"id": "WB-2"}),
        ("msg-3", {"id": "WB-3"}),
        ("msg-4", {"id": "WB-4"}),
        ("msg-1", {"id": "WB-1"}),   # the same message arrives again
    ]
    print("\n--- processing")
    for key, payload in messages:
        out = handler.handle(key, payload, business_key=payload["id"])
        print("=> %-6s %-9s attempts=%d" % (key, out.status, out.attempts))

    print("\n--- reconciliation before replay")
    report = reconcile([p["id"] for _, p in messages], target.written_ids())
    print(report.summary(), "missing:", report.missing_in_target)

    print("\n--- replay of dead letters (the target accepts the message this time)")
    for out in handler.replay_all():
        print("=> %-6s %-9s attempts=%d" % (out.key, out.status, out.attempts))

    print("\n--- reconciliation after replay")
    report = reconcile([p["id"] for _, p in messages], target.written_ids())
    print(report.summary(), "ok" if report.ok else "MISMATCH")


if __name__ == "__main__":
    main()
