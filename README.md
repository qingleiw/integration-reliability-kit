# integration-reliability-kit

The handful of patterns that decide whether an integration can be trusted, written down as a
small working reference with tests. No framework, no dependencies, Python standard library only.

```
 source ──▶ handle(key, payload) ──▶ claim key ──▶ target(payload, key)
                    │                   │              │
                    │            already done?     transient error?
                    │            ──▶ return the      ──▶ back off, retry (bounded)
                    │               stored result    permanent error / retries used up?
                    │                                ──▶ keep it as a dead letter
                    ▼
            every step logged under one correlation ID
                    │
    replay(key) ◀───┘   dead letters go back through the same path
    reconcile(source_ids, target_ids)   proves nothing was lost or doubled
```

## What it does

| Problem | Pattern | Where |
|---|---|---|
| The same message arrives twice | A stable message key is claimed atomically; a second arrival returns the stored result and touches nothing | `store.py`, `handler.py` |
| The target is briefly unavailable | Bounded retries with exponential backoff and jitter; only transient errors are retried | `handler.py` |
| The target rejects the message for good | No retry; the message and its error are kept as a dead letter | `handler.py` |
| The write succeeded but the answer was lost | The idempotency key is passed to the target, so the retry is a no-op there too | `handler.py`, `testing.py` |
| Something must be fixed and re-sent | `replay()` runs a dead letter through the same path; a delivered message is never re-sent | `handler.py` |
| A worker died holding a message | The claim has a lease; an expired claim is taken over | `store.py` |
| "Whose fault is it?" | One correlation ID on every log line, with the business key beside it | `tracing.py` |
| Did everything arrive exactly once? | Reconciliation of source IDs against target IDs: missing, unexpected, duplicated | `reconcile.py` |

## Run it

```bash
python examples/demo.py                     # a scripted target that fails in different ways
python -m unittest discover -s tests -v     # 23 tests
```

The demo prints JSON log lines. Filter on one `correlation_id` and you have the whole life of
that message: received, failed, retried, delivered or dead-lettered, replayed.

## Use it

```python
from reliability import ReliableHandler, Store

def send_to_erp(payload, idempotency_key):
    ...  # call the real system; pass idempotency_key on if it supports one
    return {"erp_id": "..."}

handler = ReliableHandler(Store("inbox.db"), send_to_erp)
outcome = handler.handle("order-8841", {"id": "8841", "qty": 3}, business_key="8841")
# outcome.status is "done", "duplicate", "dead" or "in_progress"
```

Raise `TransientError` (or `TimeoutError` / `ConnectionError`) for failures worth retrying and
`PermanentError` for ones that are not. Any other exception is treated as permanent, because
retrying an unknown failure usually makes things worse.

## Choices that are worth arguing about

- **Unknown errors are not retried.** Hammering a system that is failing in a way you do not
  understand is how an outage becomes a bigger one. Classify errors deliberately.
- **A dead letter is kept, not dropped.** Losing a message silently is the one outcome that
  nobody finds until a customer does.
- **Reconciliation is a separate job on purpose.** Idempotency and retries make duplicates and
  losses unlikely. They do not make them impossible, and a check that runs on a schedule is
  how you know.
- **The test `test_without_it_the_retry_writes_twice` is there to fail safe.** It documents
  what happens when the target ignores the key, so nobody assumes the retry is harmless.

## What it deliberately does not do

- It is not a message broker or a framework. It is the logic you wrap around the call that
  talks to another system.
- One SQLite file means one process or a few threads. Across machines, the claim in `store.py`
  has to move to a shared database; the interface stays the same.
- Ordering is not preserved across retries. If order matters, key and sequence by entity.
- It does not decide what counts as a transient error for your target. That is yours to define.

## Layout

```
reliability/   tracing.py  store.py  handler.py  reconcile.py  testing.py
examples/      demo.py
tests/         test_handler.py  test_reconcile.py
```

## Licence

MIT.
