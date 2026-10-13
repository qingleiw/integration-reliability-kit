"""A scripted target system for tests and the demo.

Each call to the target takes the next step from a script, so a scenario reads as a list:

    FlakyTarget(["transient", "transient", "ok"])

Steps:
  ok                    the write succeeds and the answer arrives
  transient             nothing was written; the target says try again later
  permanent             nothing was written; the target will never accept it
  timeout_after_write   the write succeeds but the answer is lost, so the caller sees a timeout

When honors_idempotency_key is true the target ignores a key it has already written, as a
well-behaved API does. Setting it false reproduces the classic double-write.
"""

from .handler import PermanentError, TransientError


class FlakyTarget:
    def __init__(self, script=(), *, default="ok", honors_idempotency_key=True):
        self._script = list(script)
        self._default = default
        self.honors_idempotency_key = honors_idempotency_key
        self.calls = 0
        self.writes = []            # every record actually written, in order
        self._written_keys = {}

    def __call__(self, payload, idempotency_key):
        self.calls += 1
        step = self._script.pop(0) if self._script else self._default
        if step == "transient":
            raise TransientError("target busy")
        if step == "permanent":
            raise PermanentError("target rejected the message")
        result = self._write(payload, idempotency_key)
        if step == "timeout_after_write":
            raise TimeoutError("no response from target")
        return result

    def _write(self, payload, key):
        if self.honors_idempotency_key and key in self._written_keys:
            return self._written_keys[key]
        record = {"id": payload.get("id", key), "key": key}
        self.writes.append(record)
        self._written_keys[key] = record
        return record

    def written_ids(self):
        return [w["id"] for w in self.writes]
