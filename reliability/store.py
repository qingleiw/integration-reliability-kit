"""The record of what has been received and what happened to it.

One SQLite table is enough to show the pattern. In production this is whatever durable
store the integration already has (a database table, a queue's own bookkeeping). What matters
is that the claim on a message key is atomic and survives a restart.
"""

import json
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
    key            TEXT PRIMARY KEY,
    business_key   TEXT,
    correlation_id TEXT,
    payload        TEXT NOT NULL,
    status         TEXT NOT NULL,          -- processing | done | dead
    attempts       INTEGER NOT NULL DEFAULT 0,
    result         TEXT,
    last_error     TEXT,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_status ON inbox(status);
"""


class Store:
    def __init__(self, path=":memory:"):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock, self._db:
            self._db.executescript(SCHEMA)

    # -- claiming ---------------------------------------------------------------------

    def claim(self, key, payload, business_key, correlation_id, now, lease_seconds):
        """Atomically take a message key. Returns (state, row).

        state is one of:
          new         first time this key is seen; the caller owns it now
          duplicate   already delivered; row["result"] holds the stored result
          dead        gave up earlier; only an explicit replay will try it again
          in_progress another worker holds it and its lease has not expired
          reclaimed   a previous worker died holding it; the caller owns it now
        """
        with self._lock, self._db:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO inbox"
                "(key, business_key, correlation_id, payload, status, attempts, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, 'processing', 0, ?, ?)",
                (key, business_key, correlation_id, json.dumps(payload, sort_keys=True), now, now),
            )
            if cur.rowcount == 1:
                return "new", self.get(key)
            row = self.get(key)
            if row["status"] == "done":
                return "duplicate", row
            if row["status"] == "dead":
                return "dead", row
            if now - row["updated_at"] > lease_seconds:
                self._db.execute("UPDATE inbox SET updated_at = ? WHERE key = ?", (now, key))
                return "reclaimed", self.get(key)
            return "in_progress", row

    # -- progress ---------------------------------------------------------------------

    def record_attempt(self, key, attempts, now):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE inbox SET attempts = ?, updated_at = ? WHERE key = ?", (attempts, now, key)
            )

    def mark_done(self, key, result, now):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE inbox SET status = 'done', result = ?, last_error = NULL, updated_at = ?"
                " WHERE key = ?",
                (json.dumps(result, sort_keys=True), now, key),
            )

    def mark_dead(self, key, error, now):
        with self._lock, self._db:
            self._db.execute(
                "UPDATE inbox SET status = 'dead', last_error = ?, updated_at = ? WHERE key = ?",
                (error, now, key),
            )

    def reopen(self, key, now):
        """Put a dead message back in play for a replay."""
        with self._lock, self._db:
            self._db.execute(
                "UPDATE inbox SET status = 'processing', updated_at = ? WHERE key = ? AND status = 'dead'",
                (now, key),
            )

    # -- reading ----------------------------------------------------------------------

    def get(self, key):
        with self._lock:
            return self._db.execute("SELECT * FROM inbox WHERE key = ?", (key,)).fetchone()

    def dead_letters(self):
        with self._lock:
            return self._db.execute(
                "SELECT * FROM inbox WHERE status = 'dead' ORDER BY created_at, key"
            ).fetchall()

    def counts(self):
        with self._lock:
            rows = self._db.execute("SELECT status, COUNT(*) AS n FROM inbox GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}
