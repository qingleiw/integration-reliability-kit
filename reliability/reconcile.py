"""Reconciliation: compare what the source sent with what the target holds.

Retries, replays and de-duplication make it likely that nothing is lost or doubled, and this
is the check that proves it. Run it on a schedule. A mismatch is found by a job, not by a
customer.
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, List


@dataclass
class Report:
    matched: int = 0
    missing_in_target: List[str] = field(default_factory=list)
    unexpected_in_target: List[str] = field(default_factory=list)
    duplicated_in_target: List[str] = field(default_factory=list)

    @property
    def ok(self):
        return not (self.missing_in_target or self.unexpected_in_target or self.duplicated_in_target)

    def summary(self):
        return (
            "matched=%d missing=%d unexpected=%d duplicated=%d"
            % (
                self.matched,
                len(self.missing_in_target),
                len(self.unexpected_in_target),
                len(self.duplicated_in_target),
            )
        )


def reconcile(source_ids: Iterable[str], target_ids: Iterable[str]) -> Report:
    source = set(source_ids)
    target_counts = Counter(target_ids)
    target = set(target_counts)
    return Report(
        matched=len(source & target),
        missing_in_target=sorted(source - target),
        unexpected_in_target=sorted(target - source),
        duplicated_in_target=sorted(k for k, n in target_counts.items() if n > 1),
    )
