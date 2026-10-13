import unittest

from reliability import reconcile


class Reconcile(unittest.TestCase):
    def test_clean_run(self):
        report = reconcile(["a", "b", "c"], ["c", "a", "b"])
        self.assertTrue(report.ok)
        self.assertEqual(report.matched, 3)

    def test_missing_in_target(self):
        report = reconcile(["a", "b", "c"], ["a", "c"])
        self.assertFalse(report.ok)
        self.assertEqual(report.missing_in_target, ["b"])

    def test_unexpected_in_target(self):
        report = reconcile(["a"], ["a", "z"])
        self.assertEqual(report.unexpected_in_target, ["z"])

    def test_duplicates_in_target(self):
        report = reconcile(["a", "b"], ["a", "b", "b"])
        self.assertFalse(report.ok)
        self.assertEqual(report.duplicated_in_target, ["b"])
        self.assertEqual(report.matched, 2)

    def test_summary(self):
        report = reconcile(["a", "b"], ["a", "a"])
        self.assertEqual(report.summary(), "matched=1 missing=1 unexpected=0 duplicated=1")


if __name__ == "__main__":
    unittest.main()
