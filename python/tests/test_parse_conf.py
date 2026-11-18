"""Tests for --conf parsing in kyuubi-submit.

Covers both the documented legacy form (`--conf "k1=v1,k2=v2"` split into
multiple pairs) and the regression case where a single-pair value contained
commas (e.g. the Kyuubi Spark authz excluded.catalogs list
'mongodb,org.neo4j.spark.') and was being truncated mid-value.
"""

import importlib
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

kyuubi_submit = importlib.import_module("kyuubi-submit")
KyuubiBatchSubmitter = kyuubi_submit.KyuubiBatchSubmitter
normalize_conf = kyuubi_submit.normalize_conf


def _submitter():
    return KyuubiBatchSubmitter.__new__(KyuubiBatchSubmitter)


class TestParseConf(unittest.TestCase):

    def test_empty_inputs(self):
        self.assertEqual(_submitter().parse_conf(None), {})
        self.assertEqual(_submitter().parse_conf([]), {})
        self.assertEqual(_submitter().parse_conf(""), {})

    def test_single_pair_list(self):
        result = _submitter().parse_conf(["spark.executor.memory=4g"])
        self.assertEqual(result, {"spark.executor.memory": "4g"})

    def test_multiple_pairs_as_list(self):
        result = _submitter().parse_conf([
            "spark.executor.memory=4g",
            "spark.driver.memory=2g",
        ])
        self.assertEqual(
            result,
            {"spark.executor.memory": "4g", "spark.driver.memory": "2g"},
        )

    def test_legacy_comma_separated_pairs_in_single_string(self):
        """Documented form from README: --conf "k1=v1,k2=v2" produces two
        pairs. Preserved by the heuristic because every comma-split piece
        itself contains '='."""
        result = _submitter().parse_conf("spark.executor.memory=4g,spark.driver.memory=2g")
        self.assertEqual(
            result,
            {"spark.executor.memory": "4g", "spark.driver.memory": "2g"},
        )

    def test_value_with_comma_is_preserved(self):
        """Regression: --conf key=mongodb,org.neo4j.spark. must produce the
        full string as the value, not key -> 'mongodb' with the Neo4j class
        prefix silently dropped. The second comma-split piece lacks '=' so
        the heuristic keeps the entry whole."""
        result = _submitter().parse_conf([
            "spark.ranger.plugin.spark.authz.excluded.catalogs=mongodb,org.neo4j.spark."
        ])
        self.assertEqual(
            result,
            {"spark.ranger.plugin.spark.authz.excluded.catalogs":
             "mongodb,org.neo4j.spark."},
        )

    def test_jars_value_with_comma_is_preserved(self):
        """spark.jars=a.jar,b.jar is a single pair with a list-valued value.
        Only the first piece has '=', so the entry stays whole."""
        result = _submitter().parse_conf(["spark.jars=a.jar,b.jar,c.jar"])
        self.assertEqual(result, {"spark.jars": "a.jar,b.jar,c.jar"})

    def test_whitespace_is_trimmed(self):
        result = _submitter().parse_conf(["  spark.executor.memory = 4g  "])
        self.assertEqual(result, {"spark.executor.memory": "4g"})


class TestNormalizeConf(unittest.TestCase):

    def test_empty_inputs(self):
        self.assertEqual(normalize_conf(None), [])
        self.assertEqual(normalize_conf({}), [])
        self.assertEqual(normalize_conf([]), [])
        self.assertEqual(normalize_conf(""), [])

    def test_dict_input(self):
        result = normalize_conf({"spark.executor.memory": "4g"})
        self.assertEqual(result, ["spark.executor.memory=4g"])

    def test_dict_value_with_comma_is_preserved(self):
        """YAML path: dict values containing commas must not be split."""
        result = normalize_conf({
            "spark.ranger.plugin.spark.authz.excluded.catalogs":
                "mongodb,org.neo4j.spark.",
        })
        self.assertEqual(
            result,
            ["spark.ranger.plugin.spark.authz.excluded.catalogs="
             "mongodb,org.neo4j.spark."],
        )

    def test_list_entry_with_comma_in_value_is_preserved(self):
        """CLI path (argparse action='append'): one piece lacks '=', so the
        entry is kept whole and the comma-bearing value survives."""
        result = normalize_conf([
            "spark.ranger.plugin.spark.authz.excluded.catalogs="
            "mongodb,org.neo4j.spark.",
        ])
        self.assertEqual(
            result,
            ["spark.ranger.plugin.spark.authz.excluded.catalogs="
             "mongodb,org.neo4j.spark."],
        )

    def test_legacy_multi_pair_string_still_splits(self):
        """Every comma-split piece contains '=', so the legacy form is
        preserved: one string becomes two list entries."""
        result = normalize_conf(["spark.a=1,spark.b=2"])
        self.assertEqual(result, ["spark.a=1", "spark.b=2"])

    def test_bare_comma_separated_string_without_equals(self):
        """Entries without '=' in every piece fall into the single-entry
        bucket; parse_conf will later filter out items without '='."""
        result = normalize_conf("foo,bar,baz")
        self.assertEqual(result, ["foo,bar,baz"])


if __name__ == "__main__":
    unittest.main()
