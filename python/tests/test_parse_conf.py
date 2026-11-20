"""Tests for --conf parsing in kyuubi-submit.

Covers three delivery shapes that all route through parse_conf / normalize_conf:

1. One --conf per k=v entry (argparse action='append' producing a list) --
   the canonical Spark-ecosystem pattern.
2. One --conf string carrying multiple k=v joined by commas -- the Scala
   caller pattern (seq joined with mkString(",")), and the documented
   legacy '--conf "k1=v1,k2=v2"' form.
3. YAML dict in a config file -- dict values can contain commas.

The regex-based split is designed to preserve commas that are part of a
legitimate value (e.g. 'mongodb,org.neo4j.spark.' for excluded.catalogs,
'a.jar,b.jar' for spark.jars) while still breaking apart k=v,k=v monster
strings.
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
        """Documented README form: --conf "k1=v1,k2=v2" produces two pairs.
        Regex splits at the comma followed by 'spark.driver.memory='."""
        result = _submitter().parse_conf("spark.executor.memory=4g,spark.driver.memory=2g")
        self.assertEqual(
            result,
            {"spark.executor.memory": "4g", "spark.driver.memory": "2g"},
        )

    def test_value_with_comma_is_preserved(self):
        """Regression: --conf key=mongodb,org.neo4j.spark. must deliver the
        full string as the value. The comma after 'mongodb' is NOT followed
        by a 'key=' pattern ('org.neo4j.spark.' ends with '.' then nothing),
        so no split."""
        result = _submitter().parse_conf([
            "spark.ranger.plugin.spark.authz.excluded.catalogs=mongodb,org.neo4j.spark."
        ])
        self.assertEqual(
            result,
            {"spark.ranger.plugin.spark.authz.excluded.catalogs":
             "mongodb,org.neo4j.spark."},
        )

    def test_jars_value_with_comma_is_preserved(self):
        """spark.jars is list-valued; commas inside the value must survive.
        No 'key=' follows the commas between jar names."""
        result = _submitter().parse_conf(["spark.jars=a.jar,b.jar,c.jar"])
        self.assertEqual(result, {"spark.jars": "a.jar,b.jar,c.jar"})

    def test_scala_caller_monster_string_with_neo4j_prefix(self):
        """Case B regression: a Scala caller that joins all configs with
        commas into a single --conf, and the excluded.catalogs value
        contains the Neo4j class prefix. Previous heuristic ('all pieces
        have =') collapsed the whole string into one key's value and lost
        the other 7 configs. Regex-based split must preserve all 8."""
        monster = ",".join([
            "spark.hadoop.fs.s3a.committer.staging.conflict-mode=append",
            "spark.hadoop.fs.s3a.connection.request.timeout=30m",
            "spark.ranger.plugin.spark.authz.excluded.catalogs=mongodb,org.neo4j.spark.",
            "spark.executor.memoryOverhead=1g",
            "spark.driver.memoryOverhead=512m",
            "spark.executor.processTreeMetrics.enabled=true",
            "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties",
            "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.properties",
        ])
        result = _submitter().parse_conf([monster])
        self.assertEqual(result, {
            "spark.hadoop.fs.s3a.committer.staging.conflict-mode": "append",
            "spark.hadoop.fs.s3a.connection.request.timeout": "30m",
            "spark.ranger.plugin.spark.authz.excluded.catalogs": "mongodb,org.neo4j.spark.",
            "spark.executor.memoryOverhead": "1g",
            "spark.driver.memoryOverhead": "512m",
            "spark.executor.processTreeMetrics.enabled": "true",
            "spark.driver.extraJavaOptions": "-Dlog4j.configurationFile=log4j2.properties",
            "spark.executor.extraJavaOptions": "-Dlog4j.configurationFile=log4j2.properties",
        })

    def test_url_value_with_equals_inside_is_not_split(self):
        """A value containing '=' but not preceded by ',' must not split.
        E.g. 'url=https://host?a=1' -- the '=' inside the querystring is
        not a conf-key boundary."""
        result = _submitter().parse_conf(["spark.foo.url=https://host/path?a=1&b=2"])
        self.assertEqual(result, {"spark.foo.url": "https://host/path?a=1&b=2"})

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
        """CLI path (argparse action='append'): each list entry comes in
        already as one k=v; the comma inside the value is not followed by
        a 'key=' prefix, so no split."""
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
        """Comma followed by 'spark.b=' matches the split pattern, so the
        legacy one-string form becomes two list entries."""
        result = normalize_conf(["spark.a=1,spark.b=2"])
        self.assertEqual(result, ["spark.a=1", "spark.b=2"])

    def test_scala_caller_monster_string_splits_cleanly(self):
        """Case B at the normalize stage -- Scala caller joins N configs
        with commas into one string. Regex splits before each 'spark.*='
        prefix, so the Neo4j-prefix value stays with its own key and the
        following keys survive as separate entries."""
        monster = (
            "spark.hadoop.fs.s3a.connection.request.timeout=30m,"
            "spark.ranger.plugin.spark.authz.excluded.catalogs=mongodb,org.neo4j.spark.,"
            "spark.executor.memoryOverhead=1g"
        )
        self.assertEqual(normalize_conf([monster]), [
            "spark.hadoop.fs.s3a.connection.request.timeout=30m",
            "spark.ranger.plugin.spark.authz.excluded.catalogs=mongodb,org.neo4j.spark.",
            "spark.executor.memoryOverhead=1g",
        ])

    def test_bare_comma_separated_string_without_equals(self):
        """No comma is followed by 'key=', so no split happens."""
        result = normalize_conf("foo,bar,baz")
        self.assertEqual(result, ["foo,bar,baz"])


if __name__ == "__main__":
    unittest.main()
