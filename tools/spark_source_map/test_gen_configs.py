"""Unit tests for the Spark config parser.

Run: python -m pytest tools/spark_source_map/test_gen_configs.py
The floor test (`test_real_source_floor`) runs against the local Spark checkout and
is skipped when it is absent.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

import gen_configs as g


def _one(text: str, rel="sql/catalyst/src/main/scala/Foo.scala"):
    configs, unparsed = g.parse_file(text, rel)
    return configs, unparsed


def test_int_conf_with_default_and_version():
    src = '''
  val SHUFFLE_PARTITIONS = buildConf("spark.sql.shuffle.partitions")
    .doc("The default number of partitions to use when shuffling data for joins or aggregations.")
    .version("1.1.0")
    .intConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(200)
'''
    configs, unparsed = _one(src)
    assert len(configs) == 1 and not unparsed
    c = configs[0]
    assert c.key == "spark.sql.shuffle.partitions"
    assert c.type == "int"
    assert c.default == 200 and c.default_kind == "literal"
    assert c.version == "1.1.0"
    assert c.doc.startswith("The default number of partitions")
    assert c.prefix == "spark.sql.shuffle"
    assert c.subsystem == "sql/catalyst"


def test_boolean_default():
    src = '''
  val X = buildConf("spark.sql.adaptive.enabled")
    .version("1.6.0").booleanConf.createWithDefault(true)
'''
    c = _one(src)[0][0]
    assert c.type == "boolean" and c.default is True and c.default_kind == "literal"


def test_multiline_doc_concatenation():
    src = '''
  val Y = buildConf("spark.sql.optimizer.bloom.threshold")
    .doc("Byte size threshold of the Bloom filter application side plan's aggregated scan " +
      "size. Aggregated scan byte size needs to be over " +
      "this value to inject a bloom filter.")
    .version("3.3.0").bytesConf.createWithDefaultString("10GB")
'''
    c = _one(src)[0][0]
    assert c.type == "bytes"
    assert c.default == "10GB" and c.default_kind == "string"
    assert "Aggregated scan byte size needs to be over this value" in c.doc
    assert "  " not in c.doc  # whitespace collapsed


def test_create_optional():
    src = '''
  val Z = buildConf("spark.sql.warehouse.dir").stringConf.createOptional
'''
    c = _one(src)[0][0]
    assert c.default is None and c.default_kind == "optional" and c.type == "string"


def test_fallback_conf():
    src = '''
  val F = buildConf("spark.sql.x.maxRecords")
    .version("2.2.0").fallbackConf(SOME_OTHER_CONF)
'''
    c = _one(src)[0][0]
    assert c.default_kind == "fallback" and c.default == "SOME_OTHER_CONF"
    assert c.type == "fallback"


def test_expr_default_not_coerced():
    src = '''
  val E = buildConf("spark.sql.timestampType")
    .version("3.0.0").stringConf
    .createWithDefault(TimestampTypes.TIMESTAMP_LTZ.toString)
'''
    c = _one(src)[0][0]
    assert c.default_kind == "expr"
    assert c.default == "TimestampTypes.TIMESTAMP_LTZ.toString"


def test_config_builder_core_no_version():
    src = '''
  private[spark] val DRIVER_CORES = ConfigBuilder("spark.driver.cores")
    .doc("Number of cores to use for the driver process.")
    .intConf
    .createWithDefault(1)
'''
    c = _one(src, rel="core/src/main/scala/org/apache/spark/internal/config/package.scala")[0][0]
    assert c.key == "spark.driver.cores" and c.version is None
    assert c.default == 1 and c.subsystem == "core"


def test_dynamic_key_is_flagged_not_dropped():
    src = '''
  def dynamic(name: String) = buildConf(s"spark.sql.$name.enabled")
    .booleanConf.createWithDefault(false)
'''
    configs, unparsed = _one(src)
    assert not configs
    assert len(unparsed) == 1 and unparsed[0].reason == "dynamic-key"


def test_method_definition_is_skipped():
    # The `def buildConf(key: String)` definition must not become an entry.
    src = '''
  private def buildConf(key: String): ConfigBuilder = ConfigBuilder(key)
    .booleanConf.createWithDefault(false)
'''
    configs, unparsed = _one(src)
    assert not configs and not unparsed


def test_constant_key_resolved_from_symbol_table():
    src = '''
  val ANSI = buildConf(SqlApiConfHelper.ANSI_ENABLED_KEY)
    .doc("When true, Spark uses ANSI semantics.")
    .version("3.0.0").booleanConf.createWithDefault(false)
'''
    symbols = {"ANSI_ENABLED_KEY": "spark.sql.ansi.enabled"}
    configs, unparsed = g.parse_file(src, "sql/catalyst/src/main/scala/Foo.scala", symbols)
    assert not unparsed
    assert configs[0].key == "spark.sql.ansi.enabled"
    assert configs[0].default is False


def test_unresolvable_constant_key_flagged():
    src = '''
  val U = buildConf(Mystery.UNKNOWN_KEY).booleanConf.createWithDefault(true)
'''
    configs, unparsed = g.parse_file(src, "core/src/main/scala/X.scala", {})
    assert not configs and unparsed and unparsed[0].reason == "dynamic-key"


def test_interpolated_key_resolved():
    src = '''
  val C = buildConf(s"spark.sql.catalog.$SESSION_CATALOG_NAME")
    .stringConf.createOptional
'''
    symbols = {"SESSION_CATALOG_NAME": "spark.sql.catalog.spark_catalog"}
    # last-segment lookup: SESSION_CATALOG_NAME -> spark_catalog requires the value
    symbols = {"SESSION_CATALOG_NAME": "spark_catalog"}
    configs, _ = g.parse_file(src, "sql/catalyst/src/main/scala/X.scala", symbols)
    assert configs and configs[0].key == "spark.sql.catalog.spark_catalog"


def test_symbol_table_from_real_source():
    src = '''
  object SqlApiConfHelper {
    val ANSI_ENABLED_KEY = "spark.sql.ansi.enabled"
    val SESSION_LOCAL_TIMEZONE_KEY: String = "spark.sql.session.timeZone"
  }
'''
    # Simulate via the regex directly.
    found = dict((m.group(1), m.group(2)) for m in g.SYMBOL_DEF_RE.finditer(src))
    assert found["ANSI_ENABLED_KEY"] == "spark.sql.ansi.enabled"
    assert found["SESSION_LOCAL_TIMEZONE_KEY"] == "spark.sql.session.timeZone"


def test_string_default_literal():
    src = '''
  val S = buildConf("spark.sql.session.timeZone")
    .version("2.2.0").stringConf.createWithDefault("America/Los_Angeles")
'''
    c = _one(src)[0][0]
    assert c.default == "America/Los_Angeles" and c.default_kind == "literal"


def test_subsystem_derivation():
    assert g.subsystem_of("sql/core/src/main/scala/X.scala") == "sql/core"
    assert g.subsystem_of("core/src/main/scala/X.scala") == "core"
    assert g.subsystem_of("connector/kafka-0-10-sql/src/main/scala/X.scala") == \
        "connector/kafka-0-10-sql"
    assert g.subsystem_of("mllib/src/main/scala/X.scala") == "mllib"


SPARK_SRC = Path(os.environ.get("SPARK_SRC", r"C:/opt/learn/spark/spark"))


@pytest.mark.skipif(not SPARK_SRC.exists(), reason="local Spark source not present")
def test_real_source_floor():
    """A regex regression that silently drops configs must fail loudly."""
    cat = g.build_catalog(SPARK_SRC)
    assert cat.meta["entry_count"] > 1000, cat.meta
    keys = {c["key"] for c in cat.configs}
    assert "spark.sql.shuffle.partitions" in keys
    assert "spark.sql.adaptive.enabled" in keys
    # constant-keyed configs must be resolved, not left unparsed
    assert "spark.sql.ansi.enabled" in keys
    assert "spark.sql.session.timeZone" in keys
    sp = next(c for c in cat.configs if c["key"] == "spark.sql.shuffle.partitions")
    assert sp["default"] == 200 and sp["version"] == "1.1.0"
