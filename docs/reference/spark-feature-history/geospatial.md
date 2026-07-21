# Geospatial

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

### 4.x era — GEOMETRY/GEOGRAPHY types and ST_* functions

Geospatial support did not exist before 4.2.0, which introduced `GEOMETRY` and `GEOGRAPHY` data types with a full `ST_*` function library, WKB/WKT parsing, Parquet I/O, and a SRID registry in one SPIP (SPARK-51658) — enabled by default from the start (SPARK-56771).

The release rounded the feature out immediately: Parquet read (SPARK-55261) and write (SPARK-55260) support, a WKT writer (SPARK-55339), WKB parsing/writing for `GEOGRAPHY` (SPARK-55449), an optional SRID argument for `ST_GeomFromWKB` (SPARK-55295), casting from `GeographyType` to `GeometryType` (SPARK-55539), a complete SRS registry built on PROJ 9.7.1 data (SPARK-55790), and Hive/Thrift server support for geo result sets (SPARK-55530). As of 4.2.0 this is a young but complete first cut — a single-release origin story rather than a multi-release arc.

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 4.1.1 | [SPARK-54683](https://issues.apache.org/jira/browse/SPARK-54683) | Improvement | Unify geo and time types blocking |
| 4.2.0 | [SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658) | prose | SPIP: Geospatial types â new GEOMETRY and GEOGRAPHY data types with ST_* functions, WKB/WKT and Parquet I/O, and a full SRID registry,... |
| 4.2.0 | [SPARK-55260](https://issues.apache.org/jira/browse/SPARK-55260) | prose | Parquet write support for Geo types |
| 4.2.0 | [SPARK-55261](https://issues.apache.org/jira/browse/SPARK-55261) | prose | Parquet read support for Geo types |
| 4.2.0 | [SPARK-55295](https://issues.apache.org/jira/browse/SPARK-55295) | prose | Extend ST_GeomFromWKB to take an optional SRID value |
| 4.2.0 | [SPARK-55339](https://issues.apache.org/jira/browse/SPARK-55339) | prose | WKT writer support for Geo objects |
| 4.2.0 | [SPARK-55449](https://issues.apache.org/jira/browse/SPARK-55449) | prose | Enable WKB parsing and writing for Geography |
| 4.2.0 | [SPARK-55530](https://issues.apache.org/jira/browse/SPARK-55530) | prose | Support Geo result sets in Hive and Thrift server |
| 4.2.0 | [SPARK-55539](https://issues.apache.org/jira/browse/SPARK-55539) | prose | Allow casting from GeographyType to GeometryType |
| 4.2.0 | [SPARK-55790](https://issues.apache.org/jira/browse/SPARK-55790) | prose | Build a complete SRS registry using PROJ 9.7.1 data |
| 4.2.0 | [SPARK-56682](https://issues.apache.org/jira/browse/SPARK-56682) | prose | Extend ST_AsBinary to take optional endianness |
| 4.2.0 | [SPARK-56771](https://issues.apache.org/jira/browse/SPARK-56771) | prose | Enable geospatial support by default |
<!-- AUTO:timeline END -->
