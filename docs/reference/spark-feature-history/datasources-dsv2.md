# Data Sources & DSv2

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-6941](https://issues.apache.org/jira/browse/SPARK-6941) | Improvement | Provide a better error message to explain that tables created from RDDs are immutable |
| 1.5.0 | [SPARK-7637](https://issues.apache.org/jira/browse/SPARK-7637) | Improvement | StructType.merge slow with large nenormalised tables O(N2) |
| 1.5.0 | [SPARK-9293](https://issues.apache.org/jira/browse/SPARK-9293) | Improvement | Analysis should detect when set operations are performed on tables with different numbers of columns |
| 1.5.0 | [SPARK-10178](https://issues.apache.org/jira/browse/SPARK-10178) | Improvement | HiveComparision test should print out dependent tables |
| 1.6.0 | [SPARK-7275](https://issues.apache.org/jira/browse/SPARK-7275) | Improvement | Make LogicalRelation public |
| 1.6.0 | [SPARK-10104](https://issues.apache.org/jira/browse/SPARK-10104) | Improvement | Consolidate different forms of table identifiers |
| 1.6.0 | [SPARK-10537](https://issues.apache.org/jira/browse/SPARK-10537) | Improvement | Document LIBSVM data source options in public doc and minor improvements |
| 1.6.0 | [SPARK-11197](https://issues.apache.org/jira/browse/SPARK-11197) | New Feature | Run SQL query on files directly without create a table |
| 2.0.0 | [SPARK-7179](https://issues.apache.org/jira/browse/SPARK-7179) | New Feature | Add pattern after "show tables" to filter desire tablename |
| 2.0.0 | [SPARK-12394](https://issues.apache.org/jira/browse/SPARK-12394) | New Feature | Support writing out pre-hash-partitioned data and exploit that in join optimizations to avoid shuffle (i.e. bucketing in Hive) |
| 2.0.0 | [SPARK-12490](https://issues.apache.org/jira/browse/SPARK-12490) | Improvement | Don't use Javascript for web UI's paginated table navigation controls |
| 2.0.0 | [SPARK-12538](https://issues.apache.org/jira/browse/SPARK-12538) | New Feature | bucketed table support |
| 2.0.0 | [SPARK-12975](https://issues.apache.org/jira/browse/SPARK-12975) | Improvement | Throwing Exception when Bucketing Columns are part of Partitioning Columns |
| 2.0.0 | [SPARK-13075](https://issues.apache.org/jira/browse/SPARK-13075) | New Feature | Native database/table system catalog |
| 2.0.0 | [SPARK-13459](https://issues.apache.org/jira/browse/SPARK-13459) | Improvement | Separate Alive and Dead Executors in Executor Totals Table |
| 2.0.0 | [SPARK-13893](https://issues.apache.org/jira/browse/SPARK-13893) | Improvement | Remove SQLContext.catalog (internal method) |
| 2.0.0 | [SPARK-14161](https://issues.apache.org/jira/browse/SPARK-14161) | Improvement | Parse Drop Database DDL command |
| 2.0.0 | [SPARK-14177](https://issues.apache.org/jira/browse/SPARK-14177) | Improvement | Parse DDL command: "DESCRIBE DATABASE" and "ALTER DATABASE SET DBPROPERTIES" |
| 2.0.0 | [SPARK-14410](https://issues.apache.org/jira/browse/SPARK-14410) | Improvement | SessionCatalog needs to check function existence |
| 2.0.0 | [SPARK-14476](https://issues.apache.org/jira/browse/SPARK-14476) | New Feature | Show table name or path in string of DataSourceScan |
| 2.0.0 | [SPARK-14582](https://issues.apache.org/jira/browse/SPARK-14582) | Improvement | Increase the parallelism for small tables |
| 2.0.0 | [SPARK-15093](https://issues.apache.org/jira/browse/SPARK-15093) | Improvement | create/delete/rename directory for InMemoryCatalog operations if needed |
| 2.0.0 | [SPARK-15160](https://issues.apache.org/jira/browse/SPARK-15160) | Improvement | support data source table in InMemoryCatalog |
| 2.0.0 | [SPARK-15249](https://issues.apache.org/jira/browse/SPARK-15249) | Improvement | Use FunctionResource instead of (String, String) in CreateFunction and CatalogFunction for resource |
| 2.0.0 | [SPARK-15335](https://issues.apache.org/jira/browse/SPARK-15335) | Improvement | Implement TRUNCATE TABLE Command |
| 2.0.0 | [SPARK-15365](https://issues.apache.org/jira/browse/SPARK-15365) | Improvement | Metastore relation should fallback to HDFS size if statistics are not available from table meta data. |
| 2.0.0 | [SPARK-15585](https://issues.apache.org/jira/browse/SPARK-15585) | Improvement | Don't use null in data source options to indicate default value |
| 2.0.0 | [SPARK-15718](https://issues.apache.org/jira/browse/SPARK-15718) | Improvement | better error message for writing bucketing data |
| 2.0.0 | [SPARK-15862](https://issues.apache.org/jira/browse/SPARK-15862) | Improvement | Better Error Message When Having Database Name in CACHE TABLE AS SELECT |
| 2.0.0 | [SPARK-15868](https://issues.apache.org/jira/browse/SPARK-15868) | Improvement | Executors table in Executors tab should sort Executor IDs in numerical order (not alphabetical order) |
| 2.0.0 | [SPARK-15983](https://issues.apache.org/jira/browse/SPARK-15983) | Improvement | Remove FileFormat.prepareRead() |
| 2.0.0 | [SPARK-16084](https://issues.apache.org/jira/browse/SPARK-16084) | Improvement | Minor javadoc issue with "Describe" table in the parser |
| 2.0.0 | [SPARK-16458](https://issues.apache.org/jira/browse/SPARK-16458) | Improvement | SessionCatalog should support `listColumns` for temporary tables |
| 2.0.1 | [SPARK-16935](https://issues.apache.org/jira/browse/SPARK-16935) | Improvement | Verification of Function-related ExternalCatalog APIs |
| 2.0.1 | [SPARK-16947](https://issues.apache.org/jira/browse/SPARK-16947) | Improvement | Support type coercion and foldable expression for inline tables |
| 2.0.1 | [SPARK-17069](https://issues.apache.org/jira/browse/SPARK-17069) | New Feature | Expose spark.range() as table-valued function in SQL |
| 2.0.1 | [SPARK-17150](https://issues.apache.org/jira/browse/SPARK-17150) | New Feature | Support SQL generation for inline tables |
| 2.0.1 | [SPARK-17186](https://issues.apache.org/jira/browse/SPARK-17186) | Improvement | remove catalog table type INDEX |
| 2.0.1 | [SPARK-17609](https://issues.apache.org/jira/browse/SPARK-17609) | Improvement | SessionCatalog.tableExists should not check temp view |
| 2.1.0 | [SPARK-16358](https://issues.apache.org/jira/browse/SPARK-16358) | Improvement | Remove InsertIntoHiveTable From Logical Plan |
| 2.1.0 | [SPARK-16552](https://issues.apache.org/jira/browse/SPARK-16552) | Improvement | Store the Inferred Schemas into External Catalog Tables when Creating Tables |
| 2.1.0 | [SPARK-16596](https://issues.apache.org/jira/browse/SPARK-16596) | Improvement | Refactor DataSourceScanExec to do partition discovery at execution instead of planning time |
| 2.1.0 | [SPARK-16645](https://issues.apache.org/jira/browse/SPARK-16645) | Improvement | rename CatalogStorageFormat.serdeProperties to properties |
| 2.1.0 | [SPARK-16660](https://issues.apache.org/jira/browse/SPARK-16660) | Improvement | CreateViewCommand should not take CatalogTable |
| 2.1.0 | [SPARK-16691](https://issues.apache.org/jira/browse/SPARK-16691) | Improvement | move BucketSpec to catalyst module and use it in CatalogTable |
| 2.1.0 | [SPARK-16731](https://issues.apache.org/jira/browse/SPARK-16731) | Improvement | use StructType in CatalogTable and remove CatalogColumn |
| 2.1.0 | [SPARK-16867](https://issues.apache.org/jira/browse/SPARK-16867) | Improvement | createTable and alterTable in ExternalCatalog should not take db |
| 2.1.0 | [SPARK-16879](https://issues.apache.org/jira/browse/SPARK-16879) | Improvement | unify logical plans for CREATE TABLE and CTAS |
| 2.1.0 | [SPARK-16884](https://issues.apache.org/jira/browse/SPARK-16884) | Improvement | Move DataSourceScanExec out of ExistingRDD.scala file |
| 2.1.0 | [SPARK-16935](https://issues.apache.org/jira/browse/SPARK-16935) | Improvement | Verification of Function-related ExternalCatalog APIs |
| 2.1.0 | [SPARK-16947](https://issues.apache.org/jira/browse/SPARK-16947) | Improvement | Support type coercion and foldable expression for inline tables |
| 2.1.0 | [SPARK-17069](https://issues.apache.org/jira/browse/SPARK-17069) | New Feature | Expose spark.range() as table-valued function in SQL |
| 2.1.0 | [SPARK-17150](https://issues.apache.org/jira/browse/SPARK-17150) | New Feature | Support SQL generation for inline tables |
| 2.1.0 | [SPARK-17179](https://issues.apache.org/jira/browse/SPARK-17179) | Improvement | Consider improving partition pruning in HiveMetastoreCatalog |
| 2.1.0 | [SPARK-17186](https://issues.apache.org/jira/browse/SPARK-17186) | Improvement | remove catalog table type INDEX |
| 2.1.0 | [SPARK-17250](https://issues.apache.org/jira/browse/SPARK-17250) | Improvement | Remove HiveClient and setCurrentDatabase from HiveSessionCatalog |
| 2.1.0 | [SPARK-17257](https://issues.apache.org/jira/browse/SPARK-17257) | Improvement | the physical plan of CREATE TABLE or CTAS should take CatalogTable |
| 2.1.0 | [SPARK-17260](https://issues.apache.org/jira/browse/SPARK-17260) | Improvement | move CreateTables to HiveStrategies |
| 2.1.0 | [SPARK-17324](https://issues.apache.org/jira/browse/SPARK-17324) | Improvement | Remove Direct Usage of HiveClient in InsertIntoHiveTable |
| 2.1.0 | [SPARK-17609](https://issues.apache.org/jira/browse/SPARK-17609) | Improvement | SessionCatalog.tableExists should not check temp view |
| 2.1.0 | [SPARK-17717](https://issues.apache.org/jira/browse/SPARK-17717) | Improvement | Add existence checks to user facing catalog |
| 2.1.0 | [SPARK-17899](https://issues.apache.org/jira/browse/SPARK-17899) | Improvement | add a debug mode to keep raw table properties in HiveExternalCatalog |
| 2.1.0 | [SPARK-18028](https://issues.apache.org/jira/browse/SPARK-18028) | Improvement | simplify TableFileCatalog |
| 2.1.0 | [SPARK-18103](https://issues.apache.org/jira/browse/SPARK-18103) | Improvement | Rename *FileCatalog to *FileProvider |
| 2.1.0 | [SPARK-18465](https://issues.apache.org/jira/browse/SPARK-18465) | Improvement | Uncache Table shouldn't throw an exception when table doesn't exist |
| 2.2.0 | [SPARK-17203](https://issues.apache.org/jira/browse/SPARK-17203) | Improvement | data source options should always be case insensitive |
| 2.2.0 | [SPARK-18566](https://issues.apache.org/jira/browse/SPARK-18566) | Improvement | remove OverwriteOptions |
| 2.2.0 | [SPARK-18567](https://issues.apache.org/jira/browse/SPARK-18567) | Improvement | Simplify CreateDataSourceTableAsSelectCommand |
| 2.2.0 | [SPARK-18947](https://issues.apache.org/jira/browse/SPARK-18947) | Improvement | SQLContext.tableNames should not call Catalog.listTables |
| 2.2.0 | [SPARK-18949](https://issues.apache.org/jira/browse/SPARK-18949) | Improvement | Add recoverPartitions API to Catalog |
| 2.2.0 | [SPARK-18961](https://issues.apache.org/jira/browse/SPARK-18961) | Improvement | Support `SHOW TABLE EXTENDED ... PARTITION` statement |
| 2.2.0 | [SPARK-19029](https://issues.apache.org/jira/browse/SPARK-19029) | Improvement | Remove databaseName from SimpleCatalogRelation |
| 2.2.0 | [SPARK-19148](https://issues.apache.org/jira/browse/SPARK-19148) | Improvement | do not expose the external table concept in Catalog |
| 2.2.0 | [SPARK-19240](https://issues.apache.org/jira/browse/SPARK-19240) | Improvement | add test for setting location for managed table |
| 2.2.0 | [SPARK-19257](https://issues.apache.org/jira/browse/SPARK-19257) | Improvement | The type of CatalogStorageFormat.locationUri should be java.net.URI instead of String |
| 2.2.0 | [SPARK-19261](https://issues.apache.org/jira/browse/SPARK-19261) | Improvement | Support `ALTER TABLE table_name ADD COLUMNS(..)` statement |
| 2.2.0 | [SPARK-19723](https://issues.apache.org/jira/browse/SPARK-19723) | Improvement | create table for data source tables should work with an non-existent location |
| 2.2.0 | [SPARK-19735](https://issues.apache.org/jira/browse/SPARK-19735) | Improvement | Remove HOLD_DDLTIME from Catalog APIs |
| 2.2.0 | [SPARK-20067](https://issues.apache.org/jira/browse/SPARK-20067) | Improvement | Unify and Clean Up Desc Commands Using Catalog Interface |
| 2.2.0 | [SPARK-20126](https://issues.apache.org/jira/browse/SPARK-20126) | Improvement | Remove HiveSessionState |
| 2.2.0 | [SPARK-20194](https://issues.apache.org/jira/browse/SPARK-20194) | Improvement | Support partition pruning for InMemoryCatalog |
| 2.2.0 | [SPARK-20245](https://issues.apache.org/jira/browse/SPARK-20245) | Improvement | pass output to LogicalRelation directly |
| 2.2.0 | [SPARK-20385](https://issues.apache.org/jira/browse/SPARK-20385) | Improvement | 'Submitted Time' field, the date format needs to be formatted, in running Drivers table or Completed Drivers table in master web ui |
| 2.2.0 | [SPARK-20420](https://issues.apache.org/jira/browse/SPARK-20420) | Improvement | Add events to the external catalog |
| 2.2.0 | [SPARK-20967](https://issues.apache.org/jira/browse/SPARK-20967) | Improvement | SharedState.externalCatalog is not really lazy |
| 3.0.0 | [SPARK-25196](https://issues.apache.org/jira/browse/SPARK-25196) | New Feature | Extends the analyze column command for cached tables |
| 3.0.0 | [SPARK-25269](https://issues.apache.org/jira/browse/SPARK-25269) | Improvement | SQL interface support specify StorageLevel when cache table |
| 3.0.0 | [SPARK-25390](https://issues.apache.org/jira/browse/SPARK-25390) | Improvement | Data source V2 API refactoring |
| 3.0.0 | [SPARK-25423](https://issues.apache.org/jira/browse/SPARK-25423) | Improvement | Output "dataFilters" in DataSourceScanExec.metadata |
| 3.0.0 | [SPARK-25458](https://issues.apache.org/jira/browse/SPARK-25458) | Improvement | Support FOR ALL COLUMNS in ANALYZE TABLE |
| 3.0.0 | [SPARK-25531](https://issues.apache.org/jira/browse/SPARK-25531) | Improvement | new write APIs for data source v2 |
| 3.0.0 | [SPARK-25575](https://issues.apache.org/jira/browse/SPARK-25575) | Improvement | SQL tab in the spark UI doesn't have option of hiding tables, eventhough other UI tabs has. |
| 3.0.0 | [SPARK-25884](https://issues.apache.org/jira/browse/SPARK-25884) | Improvement | Add TBLPROPERTIES and COMMENT, and use LOCATION when SHOW CREATE TABLE. |
| 3.0.0 | [SPARK-25993](https://issues.apache.org/jira/browse/SPARK-25993) | Improvement | Add test cases for CREATE EXTERNAL TABLE with subdirectories |
| 3.0.0 | [SPARK-26176](https://issues.apache.org/jira/browse/SPARK-26176) | Improvement | Verify column name when creating table via `STORED AS` |
| 3.0.0 | [SPARK-26313](https://issues.apache.org/jira/browse/SPARK-26313) | Improvement | move read related methods from Table to read related mix-in traits |
| 3.0.0 | [SPARK-26363](https://issues.apache.org/jira/browse/SPARK-26363) | Improvement | Avoid duplicated KV store lookups for task table |
| 3.0.0 | [SPARK-26893](https://issues.apache.org/jira/browse/SPARK-26893) | Improvement | Allow partition pruning with subquery filters on file source |
| 3.0.0 | [SPARK-26946](https://issues.apache.org/jira/browse/SPARK-26946) | New Feature | Identifiers for multi-catalog Spark |
| 3.0.0 | [SPARK-27108](https://issues.apache.org/jira/browse/SPARK-27108) | Improvement | Add parsed CreateTable plans to Catalyst |
| 3.0.0 | [SPARK-27181](https://issues.apache.org/jira/browse/SPARK-27181) | Improvement | Add public expression and transform API for DSv2 partitioning |
| 3.0.0 | [SPARK-27266](https://issues.apache.org/jira/browse/SPARK-27266) | New Feature | Support ANALYZE TABLE to collect tables stats for cached catalog views |
| 3.0.0 | [SPARK-27322](https://issues.apache.org/jira/browse/SPARK-27322) | New Feature | DataSourceV2 table relation |
| 3.0.0 | [SPARK-27471](https://issues.apache.org/jira/browse/SPARK-27471) | Improvement | Reorganize public v2 catalog API |
| 3.0.0 | [SPARK-27531](https://issues.apache.org/jira/browse/SPARK-27531) | Improvement | Improve explain output of describe table command to show the inputs to the command. |
| 3.0.0 | [SPARK-27563](https://issues.apache.org/jira/browse/SPARK-27563) | Improvement | automatically get the latest Spark versions in HiveExternalCatalogVersionsSuite |
| 3.0.0 | [SPARK-27592](https://issues.apache.org/jira/browse/SPARK-27592) | Improvement | Set the bucketed data source table SerDe correctly |
| 3.0.0 | [SPARK-27618](https://issues.apache.org/jira/browse/SPARK-27618) | Improvement | Unnecessary access to externalCatalog |
| 3.0.0 | [SPARK-27627](https://issues.apache.org/jira/browse/SPARK-27627) | Improvement | Make option "pathGlobFilter" as a general option for all file sources |
| 3.0.0 | [SPARK-27639](https://issues.apache.org/jira/browse/SPARK-27639) | Improvement | InMemoryTableScan shows the table name on UI if possible |
| 3.0.0 | [SPARK-27690](https://issues.apache.org/jira/browse/SPARK-27690) | Improvement | Remove materialized views first in `HiveClientImpl.reset` |
| 3.0.0 | [SPARK-27693](https://issues.apache.org/jira/browse/SPARK-27693) | Improvement | DataSourceV2: Add default catalog property |
| 3.0.0 | [SPARK-27694](https://issues.apache.org/jira/browse/SPARK-27694) | Improvement | Support auto-updating table statistics for data source CTAS command |
| 3.0.0 | [SPARK-27813](https://issues.apache.org/jira/browse/SPARK-27813) | Improvement | DataSourceV2: Add DropTable logical operation |
| 3.0.0 | [SPARK-27845](https://issues.apache.org/jira/browse/SPARK-27845) | Improvement | DataSourceV2: InsertTable |
| 3.0.0 | [SPARK-27899](https://issues.apache.org/jira/browse/SPARK-27899) | Improvement | Make HiveMetastoreClient.getTableObjectsByName available in ExternalCatalog/SessionCatalog API |
| 3.0.0 | [SPARK-27919](https://issues.apache.org/jira/browse/SPARK-27919) | Improvement | DataSourceV2: Add v2 session catalog |
| 3.0.0 | [SPARK-27964](https://issues.apache.org/jira/browse/SPARK-27964) | Improvement | Create CatalogV2Util |
| 3.0.0 | [SPARK-27990](https://issues.apache.org/jira/browse/SPARK-27990) | New Feature | Recursive data loading from file sources |
| 3.0.0 | [SPARK-28063](https://issues.apache.org/jira/browse/SPARK-28063) | Improvement | Replace deprecated `.newInstance()` in DSv2 `Catalogs` |
| 3.0.0 | [SPARK-28178](https://issues.apache.org/jira/browse/SPARK-28178) | Improvement | DataSourceV2: DataFrameWriter.insertInfo |
| 3.0.0 | [SPARK-28196](https://issues.apache.org/jira/browse/SPARK-28196) | Improvement | Add a new `listTables` and `listLocalTempViews` APIs for SessionCatalog |
| 3.0.0 | [SPARK-28238](https://issues.apache.org/jira/browse/SPARK-28238) | New Feature | DESCRIBE TABLE for Data Source V2 tables |
| 3.0.0 | [SPARK-28265](https://issues.apache.org/jira/browse/SPARK-28265) | Improvement | Missing TableCatalog API to rename table |
| 3.0.0 | [SPARK-28303](https://issues.apache.org/jira/browse/SPARK-28303) | Improvement | Support DELETE/UPDATE/MERGE Operations in DataSource V2 |
| 3.0.0 | [SPARK-28383](https://issues.apache.org/jira/browse/SPARK-28383) | Improvement | SHOW CREATE TABLE is not supported on a temporary view |
| 3.0.0 | [SPARK-28476](https://issues.apache.org/jira/browse/SPARK-28476) | Improvement | Support ALTER DATABASE SET LOCATION |
| 3.0.0 | [SPARK-28565](https://issues.apache.org/jira/browse/SPARK-28565) | Improvement | DataSourceV2: DataFrameWriter.saveAsTable |
| 3.0.0 | [SPARK-28573](https://issues.apache.org/jira/browse/SPARK-28573) | Improvement | Convert InsertIntoTable(HiveTableRelation) to Datasource inserting for partitioned table |
| 3.0.0 | [SPARK-28635](https://issues.apache.org/jira/browse/SPARK-28635) | Improvement | create CatalogManager to track registered v2 catalogs |
| 3.0.0 | [SPARK-28666](https://issues.apache.org/jira/browse/SPARK-28666) | Planned Work | Support the V2SessionCatalog in saveAsTable |
| 3.0.0 | [SPARK-28667](https://issues.apache.org/jira/browse/SPARK-28667) | Planned Work | Support the V2SessionCatalog in insertInto |
| 3.0.0 | [SPARK-28668](https://issues.apache.org/jira/browse/SPARK-28668) | Planned Work | Support the V2SessionCatalog with AlterTable commands |
| 3.0.0 | [SPARK-28675](https://issues.apache.org/jira/browse/SPARK-28675) | Improvement | Replace CatalogUtils.maskCredentials with SQLConf.get.redactOptions |
| 3.0.0 | [SPARK-28747](https://issues.apache.org/jira/browse/SPARK-28747) | Improvement | merge the two data source v2 fallback configs |
| 3.0.0 | [SPARK-28847](https://issues.apache.org/jira/browse/SPARK-28847) | Improvement | Annotate HiveExternalCatalogVersionsSuite with ExtendedHiveTest |
| 3.0.0 | [SPARK-28878](https://issues.apache.org/jira/browse/SPARK-28878) | Improvement | DataSourceV2 should not insert extra projection for columnar batches |
| 3.0.0 | [SPARK-28970](https://issues.apache.org/jira/browse/SPARK-28970) | New Feature | implement USE CATALOG/NAMESPACE for Data Source V2 |
| 3.0.0 | [SPARK-28974](https://issues.apache.org/jira/browse/SPARK-28974) | Improvement | centralize the Data Source V2 table capability checks |
| 3.0.0 | [SPARK-28979](https://issues.apache.org/jira/browse/SPARK-28979) | Improvement | DataSourceV2: Rename UnresolvedTable |
| 3.0.0 | [SPARK-28996](https://issues.apache.org/jira/browse/SPARK-28996) | Improvement | Add tests regarding username of HiveClient |
| 3.0.0 | [SPARK-29057](https://issues.apache.org/jira/browse/SPARK-29057) | Improvement | remove InsertIntoTable |
| 3.0.0 | [SPARK-29063](https://issues.apache.org/jira/browse/SPARK-29063) | Improvement | fillna support for joined table |
| 3.0.0 | [SPARK-29069](https://issues.apache.org/jira/browse/SPARK-29069) | Improvement | ResolveInsertInto should not do table look up |
| 3.0.0 | [SPARK-29158](https://issues.apache.org/jira/browse/SPARK-29158) | Improvement | Expose SerializableConfiguration for DSv2 |
| 3.0.0 | [SPARK-29197](https://issues.apache.org/jira/browse/SPARK-29197) | Improvement | Remove saveModeForDSV2 in DataFrameWriter |
| 3.0.0 | [SPARK-29215](https://issues.apache.org/jira/browse/SPARK-29215) | Improvement | current namespace should be tracked in SessionCatalog if the current catalog is session catalog |
| 3.0.0 | [SPARK-29219](https://issues.apache.org/jira/browse/SPARK-29219) | Improvement | DataSourceV2: Support all SaveModes in DataFrameWriter.save |
| 3.0.0 | [SPARK-29247](https://issues.apache.org/jira/browse/SPARK-29247) | Improvement | HiveClientImpl may be log sensitive information |
| 3.0.0 | [SPARK-29279](https://issues.apache.org/jira/browse/SPARK-29279) | New Feature | DataSourceV2: merge SHOW NAMESPACES and SHOW DATABASES code path |
| 3.0.0 | [SPARK-29412](https://issues.apache.org/jira/browse/SPARK-29412) | Improvement | refine the document of v2 session catalog config |
| 3.0.0 | [SPARK-29421](https://issues.apache.org/jira/browse/SPARK-29421) | Improvement | Using 'USING provider' to specify a different table provider in CREATE TABLE LIKE |
| 3.0.0 | [SPARK-29665](https://issues.apache.org/jira/browse/SPARK-29665) | Improvement | refine the TableProvider interface |
| 3.0.0 | [SPARK-29753](https://issues.apache.org/jira/browse/SPARK-29753) | Improvement | refine the default catalog config |
| 3.0.0 | [SPARK-29763](https://issues.apache.org/jira/browse/SPARK-29763) | Story | Stage UI Page not showing all accumulators in Task Table |
| 3.0.0 | [SPARK-29839](https://issues.apache.org/jira/browse/SPARK-29839) | Improvement | Supporting STORED AS in CREATE TABLE LIKE |
| 3.0.0 | [SPARK-29851](https://issues.apache.org/jira/browse/SPARK-29851) | Improvement | V2 Catalog: Default behavior of dropping namespace is cascading |
| 3.0.0 | [SPARK-29876](https://issues.apache.org/jira/browse/SPARK-29876) | Improvement | Delete/archive file source completed files in separate thread |
| 3.0.0 | [SPARK-29966](https://issues.apache.org/jira/browse/SPARK-29966) | Improvement | avoid load table twice |
| 3.0.0 | [SPARK-29979](https://issues.apache.org/jira/browse/SPARK-29979) | Improvement | Add basic/reserved property key constants in Table and SupportsNamespaces |
| 3.0.0 | [SPARK-30016](https://issues.apache.org/jira/browse/SPARK-30016) | Umbrella | Support ownership for DS v2 tables/namespaces |
| 3.0.0 | [SPARK-30106](https://issues.apache.org/jira/browse/SPARK-30106) | Improvement | DynamicPartitionPruningSuite#"no predicate on the dimension table" is not be tested |
| 3.0.0 | [SPARK-30112](https://issues.apache.org/jira/browse/SPARK-30112) | Improvement | Insert overwrite should be able to overwrite to same table under dynamic partition overwrite |
| 3.0.0 | [SPARK-30302](https://issues.apache.org/jira/browse/SPARK-30302) | Improvement | Complete info for show create table for views |
| 3.0.0 | [SPARK-30314](https://issues.apache.org/jira/browse/SPARK-30314) | Improvement | Add identifier and catalog information to DataSourceV2Relation |
| 3.0.0 | [SPARK-30384](https://issues.apache.org/jira/browse/SPARK-30384) | Improvement | Needs to improve the Column name and tooltips for the Fair Scheduler Pool Table |
| 3.0.0 | [SPARK-30410](https://issues.apache.org/jira/browse/SPARK-30410) | Improvement | Calculating size of table having large number of partitions causes flooding logs |
| 3.0.0 | [SPARK-30468](https://issues.apache.org/jira/browse/SPARK-30468) | Improvement | Use multiple lines to display data columns for show create table command |
| 3.0.0 | [SPARK-30475](https://issues.apache.org/jira/browse/SPARK-30475) | Improvement | File source V2: Push data filters for file listing |
| 3.0.0 | [SPARK-30578](https://issues.apache.org/jira/browse/SPARK-30578) | Improvement | Explicitly set conf to use datasource v2 for v2.3/OrcFilterSuite |
| 3.0.0 | [SPARK-30603](https://issues.apache.org/jira/browse/SPARK-30603) | Improvement | Keep the reserved properties of namespaces and tables private |
| 3.0.0 | [SPARK-30605](https://issues.apache.org/jira/browse/SPARK-30605) | Improvement | move defaultNamespace from SupportsNamespace to CatalogPlugin |
| 3.0.0 | [SPARK-30609](https://issues.apache.org/jira/browse/SPARK-30609) | Improvement | Allow default merge command resolution to be bypassed by DSv2 sources |
| 3.0.0 | [SPARK-30680](https://issues.apache.org/jira/browse/SPARK-30680) | Improvement | ResolvedNamespace does not require a namespace catalog |
| 3.0.0 | [SPARK-30757](https://issues.apache.org/jira/browse/SPARK-30757) | Improvement | Update the doc on TableCatalog.alterTable's behavior |
| 3.0.0 | [SPARK-30844](https://issues.apache.org/jira/browse/SPARK-30844) | Improvement | Static partition should also follow StoreAssignmentPolicy when insert into table |
| 3.0.0 | [SPARK-31024](https://issues.apache.org/jira/browse/SPARK-31024) | Improvement | Allow specifying session catalog name (spark_catalog) in qualified column names |
| 3.0.0 | [SPARK-31121](https://issues.apache.org/jira/browse/SPARK-31121) | New Feature | catalog plugin API |
| 3.0.0 | [SPARK-31204](https://issues.apache.org/jira/browse/SPARK-31204) | Improvement | HiveResult compatibility for DatasourceV2 command |
| 3.0.0 | [SPARK-31224](https://issues.apache.org/jira/browse/SPARK-31224) | Improvement | Support views in both SHOW CREATE TABLE and SHOW CREATE TABLE AS SERDE |
| 3.0.0 | [SPARK-31516](https://issues.apache.org/jira/browse/SPARK-31516) | Improvement | Non-existed metric hiveClientCalls.count of CodeGenerator in Monitoring Doc |
| 3.0.0 | [SPARK-35444](https://issues.apache.org/jira/browse/SPARK-35444) | Improvement | Improve createTable logic if table exists |
| 3.1.1 | [SPARK-30497](https://issues.apache.org/jira/browse/SPARK-30497) | Improvement | migrate DESCRIBE TABLE to the new framework |
| 3.1.1 | [SPARK-34153](https://issues.apache.org/jira/browse/SPARK-34153) | Improvement | Remove unused `getRawTable()` from `HiveExternalCatalog.alterPartitions()` |
| 3.2.0 | [SPARK-27658](https://issues.apache.org/jira/browse/SPARK-27658) | Improvement | Catalog API to load functions |
| 3.2.0 | [SPARK-31891](https://issues.apache.org/jira/browse/SPARK-31891) | Improvement | `ALTER TABLE multipartIdentifier RECOVER PARTITIONS` should drop partition if partition specific location is not exist any more |
| 3.2.0 | [SPARK-32985](https://issues.apache.org/jira/browse/SPARK-32985) | Improvement | Decouple bucket filter pruning and bucket table scan |
| 3.2.0 | [SPARK-33617](https://issues.apache.org/jira/browse/SPARK-33617) | Improvement | Avoid generating small files for INSERT INTO TABLE from VALUES |
| 3.2.0 | [SPARK-33651](https://issues.apache.org/jira/browse/SPARK-33651) | Improvement | allow CREATE EXTERNAL TABLE with LOCATION for data source tables |
| 3.2.0 | [SPARK-34001](https://issues.apache.org/jira/browse/SPARK-34001) | Improvement | Remove unused runShowTablesSql() in DataSourceV2SQLSuite.scala |
| 3.2.0 | [SPARK-34074](https://issues.apache.org/jira/browse/SPARK-34074) | Improvement | Update stats when table size changes |
| 3.2.0 | [SPARK-34099](https://issues.apache.org/jira/browse/SPARK-34099) | Improvement | Refactor table caching in `DataSourceV2Strategy` |
| 3.2.0 | [SPARK-34129](https://issues.apache.org/jira/browse/SPARK-34129) | Improvement | Add table name to LogicalRelation.simpleString |
| 3.2.0 | [SPARK-34153](https://issues.apache.org/jira/browse/SPARK-34153) | Improvement | Remove unused `getRawTable()` from `HiveExternalCatalog.alterPartitions()` |
| 3.2.0 | [SPARK-34155](https://issues.apache.org/jira/browse/SPARK-34155) | Improvement | Add partition columns for TPCDS tables |
| 3.2.0 | [SPARK-34207](https://issues.apache.org/jira/browse/SPARK-34207) | Improvement | Rename `isTemporaryTable` to `isTempView` in `SessionCatalog` |
| 3.2.0 | [SPARK-34209](https://issues.apache.org/jira/browse/SPARK-34209) | Improvement | Allow multiple namespaces with session catalog |
| 3.2.0 | [SPARK-34255](https://issues.apache.org/jira/browse/SPARK-34255) | Improvement | DataSource V2: support static partitioning on required distribution and ordering |
| 3.2.0 | [SPARK-34299](https://issues.apache.org/jira/browse/SPARK-34299) | Improvement | Clean up ResolveSessionCatalog |
| 3.2.0 | [SPARK-34324](https://issues.apache.org/jira/browse/SPARK-34324) | Improvement | FileTable should not list TRUNCATE in capabilities by default |
| 3.2.0 | [SPARK-34335](https://issues.apache.org/jira/browse/SPARK-34335) | Improvement | Support referencing subquery with column aliases by table alias |
| 3.2.0 | [SPARK-34338](https://issues.apache.org/jira/browse/SPARK-34338) | Umbrella | Report metrics from Datasource v2 scan |
| 3.2.0 | [SPARK-34456](https://issues.apache.org/jira/browse/SPARK-34456) | Improvement | Remove unused write options from BatchWriteHelper |
| 3.2.0 | [SPARK-34457](https://issues.apache.org/jira/browse/SPARK-34457) | Improvement | DataSource V2: Add default null ordering to SortDirection |
| 3.2.0 | [SPARK-34518](https://issues.apache.org/jira/browse/SPARK-34518) | Improvement | Rename `AlterTableRecoverPartitionsCommand` to `RepairTableCommand` |
| 3.2.0 | [SPARK-34585](https://issues.apache.org/jira/browse/SPARK-34585) | Improvement | Remove BatchWriteHelper completely |
| 3.2.0 | [SPARK-34603](https://issues.apache.org/jira/browse/SPARK-34603) | Improvement | Support ADD ARCHIVE and LIST ARCHIVES command |
| 3.2.0 | [SPARK-34678](https://issues.apache.org/jira/browse/SPARK-34678) | Improvement | Add table function registry |
| 3.2.0 | [SPARK-34800](https://issues.apache.org/jira/browse/SPARK-34800) | Improvement | Use fine-grained lock in SessionCatalog.tableExists |
| 3.2.0 | [SPARK-34935](https://issues.apache.org/jira/browse/SPARK-34935) | Improvement | CREATE TABLE LIKE should respect the reserved properties of tables |
| 3.2.0 | [SPARK-35087](https://issues.apache.org/jira/browse/SPARK-35087) | Improvement | Some columns in table ` Aggregated Metrics by Executor` of stage-detail page shows incorrectly. |
| 3.2.0 | [SPARK-35122](https://issues.apache.org/jira/browse/SPARK-35122) | Improvement | Migrate CACHE/UNCACHE TABLE to use AnalysisOnlyCommand |
| 3.2.0 | [SPARK-35236](https://issues.apache.org/jira/browse/SPARK-35236) | Improvement | Support archive files as resources for CREATE FUNCTION USING syntax |
| 3.2.0 | [SPARK-35332](https://issues.apache.org/jira/browse/SPARK-35332) | Improvement | Not Coalesce shuffle partitions when cache table |
| 3.2.0 | [SPARK-35360](https://issues.apache.org/jira/browse/SPARK-35360) | Improvement | Spark make add partition batch size configurable when call RepairTableCommand |
| 3.2.0 | [SPARK-35556](https://issues.apache.org/jira/browse/SPARK-35556) | Improvement | Remove the close HiveClient's SessionState |
| 3.2.0 | [SPARK-35629](https://issues.apache.org/jira/browse/SPARK-35629) | Improvement | Use better exception type if database doesn't exist on `drop database` |
| 3.2.0 | [SPARK-35779](https://issues.apache.org/jira/browse/SPARK-35779) | Improvement | Support dynamic filtering for v2 tables |
| 3.2.0 | [SPARK-36178](https://issues.apache.org/jira/browse/SPARK-36178) | Improvement | Document PySpark Catalog APIs in docs/source/reference/pyspark.sql.rst |
| 4.1.3 | [SPARK-57642](https://issues.apache.org/jira/browse/SPARK-57642) | Improvement | Require predicateSql to be present for the DSv2 CHECK constraint |
<!-- AUTO:timeline END -->
