# Security

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-6284](https://issues.apache.org/jira/browse/SPARK-6284) | Improvement | Support framework authentication and role in Mesos framework |
| 1.5.0 | [SPARK-8129](https://issues.apache.org/jira/browse/SPARK-8129) | New Feature | Securely pass auth secrets to executors in standalone cluster mode |
| 1.5.0 | [SPARK-8740](https://issues.apache.org/jira/browse/SPARK-8740) | Improvement | Support GitHub OAuth tokens in dev/merge_spark_pr.py |
| 1.5.0 | [SPARK-8863](https://issues.apache.org/jira/browse/SPARK-8863) | Improvement | 'spark_ec2.py' doesn't check '~/.aws/credentials' even if boto can support '~/.aws/credentials' |
| 1.5.0 | [SPARK-9062](https://issues.apache.org/jira/browse/SPARK-9062) | Improvement | Change output type of Tokenizer to Array(String, true) |
| 1.6.0 | [SPARK-4223](https://issues.apache.org/jira/browse/SPARK-4223) | Improvement | Support * (meaning all users) as part of the acls |
| 1.6.0 | [SPARK-9833](https://issues.apache.org/jira/browse/SPARK-9833) | Improvement | Add options to explicitly disable delegation token retrieval for non-HDFS |
| 1.6.0 | [SPARK-10532](https://issues.apache.org/jira/browse/SPARK-10532) | New Feature | Added new option to specify "user profile" of AWS credentials in spark/spark-ec2.py |
| 1.6.0 | [SPARK-10676](https://issues.apache.org/jira/browse/SPARK-10676) | Improvement | Update documentation with instructions to enable block manager wire encryption |
| 2.0.0 | [SPARK-4224](https://issues.apache.org/jira/browse/SPARK-4224) | Improvement | Support group acls |
| 2.1.0 | [SPARK-14743](https://issues.apache.org/jira/browse/SPARK-14743) | Improvement | Improve delegation token handling in secure clusters |
| 2.1.0 | [SPARK-17002](https://issues.apache.org/jira/browse/SPARK-17002) | Improvement | Document that spark.ssl.protocol. is required for SSL |
| 2.1.0 | [SPARK-18547](https://issues.apache.org/jira/browse/SPARK-18547) | Improvement | Decouple I/O encryption key propagation from UserGroupInformation |
| 2.2.0 | [SPARK-13331](https://issues.apache.org/jira/browse/SPARK-13331) | Improvement | AES support for over-the-wire encryption |
| 2.2.0 | [SPARK-17874](https://issues.apache.org/jira/browse/SPARK-17874) | Improvement | Additional SSL port on HistoryServer should be configurable |
| 2.2.0 | [SPARK-18773](https://issues.apache.org/jira/browse/SPARK-18773) | Improvement | Make translation of Spark configs to commons-crypto configs consistent |
| 2.2.0 | [SPARK-19021](https://issues.apache.org/jira/browse/SPARK-19021) | Improvement | Generailize HDFSCredentialProvider to support non HDFS security FS |
| 2.2.0 | [SPARK-19139](https://issues.apache.org/jira/browse/SPARK-19139) | New Feature | AES-based authentication mechanism for Spark |
| 2.2.0 | [SPARK-19302](https://issues.apache.org/jira/browse/SPARK-19302) | Improvement | Fix the wrong item format in security.md |
| 3.0.0 | [SPARK-23781](https://issues.apache.org/jira/browse/SPARK-23781) | Improvement | Merge YARN and Mesos token renewal code |
| 3.0.0 | [SPARK-24345](https://issues.apache.org/jira/browse/SPARK-24345) | Improvement | Improve ParseError stop location when offending symbol is a token |
| 3.0.0 | [SPARK-24522](https://issues.apache.org/jira/browse/SPARK-24522) | Improvement | Centralize code to deal with security-related HTTP features |
| 3.0.0 | [SPARK-25023](https://issues.apache.org/jira/browse/SPARK-25023) | Improvement | Clarify Spark security documentation |
| 3.0.0 | [SPARK-25689](https://issues.apache.org/jira/browse/SPARK-25689) | Improvement | Move token renewal logic to driver in yarn-client mode |
| 3.0.0 | [SPARK-25815](https://issues.apache.org/jira/browse/SPARK-25815) | New Feature | Kerberos Support in Kubernetes resource manager (Client Mode) |
| 3.0.0 | [SPARK-25857](https://issues.apache.org/jira/browse/SPARK-25857) | Improvement | Document delegation token code in Spark |
| 3.0.0 | [SPARK-26239](https://issues.apache.org/jira/browse/SPARK-26239) | New Feature | Add configurable auth secret source in k8s backend |
| 3.0.0 | [SPARK-26254](https://issues.apache.org/jira/browse/SPARK-26254) | Improvement | Move delegation token providers into a separate project |
| 3.0.0 | [SPARK-26324](https://issues.apache.org/jira/browse/SPARK-26324) | Improvement | Document Mesos SSL config |
| 3.0.0 | [SPARK-26432](https://issues.apache.org/jira/browse/SPARK-26432) | Improvement | Not able to connect Hbase 2.1 service Getting NoSuchMethodException while trying to obtain token from Hbase 2.1 service. |
| 3.0.0 | [SPARK-26595](https://issues.apache.org/jira/browse/SPARK-26595) | New Feature | Allow delegation token renewal without a keytab |
| 3.0.0 | [SPARK-27358](https://issues.apache.org/jira/browse/SPARK-27358) | Improvement | Update jquery to 1.12.x to pick up security fixes |
| 3.0.0 | [SPARK-28055](https://issues.apache.org/jira/browse/SPARK-28055) | Improvement | Add delegation token custom AdminClient configurations. |
| 3.0.0 | [SPARK-28290](https://issues.apache.org/jira/browse/SPARK-28290) | Improvement | Use `SslContextFactory.Server` instead of `SslContextFactory` |
| 3.0.0 | [SPARK-30370](https://issues.apache.org/jira/browse/SPARK-30370) | Improvement | Update SqlBase.g4 to combine namespace and database tokens. |
| 3.2.0 | [SPARK-33720](https://issues.apache.org/jira/browse/SPARK-33720) | Improvement | Support submit to k8s only with token |
| 3.2.0 | [SPARK-33925](https://issues.apache.org/jira/browse/SPARK-33925) | Improvement | Remove unused SecurityManager in Utils.fetchFile |
| 3.2.0 | [SPARK-34520](https://issues.apache.org/jira/browse/SPARK-34520) | Improvement | Remove unused SecurityManager references |
| 3.2.0 | [SPARK-34752](https://issues.apache.org/jira/browse/SPARK-34752) | Improvement | Upgrade Jetty to 9.4.37 to fix CVE-2020-27223 |
| 3.5.9 | [SPARK-56998](https://issues.apache.org/jira/browse/SPARK-56998) | Improvement | Add SECURITY.md + AGENTS.md Security section for scan-agent discoverability |
| 3.5.9 | [SPARK-57962](https://issues.apache.org/jira/browse/SPARK-57962) | Improvement | Guard against path traversal in install_spark tar extraction |
| 4.0.3 | [SPARK-56998](https://issues.apache.org/jira/browse/SPARK-56998) | Improvement | Add SECURITY.md + AGENTS.md Security section for scan-agent discoverability |
| 4.0.4 | [SPARK-57962](https://issues.apache.org/jira/browse/SPARK-57962) | Improvement | Guard against path traversal in install_spark tar extraction |
| 4.1.3 | [SPARK-56998](https://issues.apache.org/jira/browse/SPARK-56998) | Improvement | Add SECURITY.md + AGENTS.md Security section for scan-agent discoverability |
| 4.1.3 | [SPARK-57962](https://issues.apache.org/jira/browse/SPARK-57962) | Improvement | Guard against path traversal in install_spark tar extraction |
<!-- AUTO:timeline END -->
