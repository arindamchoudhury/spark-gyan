# Build & Language support

> Source: `release-notes/spark_all_changelogs.txt`. Timeline rows below are generated from `_catalog.jsonl`; prose outside the AUTO markers is hand-written.

## How it evolved

_TODO: connective prose added during the era passes._

## Timeline

<!-- AUTO:timeline START -->
| Release | JIRA | Type | Title |
|---|---|---|---|
| 1.5.0 | [SPARK-6782](https://issues.apache.org/jira/browse/SPARK-6782) | Improvement | add sbt-revolver plugin to sbt build |
| 1.5.0 | [SPARK-7389](https://issues.apache.org/jira/browse/SPARK-7389) | Improvement | Tachyon integration improvement |
| 1.5.0 | [SPARK-7801](https://issues.apache.org/jira/browse/SPARK-7801) | Improvement | Upgrade master versions to Spark 1.5.0 |
| 1.5.0 | [SPARK-8316](https://issues.apache.org/jira/browse/SPARK-8316) | Improvement | Upgrade Maven to 3.3.3 |
| 1.5.0 | [SPARK-8933](https://issues.apache.org/jira/browse/SPARK-8933) | Improvement | Provide a --force flag to build/mvn that always uses downloaded maven |
| 1.5.0 | [SPARK-9015](https://issues.apache.org/jira/browse/SPARK-9015) | Improvement | Maven cleanup / Clean Project Import in scala-ide |
| 1.5.0 | [SPARK-9094](https://issues.apache.org/jira/browse/SPARK-9094) | Improvement | Increase io.dropwizard.metrics dependency to 3.1.2 |
| 1.5.0 | [SPARK-9199](https://issues.apache.org/jira/browse/SPARK-9199) | Improvement | Upgrade Tachyon dependency to 0.7.0 |
| 1.5.0 | [SPARK-9250](https://issues.apache.org/jira/browse/SPARK-9250) | Improvement | ./dev/change-scala-version.sh should offer guidance what versions are accepted, i.e. 2.10 or 2.11 |
| 1.5.0 | [SPARK-9507](https://issues.apache.org/jira/browse/SPARK-9507) | Improvement | Remove dependency reduced POM hack now that shade plugin is updated |
| 1.5.0 | [SPARK-9521](https://issues.apache.org/jira/browse/SPARK-9521) | Improvement | Require Maven 3.3.3+ in the build |
| 1.5.0 | [SPARK-9633](https://issues.apache.org/jira/browse/SPARK-9633) | Improvement | SBT download locations outdated; need an update |
| 1.5.0 | [SPARK-10070](https://issues.apache.org/jira/browse/SPARK-10070) | Improvement | Remove Guava dependencies in user guides |
| 1.6.0 | [SPARK-9545](https://issues.apache.org/jira/browse/SPARK-9545) | Improvement | Run Maven tests in pull request builder if title has "[test-maven]" in it |
| 1.6.0 | [SPARK-10447](https://issues.apache.org/jira/browse/SPARK-10447) | Improvement | Upgrade pyspark to use py4j 0.9 |
| 1.6.0 | [SPARK-10657](https://issues.apache.org/jira/browse/SPARK-10657) | Improvement | Remove legacy SCP-based Jenkins log archiving code |
| 1.6.0 | [SPARK-10949](https://issues.apache.org/jira/browse/SPARK-10949) | Improvement | Upgrade Snappy Java to 1.1.2 |
| 1.6.0 | [SPARK-10997](https://issues.apache.org/jira/browse/SPARK-10997) | Improvement | Netty-based RPC env should support a "client-only" mode. |
| 1.6.0 | [SPARK-11122](https://issues.apache.org/jira/browse/SPARK-11122) | Improvement | Fatal warnings in sbt are not displayed as such |
| 1.6.0 | [SPARK-11236](https://issues.apache.org/jira/browse/SPARK-11236) | Improvement | Upgrade Tachyon dependency to 0.8.0 |
| 1.6.0 | [SPARK-11245](https://issues.apache.org/jira/browse/SPARK-11245) | Improvement | Upgrade twitter4j to version 4.x |
| 1.6.0 | [SPARK-11491](https://issues.apache.org/jira/browse/SPARK-11491) | Improvement | Use Scala 2.10.5 |
| 1.6.0 | [SPARK-12065](https://issues.apache.org/jira/browse/SPARK-12065) | Improvement | Upgrade Tachyon dependency to 0.8.2 |
| 1.6.3 | [SPARK-17378](https://issues.apache.org/jira/browse/SPARK-17378) | Improvement | Upgrade snappy-java to 1.1.2.6 |
| 2.0.0 | [SPARK-6363](https://issues.apache.org/jira/browse/SPARK-6363) | Improvement | Switch to Scala 2.11 for default build |
| 2.0.0 | [SPARK-10359](https://issues.apache.org/jira/browse/SPARK-10359) | New Feature | Enumerate Spark's dependencies in a file and diff against it for new pull requests |
| 2.0.0 | [SPARK-12269](https://issues.apache.org/jira/browse/SPARK-12269) | Improvement | Update aws-java-sdk version |
| 2.0.0 | [SPARK-12475](https://issues.apache.org/jira/browse/SPARK-12475) | Improvement | Upgrade Zinc from 0.3.5.3 to 0.3.9 |
| 2.0.0 | [SPARK-12500](https://issues.apache.org/jira/browse/SPARK-12500) | Improvement | Fix Tachyon deprecations; pull Tachyon dependency into one class |
| 2.0.0 | [SPARK-12643](https://issues.apache.org/jira/browse/SPARK-12643) | Improvement | Set lib directory for antlr |
| 2.0.0 | [SPARK-12761](https://issues.apache.org/jira/browse/SPARK-12761) | Improvement | Clean up duplicated code in scala 2.11 repl.Main |
| 2.0.0 | [SPARK-12967](https://issues.apache.org/jira/browse/SPARK-12967) | Improvement | NettyRPC races with SparkContext.stop() and throws exception |
| 2.0.0 | [SPARK-13175](https://issues.apache.org/jira/browse/SPARK-13175) | Improvement | Cleanup deprecation warnings from Scala 2.11 upgrade |
| 2.0.0 | [SPARK-13189](https://issues.apache.org/jira/browse/SPARK-13189) | Improvement | Cleanup build references to Scala 2.10 |
| 2.0.0 | [SPARK-13203](https://issues.apache.org/jira/browse/SPARK-13203) | Improvement | Add scalastyle rule banning use of mutable.SynchronizedBuffer |
| 2.0.0 | [SPARK-13324](https://issues.apache.org/jira/browse/SPARK-13324) | Improvement | Update plugin, test, example dependencies for 2.x |
| 2.0.0 | [SPARK-13663](https://issues.apache.org/jira/browse/SPARK-13663) | Improvement | Upgrade Snappy Java to 1.1.2.1 |
| 2.0.0 | [SPARK-13825](https://issues.apache.org/jira/browse/SPARK-13825) | Improvement | Upgrade to Scala 2.11.8 |
| 2.0.0 | [SPARK-13834](https://issues.apache.org/jira/browse/SPARK-13834) | Improvement | Update sbt and sbt plugins for 2.x. |
| 2.0.0 | [SPARK-13884](https://issues.apache.org/jira/browse/SPARK-13884) | Improvement | Remove DescribeCommand's dependency on LogicalPlan |
| 2.0.0 | [SPARK-13890](https://issues.apache.org/jira/browse/SPARK-13890) | Improvement | Remove some internal classes' dependency on SQLContext |
| 2.0.0 | [SPARK-14281](https://issues.apache.org/jira/browse/SPARK-14281) | Improvement | Fix the java8-tests profile and run those tests in Jenkins |
| 2.0.0 | [SPARK-14290](https://issues.apache.org/jira/browse/SPARK-14290) | Improvement | Fully utilize the network bandwidth for Netty RPC by avoid significant underlying memory copy |
| 2.0.0 | [SPARK-14444](https://issues.apache.org/jira/browse/SPARK-14444) | Improvement | Add a new scalastyle `NoScalaDoc` to prevent ScalaDoc-style multiline comments |
| 2.0.0 | [SPARK-14470](https://issues.apache.org/jira/browse/SPARK-14470) | Improvement | Allow for overriding both httpclient and httpcore versions |
| 2.0.0 | [SPARK-14508](https://issues.apache.org/jira/browse/SPARK-14508) | Improvement | Add a new ScalaStyle Rule `OmitBracesInCase` |
| 2.0.0 | [SPARK-14601](https://issues.apache.org/jira/browse/SPARK-14601) | Improvement | Minor doc/usage changes related to removal of Spark assembly |
| 2.0.0 | [SPARK-14787](https://issues.apache.org/jira/browse/SPARK-14787) | Improvement | Upgrade Joda-Time library from 2.9 to 2.9.3 |
| 2.0.0 | [SPARK-14790](https://issues.apache.org/jira/browse/SPARK-14790) | Improvement | Scalastyle should run on compile in sbt |
| 2.0.0 | [SPARK-14867](https://issues.apache.org/jira/browse/SPARK-14867) | Improvement | Remove `--force` option in `build/mvn`. |
| 2.0.0 | [SPARK-14897](https://issues.apache.org/jira/browse/SPARK-14897) | Improvement | Upgrade Jetty to latest version of 8/9 |
| 2.0.0 | [SPARK-15061](https://issues.apache.org/jira/browse/SPARK-15061) | Improvement | Upgrade Py4J to 0.10.1 |
| 2.0.0 | [SPARK-15123](https://issues.apache.org/jira/browse/SPARK-15123) | Improvement | upgrade org.json4s to 3.2.11 version |
| 2.0.0 | [SPARK-15737](https://issues.apache.org/jira/browse/SPARK-15737) | Improvement | Fix Jetty server start warning |
| 2.0.0 | [SPARK-15827](https://issues.apache.org/jira/browse/SPARK-15827) | Improvement | Publish Spark's forked sbt-pom-reader to Maven Central |
| 2.0.0 | [SPARK-16155](https://issues.apache.org/jira/browse/SPARK-16155) | Improvement | Remove package grouping in genjavadoc |
| 2.0.1 | [SPARK-17378](https://issues.apache.org/jira/browse/SPARK-17378) | Improvement | Upgrade snappy-java to 1.1.2.6 |
| 2.0.1 | [SPARK-17421](https://issues.apache.org/jira/browse/SPARK-17421) | Improvement | Document warnings about "MaxPermSize" parameter when building with Maven and Java 8 |
| 2.1.0 | [SPARK-15207](https://issues.apache.org/jira/browse/SPARK-15207) | New Feature | Use Travis CI for Java Linter and JDK7/8 compilation test |
| 2.1.0 | [SPARK-15271](https://issues.apache.org/jira/browse/SPARK-15271) | New Feature | Allow force pulling executor docker images |
| 2.1.0 | [SPARK-16494](https://issues.apache.org/jira/browse/SPARK-16494) | Improvement | Upgrade breeze version to 0.12 |
| 2.1.0 | [SPARK-16535](https://issues.apache.org/jira/browse/SPARK-16535) | Improvement | pom.xml warning: "Definition of groupId is redundant, because it's inherited from the parent" |
| 2.1.0 | [SPARK-17058](https://issues.apache.org/jira/browse/SPARK-17058) | Improvement | Add maven snapshots-and-staging profile to build/test against staging artifacts |
| 2.1.0 | [SPARK-17276](https://issues.apache.org/jira/browse/SPARK-17276) | Improvement | Stop environment parameters flooding Jenkins build output |
| 2.1.0 | [SPARK-17314](https://issues.apache.org/jira/browse/SPARK-17314) | Improvement | Use Netty's DefaultThreadFactory to enable its fast ThreadLocal impl |
| 2.1.0 | [SPARK-17378](https://issues.apache.org/jira/browse/SPARK-17378) | Improvement | Upgrade snappy-java to 1.1.2.6 |
| 2.1.0 | [SPARK-17379](https://issues.apache.org/jira/browse/SPARK-17379) | Improvement | Upgrade netty-all to 4.0.41.Final (4.1.5-Final not compatible) |
| 2.1.0 | [SPARK-17421](https://issues.apache.org/jira/browse/SPARK-17421) | Improvement | Document warnings about "MaxPermSize" parameter when building with Maven and Java 8 |
| 2.1.0 | [SPARK-17960](https://issues.apache.org/jira/browse/SPARK-17960) | Improvement | Upgrade to Py4J 0.10.4 |
| 2.1.0 | [SPARK-18375](https://issues.apache.org/jira/browse/SPARK-18375) | Improvement | Upgrade netty to 4.0.42.Final |
| 2.1.0 | [SPARK-18585](https://issues.apache.org/jira/browse/SPARK-18585) | Improvement | Use `ev.isNull = "false"` if possible for Janino to have a chance to optimize. |
| 2.1.0 | [SPARK-18615](https://issues.apache.org/jira/browse/SPARK-18615) | Improvement | Switch to multi-line doc to avoid a genjavadoc bug for backticks |
| 2.2.0 | [SPARK-17058](https://issues.apache.org/jira/browse/SPARK-17058) | Improvement | Add maven snapshots-and-staging profile to build/test against staging artifacts |
| 2.2.0 | [SPARK-18638](https://issues.apache.org/jira/browse/SPARK-18638) | Improvement | Upgrade sbt, zinc and maven plugins |
| 2.2.0 | [SPARK-18654](https://issues.apache.org/jira/browse/SPARK-18654) | Improvement | JacksonParser.makeRootConverter has effectively unreachable code |
| 2.2.0 | [SPARK-18697](https://issues.apache.org/jira/browse/SPARK-18697) | Improvement | Upgrade sbt plugins |
| 2.2.0 | [SPARK-18972](https://issues.apache.org/jira/browse/SPARK-18972) | Improvement | Fix the netty thread names for RPC |
| 2.2.0 | [SPARK-20064](https://issues.apache.org/jira/browse/SPARK-20064) | Improvement | Bump the PySpark verison number to 2.2 |
| 2.2.0 | [SPARK-20449](https://issues.apache.org/jira/browse/SPARK-20449) | Improvement | Upgrade breeze version to 0.13.1 |
| 2.2.0 | [SPARK-20759](https://issues.apache.org/jira/browse/SPARK-20759) | Improvement | SCALA_VERSION in _config.yml,LICENSE and Dockerfile should be consistent with pom.xml |
| 3.0.0 | [SPARK-17875](https://issues.apache.org/jira/browse/SPARK-17875) | Improvement | Remove unneeded direct dependence on Netty 3.x |
| 3.0.0 | [SPARK-23153](https://issues.apache.org/jira/browse/SPARK-23153) | Improvement | Support application dependencies in submission client's local file system |
| 3.0.0 | [SPARK-24920](https://issues.apache.org/jira/browse/SPARK-24920) | Improvement | Spark should allow sharing netty's memory pools across all uses |
| 3.0.0 | [SPARK-24938](https://issues.apache.org/jira/browse/SPARK-24938) | Improvement | Understand usage of netty's onheap memory use, even with offheap pools |
| 3.0.0 | [SPARK-25079](https://issues.apache.org/jira/browse/SPARK-25079) | Improvement | [PYTHON] upgrade python 3.4 -> 3.6 |
| 3.0.0 | [SPARK-25408](https://issues.apache.org/jira/browse/SPARK-25408) | Improvement | Move to idiomatic Java 8 |
| 3.0.0 | [SPARK-25436](https://issues.apache.org/jira/browse/SPARK-25436) | Improvement | Bump master branch version to 2.5.0-SNAPSHOT |
| 3.0.0 | [SPARK-25494](https://issues.apache.org/jira/browse/SPARK-25494) | Improvement | Upgrade Spark's use of Janino to 3.0.10 |
| 3.0.0 | [SPARK-25592](https://issues.apache.org/jira/browse/SPARK-25592) | Improvement | Bump master branch version to 3.0.0-SNAPSHOT |
| 3.0.0 | [SPARK-25808](https://issues.apache.org/jira/browse/SPARK-25808) | Improvement | upgrade jsr305 version from 1.3.9 to 3.0.0 |
| 3.0.0 | [SPARK-25891](https://issues.apache.org/jira/browse/SPARK-25891) | Improvement | Upgrade to Py4J 0.10.8.1 |
| 3.0.0 | [SPARK-25957](https://issues.apache.org/jira/browse/SPARK-25957) | Improvement | Skip building spark-r docker image if spark distribution does not have R support |
| 3.0.0 | [SPARK-26013](https://issues.apache.org/jira/browse/SPARK-26013) | Improvement | Upgrade R tools version to 3.5.1 in AppVeyor build |
| 3.0.0 | [SPARK-26015](https://issues.apache.org/jira/browse/SPARK-26015) | Improvement | Include a USER directive in project provided Spark Dockerfiles |
| 3.0.0 | [SPARK-26025](https://issues.apache.org/jira/browse/SPARK-26025) | Improvement | Docker image build on dev builds is slow |
| 3.0.0 | [SPARK-26118](https://issues.apache.org/jira/browse/SPARK-26118) | New Feature | Make Jetty's requestHeaderSize configurable in Spark |
| 3.0.0 | [SPARK-26132](https://issues.apache.org/jira/browse/SPARK-26132) | Improvement | Remove support for Scala 2.11 in Spark 3.0.0 |
| 3.0.0 | [SPARK-26212](https://issues.apache.org/jira/browse/SPARK-26212) | Improvement | Upgrade maven from 3.5.4 to 3.6.0 |
| 3.0.0 | [SPARK-26266](https://issues.apache.org/jira/browse/SPARK-26266) | Improvement | Update to Scala 2.12.8 |
| 3.0.0 | [SPARK-26298](https://issues.apache.org/jira/browse/SPARK-26298) | Improvement | Upgrade Janino version to 3.0.11 |
| 3.0.0 | [SPARK-26508](https://issues.apache.org/jira/browse/SPARK-26508) | Improvement | Address warning messages in Java by lgtm.com |
| 3.0.0 | [SPARK-26541](https://issues.apache.org/jira/browse/SPARK-26541) | Improvement | Add `-Pdocker-integration-tests` to `dev/scalastyle` |
| 3.0.0 | [SPARK-26566](https://issues.apache.org/jira/browse/SPARK-26566) | Improvement | Upgrade apache/arrow to 0.12.0 |
| 3.0.0 | [SPARK-26580](https://issues.apache.org/jira/browse/SPARK-26580) | Improvement | remove Scala 2.11 hack for Scala UDF |
| 3.0.0 | [SPARK-26640](https://issues.apache.org/jira/browse/SPARK-26640) | Improvement | Code cleanup from lgtm.com analysis |
| 3.0.0 | [SPARK-26687](https://issues.apache.org/jira/browse/SPARK-26687) | Improvement | Building Spark Images has non-intuitive behaviour with paths to custom Dockerfiles |
| 3.0.0 | [SPARK-26918](https://issues.apache.org/jira/browse/SPARK-26918) | Improvement | All .md should have ASF license header |
| 3.0.0 | [SPARK-27016](https://issues.apache.org/jira/browse/SPARK-27016) | Improvement | Treat all antlr warnings as errors while generating parser from the sql grammar file. |
| 3.0.0 | [SPARK-27026](https://issues.apache.org/jira/browse/SPARK-27026) | Improvement | Upgrade Docker image for release build to Ubuntu 18.04 |
| 3.0.0 | [SPARK-27120](https://issues.apache.org/jira/browse/SPARK-27120) | Improvement | Upgrade scalatest version to 3.0.5 |
| 3.0.0 | [SPARK-27323](https://issues.apache.org/jira/browse/SPARK-27323) | Improvement | Use Single-Abstract-Method support in Scala 2.12 to simplify code |
| 3.0.0 | [SPARK-27451](https://issues.apache.org/jira/browse/SPARK-27451) | Improvement | Upgrade lz4-java to 1.5.1 |
| 3.0.0 | [SPARK-27452](https://issues.apache.org/jira/browse/SPARK-27452) | Improvement | Update zstd-jni to 1.3.8-9 |
| 3.0.0 | [SPARK-27458](https://issues.apache.org/jira/browse/SPARK-27458) | Improvement | Remind developer using IntelliJ to update maven version |
| 3.0.0 | [SPARK-27469](https://issues.apache.org/jira/browse/SPARK-27469) | Improvement | Update Commons BeanUtils to 1.9.3 |
| 3.0.0 | [SPARK-27470](https://issues.apache.org/jira/browse/SPARK-27470) | Improvement | Upgrade pyrolite to 4.23 |
| 3.0.0 | [SPARK-27601](https://issues.apache.org/jira/browse/SPARK-27601) | Improvement | Upgrade stream-lib to 2.9.6 |
| 3.0.0 | [SPARK-27620](https://issues.apache.org/jira/browse/SPARK-27620) | Improvement | Update jetty to 9.4.18.v20190429 |
| 3.0.0 | [SPARK-27637](https://issues.apache.org/jira/browse/SPARK-27637) | Improvement | If exception occured while fetching blocks by netty block transfer service, check whether the relative executor is alive before retry |
| 3.0.0 | [SPARK-27721](https://issues.apache.org/jira/browse/SPARK-27721) | Improvement | spark ./build/mvn test failed on aarch64 |
| 3.0.0 | [SPARK-27755](https://issues.apache.org/jira/browse/SPARK-27755) | Improvement | Update zstd-jni to 1.4.0-1 |
| 3.0.0 | [SPARK-27757](https://issues.apache.org/jira/browse/SPARK-27757) | Improvement | Bump Jackson to 2.9.9 |
| 3.0.0 | [SPARK-27862](https://issues.apache.org/jira/browse/SPARK-27862) | Improvement | Upgrade json4s-jackson to 3.6.6 |
| 3.0.0 | [SPARK-27909](https://issues.apache.org/jira/browse/SPARK-27909) | Improvement | Fix CTE substitution dependence on ResolveRelations throwing AnalysisException |
| 3.0.0 | [SPARK-27979](https://issues.apache.org/jira/browse/SPARK-27979) | Improvement | Remove deprecated `--force` option in `build/mvn` and `run-tests.py` |
| 3.0.0 | [SPARK-28131](https://issues.apache.org/jira/browse/SPARK-28131) | Improvement | Update document type conversion between Python data and SQL types in normal UDFs (Python 3.7) |
| 3.0.0 | [SPARK-28248](https://issues.apache.org/jira/browse/SPARK-28248) | Improvement | Upgrade docker image and library for PostgreSQL integration test |
| 3.0.0 | [SPARK-28381](https://issues.apache.org/jira/browse/SPARK-28381) | Improvement | Upgraded version of Pyrolite to 4.30 |
| 3.0.0 | [SPARK-28544](https://issues.apache.org/jira/browse/SPARK-28544) | Improvement | Update zstd-jni to 1.4.2-1 |
| 3.0.0 | [SPARK-28713](https://issues.apache.org/jira/browse/SPARK-28713) | Improvement | Bump checkstyle from 8.14 to 8.23 |
| 3.0.0 | [SPARK-28719](https://issues.apache.org/jira/browse/SPARK-28719) | New Feature | Enable Github Actions for building master |
| 3.0.0 | [SPARK-28720](https://issues.apache.org/jira/browse/SPARK-28720) | Improvement | Update AppVeyor R version to 3.6.1 |
| 3.0.0 | [SPARK-28758](https://issues.apache.org/jira/browse/SPARK-28758) | Improvement | Upgrade Janino to 3.0.15 |
| 3.0.0 | [SPARK-28961](https://issues.apache.org/jira/browse/SPARK-28961) | Improvement | Upgrade Maven to 3.6.2 |
| 3.0.0 | [SPARK-29011](https://issues.apache.org/jira/browse/SPARK-29011) | Improvement | Upgrade netty-all to 4.1.39-Final |
| 3.0.0 | [SPARK-29075](https://issues.apache.org/jira/browse/SPARK-29075) | Improvement | Add enforcer rule to ban duplicated pom dependency |
| 3.0.0 | [SPARK-29175](https://issues.apache.org/jira/browse/SPARK-29175) | Improvement | Make maven central repository in IsolatedClientLoader configurable |
| 3.0.0 | [SPARK-29199](https://issues.apache.org/jira/browse/SPARK-29199) | Improvement | Add linters and license/dependency checkers to GitHub Action |
| 3.0.0 | [SPARK-29307](https://issues.apache.org/jira/browse/SPARK-29307) | Improvement | Remove scalatest compile warnings |
| 3.0.0 | [SPARK-29332](https://issues.apache.org/jira/browse/SPARK-29332) | Improvement | Upgrade zstd-jni library to 1.4.3 |
| 3.0.0 | [SPARK-29341](https://issues.apache.org/jira/browse/SPARK-29341) | Improvement | Upgrade cloudpickle to 1.0.0 |
| 3.0.0 | [SPARK-29410](https://issues.apache.org/jira/browse/SPARK-29410) | Improvement | Update Commons BeanUtils to 1.9.4 |
| 3.0.0 | [SPARK-29444](https://issues.apache.org/jira/browse/SPARK-29444) | Improvement | Add configuration to support JacksonGenrator to keep fields with null values |
| 3.0.0 | [SPARK-29483](https://issues.apache.org/jira/browse/SPARK-29483) | Improvement | Bump Jackson to 2.10.0 |
| 3.0.0 | [SPARK-29646](https://issues.apache.org/jira/browse/SPARK-29646) | Improvement | Allow pyspark version name format `${versionNumber}-preview` in release script |
| 3.0.0 | [SPARK-29729](https://issues.apache.org/jira/browse/SPARK-29729) | Improvement | Upgrade ASM to 7.2 |
| 3.0.0 | [SPARK-29747](https://issues.apache.org/jira/browse/SPARK-29747) | Improvement | Upgrade Joda-Time library from 2.9.3 to 2.10.5 |
| 3.0.0 | [SPARK-30142](https://issues.apache.org/jira/browse/SPARK-30142) | Improvement | Upgrade Maven to 3.6.3 |
| 3.0.0 | [SPARK-30156](https://issues.apache.org/jira/browse/SPARK-30156) | Improvement | Upgrade Jersey from 2.29 to 2.29.1 |
| 3.0.0 | [SPARK-30163](https://issues.apache.org/jira/browse/SPARK-30163) | Improvement | Use Google Maven mirror in GitHub Action |
| 3.0.0 | [SPARK-30196](https://issues.apache.org/jira/browse/SPARK-30196) | Improvement | Bump lz4-java version to 1.7.0 |
| 3.0.0 | [SPARK-30272](https://issues.apache.org/jira/browse/SPARK-30272) | Improvement | Remove usage of Guava that breaks in Guava 27 |
| 3.0.0 | [SPARK-30453](https://issues.apache.org/jira/browse/SPARK-30453) | Improvement | Update AppVeyor R version to 3.6.2 |
| 3.0.0 | [SPARK-30601](https://issues.apache.org/jira/browse/SPARK-30601) | Improvement | Add a Google Maven Central as a primary repository |
| 3.0.0 | [SPARK-30665](https://issues.apache.org/jira/browse/SPARK-30665) | Improvement | Eliminate pypandoc dependency |
| 3.0.0 | [SPARK-30760](https://issues.apache.org/jira/browse/SPARK-30760) | Improvement | Port `millisToDays` and `daysToMillis` on Java 8 time API |
| 3.0.0 | [SPARK-30944](https://issues.apache.org/jira/browse/SPARK-30944) | Improvement | Use latest URL for Google Cloud Storage mirror of Maven Central |
| 3.0.0 | [SPARK-30994](https://issues.apache.org/jira/browse/SPARK-30994) | Improvement | Update Xerces to 2.12.0 |
| 3.0.0 | [SPARK-31200](https://issues.apache.org/jira/browse/SPARK-31200) | Improvement | Docker image build fails with Mirror sync in progress? errors. |
| 3.0.0 | [SPARK-31644](https://issues.apache.org/jira/browse/SPARK-31644) | Improvement | Make Spark's guava version configurable from the maven command line. |
| 3.0.0 | [SPARK-31778](https://issues.apache.org/jira/browse/SPARK-31778) | Improvement | Support cross-building docker images |
| 3.0.0 | [SPARK-31889](https://issues.apache.org/jira/browse/SPARK-31889) | Improvement | Docker release script does not allocate enough memory to reliably publish |
| 3.1.1 | [SPARK-34158](https://issues.apache.org/jira/browse/SPARK-34158) | Improvement | Incorrect url of the only developer Matei in pom.xml |
| 3.2.0 | [SPARK-25075](https://issues.apache.org/jira/browse/SPARK-25075) | Umbrella | Build and test Spark against Scala 2.13 |
| 3.2.0 | [SPARK-33084](https://issues.apache.org/jira/browse/SPARK-33084) | Improvement | Add jar support ivy path |
| 3.2.0 | [SPARK-33662](https://issues.apache.org/jira/browse/SPARK-33662) | New Feature | Setting version to 3.2.0-SNAPSHOT |
| 3.2.0 | [SPARK-33684](https://issues.apache.org/jira/browse/SPARK-33684) | Improvement | Upgrade httpclient from 4.5.6 to 4.5.13 |
| 3.2.0 | [SPARK-33801](https://issues.apache.org/jira/browse/SPARK-33801) | Improvement | Cleanup "Unicode escapes in triple quoted strings are deprecated" compilation warnings |
| 3.2.0 | [SPARK-33804](https://issues.apache.org/jira/browse/SPARK-33804) | Improvement | Cleanup "view bounds are deprecated" compilation warnings |
| 3.2.0 | [SPARK-33984](https://issues.apache.org/jira/browse/SPARK-33984) | Improvement | Upgrade to Py4J 0.10.9.1 |
| 3.2.0 | [SPARK-34008](https://issues.apache.org/jira/browse/SPARK-34008) | Improvement | Upgrade derby to 10.14.2.0 |
| 3.2.0 | [SPARK-34028](https://issues.apache.org/jira/browse/SPARK-34028) | Improvement | Cleanup "unreachable code" compilation warnings |
| 3.2.0 | [SPARK-34065](https://issues.apache.org/jira/browse/SPARK-34065) | Improvement | Cancel the duplicated jobs only in PRs at GitHub Actions |
| 3.2.0 | [SPARK-34284](https://issues.apache.org/jira/browse/SPARK-34284) | Improvement | Fix deprecated API usage of commons-io |
| 3.2.0 | [SPARK-34375](https://issues.apache.org/jira/browse/SPARK-34375) | Improvement | Replaces `Mockito.initMocks` with `Mockito.openMocks` |
| 3.2.0 | [SPARK-34391](https://issues.apache.org/jira/browse/SPARK-34391) | Improvement | Upgrade commons-io to 2.8.0 |
| 3.2.0 | [SPARK-34428](https://issues.apache.org/jira/browse/SPARK-34428) | Improvement | Update sbt version to 1.4.7 |
| 3.2.0 | [SPARK-34467](https://issues.apache.org/jira/browse/SPARK-34467) | Improvement | Upgrade Zstd-jni to 1.4.8-4 |
| 3.2.0 | [SPARK-34685](https://issues.apache.org/jira/browse/SPARK-34685) | Improvement | Upgrade to Py4J 0.10.9.2 |
| 3.2.0 | [SPARK-34686](https://issues.apache.org/jira/browse/SPARK-34686) | Improvement | Py4J 0.10.9.1 is out with bug fixes. we should better upgrade in PySpark as well. |
| 3.2.0 | [SPARK-34688](https://issues.apache.org/jira/browse/SPARK-34688) | Improvement | Upgrade to Py4J 0.10.9.2 |
| 3.2.0 | [SPARK-34762](https://issues.apache.org/jira/browse/SPARK-34762) | Improvement | Many PR's Scala 2.13 build action failed |
| 3.2.0 | [SPARK-34766](https://issues.apache.org/jira/browse/SPARK-34766) | Improvement | Do not capture maven config for views |
| 3.2.0 | [SPARK-34784](https://issues.apache.org/jira/browse/SPARK-34784) | Improvement | Upgrade Jackson to 2.12.2 |
| 3.2.0 | [SPARK-34789](https://issues.apache.org/jira/browse/SPARK-34789) | Improvement | Introduce Jetty based construct for integration tests where HTTP(S) is used |
| 3.2.0 | [SPARK-34950](https://issues.apache.org/jira/browse/SPARK-34950) | Improvement | Update benchmark results to the ones created by GitHub Actions machines |
| 3.2.0 | [SPARK-35023](https://issues.apache.org/jira/browse/SPARK-35023) | Improvement | Remove deprecated syntex in SBT build file |
| 3.2.0 | [SPARK-35061](https://issues.apache.org/jira/browse/SPARK-35061) | Improvement | Upgrade pycodestyle from 2.6.0 to 2.7.0 |
| 3.2.0 | [SPARK-35132](https://issues.apache.org/jira/browse/SPARK-35132) | Improvement | Upgrade netty-all to 4.1.63.Final |
| 3.2.0 | [SPARK-35138](https://issues.apache.org/jira/browse/SPARK-35138) | Improvement | Remove ANTLR 4.7 workaround |
| 3.2.0 | [SPARK-35175](https://issues.apache.org/jira/browse/SPARK-35175) | Improvement | Add linter for JavaScript source files |
| 3.2.0 | [SPARK-35254](https://issues.apache.org/jira/browse/SPARK-35254) | Improvement | Upgrade SBT to 1.5.1 |
| 3.2.0 | [SPARK-35269](https://issues.apache.org/jira/browse/SPARK-35269) | Improvement | Update commons-lang3 to 3.12 |
| 3.2.0 | [SPARK-35277](https://issues.apache.org/jira/browse/SPARK-35277) | Improvement | Upgrade snappy to 1.1.8.4 |
| 3.2.0 | [SPARK-35373](https://issues.apache.org/jira/browse/SPARK-35373) | Improvement | Verify checksums of downloaded artifacts in build/mvn |
| 3.2.0 | [SPARK-35377](https://issues.apache.org/jira/browse/SPARK-35377) | Improvement | Add JS linter to GA |
| 3.2.0 | [SPARK-35387](https://issues.apache.org/jira/browse/SPARK-35387) | Improvement | Increase the stack size of JVM for Java 11 build test |
| 3.2.0 | [SPARK-35488](https://issues.apache.org/jira/browse/SPARK-35488) | Improvement | Upgrade ASM to 7.3.1 |
| 3.2.0 | [SPARK-35490](https://issues.apache.org/jira/browse/SPARK-35490) | Improvement | Update json4s to 3.7.0-M11 |
| 3.2.0 | [SPARK-35492](https://issues.apache.org/jira/browse/SPARK-35492) | Improvement | Upgrade Apache HttpCore from 4.4.12 to 4.4.14 |
| 3.2.0 | [SPARK-35506](https://issues.apache.org/jira/browse/SPARK-35506) | Improvement | Run tests with Python 3.9 in GitHub Actions |
| 3.2.0 | [SPARK-35507](https://issues.apache.org/jira/browse/SPARK-35507) | Improvement | Move Python 3.9 installtation to the docker image for GitHub Actions |
| 3.2.0 | [SPARK-35513](https://issues.apache.org/jira/browse/SPARK-35513) | Improvement | Upgrade joda-time to 2.10.10 |
| 3.2.0 | [SPARK-35550](https://issues.apache.org/jira/browse/SPARK-35550) | Improvement | Upgrade Jackson to 2.12.3 |
| 3.2.0 | [SPARK-35609](https://issues.apache.org/jira/browse/SPARK-35609) | Improvement | Add style rules to prohibit to use a Guava's API which is incompatible with newer versions |
| 3.2.0 | [SPARK-35620](https://issues.apache.org/jira/browse/SPARK-35620) | Improvement | Remove documentation build in Python linter |
| 3.2.0 | [SPARK-35648](https://issues.apache.org/jira/browse/SPARK-35648) | Improvement | Refine and add dependencies needed for dev in dev/requirement.txt |
| 3.2.0 | [SPARK-35655](https://issues.apache.org/jira/browse/SPARK-35655) | Improvement | Upgrade HtmlUnit and its related artifacts to 2.50. |
| 3.2.0 | [SPARK-35682](https://issues.apache.org/jira/browse/SPARK-35682) | Improvement | Pin mypy version in GitHub Actions CI |
| 3.2.0 | [SPARK-35684](https://issues.apache.org/jira/browse/SPARK-35684) | Improvement | Bump up mypy version in GitHub Actions |
| 3.2.0 | [SPARK-35863](https://issues.apache.org/jira/browse/SPARK-35863) | Improvement | Upgrade Ivy to 2.5.0 |
| 3.2.0 | [SPARK-35922](https://issues.apache.org/jira/browse/SPARK-35922) | Improvement | Upgrade maven-shade-plugin to 3.2.4 |
| 3.2.0 | [SPARK-35928](https://issues.apache.org/jira/browse/SPARK-35928) | Improvement | Upgrade ASM to 9.1 |
| 3.2.0 | [SPARK-35936](https://issues.apache.org/jira/browse/SPARK-35936) | Story | Deprecate Python 3.6 support |
| 3.2.0 | [SPARK-35948](https://issues.apache.org/jira/browse/SPARK-35948) | Improvement | Simplify release scripts by removing Spark 2.4/Java7 parts |
| 3.2.0 | [SPARK-35960](https://issues.apache.org/jira/browse/SPARK-35960) | Improvement | sbt test:compile of tags is broken |
| 3.2.0 | [SPARK-35966](https://issues.apache.org/jira/browse/SPARK-35966) | Improvement | Port HIVE-17952: Fix license headers to avoid dangling javadoc warnings |
| 3.2.0 | [SPARK-36092](https://issues.apache.org/jira/browse/SPARK-36092) | Improvement | Migrate to GitHub Actions Codecov from Jenkins |
| 3.2.0 | [SPARK-36345](https://issues.apache.org/jira/browse/SPARK-36345) | Improvement | Add mlflow/sklearn to GHA docker image |
| 3.2.0 | [SPARK-36547](https://issues.apache.org/jira/browse/SPARK-36547) | Improvement | Downgrade scala-maven-plugin to 4.3.0 |
| 3.5.9 | [SPARK-55115](https://issues.apache.org/jira/browse/SPARK-55115) | Improvement | Use master branch's Dockerfile for release builds |
| 4.0.4 | [SPARK-57254](https://issues.apache.org/jira/browse/SPARK-57254) | Improvement | Do not trigger CI when unrelated file is changed |
| 4.0.4 | [SPARK-57976](https://issues.apache.org/jira/browse/SPARK-57976) | Improvement | Fix python3.9 pip for branch-4.0 |
| 4.1.1 | [SPARK-54847](https://issues.apache.org/jira/browse/SPARK-54847) | Improvement | unify the proto output folder between sbt and maven |
| 4.1.1 | [SPARK-54851](https://issues.apache.org/jira/browse/SPARK-54851) | Improvement | support generating bloop files via sbt |
| 4.1.2 | [SPARK-55115](https://issues.apache.org/jira/browse/SPARK-55115) | Improvement | Use master branch's Dockerfile for release builds |
| 4.1.2 | [SPARK-56989](https://issues.apache.org/jira/browse/SPARK-56989) | Improvement | Publish Apache Spark 4.1.2 to docker registry |
| 4.1.3 | [SPARK-57254](https://issues.apache.org/jira/browse/SPARK-57254) | Improvement | Do not trigger CI when unrelated file is changed |
<!-- AUTO:timeline END -->
