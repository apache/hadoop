
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop Changelog

## Release 2.3.0 - 2014-02-20

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4997](https://issues.apache.org/jira/browse/HDFS-4997) | libhdfs doesn't return correct error codes in most cases |  Major | libhdfs | Colin P. McCabe | Colin P. McCabe |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9432](https://issues.apache.org/jira/browse/HADOOP-9432) | Add support for markdown .md files in site documentation |  Minor | build, documentation | Steve Loughran | Steve Loughran |
| [HADOOP-9618](https://issues.apache.org/jira/browse/HADOOP-9618) | Add thread which detects JVM pauses |  Major | util | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-5265](https://issues.apache.org/jira/browse/MAPREDUCE-5265) | History server admin service to refresh user and superuser group mappings |  Major | jobhistoryserver | Jason Lowe | Ashwin Shankar |
| [MAPREDUCE-5266](https://issues.apache.org/jira/browse/MAPREDUCE-5266) | Ability to refresh retention settings on history server |  Major | jobhistoryserver | Jason Lowe | Ashwin Shankar |
| [HADOOP-9848](https://issues.apache.org/jira/browse/HADOOP-9848) | Create a MiniKDC for use with security testing |  Major | security, test | Wei Yan | Wei Yan |
| [HADOOP-8545](https://issues.apache.org/jira/browse/HADOOP-8545) | Filesystem Implementation for OpenStack Swift |  Major | fs | Tim Miller | Dmitry Mezhensky |
| [MAPREDUCE-5332](https://issues.apache.org/jira/browse/MAPREDUCE-5332) | Support token-preserving restart of history server |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [YARN-1021](https://issues.apache.org/jira/browse/YARN-1021) | Yarn Scheduler Load Simulator |  Major | scheduler | Wei Yan | Wei Yan |
| [HDFS-5260](https://issues.apache.org/jira/browse/HDFS-5260) | Merge zero-copy memory-mapped HDFS client reads to trunk and branch-2. |  Major | hdfs-client, libhdfs | Chris Nauroth | Chris Nauroth |
| [YARN-1253](https://issues.apache.org/jira/browse/YARN-1253) | Changes to LinuxContainerExecutor to run containers as a single dedicated user in non-secure mode |  Blocker | nodemanager | Alejandro Abdelnur | Roman Shaposhnik |
| [MAPREDUCE-1176](https://issues.apache.org/jira/browse/MAPREDUCE-1176) | FixedLengthInputFormat and FixedLengthRecordReader |  Major | . | BitsOfInfo | Mariappan Asokan |
| [YARN-1392](https://issues.apache.org/jira/browse/YARN-1392) | Allow sophisticated app-to-queue placement policies in the Fair Scheduler |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-10047](https://issues.apache.org/jira/browse/HADOOP-10047) | Add a directbuffer Decompressor API to hadoop |  Major | io | Gopal V | Gopal V |
| [HDFS-5703](https://issues.apache.org/jira/browse/HDFS-5703) | Add support for HTTPS and swebhdfs to HttpFS |  Major | webhdfs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-4949](https://issues.apache.org/jira/browse/HDFS-4949) | Centralized cache management in HDFS |  Major | datanode, namenode | Andrew Wang | Andrew Wang |
| [HDFS-2832](https://issues.apache.org/jira/browse/HDFS-2832) | Enable support for heterogeneous storages in HDFS - DN as a collection of storages |  Major | datanode, namenode | Suresh Srinivas | Arpit Agarwal |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4860](https://issues.apache.org/jira/browse/HDFS-4860) | Add additional attributes to JMX beans |  Major | namenode | Trevor Lorimer | Trevor Lorimer |
| [HADOOP-9241](https://issues.apache.org/jira/browse/HADOOP-9241) | DU refresh interval is not configurable |  Trivial | . | Harsh J | Harsh J |
| [HDFS-4278](https://issues.apache.org/jira/browse/HDFS-4278) | Log an ERROR when DFS\_BLOCK\_ACCESS\_TOKEN\_ENABLE config  is disabled but security is turned on. |  Major | datanode, namenode | Harsh J | Kousuke Saruta |
| [HDFS-5034](https://issues.apache.org/jira/browse/HDFS-5034) | Remove debug prints from getFileLinkInfo |  Trivial | namenode | Andrew Wang | Andrew Wang |
| [HDFS-5004](https://issues.apache.org/jira/browse/HDFS-5004) | Add additional JMX bean for NameNode status data |  Major | namenode | Trevor Lorimer | Trevor Lorimer |
| [HADOOP-9319](https://issues.apache.org/jira/browse/HADOOP-9319) | Update bundled lz4 source to latest version |  Major | . | Arpit Agarwal | Binglin Chang |
| [MAPREDUCE-434](https://issues.apache.org/jira/browse/MAPREDUCE-434) | LocalJobRunner limited to single reducer |  Minor | . | Yoram Arnon | Aaron Kimball |
| [YARN-985](https://issues.apache.org/jira/browse/YARN-985) | Nodemanager should log where a resource was localized |  Major | nodemanager | Ravi Prakash | Ravi Prakash |
| [HDFS-5068](https://issues.apache.org/jira/browse/HDFS-5068) | Convert NNThroughputBenchmark to a Tool to allow generic options. |  Major | benchmarks | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-9860](https://issues.apache.org/jira/browse/HADOOP-9860) | Remove class HackedKeytab and HackedKeytabEncoder from hadoop-minikdc once jira DIRSERVER-1882 solved |  Major | . | Wei Yan | Wei Yan |
| [HADOOP-9487](https://issues.apache.org/jira/browse/HADOOP-9487) | Deprecation warnings in Configuration should go to their own log or otherwise be suppressible |  Major | conf | Steve Loughran |  |
| [HDFS-2933](https://issues.apache.org/jira/browse/HDFS-2933) | Improve DataNode Web UI Index Page |  Major | datanode | Philip Zeyliger | Vivek Ganesan |
| [HADOOP-9693](https://issues.apache.org/jira/browse/HADOOP-9693) | Shell should add a probe for OSX |  Trivial | . | Steve Loughran |  |
| [HADOOP-9784](https://issues.apache.org/jira/browse/HADOOP-9784) | Add a builder for HttpServer |  Major | . | Junping Du | Junping Du |
| [MAPREDUCE-5484](https://issues.apache.org/jira/browse/MAPREDUCE-5484) | YarnChild unnecessarily loads job conf twice |  Major | task | Sandy Ryza | Sandy Ryza |
| [HADOOP-9909](https://issues.apache.org/jira/browse/HADOOP-9909) | org.apache.hadoop.fs.Stat should permit other LANG |  Major | fs | Shinichi Yamashita |  |
| [HDFS-5144](https://issues.apache.org/jira/browse/HDFS-5144) | Document time unit to NameNodeMetrics.java |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-9915](https://issues.apache.org/jira/browse/HADOOP-9915) | o.a.h.fs.Stat support on Macosx |  Trivial | . | Binglin Chang | Binglin Chang |
| [HDFS-4879](https://issues.apache.org/jira/browse/HDFS-4879) | Add "blocked ArrayList" collection to avoid CMS full GCs |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HADOOP-8704](https://issues.apache.org/jira/browse/HADOOP-8704) | add request logging to jetty/httpserver |  Major | . | Thomas Graves | Jonathan Eagles |
| [HDFS-5188](https://issues.apache.org/jira/browse/HDFS-5188) | Clean up BlockPlacementPolicy and its implementations |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-5207](https://issues.apache.org/jira/browse/HDFS-5207) | In BlockPlacementPolicy, update 2 parameters of chooseTarget() |  Major | namenode | Junping Du | Junping Du |
| [MAPREDUCE-5487](https://issues.apache.org/jira/browse/MAPREDUCE-5487) | In task processes, JobConf is unnecessarily loaded again in Limits |  Major | performance, task | Sandy Ryza | Sandy Ryza |
| [HADOOP-9998](https://issues.apache.org/jira/browse/HADOOP-9998) | Provide methods to clear only part of the DNSToSwitchMapping |  Major | net | Junping Du | Junping Du |
| [YARN-1010](https://issues.apache.org/jira/browse/YARN-1010) | FairScheduler: decouple container scheduling from nodemanager heartbeats |  Critical | scheduler | Alejandro Abdelnur | Wei Yan |
| [YARN-1199](https://issues.apache.org/jira/browse/YARN-1199) | Make NM/RM Versions Available |  Major | . | Mit Desai | Mit Desai |
| [YARN-1258](https://issues.apache.org/jira/browse/YARN-1258) | Allow configuring the Fair Scheduler root queue |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-9470](https://issues.apache.org/jira/browse/HADOOP-9470) | eliminate duplicate FQN tests in different Hadoop modules |  Major | test | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-5323](https://issues.apache.org/jira/browse/HDFS-5323) | Remove some deadcode in BlockManager |  Minor | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5267](https://issues.apache.org/jira/browse/HDFS-5267) | Remove volatile from LightWeightHashSet |  Minor | . | Junping Du | Junping Du |
| [HADOOP-9494](https://issues.apache.org/jira/browse/HADOOP-9494) | Excluded auto-generated and examples code from clover reports |  Major | . | Dennis Y | Andrey Klochkov |
| [HDFS-5338](https://issues.apache.org/jira/browse/HDFS-5338) | Add a conf to disable hostname check in DN registration |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10046](https://issues.apache.org/jira/browse/HADOOP-10046) | Print a log message when SSL is enabled |  Trivial | . | David S. Wang | David S. Wang |
| [HADOOP-9897](https://issues.apache.org/jira/browse/HADOOP-9897) | Add method to get path start position without drive specifier in o.a.h.fs.Path |  Trivial | fs | Binglin Chang | Binglin Chang |
| [HADOOP-10005](https://issues.apache.org/jira/browse/HADOOP-10005) | No need to check INFO severity level is enabled or not |  Trivial | . | Jackie Chang | Jackie Chang |
| [HDFS-5360](https://issues.apache.org/jira/browse/HDFS-5360) | Improvement of usage message of renameSnapshot and deleteSnapshot |  Minor | snapshots | Shinichi Yamashita | Shinichi Yamashita |
| [MAPREDUCE-5457](https://issues.apache.org/jira/browse/MAPREDUCE-5457) | Add a KeyOnlyTextOutputReader to enable streaming to write out text files without separators |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5331](https://issues.apache.org/jira/browse/HDFS-5331) | make SnapshotDiff.java to a o.a.h.util.Tool interface implementation |  Major | snapshots | Vinayakumar B | Vinayakumar B |
| [HADOOP-10064](https://issues.apache.org/jira/browse/HADOOP-10064) | Upgrade to maven antrun plugin version 1.7 |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [YARN-1335](https://issues.apache.org/jira/browse/YARN-1335) | Move duplicate code from FSSchedulerApp and FiCaSchedulerApp into SchedulerApplication |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1333](https://issues.apache.org/jira/browse/YARN-1333) | Support blacklisting in the Fair Scheduler |  Major | scheduler | Sandy Ryza | Tsuyoshi Ozawa |
| [YARN-1109](https://issues.apache.org/jira/browse/YARN-1109) | Demote NodeManager "Sending out status for container" logs to debug |  Major | nodemanager | Sandy Ryza | haosdent |
| [MAPREDUCE-5596](https://issues.apache.org/jira/browse/MAPREDUCE-5596) | Allow configuring the number of threads used to serve shuffle connections |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-1290](https://issues.apache.org/jira/browse/YARN-1290) | Let continuous scheduling achieve more balanced task assignment |  Major | . | Wei Yan | Wei Yan |
| [MAPREDUCE-5601](https://issues.apache.org/jira/browse/MAPREDUCE-5601) | ShuffleHandler fadvises file regions as DONTNEED even when fetch fails |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5037](https://issues.apache.org/jira/browse/HDFS-5037) | Active NN should trigger its own edit log rolls |  Critical | ha, namenode | Todd Lipcon | Andrew Wang |
| [HADOOP-10079](https://issues.apache.org/jira/browse/HADOOP-10079) | log a warning message if group resolution takes too long. |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5344](https://issues.apache.org/jira/browse/HDFS-5344) | Make LsSnapshottableDir as Tool interface implementation |  Minor | snapshots, tools | sathish | sathish |
| [HADOOP-9623](https://issues.apache.org/jira/browse/HADOOP-9623) | Update jets3t dependency to  0.9.0 |  Major | fs/s3 | Timothy St. Clair | Amandeep Khurana |
| [HDFS-5371](https://issues.apache.org/jira/browse/HDFS-5371) | Let client retry the same NN when "dfs.client.test.drop.namenode.response.number" is enabled |  Minor | ha, test | Jing Zhao | Jing Zhao |
| [HDFS-5467](https://issues.apache.org/jira/browse/HDFS-5467) | Remove tab characters in hdfs-default.xml |  Trivial | . | Andrew Wang | Shinichi Yamashita |
| [YARN-1387](https://issues.apache.org/jira/browse/YARN-1387) | RMWebServices should use ClientRMService for filtering applications |  Major | api | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5495](https://issues.apache.org/jira/browse/HDFS-5495) | Remove further JUnit3 usages from HDFS |  Major | . | Andrew Wang | Jarek Jarcec Cecho |
| [HADOOP-9594](https://issues.apache.org/jira/browse/HADOOP-9594) | Update apache commons math dependency |  Major | build | Timothy St. Clair | Timothy St. Clair |
| [HADOOP-10095](https://issues.apache.org/jira/browse/HADOOP-10095) | Performance improvement in CodecPool |  Minor | io | Nicolas Liochon | Nicolas Liochon |
| [HADOOP-10067](https://issues.apache.org/jira/browse/HADOOP-10067) | Missing POM dependency on jsr305 |  Minor | . | Robert Rati | Robert Rati |
| [YARN-786](https://issues.apache.org/jira/browse/YARN-786) | Expose application resource usage in RM REST API |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-1303](https://issues.apache.org/jira/browse/YARN-1303) | Allow multiple commands separating with ";" in distributed-shell |  Major | applications/distributed-shell | Tassapol Athiapinya | Xuan Gong |
| [HDFS-5532](https://issues.apache.org/jira/browse/HDFS-5532) | Enable the webhdfs by default to support new HDFS web UI |  Major | webhdfs | Vinayakumar B | Vinayakumar B |
| [HADOOP-10111](https://issues.apache.org/jira/browse/HADOOP-10111) | Allow DU to be initialized with an initial value |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-1423](https://issues.apache.org/jira/browse/YARN-1423) | Support queue placement by secondary group in the Fair Scheduler |  Major | scheduler | Sandy Ryza | Theodore michael Malaska |
| [HDFS-5548](https://issues.apache.org/jira/browse/HDFS-5548) | Use ConcurrentHashMap in portmap |  Major | nfs | Haohui Mai | Haohui Mai |
| [HDFS-5561](https://issues.apache.org/jira/browse/HDFS-5561) | FSNameSystem#getNameJournalStatus() in JMX should return plain text instead of HTML |  Minor | namenode | Fengdong Yu | Haohui Mai |
| [HDFS-5568](https://issues.apache.org/jira/browse/HDFS-5568) | Support inclusion of snapshot paths in Namenode fsck |  Major | snapshots | Vinayakumar B | Vinayakumar B |
| [HADOOP-10132](https://issues.apache.org/jira/browse/HADOOP-10132) | RPC#stopProxy() should log the class of proxy when IllegalArgumentException is encountered |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-5577](https://issues.apache.org/jira/browse/HDFS-5577) | NFS user guide update |  Trivial | documentation | Brandon Li | Brandon Li |
| [HDFS-5563](https://issues.apache.org/jira/browse/HDFS-5563) | NFS gateway should commit the buffered data when read request comes after write to the same file |  Major | nfs | Brandon Li | Brandon Li |
| [MAPREDUCE-5640](https://issues.apache.org/jira/browse/MAPREDUCE-5640) | Rename TestLineRecordReader in jobclient module |  Trivial | test | Jason Lowe | Jason Lowe |
| [YARN-1332](https://issues.apache.org/jira/browse/YARN-1332) | In TestAMRMClient, replace assertTrue with assertEquals where possible |  Minor | . | Sandy Ryza | Sebastian Wong |
| [YARN-1403](https://issues.apache.org/jira/browse/YARN-1403) | Separate out configuration loading from QueueManager in the Fair Scheduler |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5587](https://issues.apache.org/jira/browse/HDFS-5587) | add debug information when NFS fails to start with duplicate user or group names |  Minor | nfs | Brandon Li | Brandon Li |
| [HDFS-5633](https://issues.apache.org/jira/browse/HDFS-5633) | Improve OfflineImageViewer to use less memory |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-4983](https://issues.apache.org/jira/browse/HDFS-4983) | Numeric usernames do not work with WebHDFS FS |  Major | webhdfs | Harsh J | Yongjun Zhang |
| [YARN-807](https://issues.apache.org/jira/browse/YARN-807) | When querying apps by queue, iterating over all apps is inefficient and limiting |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5637](https://issues.apache.org/jira/browse/HDFS-5637) | try to refeatchToken while local read InvalidToken occurred |  Major | hdfs-client, security | Liang Xie | Liang Xie |
| [HDFS-5652](https://issues.apache.org/jira/browse/HDFS-5652) | refactoring/uniforming invalid block token exception handling in DFSInputStream |  Minor | hdfs-client | Liang Xie | Liang Xie |
| [HDFS-5665](https://issues.apache.org/jira/browse/HDFS-5665) | Remove the unnecessary writeLock while initializing CacheManager in FsNameSystem Ctor |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-5350](https://issues.apache.org/jira/browse/HDFS-5350) | Name Node should report fsimage transfer time as a metric |  Minor | namenode | Rob Weltman | Jimmy Xiang |
| [HDFS-5674](https://issues.apache.org/jira/browse/HDFS-5674) | Editlog code cleanup |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9611](https://issues.apache.org/jira/browse/HADOOP-9611) | mvn-rpmbuild against google-guice \> 3.0 yields missing cglib dependency |  Major | build | Timothy St. Clair | Timothy St. Clair |
| [HADOOP-10164](https://issues.apache.org/jira/browse/HADOOP-10164) | Allow UGI to login with a known Subject |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-5662](https://issues.apache.org/jira/browse/HDFS-5662) | Can't decommission a DataNode due to file's replication factor larger than the rest of the cluster size |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-5676](https://issues.apache.org/jira/browse/HDFS-5676) | fix inconsistent synchronization of CachingStrategy |  Minor | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5663](https://issues.apache.org/jira/browse/HDFS-5663) | make the retry time and interval value configurable in openInfo() |  Major | hdfs-client | Liang Xie | Liang Xie |
| [HADOOP-10172](https://issues.apache.org/jira/browse/HADOOP-10172) | Cache SASL server factories |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5692](https://issues.apache.org/jira/browse/MAPREDUCE-5692) | Add explicit diagnostics when a task attempt is killed due to speculative execution |  Major | mrv2 | Gera Shegalov | Gera Shegalov |
| [HADOOP-10169](https://issues.apache.org/jira/browse/HADOOP-10169) | remove the unnecessary  synchronized in JvmMetrics class |  Minor | metrics | Liang Xie | Liang Xie |
| [HADOOP-10173](https://issues.apache.org/jira/browse/HADOOP-10173) | Remove UGI from DIGEST-MD5 SASL server creation |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-5695](https://issues.apache.org/jira/browse/HDFS-5695) | Clean up TestOfflineEditsViewer and OfflineEditsViewerHelper |  Major | test | Haohui Mai | Haohui Mai |
| [HADOOP-10198](https://issues.apache.org/jira/browse/HADOOP-10198) | DomainSocket: add support for socketpair |  Minor | native | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5220](https://issues.apache.org/jira/browse/HDFS-5220) | Expose group resolution time as metric |  Major | namenode | Rob Weltman | Jimmy Xiang |
| [MAPREDUCE-3310](https://issues.apache.org/jira/browse/MAPREDUCE-3310) | Custom grouping comparator cannot be set for Combiners |  Major | client | Mathias Herberts | Alejandro Abdelnur |
| [HADOOP-10208](https://issues.apache.org/jira/browse/HADOOP-10208) | Remove duplicate initialization in StringUtils.getStringCollection |  Trivial | . | Benoy Antony | Benoy Antony |
| [HDFS-5721](https://issues.apache.org/jira/browse/HDFS-5721) | sharedEditsImage in Namenode#initializeSharedEdits() should be closed before method returns |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-10228](https://issues.apache.org/jira/browse/HADOOP-10228) | FsPermission#fromShort() should cache FsAction.values() |  Minor | fs | Haohui Mai | Haohui Mai |
| [HDFS-5677](https://issues.apache.org/jira/browse/HDFS-5677) | Need error checking for HA cluster configuration |  Minor | datanode, ha | Vincent Sheffer | Vincent Sheffer |
| [YARN-1567](https://issues.apache.org/jira/browse/YARN-1567) | In Fair Scheduler, allow empty queues to change between leaf and parent on allocation file reload |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5672](https://issues.apache.org/jira/browse/MAPREDUCE-5672) | Provide optional RollingFileAppender for container log4j (syslog) |  Major | mr-am, mrv2 | Gera Shegalov | Gera Shegalov |
| [YARN-1616](https://issues.apache.org/jira/browse/YARN-1616) | RMFatalEventDispatcher should log the cause of the event |  Trivial | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10143](https://issues.apache.org/jira/browse/HADOOP-10143) | replace WritableFactories's hashmap with ConcurrentHashMap |  Major | io | Liang Xie | Liang Xie |
| [HDFS-5748](https://issues.apache.org/jira/browse/HDFS-5748) | Too much information shown in the dfs health page. |  Major | . | Kihwal Lee | Haohui Mai |
| [YARN-1623](https://issues.apache.org/jira/browse/YARN-1623) | Include queue name in RegisterApplicationMasterResponse |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HDFS-5788](https://issues.apache.org/jira/browse/HDFS-5788) | listLocatedStatus response can be very large |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HADOOP-10167](https://issues.apache.org/jira/browse/HADOOP-10167) | Mark hadoop-common source as UTF-8 in Maven pom files / refactoring |  Major | build | Mikhail Antonov | Mikhail Antonov |
| [HADOOP-10248](https://issues.apache.org/jira/browse/HADOOP-10248) | Property name should be included in the exception where property value is null |  Major | . | Ted Yu | Akira Ajisaka |
| [HADOOP-9652](https://issues.apache.org/jira/browse/HADOOP-9652) | Allow RawLocalFs#getFileLinkStatus to fill in the link owner and mode if requested |  Major | . | Colin P. McCabe | Andrew Wang |
| [HADOOP-10086](https://issues.apache.org/jira/browse/HADOOP-10086) | User document for authentication in secure cluster |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-5825](https://issues.apache.org/jira/browse/HDFS-5825) | Use FileUtils.copyFile() to implement DFSTestUtils.copyFile() |  Minor | . | Haohui Mai | Haohui Mai |
| [HADOOP-10274](https://issues.apache.org/jira/browse/HADOOP-10274) | Lower the logging level from ERROR to WARN for UGI.doAs method |  Minor | security | takeshi.miao | takeshi.miao |
| [HDFS-5833](https://issues.apache.org/jira/browse/HDFS-5833) | SecondaryNameNode have an incorrect java doc |  Trivial | namenode | Bangtao Zhou |  |
| [HDFS-5841](https://issues.apache.org/jira/browse/HDFS-5841) | Update HDFS caching documentation with new changes |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-5399](https://issues.apache.org/jira/browse/HDFS-5399) | Revisit SafeModeException and corresponding retry policies |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-5333](https://issues.apache.org/jira/browse/HDFS-5333) | Improvement of current HDFS Web UI |  Major | . | Jing Zhao | Haohui Mai |
| [MAPREDUCE-2734](https://issues.apache.org/jira/browse/MAPREDUCE-2734) | DistCp with FairScheduler assign all map tasks in one TT |  Trivial | jobtracker | Bochun Bai | Bochun Bai |
| [YARN-905](https://issues.apache.org/jira/browse/YARN-905) | Add state filters to nodes CLI |  Major | . | Sandy Ryza | Wei Yan |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4633](https://issues.apache.org/jira/browse/HDFS-4633) | TestDFSClientExcludedNodes fails sporadically if excluded nodes cache expires too quickly |  Major | hdfs-client, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-9635](https://issues.apache.org/jira/browse/HADOOP-9635) | Fix Potential Stack Overflow in DomainSocket.c |  Major | native | V. Karthik Kumar |  |
| [MAPREDUCE-5316](https://issues.apache.org/jira/browse/MAPREDUCE-5316) | job -list-attempt-ids command does not handle illegal task-state |  Major | client | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-9582](https://issues.apache.org/jira/browse/HADOOP-9582) | Non-existent file to "hadoop fs -conf" doesn't throw error |  Major | conf | Ashwin Shankar | Ashwin Shankar |
| [HADOOP-9660](https://issues.apache.org/jira/browse/HADOOP-9660) | [WINDOWS] Powershell / cmd parses -Dkey=value from command line as [-Dkey, value] which breaks GenericsOptionParser |  Major | scripts, util | Enis Soztutar | Enis Soztutar |
| [HADOOP-9703](https://issues.apache.org/jira/browse/HADOOP-9703) | org.apache.hadoop.ipc.Client leaks threads on stop. |  Minor | . | Mark Miller | Tsuyoshi Ozawa |
| [HDFS-4657](https://issues.apache.org/jira/browse/HDFS-4657) | Limit the number of blocks logged by the NN after a block report to a configurable value. |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-5380](https://issues.apache.org/jira/browse/MAPREDUCE-5380) | Invalid mapred command should return non-zero exit code |  Major | . | Stephen Chu | Stephen Chu |
| [MAPREDUCE-5404](https://issues.apache.org/jira/browse/MAPREDUCE-5404) | HSAdminServer does not use ephemeral ports in minicluster mode |  Major | jobhistoryserver | Ted Yu | Ted Yu |
| [HADOOP-9787](https://issues.apache.org/jira/browse/HADOOP-9787) | ShutdownHelper util to shutdown threads and threadpools |  Major | util | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9817](https://issues.apache.org/jira/browse/HADOOP-9817) | FileSystem#globStatus and FileContext#globStatus need to work with symlinks |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5065](https://issues.apache.org/jira/browse/HDFS-5065) | TestSymlinkHdfsDisable fails on Windows |  Major | hdfs-client, test | Ivan Mitic | Ivan Mitic |
| [HADOOP-9847](https://issues.apache.org/jira/browse/HADOOP-9847) | TestGlobPath symlink tests fail to cleanup properly |  Minor | . | Andrew Wang | Colin P. McCabe |
| [YARN-1060](https://issues.apache.org/jira/browse/YARN-1060) | Two tests in TestFairScheduler are missing @Test annotation |  Major | scheduler | Sandy Ryza | Niranjan Singh |
| [HADOOP-9871](https://issues.apache.org/jira/browse/HADOOP-9871) | Fix intermittent findbug warnings in DefaultMetricsSystem |  Minor | . | Luke Lu | Junping Du |
| [HDFS-4816](https://issues.apache.org/jira/browse/HDFS-4816) | transitionToActive blocks if the SBN is doing checkpoint image transfer |  Major | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-9875](https://issues.apache.org/jira/browse/HADOOP-9875) | TestDoAsEffectiveUser can fail on JDK 7 |  Minor | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-5093](https://issues.apache.org/jira/browse/HDFS-5093) | TestGlobPaths should re-use the MiniDFSCluster to avoid failure on Windows |  Minor | test | Chuan Liu | Chuan Liu |
| [HADOOP-9865](https://issues.apache.org/jira/browse/HADOOP-9865) | FileContext.globStatus() has a regression with respect to relative path |  Major | . | Chuan Liu | Chuan Liu |
| [HDFS-4994](https://issues.apache.org/jira/browse/HDFS-4994) | Audit log getContentSummary() calls |  Minor | namenode | Kihwal Lee | Robert Parker |
| [HADOOP-9887](https://issues.apache.org/jira/browse/HADOOP-9887) | globStatus does not correctly handle paths starting with a drive spec on Windows |  Major | fs | Chris Nauroth | Chuan Liu |
| [HDFS-4329](https://issues.apache.org/jira/browse/HDFS-4329) | DFSShell issues with directories with spaces in name |  Major | hdfs-client | Andy Isaacson | Cristina L. Abad |
| [HDFS-3981](https://issues.apache.org/jira/browse/HDFS-3981) | access time is set without holding FSNamesystem write lock |  Major | namenode | Xiaobo Peng | Xiaobo Peng |
| [HADOOP-9889](https://issues.apache.org/jira/browse/HADOOP-9889) | Refresh the Krb5 configuration when creating a new kdc in Hadoop-MiniKDC |  Major | . | Wei Yan | Wei Yan |
| [HADOOP-9908](https://issues.apache.org/jira/browse/HADOOP-9908) | Fix NPE when versioninfo properties file is missing |  Major | util | Todd Lipcon | Todd Lipcon |
| [HDFS-5164](https://issues.apache.org/jira/browse/HDFS-5164) | deleteSnapshot should check if OperationCategory.WRITE is possible before taking write lock |  Minor | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5170](https://issues.apache.org/jira/browse/HDFS-5170) | BlockPlacementPolicyDefault uses the wrong classname when alerting to enable debug logging |  Trivial | . | Andrew Wang | Andrew Wang |
| [HADOOP-9350](https://issues.apache.org/jira/browse/HADOOP-9350) | Hadoop not building against Java7 on OSX |  Minor | build | Steve Loughran | Robert Kanter |
| [HDFS-5122](https://issues.apache.org/jira/browse/HDFS-5122) | Support failover and retry in WebHdfsFileSystem for NN HA |  Major | ha, webhdfs | Arpit Gupta | Haohui Mai |
| [HADOOP-9929](https://issues.apache.org/jira/browse/HADOOP-9929) | Insufficient permissions for a path reported as file not found |  Major | fs | Jason Lowe | Colin P. McCabe |
| [HADOOP-9791](https://issues.apache.org/jira/browse/HADOOP-9791) | Add a test case covering long paths for new FileUtil access check methods |  Major | . | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5504](https://issues.apache.org/jira/browse/MAPREDUCE-5504) | mapred queue -info inconsistent with types |  Major | client | Thomas Graves | Kousuke Saruta |
| [HADOOP-7344](https://issues.apache.org/jira/browse/HADOOP-7344) | globStatus doesn't grok groupings with a slash |  Major | fs | Daryn Sharp | Colin P. McCabe |
| [YARN-1188](https://issues.apache.org/jira/browse/YARN-1188) | The context of QueueMetrics becomes 'default' when using FairScheduler |  Trivial | . | Akira Ajisaka | Tsuyoshi Ozawa |
| [MAPREDUCE-5522](https://issues.apache.org/jira/browse/MAPREDUCE-5522) | Incorrectly expect the array of JobQueueInfo returned by o.a.h.mapred.QueueManager#getJobQueueInfos to have a specific order. |  Minor | test | Jinghui Wang | Jinghui Wang |
| [HADOOP-9981](https://issues.apache.org/jira/browse/HADOOP-9981) | globStatus should minimize its listStatus and getFileStatus calls |  Critical | . | Kihwal Lee | Colin P. McCabe |
| [MAPREDUCE-5514](https://issues.apache.org/jira/browse/MAPREDUCE-5514) | TestRMContainerAllocator fails on trunk |  Blocker | . | Zhijie Shen | Zhijie Shen |
| [HADOOP-10006](https://issues.apache.org/jira/browse/HADOOP-10006) | Compilation failure in trunk for o.a.h.fs.swift.util.JSONUtil |  Blocker | fs, util | Junping Du | Junping Du |
| [HADOOP-9964](https://issues.apache.org/jira/browse/HADOOP-9964) | O.A.H.U.ReflectionUtils.printThreadInfo() is not thread-safe which cause TestHttpServer pending 10 minutes or longer. |  Major | util | Junping Du | Junping Du |
| [YARN-1268](https://issues.apache.org/jira/browse/YARN-1268) | TestFairScheduler.testContinuousScheduling is flaky |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-10028](https://issues.apache.org/jira/browse/HADOOP-10028) | Malformed ssl-server.xml.example |  Minor | . | Jing Zhao | Haohui Mai |
| [HDFS-5291](https://issues.apache.org/jira/browse/HDFS-5291) | Clients need to retry when Active NN is in SafeMode |  Critical | ha | Arpit Gupta | Jing Zhao |
| [HADOOP-10030](https://issues.apache.org/jira/browse/HADOOP-10030) | FsShell -put/copyFromLocal should support Windows local path |  Major | . | Chuan Liu | Chuan Liu |
| [YARN-1284](https://issues.apache.org/jira/browse/YARN-1284) | LCE: Race condition leaves dangling cgroups entries for killed containers |  Blocker | nodemanager | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-461](https://issues.apache.org/jira/browse/YARN-461) | Fair scheduler should not accept apps with empty string queue name |  Major | resourcemanager | Sandy Ryza | Wei Yan |
| [MAPREDUCE-5569](https://issues.apache.org/jira/browse/MAPREDUCE-5569) | FloatSplitter is not generating correct splits |  Major | . | Nathan Roberts | Nathan Roberts |
| [YARN-879](https://issues.apache.org/jira/browse/YARN-879) | Fix tests w.r.t o.a.h.y.server.resourcemanager.Application |  Major | . | Junping Du | Junping Du |
| [HADOOP-10031](https://issues.apache.org/jira/browse/HADOOP-10031) | FsShell -get/copyToLocal/moveFromLocal should support Windows local path |  Major | fs | Chuan Liu | Chuan Liu |
| [YARN-1265](https://issues.apache.org/jira/browse/YARN-1265) | Fair Scheduler chokes on unhealthy node reconnect |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-10029](https://issues.apache.org/jira/browse/HADOOP-10029) | Specifying har file to MR job fails in secure cluster |  Major | fs | Suresh Srinivas | Suresh Srinivas |
| [HDFS-5335](https://issues.apache.org/jira/browse/HDFS-5335) | DFSOutputStream#close() keeps throwing exceptions when it is called multiple times |  Major | . | Arpit Gupta | Haohui Mai |
| [HADOOP-10039](https://issues.apache.org/jira/browse/HADOOP-10039) | Add Hive to the list of projects using AbstractDelegationTokenSecretManager |  Major | security | Suresh Srinivas | Haohui Mai |
| [YARN-1300](https://issues.apache.org/jira/browse/YARN-1300) | SLS tests fail because conf puts yarn properties in fair-scheduler.xml |  Major | . | Ted Yu | Ted Yu |
| [HDFS-5322](https://issues.apache.org/jira/browse/HDFS-5322) | HDFS delegation token not found in cache errors seen on secure HA clusters |  Major | ha | Arpit Gupta | Jing Zhao |
| [YARN-1044](https://issues.apache.org/jira/browse/YARN-1044) | used/min/max resources do not display info in the scheduler page |  Critical | resourcemanager, scheduler | Sangjin Lee | Sangjin Lee |
| [HDFS-5329](https://issues.apache.org/jira/browse/HDFS-5329) | Update FSNamesystem#getListing() to handle inode path in startAfter token |  Major | namenode, nfs | Brandon Li | Brandon Li |
| [MAPREDUCE-5329](https://issues.apache.org/jira/browse/MAPREDUCE-5329) | APPLICATION\_INIT is never sent to AuxServices other than the builtin ShuffleHandler |  Major | mr-am | Avner BenHanoch | Avner BenHanoch |
| [YARN-305](https://issues.apache.org/jira/browse/YARN-305) | Fair scheduler logs too many "Node offered to app:..." messages |  Critical | resourcemanager | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-10040](https://issues.apache.org/jira/browse/HADOOP-10040) | hadoop.cmd in UNIX format and would not run by default on Windows |  Major | . | Yingda Chen | Chris Nauroth |
| [MAPREDUCE-5546](https://issues.apache.org/jira/browse/MAPREDUCE-5546) | mapred.cmd on Windows set HADOOP\_OPTS incorrectly |  Major | . | Chuan Liu | Chuan Liu |
| [YARN-1259](https://issues.apache.org/jira/browse/YARN-1259) | In Fair Scheduler web UI, queue num pending and num active apps switched |  Trivial | scheduler | Sandy Ryza | Robert Kanter |
| [YARN-1182](https://issues.apache.org/jira/browse/YARN-1182) | MiniYARNCluster creates and inits the RM/NM only on start() |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5352](https://issues.apache.org/jira/browse/HDFS-5352) | Server#initLog() doesn't close InputStream in httpfs |  Minor | . | Ted Yu | Ted Yu |
| [MAPREDUCE-5518](https://issues.apache.org/jira/browse/MAPREDUCE-5518) | Fix typo "can't read paritions file" |  Trivial | examples | Albert Chu | Albert Chu |
| [YARN-1295](https://issues.apache.org/jira/browse/YARN-1295) | In UnixLocalWrapperScriptBuilder, using bash -c can cause "Text file busy" errors |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [HDFS-5283](https://issues.apache.org/jira/browse/HDFS-5283) | NN not coming out of startup safemode due to under construction blocks only inside snapshots also counted in safemode threshhold |  Critical | snapshots | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-5585](https://issues.apache.org/jira/browse/MAPREDUCE-5585) | TestCopyCommitter#testNoCommitAction Fails on JDK7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-5586](https://issues.apache.org/jira/browse/MAPREDUCE-5586) | TestCopyMapper#testCopyFailOnBlockSizeDifference fails when run from hadoop-tools/hadoop-distcp directory |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-5370](https://issues.apache.org/jira/browse/HDFS-5370) | Typo in Error Message:  different between range in condition and range in error message |  Trivial | hdfs-client | Kousuke Saruta | Kousuke Saruta |
| [HDFS-5346](https://issues.apache.org/jira/browse/HDFS-5346) | Avoid unnecessary call to getNumLiveDataNodes() for each block during IBR processing |  Major | namenode, performance | Kihwal Lee | Ravi Prakash |
| [HDFS-4376](https://issues.apache.org/jira/browse/HDFS-4376) |  Fix several race conditions in Balancer and resolve intermittent timeout of TestBalancerWithNodeGroup |  Major | balancer & mover | Aaron T. Myers | Junping Du |
| [HDFS-5375](https://issues.apache.org/jira/browse/HDFS-5375) | hdfs.cmd does not expose several snapshot commands. |  Minor | tools | Chris Nauroth | Chris Nauroth |
| [HDFS-5336](https://issues.apache.org/jira/browse/HDFS-5336) | DataNode should not output 'StartupProgress' metrics |  Minor | namenode | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10055](https://issues.apache.org/jira/browse/HADOOP-10055) | FileSystemShell.apt.vm doc has typo "numRepicas" |  Trivial | documentation | Eli Collins | Akira Ajisaka |
| [HDFS-5374](https://issues.apache.org/jira/browse/HDFS-5374) | Remove deadcode in DFSOutputStream |  Trivial | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-5365](https://issues.apache.org/jira/browse/HDFS-5365) | Fix libhdfs compile error on FreeBSD9 |  Major | build, libhdfs | Radim Kolar | Radim Kolar |
| [MAPREDUCE-5587](https://issues.apache.org/jira/browse/MAPREDUCE-5587) | TestTextOutputFormat fails on JDK7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-5276](https://issues.apache.org/jira/browse/HDFS-5276) | FileSystem.Statistics got performance issue on multi-thread read/write. |  Major | . | Chengxiang Li | Colin P. McCabe |
| [YARN-1288](https://issues.apache.org/jira/browse/YARN-1288) | Make Fair Scheduler ACLs more user friendly |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1331](https://issues.apache.org/jira/browse/YARN-1331) | yarn.cmd exits with NoClassDefFoundError trying to run rmadmin or logs |  Trivial | client | Chris Nauroth | Chris Nauroth |
| [YARN-1315](https://issues.apache.org/jira/browse/YARN-1315) | TestQueueACLs should also test FairScheduler |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [HDFS-5400](https://issues.apache.org/jira/browse/HDFS-5400) | DFS\_CLIENT\_MMAP\_CACHE\_THREAD\_RUNS\_PER\_TIMEOUT constant is set to the wrong value |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-5561](https://issues.apache.org/jira/browse/MAPREDUCE-5561) | org.apache.hadoop.mapreduce.v2.app.job.impl.TestJobImpl testcase failing on trunk |  Critical | . | Cindy Li | Karthik Kambatla |
| [YARN-1183](https://issues.apache.org/jira/browse/YARN-1183) | MiniYARNCluster shutdown takes several minutes intermittently |  Major | . | Andrey Klochkov | Andrey Klochkov |
| [HDFS-5403](https://issues.apache.org/jira/browse/HDFS-5403) | WebHdfs client cannot communicate with older WebHdfs servers post HDFS-5306 |  Major | webhdfs | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-9016](https://issues.apache.org/jira/browse/HADOOP-9016) | org.apache.hadoop.fs.HarFileSystem.HarFSDataInputStream.HarFsInputStream.skip(long) must never return negative value. |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-5341](https://issues.apache.org/jira/browse/HDFS-5341) | Reduce fsdataset lock duration during directory scanning. |  Major | datanode | qus-jiawei | qus-jiawei |
| [HDFS-5257](https://issues.apache.org/jira/browse/HDFS-5257) | addBlock() retry should return LocatedBlock with locations else client will get AIOBE |  Major | hdfs-client, namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10072](https://issues.apache.org/jira/browse/HADOOP-10072) | TestNfsExports#testMultiMatchers fails due to non-deterministic timing around cache expiry check. |  Trivial | nfs, test | Chris Nauroth | Chris Nauroth |
| [YARN-1022](https://issues.apache.org/jira/browse/YARN-1022) | Unnecessary INFO logs in AMRMClientAsync |  Trivial | . | Bikas Saha | haosdent |
| [YARN-1349](https://issues.apache.org/jira/browse/YARN-1349) | yarn.cmd does not support passthrough to any arbitrary class. |  Major | client | Chris Nauroth | Chris Nauroth |
| [HDFS-5413](https://issues.apache.org/jira/browse/HDFS-5413) | hdfs.cmd does not support passthrough to any arbitrary class. |  Major | scripts | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-4680](https://issues.apache.org/jira/browse/MAPREDUCE-4680) | Job history cleaner should only check timestamps of files in old enough directories |  Major | jobhistoryserver | Sandy Ryza | Robert Kanter |
| [MAPREDUCE-5598](https://issues.apache.org/jira/browse/MAPREDUCE-5598) | TestUserDefinedCounters.testMapReduceJob is flakey |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-1306](https://issues.apache.org/jira/browse/YARN-1306) | Clean up hadoop-sls sample-conf according to YARN-1228 |  Major | . | Wei Yan | Wei Yan |
| [HDFS-5433](https://issues.apache.org/jira/browse/HDFS-5433) | When reloading fsimage during checkpointing, we should clear existing snapshottable directories |  Critical | snapshots | Aaron T. Myers | Aaron T. Myers |
| [HDFS-5432](https://issues.apache.org/jira/browse/HDFS-5432) | TestDatanodeJsp fails on Windows due to assumption that loopback address resolves to host name localhost. |  Trivial | datanode, test | Chris Nauroth | Chris Nauroth |
| [YARN-1321](https://issues.apache.org/jira/browse/YARN-1321) | NMTokenCache is a singleton, prevents multiple AMs running in a single JVM to work correctly |  Blocker | client | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-1343](https://issues.apache.org/jira/browse/YARN-1343) | NodeManagers additions/restarts are not reported as node updates in AllocateResponse responses to AMs |  Critical | resourcemanager | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-1381](https://issues.apache.org/jira/browse/YARN-1381) | Same relaxLocality appears twice in exception message of AMRMClientImpl#checkLocalityRelaxationConflict() |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-9898](https://issues.apache.org/jira/browse/HADOOP-9898) | Set SO\_KEEPALIVE on all our sockets |  Minor | ipc, net | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-5604](https://issues.apache.org/jira/browse/MAPREDUCE-5604) | TestMRAMWithNonNormalizedCapabilities fails on Windows due to exceeding max path length |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-5035](https://issues.apache.org/jira/browse/HDFS-5035) | getFileLinkStatus and rename do not correctly check permissions of symlinks |  Major | namenode | Andrew Wang | Andrew Wang |
| [YARN-1388](https://issues.apache.org/jira/browse/YARN-1388) | Fair Scheduler page always displays blank fair share |  Trivial | resourcemanager | Liyin Liang | Liyin Liang |
| [HDFS-5456](https://issues.apache.org/jira/browse/HDFS-5456) | NameNode startup progress creates new steps if caller attempts to create a counter for a step that doesn't already exist. |  Critical | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-5427](https://issues.apache.org/jira/browse/HDFS-5427) | not able to read deleted files from snapshot directly under snapshottable dir after checkpoint and NN restart |  Major | snapshots | Vinayakumar B | Vinayakumar B |
| [YARN-1374](https://issues.apache.org/jira/browse/YARN-1374) | Resource Manager fails to start due to ConcurrentModificationException |  Blocker | resourcemanager | Devaraj K | Karthik Kambatla |
| [HDFS-5458](https://issues.apache.org/jira/browse/HDFS-5458) | Datanode failed volume threshold ignored if exception is thrown in getDataDirsFromURIs |  Major | datanode | Andrew Wang | Mike Mellenthin |
| [MAPREDUCE-5451](https://issues.apache.org/jira/browse/MAPREDUCE-5451) | MR uses LD\_LIBRARY\_PATH which doesn't mean anything in Windows |  Major | . | Mostafa Elhemali | Yingda Chen |
| [HDFS-5443](https://issues.apache.org/jira/browse/HDFS-5443) | Delete 0-sized block when deleting an under-construction file that is included in snapshot |  Major | snapshots | Uma Maheswara Rao G | Jing Zhao |
| [HDFS-5468](https://issues.apache.org/jira/browse/HDFS-5468) | CacheAdmin help command does not recognize commands |  Minor | tools | Stephen Chu | Stephen Chu |
| [HDFS-5476](https://issues.apache.org/jira/browse/HDFS-5476) | Snapshot: clean the blocks/files/directories under a renamed file/directory while deletion |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-5482](https://issues.apache.org/jira/browse/HDFS-5482) | DistributedFileSystem#listPathBasedCacheDirectives must support relative paths |  Major | tools | Stephen Chu | Colin P. McCabe |
| [HADOOP-10088](https://issues.apache.org/jira/browse/HADOOP-10088) | copy-nativedistlibs.sh needs to quote snappy lib dir |  Major | build | Raja Aluri | Raja Aluri |
| [MAPREDUCE-5186](https://issues.apache.org/jira/browse/MAPREDUCE-5186) | mapreduce.job.max.split.locations causes some splits created by CombineFileInputFormat to fail |  Critical | job submission | Sangjin Lee | Robert Parker |
| [YARN-1395](https://issues.apache.org/jira/browse/YARN-1395) | Distributed shell application master launched with debug flag can hang waiting for external ls process. |  Major | applications/distributed-shell | Chris Nauroth | Chris Nauroth |
| [YARN-1400](https://issues.apache.org/jira/browse/YARN-1400) | yarn.cmd uses HADOOP\_RESOURCEMANAGER\_OPTS. Should be YARN\_RESOURCEMANAGER\_OPTS. |  Trivial | resourcemanager | Raja Aluri | Raja Aluri |
| [HDFS-5425](https://issues.apache.org/jira/browse/HDFS-5425) | Renaming underconstruction file with snapshots can make NN failure on restart |  Major | namenode, snapshots | sathish | Jing Zhao |
| [HDFS-5471](https://issues.apache.org/jira/browse/HDFS-5471) | CacheAdmin -listPools fails when user lacks permissions to view all pools |  Major | tools | Stephen Chu | Andrew Wang |
| [HADOOP-10093](https://issues.apache.org/jira/browse/HADOOP-10093) | hadoop-env.cmd sets HADOOP\_CLIENT\_OPTS with a max heap size that is too small. |  Major | conf | shanyu zhao | shanyu zhao |
| [YARN-1386](https://issues.apache.org/jira/browse/YARN-1386) | NodeManager mistakenly loses resources and relocalizes them |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5613](https://issues.apache.org/jira/browse/MAPREDUCE-5613) | DefaultSpeculator holds and checks hashmap that is always empty |  Major | applicationmaster | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5431](https://issues.apache.org/jira/browse/MAPREDUCE-5431) | Missing pom dependency in MR-client |  Major | build | Timothy St. Clair | Timothy St. Clair |
| [HDFS-5075](https://issues.apache.org/jira/browse/HDFS-5075) | httpfs-config.sh calls out incorrect env script name |  Major | . | Timothy St. Clair | Timothy St. Clair |
| [HDFS-5474](https://issues.apache.org/jira/browse/HDFS-5474) | Deletesnapshot can make Namenode in safemode on NN restarts. |  Major | snapshots | Uma Maheswara Rao G | sathish |
| [HADOOP-10078](https://issues.apache.org/jira/browse/HADOOP-10078) | KerberosAuthenticator always does SPNEGO |  Minor | security | Robert Kanter | Robert Kanter |
| [HDFS-5504](https://issues.apache.org/jira/browse/HDFS-5504) | In HA mode, OP\_DELETE\_SNAPSHOT is not decrementing the safemode threshold, leads to NN safemode. |  Major | snapshots | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-5481](https://issues.apache.org/jira/browse/MAPREDUCE-5481) | Enable uber jobs to have multiple reducers |  Blocker | mrv2, test | Jason Lowe | Sandy Ryza |
| [HDFS-4995](https://issues.apache.org/jira/browse/HDFS-4995) | Make getContentSummary() less expensive |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HADOOP-10094](https://issues.apache.org/jira/browse/HADOOP-10094) | NPE in GenericOptionsParser#preProcessForWindows() |  Trivial | util | Enis Soztutar | Enis Soztutar |
| [MAPREDUCE-5616](https://issues.apache.org/jira/browse/MAPREDUCE-5616) | MR Client-AppMaster RPC max retries on socket timeout is too high. |  Major | client | Chris Nauroth | Chris Nauroth |
| [YARN-1401](https://issues.apache.org/jira/browse/YARN-1401) | With zero sleep-delay-before-sigkill.ms, no signal is ever sent |  Major | nodemanager | Gera Shegalov | Gera Shegalov |
| [HDFS-5438](https://issues.apache.org/jira/browse/HDFS-5438) | Flaws in block report processing can cause data loss |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HADOOP-10100](https://issues.apache.org/jira/browse/HADOOP-10100) | MiniKDC shouldn't use apacheds-all artifact |  Major | . | Robert Kanter | Robert Kanter |
| [MAPREDUCE-5373](https://issues.apache.org/jira/browse/MAPREDUCE-5373) | TestFetchFailure.testFetchFailureMultipleReduces could fail intermittently |  Major | . | Chuan Liu | Jonathan Eagles |
| [HDFS-5372](https://issues.apache.org/jira/browse/HDFS-5372) | In FSNamesystem, hasReadLock() returns false if the current thread holds the write lock |  Major | namenode | Tsz Wo Nicholas Sze | Vinayakumar B |
| [YARN-1419](https://issues.apache.org/jira/browse/YARN-1419) | TestFifoScheduler.testAppAttemptMetrics fails intermittently under jdk7 |  Minor | scheduler | Jonathan Eagles | Jonathan Eagles |
| [HDFS-5512](https://issues.apache.org/jira/browse/HDFS-5512) | CacheAdmin -listPools fails with NPE when user lacks permissions to view all pools |  Major | caching, tools | Stephen Chu | Andrew Wang |
| [HDFS-5073](https://issues.apache.org/jira/browse/HDFS-5073) | TestListCorruptFileBlocks fails intermittently |  Minor | test | Kihwal Lee | Arpit Agarwal |
| [HDFS-5428](https://issues.apache.org/jira/browse/HDFS-5428) | under construction files deletion after snapshot+checkpoint+nn restart leads nn safemode |  Major | snapshots | Vinayakumar B | Jing Zhao |
| [YARN-584](https://issues.apache.org/jira/browse/YARN-584) | In scheduler web UIs, queues unexpand on refresh |  Major | scheduler | Sandy Ryza | Harshit Daga |
| [HDFS-5513](https://issues.apache.org/jira/browse/HDFS-5513) | CacheAdmin commands fail when using . as the path |  Minor | caching, tools | Stephen Chu | Andrew Wang |
| [YARN-1407](https://issues.apache.org/jira/browse/YARN-1407) | RM Web UI and REST APIs should uniformly use YarnApplicationState |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-4516](https://issues.apache.org/jira/browse/HDFS-4516) | Client crash after block allocation and NN switch before lease recovery for the same file can cause readers to fail forever |  Critical | namenode | Uma Maheswara Rao G | Vinayakumar B |
| [HDFS-5014](https://issues.apache.org/jira/browse/HDFS-5014) | BPOfferService#processCommandFromActor() synchronization on namenode RPC call delays IBR to Active NN, if Stanby NN is unstable |  Major | datanode, ha | Vinayakumar B | Vinayakumar B |
| [YARN-1425](https://issues.apache.org/jira/browse/YARN-1425) | TestRMRestart fails because MockRM.waitForState(AttemptId) uses current attempt instead of the attempt passed as argument |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-1053](https://issues.apache.org/jira/browse/YARN-1053) | Diagnostic message from ContainerExitEvent is ignored in ContainerImpl |  Blocker | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [HADOOP-9114](https://issues.apache.org/jira/browse/HADOOP-9114) | After defined the dfs.checksum.type as the NULL, write file and hflush will through java.lang.ArrayIndexOutOfBoundsException |  Minor | . | liuyang | sathish |
| [MAPREDUCE-5631](https://issues.apache.org/jira/browse/MAPREDUCE-5631) | TestJobEndNotifier.testNotifyRetries fails with Should have taken more than 5 seconds in jdk7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-1320](https://issues.apache.org/jira/browse/YARN-1320) | Custom log4j properties in Distributed shell does not work properly. |  Major | applications/distributed-shell | Tassapol Athiapinya | Xuan Gong |
| [HDFS-5407](https://issues.apache.org/jira/browse/HDFS-5407) | Fix typos in DFSClientCache |  Trivial | . | Haohui Mai | Haohui Mai |
| [HDFS-5544](https://issues.apache.org/jira/browse/HDFS-5544) | Adding Test case For Checking dfs.checksum type as NULL value |  Minor | hdfs-client | sathish | sathish |
| [HDFS-5552](https://issues.apache.org/jira/browse/HDFS-5552) | Fix wrong information of "Cluster summay" in dfshealth.html |  Major | namenode | Shinichi Yamashita | Haohui Mai |
| [HDFS-5533](https://issues.apache.org/jira/browse/HDFS-5533) | Symlink delete/create should be treated as DELETE/CREATE in snapshot diff report |  Minor | snapshots | Binglin Chang | Binglin Chang |
| [HADOOP-10126](https://issues.apache.org/jira/browse/HADOOP-10126) | LightWeightGSet log message is confusing : "2.0% max memory = 2.0 GB" |  Minor | util | Vinayakumar B | Vinayakumar B |
| [YARN-1416](https://issues.apache.org/jira/browse/YARN-1416) | InvalidStateTransitions getting reported in multiple test cases even though they pass |  Major | . | Omkar Vinit Joshi | Jian He |
| [YARN-1314](https://issues.apache.org/jira/browse/YARN-1314) | Cannot pass more than 1 argument to shell command |  Major | applications/distributed-shell | Tassapol Athiapinya | Xuan Gong |
| [HDFS-5562](https://issues.apache.org/jira/browse/HDFS-5562) | TestCacheDirectives and TestFsDatasetCache should stub out native mlock |  Major | test | Akira Ajisaka | Colin P. McCabe |
| [YARN-1241](https://issues.apache.org/jira/browse/YARN-1241) | In Fair Scheduler, maxRunningApps does not work for non-leaf queues |  Major | . | Sandy Ryza | Sandy Ryza |
| [HADOOP-10135](https://issues.apache.org/jira/browse/HADOOP-10135) | writes to swift fs over partition size leave temp files and empty output file |  Major | fs | David Dobbins | David Dobbins |
| [HDFS-5581](https://issues.apache.org/jira/browse/HDFS-5581) | NameNodeFsck should use only one instance of BlockPlacementPolicy |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10130](https://issues.apache.org/jira/browse/HADOOP-10130) | RawLocalFS::LocalFSFileInputStream.pread does not track FS::Statistics |  Minor | . | Binglin Chang | Binglin Chang |
| [HDFS-5557](https://issues.apache.org/jira/browse/HDFS-5557) | Write pipeline recovery for the last packet in the block may cause rejection of valid replicas |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-5526](https://issues.apache.org/jira/browse/HDFS-5526) | Datanode cannot roll back to previous layout version |  Blocker | datanode | Tsz Wo Nicholas Sze | Kihwal Lee |
| [HDFS-5560](https://issues.apache.org/jira/browse/HDFS-5560) | Trash configuration log statements prints incorrect units |  Major | . | Josh Elser | Josh Elser |
| [HDFS-5558](https://issues.apache.org/jira/browse/HDFS-5558) | LeaseManager monitor thread can crash if the last block is complete but another block is not. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10127](https://issues.apache.org/jira/browse/HADOOP-10127) | Add ipc.client.connect.retry.interval to control the frequency of connection retries |  Major | ipc | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5645](https://issues.apache.org/jira/browse/MAPREDUCE-5645) | TestFixedLengthInputFormat fails with native libs |  Major | . | Jonathan Eagles | Mit Desai |
| [YARN-1454](https://issues.apache.org/jira/browse/YARN-1454) | TestRMRestart.testRMDelegationTokenRestoredOnRMRestart is failing intermittently |  Critical | . | Jian He | Karthik Kambatla |
| [HDFS-5555](https://issues.apache.org/jira/browse/HDFS-5555) | CacheAdmin commands fail when first listed NameNode is in Standby |  Major | caching | Stephen Chu | Jimmy Xiang |
| [HADOOP-10129](https://issues.apache.org/jira/browse/HADOOP-10129) | Distcp may succeed when it fails |  Critical | tools/distcp | Daryn Sharp | Daryn Sharp |
| [HADOOP-10081](https://issues.apache.org/jira/browse/HADOOP-10081) | Client.setupIOStreams can leak socket resources on exception or error |  Critical | ipc | Jason Lowe | Tsuyoshi Ozawa |
| [HADOOP-10058](https://issues.apache.org/jira/browse/HADOOP-10058) | TestMetricsSystemImpl#testInitFirstVerifyStopInvokedImmediately fails on trunk |  Minor | metrics | Akira Ajisaka | Chen He |
| [YARN-1438](https://issues.apache.org/jira/browse/YARN-1438) | When a container fails, the text of the exception isn't included in the diagnostics |  Major | nodemanager | Steve Loughran | Steve Loughran |
| [YARN-546](https://issues.apache.org/jira/browse/YARN-546) | Allow disabling the Fair Scheduler event log |  Major | scheduler | Lohit Vijayarenu | Sandy Ryza |
| [HDFS-5590](https://issues.apache.org/jira/browse/HDFS-5590) | Block ID and generation stamp may be reused when persistBlocks is set to false |  Major | . | Jing Zhao | Jing Zhao |
| [YARN-1450](https://issues.apache.org/jira/browse/YARN-1450) | TestUnmanagedAMLauncher#testDSShell fails on trunk |  Major | applications/distributed-shell | Akira Ajisaka | Binglin Chang |
| [HADOOP-10142](https://issues.apache.org/jira/browse/HADOOP-10142) | Avoid groups lookup for unprivileged users such as "dr.who" |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-5353](https://issues.apache.org/jira/browse/HDFS-5353) | Short circuit reads fail when dfs.encrypt.data.transfer is enabled |  Blocker | . | Haohui Mai | Colin P. McCabe |
| [MAPREDUCE-5656](https://issues.apache.org/jira/browse/MAPREDUCE-5656) | bzip2 codec can drop records when reading data in splits |  Critical | . | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5052](https://issues.apache.org/jira/browse/MAPREDUCE-5052) | Job History UI and web services confusing job start time and job submit time |  Critical | jobhistoryserver, webapps | Kendall Thrapp | Chen He |
| [HDFS-5074](https://issues.apache.org/jira/browse/HDFS-5074) | Allow starting up from an fsimage checkpoint in the middle of a segment |  Major | ha, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-5580](https://issues.apache.org/jira/browse/HDFS-5580) | Infinite loop in Balancer.waitForMoveCompletion |  Major | . | Binglin Chang | Binglin Chang |
| [YARN-1491](https://issues.apache.org/jira/browse/YARN-1491) | Upgrade JUnit3 TestCase to JUnit 4 |  Trivial | . | Jonathan Eagles | Chen He |
| [HADOOP-10087](https://issues.apache.org/jira/browse/HADOOP-10087) | UserGroupInformation.getGroupNames() fails to return primary group first when JniBasedUnixGroupsMappingWithFallback is used |  Major | security | Yu Gao | Colin P. McCabe |
| [YARN-408](https://issues.apache.org/jira/browse/YARN-408) | Capacity Scheduler delay scheduling should not be disabled by default |  Minor | scheduler | Mayank Bansal | Mayank Bansal |
| [HDFS-5023](https://issues.apache.org/jira/browse/HDFS-5023) | TestSnapshotPathINodes.testAllowSnapshot is failing with jdk7 |  Major | snapshots, test | Ravi Prakash | Mit Desai |
| [HDFS-4201](https://issues.apache.org/jira/browse/HDFS-4201) | NPE in BPServiceActor#sendHeartBeat |  Critical | namenode | Eli Collins | Jimmy Xiang |
| [MAPREDUCE-5674](https://issues.apache.org/jira/browse/MAPREDUCE-5674) | Missing start and finish time in mapred.JobStatus |  Major | client | Chuan Liu | Chuan Liu |
| [HADOOP-10162](https://issues.apache.org/jira/browse/HADOOP-10162) | Fix symlink-related test failures in TestFileContextResolveAfs and TestStat in branch-2 |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-8753](https://issues.apache.org/jira/browse/HADOOP-8753) | LocalDirAllocator throws "ArithmeticException: / by zero" when there is no available space on configured local dir |  Minor | . | Nishan Shetty | Benoy Antony |
| [YARN-1435](https://issues.apache.org/jira/browse/YARN-1435) | Distributed Shell should not run other commands except "sh", and run the custom script at the same time. |  Major | applications/distributed-shell | Tassapol Athiapinya | Xuan Gong |
| [HDFS-5592](https://issues.apache.org/jira/browse/HDFS-5592) | "DIR\* completeFile: /file is closed by DFSClient\_" should be logged only for successful closure of the file. |  Major | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-5623](https://issues.apache.org/jira/browse/MAPREDUCE-5623) | TestJobCleanup fails because of RejectedExecutionException and NPE. |  Major | . | Tsuyoshi Ozawa | Jason Lowe |
| [HDFS-5666](https://issues.apache.org/jira/browse/HDFS-5666) | Fix inconsistent synchronization in BPOfferService |  Minor | namenode | Colin P. McCabe | Jimmy Xiang |
| [YARN-1505](https://issues.apache.org/jira/browse/YARN-1505) | WebAppProxyServer should not set localhost as YarnConfiguration.PROXY\_ADDRESS by itself |  Blocker | . | Xuan Gong | Xuan Gong |
| [YARN-1145](https://issues.apache.org/jira/browse/YARN-1145) | Potential file handle leak in aggregated logs web ui |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-10106](https://issues.apache.org/jira/browse/HADOOP-10106) | Incorrect thread name in RPC log messages |  Minor | . | Ming Ma | Ming Ma |
| [MAPREDUCE-5679](https://issues.apache.org/jira/browse/MAPREDUCE-5679) | TestJobHistoryParsing has race condition |  Major | . | Liyin Liang | Liyin Liang |
| [HADOOP-10168](https://issues.apache.org/jira/browse/HADOOP-10168) | fix javadoc of ReflectionUtils.copy |  Major | . | Thejas M Nair | Thejas M Nair |
| [YARN-1451](https://issues.apache.org/jira/browse/YARN-1451) | TestResourceManager relies on the scheduler assigning multiple containers in a single node update |  Minor | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5657](https://issues.apache.org/jira/browse/HDFS-5657) | race condition causes writeback state error in NFS gateway |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5305](https://issues.apache.org/jira/browse/HDFS-5305) | Add https support in HDFS |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-10171](https://issues.apache.org/jira/browse/HADOOP-10171) | TestRPC fails intermittently on jkd7 |  Major | . | Mit Desai | Mit Desai |
| [HDFS-5661](https://issues.apache.org/jira/browse/HDFS-5661) | Browsing FileSystem via web ui, should use datanode's fqdn instead of ip address |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-5540](https://issues.apache.org/jira/browse/HDFS-5540) | Fix intermittent failure in TestBlocksWithNotEnoughRacks |  Minor | . | Binglin Chang | Binglin Chang |
| [HDFS-5681](https://issues.apache.org/jira/browse/HDFS-5681) | renewLease should not hold fsn write lock |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-1180](https://issues.apache.org/jira/browse/YARN-1180) | Update capacity scheduler docs to include types on the configs |  Trivial | capacityscheduler | Thomas Graves | Chen He |
| [HDFS-5691](https://issues.apache.org/jira/browse/HDFS-5691) | Fix typo in ShortCircuitLocalRead document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-5550](https://issues.apache.org/jira/browse/MAPREDUCE-5550) | Task Status message (reporter.setStatus) not shown in UI with Hadoop 2.0 |  Major | . | Vrushali C | Gera Shegalov |
| [HDFS-5690](https://issues.apache.org/jira/browse/HDFS-5690) | DataNode fails to start in secure mode when dfs.http.policy equals to HTTP\_ONLY |  Blocker | . | Haohui Mai | Haohui Mai |
| [HADOOP-10175](https://issues.apache.org/jira/browse/HADOOP-10175) | Har files system authority should preserve userinfo |  Major | fs | Chuan Liu | Chuan Liu |
| [HADOOP-10090](https://issues.apache.org/jira/browse/HADOOP-10090) | Jobtracker metrics not updated properly after execution of a mapreduce job |  Major | metrics | Ivan Mitic | Ivan Mitic |
| [YARN-1527](https://issues.apache.org/jira/browse/YARN-1527) | yarn rmadmin command prints wrong usage info: |  Trivial | . | Jian He | Akira Ajisaka |
| [YARN-1541](https://issues.apache.org/jira/browse/YARN-1541) | Invalidate AM Host/Port when app attempt is done so that in the mean-while client doesn’t get wrong information. |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5694](https://issues.apache.org/jira/browse/MAPREDUCE-5694) | MR AM container syslog is empty |  Major | . | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [HDFS-5675](https://issues.apache.org/jira/browse/HDFS-5675) | Add Mkdirs operation to NNThroughputBenchmark |  Minor | benchmarks | Plamen Jeliazkov | Plamen Jeliazkov |
| [HDFS-5582](https://issues.apache.org/jira/browse/HDFS-5582) | hdfs getconf -excludeFile or -includeFile always failed |  Minor | . | Henry Hung | sathish |
| [HDFS-5701](https://issues.apache.org/jira/browse/HDFS-5701) | Fix the CacheAdmin -addPool -maxTtl option name |  Minor | caching, tools | Stephen Chu | Stephen Chu |
| [MAPREDUCE-5685](https://issues.apache.org/jira/browse/MAPREDUCE-5685) | getCacheFiles()  api doesn't work in WrappedReducer.java due to typo |  Blocker | client | Yi Song | Yi Song |
| [YARN-1522](https://issues.apache.org/jira/browse/YARN-1522) | TestApplicationCleanup.testAppCleanup occasionally fails |  Major | . | Liyin Liang | Liyin Liang |
| [HDFS-5671](https://issues.apache.org/jira/browse/HDFS-5671) | Fix socket leak in DFSInputStream#getBlockReader |  Critical | hdfs-client | JamesLi | JamesLi |
| [HADOOP-10147](https://issues.apache.org/jira/browse/HADOOP-10147) | Upgrade to commons-logging 1.1.3 to avoid potential deadlock in MiniDFSCluster |  Minor | build | Eric Sirianni | Steve Loughran |
| [HDFS-5659](https://issues.apache.org/jira/browse/HDFS-5659) | dfsadmin -report doesn't output cache information properly |  Major | caching | Akira Ajisaka | Andrew Wang |
| [MAPREDUCE-5689](https://issues.apache.org/jira/browse/MAPREDUCE-5689) | MRAppMaster does not preempt reducers when scheduled maps cannot be fulfilled |  Critical | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-10193](https://issues.apache.org/jira/browse/HADOOP-10193) | hadoop-auth's PseudoAuthenticationHandler can consume getInputStream |  Minor | security | Gregory Chanan | Gregory Chanan |
| [HDFS-5719](https://issues.apache.org/jira/browse/HDFS-5719) | FSImage#doRollback() should close prevState before return |  Minor | namenode | Ted Yu | Ted Yu |
| [YARN-1409](https://issues.apache.org/jira/browse/YARN-1409) | NonAggregatingLogHandler can throw RejectedExecutionException |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-5649](https://issues.apache.org/jira/browse/HDFS-5649) | Unregister NFS and Mount service when NFS gateway is shutting down |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-1293](https://issues.apache.org/jira/browse/YARN-1293) | TestContainerLaunch.testInvalidEnvSyntaxDiagnostics fails on trunk |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-10214](https://issues.apache.org/jira/browse/HADOOP-10214) | Fix multithreaded correctness warnings in ActiveStandbyElector |  Major | ha | Liang Xie | Liang Xie |
| [HDFS-5449](https://issues.apache.org/jira/browse/HDFS-5449) | WebHdfs compatibility broken between 2.2 and 1.x / 23.x |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [YARN-1138](https://issues.apache.org/jira/browse/YARN-1138) | yarn.application.classpath is set to point to $HADOOP\_CONF\_DIR etc., which does not work on Windows |  Major | api | Yingda Chen | Chuan Liu |
| [HADOOP-9420](https://issues.apache.org/jira/browse/HADOOP-9420) | Add percentile or max metric for rpcQueueTime, processing time |  Major | ipc, metrics | Todd Lipcon | Liang Xie |
| [HDFS-5756](https://issues.apache.org/jira/browse/HDFS-5756) | hadoopRzOptionsSetByteBufferPool does not accept NULL argument, contrary to docs |  Major | libhdfs | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-10178](https://issues.apache.org/jira/browse/HADOOP-10178) | Configuration deprecation always emit "deprecated" warnings when a new key is used |  Major | conf | shanyu zhao | shanyu zhao |
| [HDFS-5747](https://issues.apache.org/jira/browse/HDFS-5747) | BlocksMap.getStoredBlock(..) and BlockInfoUnderConstruction.addReplicaIfNotPresent(..) may throw NullPointerException |  Minor | namenode | Tsz Wo Nicholas Sze | Arpit Agarwal |
| [HADOOP-10223](https://issues.apache.org/jira/browse/HADOOP-10223) | MiniKdc#main() should close the FileReader it creates |  Minor | . | Ted Yu | Ted Yu |
| [YARN-888](https://issues.apache.org/jira/browse/YARN-888) | clean up POM dependencies |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-5710](https://issues.apache.org/jira/browse/HDFS-5710) | FSDirectory#getFullPathName should check inodes against null |  Major | . | Ted Yu | Uma Maheswara Rao G |
| [HDFS-5579](https://issues.apache.org/jira/browse/HDFS-5579) | Under construction files make DataNode decommission take very long hours |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [HADOOP-10234](https://issues.apache.org/jira/browse/HADOOP-10234) | "hadoop.cmd jar" does not propagate exit code. |  Major | scripts | Chris Nauroth | Chris Nauroth |
| [YARN-1603](https://issues.apache.org/jira/browse/YARN-1603) | Remove two \*.orig files which were unexpectedly committed |  Trivial | . | Zhijie Shen | Zhijie Shen |
| [YARN-1601](https://issues.apache.org/jira/browse/YARN-1601) | 3rd party JARs are missing from hadoop-dist output |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10236](https://issues.apache.org/jira/browse/HADOOP-10236) | Fix typo in o.a.h.ipc.Client#checkResponse |  Trivial | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-5762](https://issues.apache.org/jira/browse/HDFS-5762) | BlockReaderLocal doesn't return -1 on EOF when doing zero-length reads |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5766](https://issues.apache.org/jira/browse/HDFS-5766) | In DFSInputStream, do not add datanode to deadNodes after InvalidEncryptionKeyException in fetchBlockByteRange |  Major | hdfs-client | Liang Xie | Liang Xie |
| [HDFS-5704](https://issues.apache.org/jira/browse/HDFS-5704) | Change OP\_UPDATE\_BLOCKS  with a new OP\_ADD\_BLOCK |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [HADOOP-10125](https://issues.apache.org/jira/browse/HADOOP-10125) | no need to process RPC request if the client connection has been dropped |  Major | ipc | Ming Ma | Ming Ma |
| [YARN-1351](https://issues.apache.org/jira/browse/YARN-1351) | Invalid string format in Fair Scheduler log warn message |  Trivial | resourcemanager | Konstantin Weitz | Konstantin Weitz |
| [HDFS-5777](https://issues.apache.org/jira/browse/HDFS-5777) | Update LayoutVersion for the new editlog op OP\_ADD\_BLOCK |  Major | namenode | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5724](https://issues.apache.org/jira/browse/MAPREDUCE-5724) | JobHistoryServer does not start if HDFS is not running |  Critical | jobhistoryserver | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-1608](https://issues.apache.org/jira/browse/YARN-1608) | LinuxContainerExecutor has a few DEBUG messages at INFO level |  Trivial | nodemanager | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10146](https://issues.apache.org/jira/browse/HADOOP-10146) | Workaround JDK7 Process fd close bug |  Critical | util | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5650](https://issues.apache.org/jira/browse/MAPREDUCE-5650) | Job fails when hprof mapreduce.task.profile.map/reduce.params is specified |  Major | mrv2 | Gera Shegalov | Gera Shegalov |
| [HADOOP-10235](https://issues.apache.org/jira/browse/HADOOP-10235) | Hadoop tarball has 2 versions of stax-api JARs |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10240](https://issues.apache.org/jira/browse/HADOOP-10240) | Windows build instructions incorrectly state requirement of protoc 2.4.1 instead of 2.5.0 |  Trivial | documentation | Chris Nauroth | Chris Nauroth |
| [HDFS-5800](https://issues.apache.org/jira/browse/HDFS-5800) | Typo: soft-limit for hard-limit in DFSClient |  Trivial | hdfs-client | Kousuke Saruta | Kousuke Saruta |
| [MAPREDUCE-5729](https://issues.apache.org/jira/browse/MAPREDUCE-5729) | mapred job -list throws NPE |  Critical | mrv2 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5725](https://issues.apache.org/jira/browse/MAPREDUCE-5725) | TestNetworkedJob relies on the Capacity Scheduler |  Major | . | Sandy Ryza | Sandy Ryza |
| [HADOOP-10110](https://issues.apache.org/jira/browse/HADOOP-10110) | hadoop-auth has a build break due to missing dependency |  Blocker | build | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5693](https://issues.apache.org/jira/browse/MAPREDUCE-5693) | Restore MRv1 behavior for log flush |  Major | mrv2 | Gera Shegalov | Gera Shegalov |
| [HDFS-5434](https://issues.apache.org/jira/browse/HDFS-5434) | Write resiliency for replica count 1 |  Minor | namenode | Taylor, Buddy |  |
| [HADOOP-10252](https://issues.apache.org/jira/browse/HADOOP-10252) | HttpServer can't start if hostname is not specified |  Major | . | Jimmy Xiang | Jimmy Xiang |
| [YARN-1624](https://issues.apache.org/jira/browse/YARN-1624) | QueuePlacementPolicy format is not easily readable via a JAXB parser |  Major | scheduler | Aditya Acharya | Aditya Acharya |
| [YARN-1607](https://issues.apache.org/jira/browse/YARN-1607) | TestRM expects the capacity scheduler |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-5806](https://issues.apache.org/jira/browse/HDFS-5806) | balancer should set SoTimeout to avoid indefinite hangs |  Major | balancer & mover | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5723](https://issues.apache.org/jira/browse/MAPREDUCE-5723) | MR AM container log can be truncated or empty |  Blocker | applicationmaster | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [HDFS-5789](https://issues.apache.org/jira/browse/HDFS-5789) | Some of snapshot APIs missing checkOperation double check in fsn |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-5728](https://issues.apache.org/jira/browse/HDFS-5728) | [Diskfull] Block recovery will fail if the metafile does not have crc for all chunks of the block |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-5343](https://issues.apache.org/jira/browse/HDFS-5343) | When cat command is issued on snapshot files getting unexpected result |  Major | hdfs-client | sathish | sathish |
| [HADOOP-10203](https://issues.apache.org/jira/browse/HADOOP-10203) | Connection leak in Jets3tNativeFileSystemStore#retrieveMetadata |  Major | fs/s3 | Andrei Savu | Andrei Savu |
| [HADOOP-9982](https://issues.apache.org/jira/browse/HADOOP-9982) | Fix dead links in hadoop site docs |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10212](https://issues.apache.org/jira/browse/HADOOP-10212) | Incorrect compile command in Native Library document |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-5297](https://issues.apache.org/jira/browse/HDFS-5297) | Fix dead links in HDFS site documents |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10250](https://issues.apache.org/jira/browse/HADOOP-10250) | VersionUtil returns wrong value when comparing two versions |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10288](https://issues.apache.org/jira/browse/HADOOP-10288) | Explicit reference to Log4JLogger breaks non-log4j users |  Major | util | Todd Lipcon | Todd Lipcon |
| [HDFS-5830](https://issues.apache.org/jira/browse/HDFS-5830) | WebHdfsFileSystem.getFileBlockLocations throws IllegalArgumentException when accessing another cluster. |  Blocker | caching, hdfs-client | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-9830](https://issues.apache.org/jira/browse/HADOOP-9830) | Typo at http://hadoop.apache.org/docs/current/ |  Trivial | documentation | Dmitry Lysnichenko | Kousuke Saruta |
| [HADOOP-10255](https://issues.apache.org/jira/browse/HADOOP-10255) | Rename HttpServer to HttpServer2 to retain older HttpServer in branch-2 for compatibility |  Blocker | . | Haohui Mai | Haohui Mai |
| [HADOOP-10292](https://issues.apache.org/jira/browse/HADOOP-10292) | Restore HttpServer from branch-2.2 in branch-2 |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1629](https://issues.apache.org/jira/browse/YARN-1629) | IndexOutOfBoundsException in Fair Scheduler MaxRunningAppsEnforcer |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1630](https://issues.apache.org/jira/browse/YARN-1630) | Introduce timeout for async polling operations in YarnClientImpl |  Major | client | Aditya Acharya | Aditya Acharya |
| [HADOOP-10291](https://issues.apache.org/jira/browse/HADOOP-10291) | TestSecurityUtil#testSocketAddrWithIP fails |  Major | . | Mit Desai | Mit Desai |
| [HDFS-5844](https://issues.apache.org/jira/browse/HDFS-5844) | Fix broken link in WebHDFS.apt.vm |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-1600](https://issues.apache.org/jira/browse/YARN-1600) | RM does not startup when security is enabled without spnego configured |  Blocker | resourcemanager | Jason Lowe | Haohui Mai |
| [HDFS-5842](https://issues.apache.org/jira/browse/HDFS-5842) | Cannot create hftp filesystem when using a proxy user ugi and a doAs on a secure cluster |  Major | security | Arpit Gupta | Jing Zhao |
| [HDFS-5845](https://issues.apache.org/jira/browse/HDFS-5845) | SecondaryNameNode dies when checkpointing with cache pools |  Blocker | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-10305](https://issues.apache.org/jira/browse/HADOOP-10305) | Add "rpc.metrics.quantile.enable" and "rpc.metrics.percentiles.intervals" to core-default.xml |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10310](https://issues.apache.org/jira/browse/HADOOP-10310) | SaslRpcServer should be initialized even when no secret manager present |  Blocker | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-10317](https://issues.apache.org/jira/browse/HADOOP-10317) | Rename branch-2.3 release version from 2.4.0-SNAPSHOT to 2.3.0-SNAPSHOT |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-10313](https://issues.apache.org/jira/browse/HADOOP-10313) | Script and jenkins job to produce Hadoop release artifacts |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10311](https://issues.apache.org/jira/browse/HADOOP-10311) | Cleanup vendor names from the code base |  Blocker | . | Suresh Srinivas | Alejandro Abdelnur |
| [HADOOP-10273](https://issues.apache.org/jira/browse/HADOOP-10273) | Fix 'mvn site' |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5876](https://issues.apache.org/jira/browse/HDFS-5876) | SecureDataNodeStarter does not pick up configuration in hdfs-site.xml |  Major | datanode | Haohui Mai | Haohui Mai |
| [HDFS-5873](https://issues.apache.org/jira/browse/HDFS-5873) | dfs.http.policy should have higher precedence over dfs.https.enable |  Major | . | Yesha Vora | Haohui Mai |
| [MAPREDUCE-5743](https://issues.apache.org/jira/browse/MAPREDUCE-5743) | TestRMContainerAllocator is failing |  Major | . | Ted Yu | Ted Yu |
| [YARN-1628](https://issues.apache.org/jira/browse/YARN-1628) | TestContainerManagerSecurity fails on trunk |  Major | . | Mit Desai | Vinod Kumar Vavilapalli |
| [HADOOP-10112](https://issues.apache.org/jira/browse/HADOOP-10112) | har file listing  doesn't work with wild card |  Major | tools | Brandon Li | Brandon Li |
| [MAPREDUCE-5744](https://issues.apache.org/jira/browse/MAPREDUCE-5744) | Job hangs because RMContainerAllocator$AssignedRequests.preemptReduce() violates the comparator contract |  Blocker | . | Sangjin Lee | Gera Shegalov |
| [HDFS-5837](https://issues.apache.org/jira/browse/HDFS-5837) | dfs.namenode.replication.considerLoad does not consider decommissioned nodes |  Major | namenode | Bryan Beaudreault | Tao Luo |
| [HDFS-5921](https://issues.apache.org/jira/browse/HDFS-5921) | Cannot browse file system via NN web UI if any directory has the sticky bit set |  Critical | namenode | Aaron T. Myers | Aaron T. Myers |
| [YARN-1330](https://issues.apache.org/jira/browse/YARN-1330) | Fair Scheduler: defaultQueueSchedulingPolicy does not take effect |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-9478](https://issues.apache.org/jira/browse/HADOOP-9478) | Fix race conditions during the initialization of Configuration related to deprecatedKeyMap |  Major | conf | Dongyong Wang | Colin P. McCabe |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5084](https://issues.apache.org/jira/browse/MAPREDUCE-5084) | fix coverage  org.apache.hadoop.mapreduce.v2.app.webapp and org.apache.hadoop.mapreduce.v2.hs.webapp |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [HADOOP-9866](https://issues.apache.org/jira/browse/HADOOP-9866) | convert hadoop-auth testcases requiring kerberos to use minikdc |  Major | test | Alejandro Abdelnur | Wei Yan |
| [HDFS-4491](https://issues.apache.org/jira/browse/HDFS-4491) | Parallel testing HDFS |  Major | test | Tsuyoshi Ozawa | Andrey Klochkov |
| [YARN-1119](https://issues.apache.org/jira/browse/YARN-1119) | Add ClusterMetrics checks to tho TestRMNodeTransitions tests |  Major | resourcemanager | Robert Parker | Mit Desai |
| [HDFS-4517](https://issues.apache.org/jira/browse/HDFS-4517) | Cover class RemoteBlockReader with unit tests |  Major | . | Vadim Bondarev | Ivan A. Veselovsky |
| [HDFS-4512](https://issues.apache.org/jira/browse/HDFS-4512) | Cover package org.apache.hadoop.hdfs.server.common with tests |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [HADOOP-9063](https://issues.apache.org/jira/browse/HADOOP-9063) | enhance unit-test coverage of class org.apache.hadoop.fs.FileUtil |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9254](https://issues.apache.org/jira/browse/HADOOP-9254) | Cover packages org.apache.hadoop.util.bloom, org.apache.hadoop.util.hash |  Major | . | Vadim Bondarev | Vadim Bondarev |
| [HADOOP-9225](https://issues.apache.org/jira/browse/HADOOP-9225) | Cover package org.apache.hadoop.compress.Snappy |  Major | . | Vadim Bondarev | Andrey Klochkov |
| [HADOOP-9199](https://issues.apache.org/jira/browse/HADOOP-9199) | Cover package org.apache.hadoop.io with unit tests |  Major | . | Vadim Bondarev | Andrey Klochkov |
| [HDFS-4510](https://issues.apache.org/jira/browse/HDFS-4510) | Cover classes ClusterJspHelper/NamenodeJspHelper with unit tests |  Major | . | Vadim Bondarev | Andrey Klochkov |
| [MAPREDUCE-5102](https://issues.apache.org/jira/browse/MAPREDUCE-5102) | fix coverage  org.apache.hadoop.mapreduce.lib.db and org.apache.hadoop.mapred.lib.db |  Major | . | Aleksey Gorshkov | Andrey Klochkov |
| [HDFS-5130](https://issues.apache.org/jira/browse/HDFS-5130) | Add test for snapshot related FsShell and DFSAdmin commands |  Minor | test | Binglin Chang | Binglin Chang |
| [HADOOP-9078](https://issues.apache.org/jira/browse/HADOOP-9078) | enhance unit-test coverage of class org.apache.hadoop.fs.FileContext |  Major | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-4511](https://issues.apache.org/jira/browse/HDFS-4511) | Cover package org.apache.hadoop.hdfs.tools with unit test |  Major | . | Vadim Bondarev | Andrey Klochkov |
| [HADOOP-9291](https://issues.apache.org/jira/browse/HADOOP-9291) | enhance unit-test coverage of package o.a.h.metrics2 |  Major | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9598](https://issues.apache.org/jira/browse/HADOOP-9598) | Improve code coverage of RMAdminCLI |  Major | . | Aleksey Gorshkov | Andrey Klochkov |
| [YARN-1357](https://issues.apache.org/jira/browse/YARN-1357) | TestContainerLaunch.testContainerEnvVariables fails on Windows |  Minor | nodemanager | Chuan Liu | Chuan Liu |
| [YARN-1358](https://issues.apache.org/jira/browse/YARN-1358) | TestYarnCLI fails on Windows due to line endings |  Minor | client | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5610](https://issues.apache.org/jira/browse/MAPREDUCE-5610) | TestSleepJob fails in jdk7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-5625](https://issues.apache.org/jira/browse/MAPREDUCE-5625) | TestFixedLengthInputFormat fails in jdk7 environment |  Major | . | Jonathan Eagles | Mariappan Asokan |
| [MAPREDUCE-5632](https://issues.apache.org/jira/browse/MAPREDUCE-5632) | TestRMContainerAllocator#testUpdatedNodes fails |  Major | . | Ted Yu | Jonathan Eagles |
| [MAPREDUCE-5687](https://issues.apache.org/jira/browse/MAPREDUCE-5687) | TestYARNRunner#testResourceMgrDelegate fails with NPE after YARN-1446 |  Major | . | Ted Yu | Jian He |
| [HDFS-5679](https://issues.apache.org/jira/browse/HDFS-5679) | TestCacheDirectives should handle the case where native code is not available |  Major | . | Ted Yu | Andrew Wang |
| [YARN-1463](https://issues.apache.org/jira/browse/YARN-1463) | Tests should avoid starting http-server where possible or creates spnego keytab/principals |  Major | . | Ted Yu | Vinod Kumar Vavilapalli |
| [YARN-1549](https://issues.apache.org/jira/browse/YARN-1549) | TestUnmanagedAMLauncher#testDSShell fails in trunk |  Major | . | Ted Yu | haosdent |
| [YARN-1560](https://issues.apache.org/jira/browse/YARN-1560) | TestYarnClient#testAMMRTokens fails with null AMRM token |  Major | . | Ted Yu | Ted Yu |
| [HADOOP-10207](https://issues.apache.org/jira/browse/HADOOP-10207) | TestUserGroupInformation#testLogin is flaky |  Minor | . | Jimmy Xiang | Jimmy Xiang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-427](https://issues.apache.org/jira/browse/YARN-427) | Coverage fix for org.apache.hadoop.yarn.server.api.\* |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-478](https://issues.apache.org/jira/browse/YARN-478) | fix coverage org.apache.hadoop.yarn.webapp.log |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [HADOOP-9417](https://issues.apache.org/jira/browse/HADOOP-9417) | Support for symlink resolution in LocalFileSystem / RawLocalFileSystem |  Major | fs | Andrew Wang | Andrew Wang |
| [HADOOP-9748](https://issues.apache.org/jira/browse/HADOOP-9748) | Reduce blocking on UGI.ensureInitialized |  Critical | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5356](https://issues.apache.org/jira/browse/MAPREDUCE-5356) | Ability to refresh aggregated log retention period and check interval |  Major | jobhistoryserver | Ashwin Shankar | Ashwin Shankar |
| [MAPREDUCE-5386](https://issues.apache.org/jira/browse/MAPREDUCE-5386) | Ability to refresh history server job retention and job cleaner settings |  Major | jobhistoryserver | Ashwin Shankar | Ashwin Shankar |
| [MAPREDUCE-5411](https://issues.apache.org/jira/browse/MAPREDUCE-5411) | Refresh size of loaded job cache on history server |  Major | jobhistoryserver | Ashwin Shankar | Ashwin Shankar |
| [YARN-649](https://issues.apache.org/jira/browse/YARN-649) | Make container logs available over HTTP in plain text |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [YARN-1098](https://issues.apache.org/jira/browse/YARN-1098) | Separate out RM services into "Always On" and "Active" |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-4096](https://issues.apache.org/jira/browse/HDFS-4096) | Add snapshot information to namenode WebUI |  Major | datanode, namenode | Jing Zhao | Haohui Mai |
| [YARN-1027](https://issues.apache.org/jira/browse/YARN-1027) | Implement RMHAProtocolService |  Major | . | Bikas Saha | Karthik Kambatla |
| [YARN-353](https://issues.apache.org/jira/browse/YARN-353) | Add Zookeeper-based store implementation for RMStateStore |  Major | resourcemanager | Hitesh Shah | Karthik Kambatla |
| [HDFS-5239](https://issues.apache.org/jira/browse/HDFS-5239) | Allow FSNamesystem lock fairness to be configurable |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-5240](https://issues.apache.org/jira/browse/HDFS-5240) | Separate formatting from logging in the audit logger API |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-819](https://issues.apache.org/jira/browse/YARN-819) | ResourceManager and NodeManager should check for a minimum allowed version |  Major | nodemanager, resourcemanager | Robert Parker | Robert Parker |
| [MAPREDUCE-4421](https://issues.apache.org/jira/browse/MAPREDUCE-4421) | Run MapReduce framework via the distributed cache |  Major | . | Arun C Murthy | Jason Lowe |
| [YARN-425](https://issues.apache.org/jira/browse/YARN-425) | coverage fix for yarn api |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-1232](https://issues.apache.org/jira/browse/YARN-1232) | Configuration to support multiple RMs |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5307](https://issues.apache.org/jira/browse/HDFS-5307) | Support both HTTP and HTTPS in jsp pages |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-465](https://issues.apache.org/jira/browse/YARN-465) | fix coverage  org.apache.hadoop.yarn.server.webproxy |  Major | . | Aleksey Gorshkov | Andrey Klochkov |
| [HDFS-5317](https://issues.apache.org/jira/browse/HDFS-5317) | Go back to DFS Home link does not work on datanode webUI |  Critical | . | Suresh Srinivas | Haohui Mai |
| [HDFS-5316](https://issues.apache.org/jira/browse/HDFS-5316) | Namenode ignores the default https port |  Critical | . | Suresh Srinivas | Haohui Mai |
| [HDFS-5281](https://issues.apache.org/jira/browse/HDFS-5281) | COMMIT request should not block |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-976](https://issues.apache.org/jira/browse/YARN-976) | Document the meaning of a virtual core |  Major | documentation | Sandy Ryza | Sandy Ryza |
| [YARN-1283](https://issues.apache.org/jira/browse/YARN-1283) | Invalid 'url of job' mentioned in Job output with yarn.http.policy=HTTPS\_ONLY |  Major | . | Yesha Vora | Omkar Vinit Joshi |
| [HDFS-5337](https://issues.apache.org/jira/browse/HDFS-5337) | should do hsync for a commit request even there is no pending writes |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-7](https://issues.apache.org/jira/browse/YARN-7) | Add support for DistributedShell to ask for CPUs along with memory |  Major | . | Arun C Murthy | Junping Du |
| [HDFS-5342](https://issues.apache.org/jira/browse/HDFS-5342) | Provide more information in the FSNamesystem JMX interfaces |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5330](https://issues.apache.org/jira/browse/HDFS-5330) | fix readdir and readdirplus for large directories |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5334](https://issues.apache.org/jira/browse/HDFS-5334) | Implement dfshealth.jsp in HTML pages |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5379](https://issues.apache.org/jira/browse/HDFS-5379) | Update links to datanode information in dfshealth.html |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1185](https://issues.apache.org/jira/browse/YARN-1185) | FileSystemRMStateStore can leave partial files that prevent subsequent recovery |  Major | resourcemanager | Jason Lowe | Omkar Vinit Joshi |
| [HADOOP-10052](https://issues.apache.org/jira/browse/HADOOP-10052) | Temporarily disable client-side symlink resolution |  Major | fs | Andrew Wang | Andrew Wang |
| [HDFS-5382](https://issues.apache.org/jira/browse/HDFS-5382) | Implement the UI of browsing filesystems in HTML 5 page |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5347](https://issues.apache.org/jira/browse/HDFS-5347) | add HDFS NFS user guide |  Major | documentation | Brandon Li | Brandon Li |
| [HDFS-4885](https://issues.apache.org/jira/browse/HDFS-4885) | Update verifyBlockPlacement() API in BlockPlacementPolicy |  Major | . | Junping Du | Junping Du |
| [YARN-1305](https://issues.apache.org/jira/browse/YARN-1305) | RMHAProtocolService#serviceInit should handle HAUtil's IllegalArgumentException |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-5363](https://issues.apache.org/jira/browse/HDFS-5363) | Refactor WebHdfsFileSystem: move SPENGO-authenticated connection creation to URLConnectionFactory |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5171](https://issues.apache.org/jira/browse/HDFS-5171) | NFS should create input stream for a file and try to share it with multiple read requests |  Major | nfs | Brandon Li | Haohui Mai |
| [YARN-1068](https://issues.apache.org/jira/browse/YARN-1068) | Add admin support for HA operations |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5436](https://issues.apache.org/jira/browse/HDFS-5436) | Move HsFtpFileSystem and HFtpFileSystem into org.apache.hdfs.web |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5386](https://issues.apache.org/jira/browse/HDFS-5386) | Add feature documentation for datanode caching. |  Major | documentation | Chris Nauroth | Colin P. McCabe |
| [YARN-891](https://issues.apache.org/jira/browse/YARN-891) | Store completed application information in RM state store |  Major | resourcemanager | Bikas Saha | Jian He |
| [YARN-1323](https://issues.apache.org/jira/browse/YARN-1323) | Set HTTPS webapp address along with other RPC addresses in HAUtil |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-311](https://issues.apache.org/jira/browse/YARN-311) | Dynamic node resource configuration: core scheduler changes |  Major | graceful, resourcemanager, scheduler | Junping Du | Junping Du |
| [HDFS-5252](https://issues.apache.org/jira/browse/HDFS-5252) | Stable write is not handled correctly in someplace |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5364](https://issues.apache.org/jira/browse/HDFS-5364) | Add OpenFileCtx cache |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5394](https://issues.apache.org/jira/browse/HDFS-5394) | fix race conditions in DN caching and uncaching |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5325](https://issues.apache.org/jira/browse/HDFS-5325) | Remove WebHdfsFileSystem#ConnRunner |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5469](https://issues.apache.org/jira/browse/HDFS-5469) | Add configuration property for the sub-directroy export path |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5320](https://issues.apache.org/jira/browse/HDFS-5320) | Add datanode caching metrics |  Minor | datanode | Andrew Wang | Andrew Wang |
| [HDFS-5488](https://issues.apache.org/jira/browse/HDFS-5488) | Clean up TestHftpURLTimeout |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5450](https://issues.apache.org/jira/browse/HDFS-5450) | better API for getting the cached blocks locations |  Minor | hdfs-client | Colin P. McCabe | Andrew Wang |
| [HDFS-5440](https://issues.apache.org/jira/browse/HDFS-5440) | Extract the logic of handling delegation tokens in HftpFileSystem to the TokenAspect class |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-9956](https://issues.apache.org/jira/browse/HADOOP-9956) | RPC listener inefficiently assigns connections to readers |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-5444](https://issues.apache.org/jira/browse/HDFS-5444) | Choose default web UI based on browser capabilities |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5487](https://issues.apache.org/jira/browse/HDFS-5487) | Introduce unit test for TokenAspect |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1222](https://issues.apache.org/jira/browse/YARN-1222) | Make improvements in ZKRMStateStore for fencing |  Major | . | Bikas Saha | Karthik Kambatla |
| [HDFS-5506](https://issues.apache.org/jira/browse/HDFS-5506) | Use URLConnectionFactory in DelegationTokenFetcher |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-9955](https://issues.apache.org/jira/browse/HADOOP-9955) | RPC idle connection closing is extremely inefficient |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-5489](https://issues.apache.org/jira/browse/HDFS-5489) | Use TokenAspect in WebHDFSFileSystem |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5366](https://issues.apache.org/jira/browse/HDFS-5366) | recaching improvements |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5509](https://issues.apache.org/jira/browse/HDFS-5509) | TestPathBasedCacheRequests#testReplicationFactor is flaky |  Major | datanode, namenode | Andrew Wang | Andrew Wang |
| [YARN-1411](https://issues.apache.org/jira/browse/YARN-1411) | HA config shouldn't affect NodeManager RPC addresses |  Critical | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5502](https://issues.apache.org/jira/browse/HDFS-5502) | Fix HTTPS support in HsftpFileSystem |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-10109](https://issues.apache.org/jira/browse/HADOOP-10109) | Fix test failure in TestOfflineEditsViewer introduced by HADOOP-10052 |  Major | test | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5519](https://issues.apache.org/jira/browse/HDFS-5519) | COMMIT handler should update the commit status after sync |  Minor | nfs | Brandon Li | Brandon Li |
| [HDFS-5393](https://issues.apache.org/jira/browse/HDFS-5393) | Serve bootstrap and jQuery locally |  Minor | . | Haohui Mai | Haohui Mai |
| [YARN-709](https://issues.apache.org/jira/browse/YARN-709) | verify that new jobs submitted with old RM delegation tokens after RM restart are accepted |  Major | resourcemanager | Jian He | Jian He |
| [HDFS-5520](https://issues.apache.org/jira/browse/HDFS-5520) | loading cache path directives from edit log doesn't update nextEntryId |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-1210](https://issues.apache.org/jira/browse/YARN-1210) | During RM restart, RM should start a new attempt only when previous attempt exits for real |  Major | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [YARN-674](https://issues.apache.org/jira/browse/YARN-674) | Slow or failing DelegationToken renewals on submission itself make RM unavailable |  Major | resourcemanager | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [HADOOP-10107](https://issues.apache.org/jira/browse/HADOOP-10107) | Server.getNumOpenConnections may throw NPE |  Major | ipc | Tsz Wo Nicholas Sze | Kihwal Lee |
| [HDFS-5511](https://issues.apache.org/jira/browse/HDFS-5511) | improve CacheManipulator interface to allow better unit testing |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5525](https://issues.apache.org/jira/browse/HDFS-5525) | Inline dust templates |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5451](https://issues.apache.org/jira/browse/HDFS-5451) | Add byte and file statistics to PathBasedCacheEntry |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-3987](https://issues.apache.org/jira/browse/HDFS-3987) | Support webhdfs over HTTPS |  Major | . | Alejandro Abdelnur | Haohui Mai |
| [HADOOP-10103](https://issues.apache.org/jira/browse/HADOOP-10103) | update commons-lang to 2.6 |  Minor | build | Steve Loughran | Akira Ajisaka |
| [HDFS-5473](https://issues.apache.org/jira/browse/HDFS-5473) | Consistent naming of user-visible caching classes and methods |  Major | datanode, namenode | Andrew Wang | Colin P. McCabe |
| [HDFS-5543](https://issues.apache.org/jira/browse/HDFS-5543) | fix narrow race condition in TestPathBasedCacheRequests |  Major | test | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5288](https://issues.apache.org/jira/browse/HDFS-5288) | Close idle connections in portmap |  Major | nfs | Haohui Mai | Haohui Mai |
| [HDFS-5538](https://issues.apache.org/jira/browse/HDFS-5538) | URLConnectionFactory should pick up the SSL related configuration by default |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5565](https://issues.apache.org/jira/browse/HDFS-5565) | CacheAdmin help should match against non-dashed commands |  Minor | caching | Andrew Wang | Andrew Wang |
| [HDFS-5556](https://issues.apache.org/jira/browse/HDFS-5556) | add some more NameNode cache statistics, cache pool stats |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5545](https://issues.apache.org/jira/browse/HDFS-5545) | Allow specifying endpoints for listeners in HttpServer |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1239](https://issues.apache.org/jira/browse/YARN-1239) | Save version information in the state store |  Major | resourcemanager | Bikas Saha | Jian He |
| [HDFS-5430](https://issues.apache.org/jira/browse/HDFS-5430) | Support TTL on CacheDirectives |  Minor | datanode, namenode | Colin P. McCabe | Andrew Wang |
| [YARN-1318](https://issues.apache.org/jira/browse/YARN-1318) | Promote AdminService to an Always-On service and merge in RMHAProtocolService |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-895](https://issues.apache.org/jira/browse/YARN-895) | RM crashes if it restarts while the state-store is down |  Major | resourcemanager | Jian He | Jian He |
| [HADOOP-10102](https://issues.apache.org/jira/browse/HADOOP-10102) | update commons IO from 2.1 to 2.4 |  Minor | build | Steve Loughran | Akira Ajisaka |
| [HDFS-5536](https://issues.apache.org/jira/browse/HDFS-5536) | Implement HTTP policy for Namenode and DataNode |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5514](https://issues.apache.org/jira/browse/HDFS-5514) | FSNamesystem's fsLock should allow custom implementation |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-5409](https://issues.apache.org/jira/browse/MAPREDUCE-5409) | MRAppMaster throws InvalidStateTransitonException: Invalid event: TA\_TOO\_MANY\_FETCH\_FAILURE at KILLED for TaskAttemptImpl |  Major | . | Devaraj K | Gera Shegalov |
| [HDFS-5630](https://issues.apache.org/jira/browse/HDFS-5630) | Hook up cache directive and pool usage statistics |  Major | caching, namenode | Andrew Wang | Andrew Wang |
| [YARN-1447](https://issues.apache.org/jira/browse/YARN-1447) | Common PB type definitions for container resizing |  Major | api | Wangda Tan (No longer used) | Wangda Tan |
| [YARN-1181](https://issues.apache.org/jira/browse/YARN-1181) | Augment MiniYARNCluster to support HA mode |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5312](https://issues.apache.org/jira/browse/HDFS-5312) | Generate HTTP / HTTPS URL in DFSUtil#getInfoServer() based on the configured http policy |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1378](https://issues.apache.org/jira/browse/YARN-1378) | Implement a RMStateStore cleaner for deleting application/attempt info |  Major | resourcemanager | Jian He | Jian He |
| [YARN-1405](https://issues.apache.org/jira/browse/YARN-1405) | RM hangs on shutdown if calling system.exit in serviceInit or serviceStart |  Major | . | Yesha Vora | Jian He |
| [YARN-1448](https://issues.apache.org/jira/browse/YARN-1448) | AM-RM protocol changes to support container resizing |  Major | api, resourcemanager | Wangda Tan (No longer used) | Wangda Tan |
| [HDFS-5629](https://issues.apache.org/jira/browse/HDFS-5629) | Support HTTPS in JournalNode and SecondaryNameNode |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1325](https://issues.apache.org/jira/browse/YARN-1325) | Enabling HA should check Configuration contains multiple RMs |  Major | resourcemanager | Tsuyoshi Ozawa | Xuan Gong |
| [YARN-1311](https://issues.apache.org/jira/browse/YARN-1311) | Fix app specific scheduler-events' names to be app-attempt based |  Trivial | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1485](https://issues.apache.org/jira/browse/YARN-1485) | Enabling HA should verify the RM service addresses configurations have been set for every RM Ids defined in RM\_HA\_IDs |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-5406](https://issues.apache.org/jira/browse/HDFS-5406) | Send incremental block reports for all storages in a single call |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-5454](https://issues.apache.org/jira/browse/HDFS-5454) | DataNode UUID should be assigned prior to FsDataset initialization |  Minor | datanode | Eric Sirianni | Arpit Agarwal |
| [YARN-312](https://issues.apache.org/jira/browse/YARN-312) | Add updateNodeResource in ResourceManagerAdministrationProtocol |  Major | api, graceful | Junping Du | Junping Du |
| [YARN-1446](https://issues.apache.org/jira/browse/YARN-1446) | Change killing application to wait until state store is done |  Major | resourcemanager | Jian He | Jian He |
| [HDFS-5431](https://issues.apache.org/jira/browse/HDFS-5431) | support cachepool-based limit management in path-based caching |  Major | datanode, namenode | Colin P. McCabe | Andrew Wang |
| [YARN-1307](https://issues.apache.org/jira/browse/YARN-1307) | Rethink znode structure for RM HA |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-5634](https://issues.apache.org/jira/browse/HDFS-5634) | allow BlockReaderLocal to switch between checksumming and not |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5636](https://issues.apache.org/jira/browse/HDFS-5636) | Enforce a max TTL per cache pool |  Major | caching, namenode | Andrew Wang | Andrew Wang |
| [YARN-1028](https://issues.apache.org/jira/browse/YARN-1028) | Add FailoverProxyProvider like capability to RMProxy |  Major | . | Bikas Saha | Karthik Kambatla |
| [YARN-1172](https://issues.apache.org/jira/browse/YARN-1172) | Convert \*SecretManagers in the RM to services |  Major | resourcemanager | Karthik Kambatla | Tsuyoshi Ozawa |
| [YARN-1523](https://issues.apache.org/jira/browse/YARN-1523) | Use StandbyException instead of RMNotYetReadyException |  Major | . | Bikas Saha | Karthik Kambatla |
| [YARN-1481](https://issues.apache.org/jira/browse/YARN-1481) | Move internal services logic from AdminService to ResourceManager |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1121](https://issues.apache.org/jira/browse/YARN-1121) | RMStateStore should flush all pending store events before closing |  Major | resourcemanager | Bikas Saha | Jian He |
| [HDFS-5708](https://issues.apache.org/jira/browse/HDFS-5708) | The CacheManager throws a NPE in the DataNode logs when processing cache reports that refer to a block not known to the BlockManager |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-5667](https://issues.apache.org/jira/browse/HDFS-5667) | Include DatanodeStorage in StorageReport |  Major | datanode | Eric Sirianni | Arpit Agarwal |
| [YARN-1559](https://issues.apache.org/jira/browse/YARN-1559) | Race between ServerRMProxy and ClientRMProxy setting RMProxy#INSTANCE |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-1029](https://issues.apache.org/jira/browse/YARN-1029) | Allow embedding leader election into the RM |  Major | . | Bikas Saha | Karthik Kambatla |
| [YARN-1482](https://issues.apache.org/jira/browse/YARN-1482) | WebApplicationProxy should be always-on w.r.t HA even if it is embedded in the RM |  Major | . | Vinod Kumar Vavilapalli | Xuan Gong |
| [HDFS-5651](https://issues.apache.org/jira/browse/HDFS-5651) | Remove dfs.namenode.caching.enabled and improve CRM locking |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-1579](https://issues.apache.org/jira/browse/YARN-1579) | ActiveRMInfoProto fields should be optional |  Trivial | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5589](https://issues.apache.org/jira/browse/HDFS-5589) | Namenode loops caching and uncaching when data should be uncached |  Major | caching, namenode | Andrew Wang | Andrew Wang |
| [YARN-1033](https://issues.apache.org/jira/browse/YARN-1033) | Expose RM active/standby state to Web UI and REST API |  Major | . | Nemon Lou | Karthik Kambatla |
| [YARN-1574](https://issues.apache.org/jira/browse/YARN-1574) | RMDispatcher should be reset on transition to standby |  Blocker | . | Xuan Gong | Xuan Gong |
| [YARN-1598](https://issues.apache.org/jira/browse/YARN-1598) | HA-related rmadmin commands don't work on a secure cluster |  Critical | client, resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5784](https://issues.apache.org/jira/browse/HDFS-5784) | reserve space in edit log header and fsimage header for feature flag section |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-1573](https://issues.apache.org/jira/browse/YARN-1573) | ZK store should use a private password for root-node-acls |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-5241](https://issues.apache.org/jira/browse/HDFS-5241) | Provide alternate queuing audit logger to reduce logging contention |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-1575](https://issues.apache.org/jira/browse/YARN-1575) | Public localizer crashes with "Localized unkown resource" |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-1642](https://issues.apache.org/jira/browse/YARN-1642) | RMDTRenewer#getRMClient should use ClientRMProxy |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-1618](https://issues.apache.org/jira/browse/YARN-1618) | Fix invalid RMApp transition from NEW to FINAL\_SAVING |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5463](https://issues.apache.org/jira/browse/MAPREDUCE-5463) | Deprecate SLOTS\_MILLIS counters |  Major | . | Sandy Ryza | Tsuyoshi Ozawa |
| [YARN-1568](https://issues.apache.org/jira/browse/YARN-1568) | Rename clusterid to clusterId in ActiveRMInfoProto |  Trivial | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5464](https://issues.apache.org/jira/browse/MAPREDUCE-5464) | Add analogs of the SLOTS\_MILLIS counters that jive with the YARN resource model |  Major | . | Sandy Ryza | Sandy Ryza |


