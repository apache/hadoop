
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

## Release 0.23.3 - 2012-09-20

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4072](https://issues.apache.org/jira/browse/MAPREDUCE-4072) | User set java.library.path seems to overwrite default creating problems native lib loading |  Major | mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-3812](https://issues.apache.org/jira/browse/MAPREDUCE-3812) | Lower default allocation sizes, fix allocation configurations and document them |  Major | mrv2, performance | Vinod Kumar Vavilapalli | Harsh J |
| [HDFS-3318](https://issues.apache.org/jira/browse/HDFS-3318) | Hftp hangs on transfers \>2GB |  Blocker | hdfs-client | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4311](https://issues.apache.org/jira/browse/MAPREDUCE-4311) | Capacity scheduler.xml does not accept decimal values for capacity and maximum-capacity settings |  Major | capacity-sched, mrv2 | Thomas Graves | Karthik Kambatla |
| [HADOOP-8551](https://issues.apache.org/jira/browse/HADOOP-8551) | fs -mkdir creates parent directories without the -p option |  Major | fs | Robert Joseph Evans | John George |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2978](https://issues.apache.org/jira/browse/HDFS-2978) | The NameNode should expose name dir statuses via JMX |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-3773](https://issues.apache.org/jira/browse/MAPREDUCE-3773) | Add queue metrics with buckets for job run times |  Major | jobtracker | Owen O'Malley | Owen O'Malley |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-208](https://issues.apache.org/jira/browse/HDFS-208) | name node should warn if only one dir is listed in dfs.name.dir |  Minor | namenode | Allen Wittenauer | Uma Maheswara Rao G |
| [MAPREDUCE-3935](https://issues.apache.org/jira/browse/MAPREDUCE-3935) | Annotate Counters.Counter and Counters.Group as @Public |  Major | client | Tom White | Tom White |
| [HADOOP-8141](https://issues.apache.org/jira/browse/HADOOP-8141) | Add method to init krb5 cipher suites |  Trivial | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-8242](https://issues.apache.org/jira/browse/HADOOP-8242) | AbstractDelegationTokenIdentifier: add getter methods for owner and realuser |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-4059](https://issues.apache.org/jira/browse/MAPREDUCE-4059) | The history server should have a separate pluggable storage/query interface |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4017](https://issues.apache.org/jira/browse/MAPREDUCE-4017) | Add jobname to jobsummary log |  Trivial | jobhistoryserver, jobtracker | Koji Noguchi | Thomas Graves |
| [HADOOP-7510](https://issues.apache.org/jira/browse/HADOOP-7510) | Tokens should use original hostname provided instead of ip |  Major | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-8286](https://issues.apache.org/jira/browse/HADOOP-8286) | Simplify getting a socket address from conf |  Major | conf | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4151](https://issues.apache.org/jira/browse/MAPREDUCE-4151) | RM scheduler web page should filter apps to those that are relevant to scheduling |  Major | mrv2, webapps | Jason Lowe | Jason Lowe |
| [HDFS-2652](https://issues.apache.org/jira/browse/HDFS-2652) | Port token service changes from 205 |  Major | . | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4190](https://issues.apache.org/jira/browse/MAPREDUCE-4190) |  Improve web UI for task attempts userlog link |  Major | mrv2, webapps | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4079](https://issues.apache.org/jira/browse/MAPREDUCE-4079) | Allow MR AppMaster to limit ephemeral port range. |  Blocker | mr-am, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8227](https://issues.apache.org/jira/browse/HADOOP-8227) | Allow RPC to limit ephemeral port range. |  Blocker | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8335](https://issues.apache.org/jira/browse/HADOOP-8335) | Improve Configuration's address handling |  Major | util | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4210](https://issues.apache.org/jira/browse/MAPREDUCE-4210) | Expose listener address for WebApp |  Major | webapps | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4205](https://issues.apache.org/jira/browse/MAPREDUCE-4205) | retrofit all JVM shutdown hooks to use ShutdownHookManager |  Major | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3850](https://issues.apache.org/jira/browse/MAPREDUCE-3850) | Avoid redundant calls for tokens in TokenCache |  Major | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-8373](https://issues.apache.org/jira/browse/HADOOP-8373) | Port RPC.getServerAddress to 0.23 |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3659](https://issues.apache.org/jira/browse/MAPREDUCE-3659) | Host-based token support |  Major | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4301](https://issues.apache.org/jira/browse/MAPREDUCE-4301) | Dedupe some strings in MRAM for memory savings |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3842](https://issues.apache.org/jira/browse/MAPREDUCE-3842) | stop webpages from automatic refreshing |  Critical | mrv2, webapps | Alejandro Abdelnur | Thomas Graves |
| [MAPREDUCE-3871](https://issues.apache.org/jira/browse/MAPREDUCE-3871) | Allow symlinking in LocalJobRunner DistributedCache |  Major | distributed-cache | Tom White | Tom White |
| [HADOOP-8535](https://issues.apache.org/jira/browse/HADOOP-8535) | Cut hadoop build times in half (upgrade maven-compiler-plugin to 2.5.1) |  Major | build | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3907](https://issues.apache.org/jira/browse/MAPREDUCE-3907) | Document entries mapred-default.xml for the jobhistory server. |  Minor | documentation | Eugene Koontz | Eugene Koontz |
| [MAPREDUCE-3906](https://issues.apache.org/jira/browse/MAPREDUCE-3906) | Fix inconsistency in documentation regarding mapreduce.jobhistory.principal |  Trivial | documentation | Eugene Koontz | Eugene Koontz |
| [HADOOP-8525](https://issues.apache.org/jira/browse/HADOOP-8525) | Provide Improved Traceability for Configuration |  Trivial | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4283](https://issues.apache.org/jira/browse/MAPREDUCE-4283) | Display tail of aggregated logs by default |  Major | jobhistoryserver, mrv2 | Jason Lowe | Jason Lowe |
| [HADOOP-8635](https://issues.apache.org/jira/browse/HADOOP-8635) | Cannot cancel paths registered deleteOnExit |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4375](https://issues.apache.org/jira/browse/MAPREDUCE-4375) | Show Configuration Tracability in MR UI |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8700](https://issues.apache.org/jira/browse/HADOOP-8700) | Move the checksum type constants to an enum |  Minor | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2421](https://issues.apache.org/jira/browse/HDFS-2421) | Improve the concurrency of  SerialNumberMap in NameNode |  Major | namenode | Hairong Kuang | Jing Zhao |
| [HADOOP-8240](https://issues.apache.org/jira/browse/HADOOP-8240) | Allow users to specify a checksum type on create() |  Major | fs | Kihwal Lee | Kihwal Lee |
| [HADOOP-8239](https://issues.apache.org/jira/browse/HADOOP-8239) | Extend MD5MD5CRC32FileChecksum to show the actual checksum type being used |  Major | fs | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4614](https://issues.apache.org/jira/browse/MAPREDUCE-4614) | Simplify debugging a job's tokens |  Major | client, task | Daryn Sharp | Daryn Sharp |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7621](https://issues.apache.org/jira/browse/HADOOP-7621) | alfredo config should be in a file not readable by users |  Critical | security | Alejandro Abdelnur | Aaron T. Myers |
| [HDFS-2285](https://issues.apache.org/jira/browse/HDFS-2285) | BackupNode should reject requests trying to modify namespace |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-8104](https://issues.apache.org/jira/browse/HADOOP-8104) | Inconsistent Jackson versions |  Major | . | Colin P. McCabe | Alejandro Abdelnur |
| [MAPREDUCE-3728](https://issues.apache.org/jira/browse/MAPREDUCE-3728) | ShuffleHandler can't access results when configured in a secure mode |  Critical | mrv2, nodemanager | Roman Shaposhnik | Ding Yuan |
| [HDFS-3037](https://issues.apache.org/jira/browse/HDFS-3037) | TestMulitipleNNDataBlockScanner#testBlockScannerAfterRestart is racy |  Minor | test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-3348](https://issues.apache.org/jira/browse/MAPREDUCE-3348) | mapred job -status fails to give info even if the job is present in History |  Major | mrv2 | Devaraj K | Devaraj K |
| [HADOOP-8167](https://issues.apache.org/jira/browse/HADOOP-8167) | Configuration deprecation logic breaks backwards compatibility |  Blocker | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3067](https://issues.apache.org/jira/browse/HDFS-3067) | NPE in DFSInputStream.readBuffer if read is repeated on corrupted block |  Major | hdfs-client | Henry Robinson | Henry Robinson |
| [MAPREDUCE-4010](https://issues.apache.org/jira/browse/MAPREDUCE-4010) | TestWritableJobConf fails on trunk |  Critical | mrv2 | Jason Lowe | Alejandro Abdelnur |
| [HADOOP-8197](https://issues.apache.org/jira/browse/HADOOP-8197) | Configuration logs WARNs on every use of a deprecated key |  Critical | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3992](https://issues.apache.org/jira/browse/MAPREDUCE-3992) | Reduce fetcher doesn't verify HTTP status code of response |  Major | mrv1 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4082](https://issues.apache.org/jira/browse/MAPREDUCE-4082) | hadoop-mapreduce-client-app's mrapp-generated-classpath file should not be in the module JAR |  Critical | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4091](https://issues.apache.org/jira/browse/MAPREDUCE-4091) | tools testcases failing because of MAPREDUCE-4082 |  Critical | build, test | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4092](https://issues.apache.org/jira/browse/MAPREDUCE-4092) | commitJob Exception does not fail job (regression in 0.23 vs 0.20) |  Blocker | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4089](https://issues.apache.org/jira/browse/MAPREDUCE-4089) | Hung Tasks never time out. |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4024](https://issues.apache.org/jira/browse/MAPREDUCE-4024) | RM webservices can't query on finalStatus |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4060](https://issues.apache.org/jira/browse/MAPREDUCE-4060) | Multiple SLF4J binding warning |  Major | build | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4012](https://issues.apache.org/jira/browse/MAPREDUCE-4012) | Hadoop Job setup error leaves no useful info to users (when LinuxTaskController is used) |  Minor | . | Koji Noguchi | Thomas Graves |
| [MAPREDUCE-4062](https://issues.apache.org/jira/browse/MAPREDUCE-4062) | AM Launcher thread can hang forever |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3988](https://issues.apache.org/jira/browse/MAPREDUCE-3988) | mapreduce.job.local.dir doesn't point to a single directory on a node. |  Major | mrv2 | Vinod Kumar Vavilapalli | Eric Payne |
| [HDFS-3166](https://issues.apache.org/jira/browse/HDFS-3166) | Hftp connections do not have a timeout |  Critical | hdfs-client | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3999](https://issues.apache.org/jira/browse/MAPREDUCE-3999) | Tracking link gives an error if the AppMaster hasn't started yet |  Major | mrv2, webapps | Ravi Prakash | Ravi Prakash |
| [HDFS-3176](https://issues.apache.org/jira/browse/HDFS-3176) | JsonUtil should not parse the MD5MD5CRC32FileChecksum bytes on its own. |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4020](https://issues.apache.org/jira/browse/MAPREDUCE-4020) | Web services returns incorrect JSON for deep queue tree |  Major | mrv2, webapps | Jason Lowe | Anupam Seth |
| [MAPREDUCE-3672](https://issues.apache.org/jira/browse/MAPREDUCE-3672) | Killed maps shouldn't be counted towards JobCounter.NUM\_FAILED\_MAPS |  Major | mr-am, mrv2 | Vinod Kumar Vavilapalli | Anupam Seth |
| [MAPREDUCE-3682](https://issues.apache.org/jira/browse/MAPREDUCE-3682) | Tracker URL says AM tasks run on localhost |  Major | mrv2 | David Capwell | Ravi Prakash |
| [MAPREDUCE-3082](https://issues.apache.org/jira/browse/MAPREDUCE-3082) | archive command take wrong path for input file with current directory |  Major | harchive | Rajit Saha | John George |
| [MAPREDUCE-3650](https://issues.apache.org/jira/browse/MAPREDUCE-3650) | testGetTokensForHftpFS() fails |  Blocker | mrv2 | Thomas Graves | Ravi Prakash |
| [HADOOP-8088](https://issues.apache.org/jira/browse/HADOOP-8088) | User-group mapping cache incorrectly does negative caching on transient failures |  Major | security | Kihwal Lee | Kihwal Lee |
| [HADOOP-8179](https://issues.apache.org/jira/browse/HADOOP-8179) | risk of NPE in CopyCommands processArguments() |  Minor | fs | Steve Loughran | Daryn Sharp |
| [MAPREDUCE-4097](https://issues.apache.org/jira/browse/MAPREDUCE-4097) | tools testcases fail because missing mrapp-generated-classpath file in classpath |  Major | build | Alejandro Abdelnur | Roman Shaposhnik |
| [HADOOP-8180](https://issues.apache.org/jira/browse/HADOOP-8180) | Remove hsqldb since its not needed from pom.xml |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-6963](https://issues.apache.org/jira/browse/HADOOP-6963) | Fix FileUtil.getDU. It should not include the size of the directory or follow symbolic links |  Critical | fs | Owen O'Malley | Ravi Prakash |
| [MAPREDUCE-3621](https://issues.apache.org/jira/browse/MAPREDUCE-3621) | TestDBJob and TestDataDrivenDBInputFormat ant tests fail |  Major | mrv2 | Thomas Graves | Ravi Prakash |
| [MAPREDUCE-4073](https://issues.apache.org/jira/browse/MAPREDUCE-4073) | CS assigns multiple off-switch containers when using multi-level-queues |  Critical | mrv2, scheduler | Siddharth Seth | Siddharth Seth |
| [HDFS-3136](https://issues.apache.org/jira/browse/HDFS-3136) | Multiple SLF4J binding warning |  Major | build | Jason Lowe | Jason Lowe |
| [HADOOP-8014](https://issues.apache.org/jira/browse/HADOOP-8014) | ViewFileSystem does not correctly implement getDefaultBlockSize, getDefaultReplication, getContentSummary |  Major | fs | Daryn Sharp | John George |
| [MAPREDUCE-4117](https://issues.apache.org/jira/browse/MAPREDUCE-4117) | mapred job -status throws NullPointerException |  Critical | client, mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-4040](https://issues.apache.org/jira/browse/MAPREDUCE-4040) | History links should use hostname rather than IP address. |  Minor | jobhistoryserver, mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4099](https://issues.apache.org/jira/browse/MAPREDUCE-4099) | ApplicationMaster may fail to remove staging directory |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3932](https://issues.apache.org/jira/browse/MAPREDUCE-3932) | MR tasks failing and crashing the AM when available-resources/headRoom becomes zero |  Critical | mr-am, mrv2 | Vinod Kumar Vavilapalli | Robert Joseph Evans |
| [MAPREDUCE-4140](https://issues.apache.org/jira/browse/MAPREDUCE-4140) | mapreduce classes incorrectly importing "clover.org.apache.\*" classes |  Major | client, mrv2 | Patrick Hunt | Patrick Hunt |
| [HADOOP-8144](https://issues.apache.org/jira/browse/HADOOP-8144) | pseudoSortByDistance in NetworkTopology doesn't work properly if no local node and first node is local rack node |  Minor | io | Junping Du | Junping Du |
| [MAPREDUCE-4050](https://issues.apache.org/jira/browse/MAPREDUCE-4050) | Invalid node link |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4128](https://issues.apache.org/jira/browse/MAPREDUCE-4128) | AM Recovery expects all attempts of a completed task to also be completed. |  Major | mrv2 | Bikas Saha | Bikas Saha |
| [HADOOP-8005](https://issues.apache.org/jira/browse/HADOOP-8005) | Multiple SLF4J binding message in .out file for all daemons |  Major | scripts | Joe Crobak | Jason Lowe |
| [MAPREDUCE-4144](https://issues.apache.org/jira/browse/MAPREDUCE-4144) | ResourceManager NPE while handling NODE\_UPDATE |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4156](https://issues.apache.org/jira/browse/MAPREDUCE-4156) | ant build fails compiling JobInProgress |  Major | build | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4160](https://issues.apache.org/jira/browse/MAPREDUCE-4160) | some mrv1 ant tests fail with timeout - due to 4156 |  Major | test | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4139](https://issues.apache.org/jira/browse/MAPREDUCE-4139) | Potential ResourceManager deadlock when SchedulerEventDispatcher is stopped |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4074](https://issues.apache.org/jira/browse/MAPREDUCE-4074) | Client continuously retries to RM When RM goes down before launching Application Master |  Major | . | Devaraj K | xieguiming |
| [HADOOP-8288](https://issues.apache.org/jira/browse/HADOOP-8288) | Remove references of mapred.child.ulimit etc. since they are not being used any more |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4159](https://issues.apache.org/jira/browse/MAPREDUCE-4159) | Job is running in Uber mode after setting "mapreduce.job.ubertask.maxreduces" to zero |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-4165](https://issues.apache.org/jira/browse/MAPREDUCE-4165) | Committing is misspelled as commiting in task logs |  Trivial | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4129](https://issues.apache.org/jira/browse/MAPREDUCE-4129) | Lots of unneeded counters log messages |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-3947](https://issues.apache.org/jira/browse/MAPREDUCE-3947) | yarn.app.mapreduce.am.resource.mb not documented |  Minor | . | Todd Lipcon | Devaraj K |
| [HDFS-3308](https://issues.apache.org/jira/browse/HDFS-3308) | hftp/webhdfs can't get tokens if authority has no port |  Critical | webhdfs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4133](https://issues.apache.org/jira/browse/MAPREDUCE-4133) | MR over viewfs is broken |  Major | . | John George | John George |
| [HDFS-3312](https://issues.apache.org/jira/browse/HDFS-3312) | Hftp selects wrong token service |  Blocker | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3321](https://issues.apache.org/jira/browse/HDFS-3321) | Error message for insufficient data nodes to come out of safemode is wrong. |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4194](https://issues.apache.org/jira/browse/MAPREDUCE-4194) | ConcurrentModificationError in DirectoryCollection |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4169](https://issues.apache.org/jira/browse/MAPREDUCE-4169) | Container Logs appear in unsorted order |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4189](https://issues.apache.org/jira/browse/MAPREDUCE-4189) | TestContainerManagerSecurity is failing |  Critical | mrv2 | Devaraj K | Devaraj K |
| [HADOOP-8305](https://issues.apache.org/jira/browse/HADOOP-8305) | distcp over viewfs is broken |  Major | viewfs | John George | John George |
| [HDFS-3334](https://issues.apache.org/jira/browse/HDFS-3334) | ByteRangeInputStream leaks streams |  Major | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3331](https://issues.apache.org/jira/browse/HDFS-3331) | setBalancerBandwidth do not checkSuperuserPrivilege |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-4209](https://issues.apache.org/jira/browse/MAPREDUCE-4209) | junit dependency in hadoop-mapreduce-client is missing scope test |  Major | build | Radim Kolar |  |
| [MAPREDUCE-4206](https://issues.apache.org/jira/browse/MAPREDUCE-4206) | Sorting by Last Health-Update on the RM nodes page sorts does not work correctly |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-8334](https://issues.apache.org/jira/browse/HADOOP-8334) | HttpServer sometimes returns incorrect port |  Major | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8325](https://issues.apache.org/jira/browse/HADOOP-8325) | Add a ShutdownHookManager to be used by different components instead of the JVM shutdownhook |  Critical | fs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8330](https://issues.apache.org/jira/browse/HADOOP-8330) | TestSequenceFile.testCreateUsesFsArg() is broken |  Minor | test | John George | John George |
| [MAPREDUCE-4211](https://issues.apache.org/jira/browse/MAPREDUCE-4211) | Error conditions (missing appid, appid not found) are masked in the RM app page |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-8317](https://issues.apache.org/jira/browse/HADOOP-8317) | Update maven-assembly-plugin to 2.3 - fix build on FreeBSD |  Major | build | Radim Kolar |  |
| [HADOOP-8172](https://issues.apache.org/jira/browse/HADOOP-8172) | Configuration no longer sets all keys in a deprecated key list. |  Critical | conf | Robert Joseph Evans | Anupam Seth |
| [HADOOP-8342](https://issues.apache.org/jira/browse/HADOOP-8342) | HDFS command fails with exception following merge of HADOOP-8325 |  Major | fs | Randy Clayton | Alejandro Abdelnur |
| [HDFS-3359](https://issues.apache.org/jira/browse/HDFS-3359) | DFSClient.close should close cached sockets |  Critical | hdfs-client | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4048](https://issues.apache.org/jira/browse/MAPREDUCE-4048) | NullPointerException exception while accessing the Application Master UI |  Major | mrv2 | Devaraj K | Devaraj K |
| [HADOOP-8327](https://issues.apache.org/jira/browse/HADOOP-8327) | distcpv2 and distcpv1 jars should not coexist |  Major | . | Dave Thompson | Dave Thompson |
| [HADOOP-8328](https://issues.apache.org/jira/browse/HADOOP-8328) | Duplicate FileSystem Statistics object for 'file' scheme |  Major | fs | Tom White | Tom White |
| [MAPREDUCE-4220](https://issues.apache.org/jira/browse/MAPREDUCE-4220) | RM apps page starttime/endtime sorts are incorrect |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4226](https://issues.apache.org/jira/browse/MAPREDUCE-4226) | ConcurrentModificationException in FileSystemCounterGroup |  Major | mrv2 | Tom White | Tom White |
| [HADOOP-8341](https://issues.apache.org/jira/browse/HADOOP-8341) | Fix or filter findbugs issues in hadoop-tools |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4215](https://issues.apache.org/jira/browse/MAPREDUCE-4215) | RM app page shows 500 error on appid parse error |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4237](https://issues.apache.org/jira/browse/MAPREDUCE-4237) | TestNodeStatusUpdater can fail if localhost has a domain associated with it |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4233](https://issues.apache.org/jira/browse/MAPREDUCE-4233) | NPE can happen in RMNMNodeInfo. |  Critical | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4238](https://issues.apache.org/jira/browse/MAPREDUCE-4238) | mavenize data\_join |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [HADOOP-8393](https://issues.apache.org/jira/browse/HADOOP-8393) | hadoop-config.sh missing variable exports, causes Yarn jobs to fail with ClassNotFoundException MRAppMaster |  Major | scripts | Patrick Hunt | Patrick Hunt |
| [MAPREDUCE-4250](https://issues.apache.org/jira/browse/MAPREDUCE-4250) | hadoop-config.sh missing variable exports, causes Yarn jobs to fail with ClassNotFoundException MRAppMaster |  Major | nodemanager | Patrick Hunt | Patrick Hunt |
| [MAPREDUCE-4102](https://issues.apache.org/jira/browse/MAPREDUCE-4102) | job counters not available in Jobhistory webui for killed jobs |  Major | webapps | Thomas Graves | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4264](https://issues.apache.org/jira/browse/MAPREDUCE-4264) | Got ClassCastException when using mapreduce.history.server.delegationtoken.required=true |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3543](https://issues.apache.org/jira/browse/MAPREDUCE-3543) | Mavenize Gridmix. |  Critical | mrv2 | Mahadev konar | Thomas Graves |
| [MAPREDUCE-4197](https://issues.apache.org/jira/browse/MAPREDUCE-4197) | Include the hsqldb jar in the hadoop-mapreduce tar file |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4269](https://issues.apache.org/jira/browse/MAPREDUCE-4269) | documentation: Gridmix has javadoc warnings in StressJobFactory |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3870](https://issues.apache.org/jira/browse/MAPREDUCE-3870) | Invalid App Metrics |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4152](https://issues.apache.org/jira/browse/MAPREDUCE-4152) | map task left hanging after AM dies trying to connect to RM |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4297](https://issues.apache.org/jira/browse/MAPREDUCE-4297) | Usersmap file in gridmix should not fail on empty lines |  Major | contrib/gridmix | Ravi Prakash | Ravi Prakash |
| [HDFS-3486](https://issues.apache.org/jira/browse/HDFS-3486) | offlineimageviewer can't read fsimage files that contain persistent delegation tokens |  Minor | security, tools | Colin P. McCabe | Colin P. McCabe |
| [MAPREDUCE-4302](https://issues.apache.org/jira/browse/MAPREDUCE-4302) | NM goes down if error encountered during log aggregation |  Critical | nodemanager | Daryn Sharp | Daryn Sharp |
| [HDFS-3442](https://issues.apache.org/jira/browse/HDFS-3442) | Incorrect count for Missing Replicas in FSCK report |  Minor | . | suja s | Andrew Wang |
| [MAPREDUCE-4307](https://issues.apache.org/jira/browse/MAPREDUCE-4307) | TeraInputFormat calls FileSystem.getDefaultBlockSize() without a Path - Failure when using ViewFileSystem |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [HADOOP-8450](https://issues.apache.org/jira/browse/HADOOP-8450) | Remove src/test/system |  Trivial | test | Colin P. McCabe | Eli Collins |
| [MAPREDUCE-3350](https://issues.apache.org/jira/browse/MAPREDUCE-3350) | Per-app RM page should have the list of application-attempts like on the app JHS page |  Critical | mrv2, webapps | Vinod Kumar Vavilapalli | Jonathan Eagles |
| [MAPREDUCE-3927](https://issues.apache.org/jira/browse/MAPREDUCE-3927) | Shuffle hang when set map.failures.percent |  Critical | mrv2 | MengWang | Bhallamudi Venkata Siva Kamesh |
| [HADOOP-8501](https://issues.apache.org/jira/browse/HADOOP-8501) | Gridmix fails to compile on OpenJDK7u4 |  Major | benchmarks | Radim Kolar | Radim Kolar |
| [HADOOP-8495](https://issues.apache.org/jira/browse/HADOOP-8495) | Update Netty to avoid leaking file descriptors during shuffle |  Critical | build | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4341](https://issues.apache.org/jira/browse/MAPREDUCE-4341) | add types to capacity scheduler properties documentation |  Major | capacity-sched, mrv2 | Thomas Graves | Karthik Kambatla |
| [MAPREDUCE-4267](https://issues.apache.org/jira/browse/MAPREDUCE-4267) | mavenize pipes |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4270](https://issues.apache.org/jira/browse/MAPREDUCE-4270) | data\_join test classes are in the wrong packge |  Major | mrv2 | Brock Noland | Thomas Graves |
| [MAPREDUCE-3889](https://issues.apache.org/jira/browse/MAPREDUCE-3889) | job client tries to use /tasklog interface, but that doesn't exist anymore |  Critical | mrv2 | Thomas Graves | Devaraj K |
| [MAPREDUCE-4320](https://issues.apache.org/jira/browse/MAPREDUCE-4320) | gridmix mainClass wrong in pom.xml |  Major | contrib/gridmix | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4295](https://issues.apache.org/jira/browse/MAPREDUCE-4295) | RM crashes due to DNS issue |  Critical | mrv2, resourcemanager | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4031](https://issues.apache.org/jira/browse/MAPREDUCE-4031) | Node Manager hangs on shut down |  Critical | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4361](https://issues.apache.org/jira/browse/MAPREDUCE-4361) | Fix detailed metrics for protobuf-based RPC on 0.23 |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4290](https://issues.apache.org/jira/browse/MAPREDUCE-4290) | JobStatus.getState() API is giving ambiguous values |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-2289](https://issues.apache.org/jira/browse/MAPREDUCE-2289) | Permissions race can make getStagingDir fail on local filesystem |  Major | job submission | Todd Lipcon | Ahmed Radwan |
| [HADOOP-8129](https://issues.apache.org/jira/browse/HADOOP-8129) | ViewFileSystemTestSetup setupForViewFileSystem is erring when the user's home directory is somewhere other than /home (eg. /User) etc. |  Major | fs, test | Ravi Prakash | Ahmed Radwan |
| [MAPREDUCE-4228](https://issues.apache.org/jira/browse/MAPREDUCE-4228) | mapreduce.job.reduce.slowstart.completedmaps is not working properly to delay the scheduling of the reduce tasks |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4372](https://issues.apache.org/jira/browse/MAPREDUCE-4372) | Deadlock in Resource Manager between SchedulerEventDispatcher.EventProcessor and Shutdown hook manager |  Major | mrv2, resourcemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4376](https://issues.apache.org/jira/browse/MAPREDUCE-4376) | TestClusterMRNotification times out |  Major | mrv2, test | Jason Lowe | Kihwal Lee |
| [HADOOP-8110](https://issues.apache.org/jira/browse/HADOOP-8110) | TestViewFsTrash occasionally fails |  Major | fs | Tsz Wo Nicholas Sze | Jason Lowe |
| [HDFS-3581](https://issues.apache.org/jira/browse/HDFS-3581) | FSPermissionChecker#checkPermission sticky bit check missing range check |  Major | namenode | Eli Collins | Eli Collins |
| [MAPREDUCE-4392](https://issues.apache.org/jira/browse/MAPREDUCE-4392) | Counters.makeCompactString() changed behavior from 0.20 |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4384](https://issues.apache.org/jira/browse/MAPREDUCE-4384) | Race conditions in IndexCache |  Major | nodemanager | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4387](https://issues.apache.org/jira/browse/MAPREDUCE-4387) | RM gets fatal error and exits during TestRM |  Major | resourcemanager | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4379](https://issues.apache.org/jira/browse/MAPREDUCE-4379) | Node Manager throws java.lang.OutOfMemoryError: Java heap space due to org.apache.hadoop.fs.LocalDirAllocator.contexts |  Blocker | mrv2, nodemanager | Devaraj K | Devaraj K |
| [HDFS-3591](https://issues.apache.org/jira/browse/HDFS-3591) | Backport HDFS-3357 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3603](https://issues.apache.org/jira/browse/HDFS-3603) | Decouple TestHDFSTrash from TestTrash |  Major | test | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4402](https://issues.apache.org/jira/browse/MAPREDUCE-4402) | TestFileInputFormat fails intermittently |  Major | test | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4300](https://issues.apache.org/jira/browse/MAPREDUCE-4300) | OOM in AM can turn it into a zombie. |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4252](https://issues.apache.org/jira/browse/MAPREDUCE-4252) | MR2 job never completes with 1 pending task |  Major | mrv2 | Tom White | Tom White |
| [HADOOP-8573](https://issues.apache.org/jira/browse/HADOOP-8573) | Configuration tries to read from an inputstream resource multiple times. |  Major | conf | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8587](https://issues.apache.org/jira/browse/HADOOP-8587) | HarFileSystem access of harMetaCache isn't threadsafe |  Minor | fs | Eli Collins | Eli Collins |
| [MAPREDUCE-4419](https://issues.apache.org/jira/browse/MAPREDUCE-4419) | ./mapred queue -info \<queuename\> -showJobs displays all the jobs irrespective of \<queuename\> |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-4299](https://issues.apache.org/jira/browse/MAPREDUCE-4299) | Terasort hangs with MR2 FifoScheduler |  Major | mrv2 | Tom White | Tom White |
| [HADOOP-8543](https://issues.apache.org/jira/browse/HADOOP-8543) | Invalid pom.xml files on 0.23 branch |  Major | build | Radim Kolar | Radim Kolar |
| [MAPREDUCE-4395](https://issues.apache.org/jira/browse/MAPREDUCE-4395) | Possible NPE at ClientDistributedCacheManager#determineTimestamps |  Critical | distributed-cache, job submission, mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [HDFS-3622](https://issues.apache.org/jira/browse/HDFS-3622) | Backport HDFS-3541 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4437](https://issues.apache.org/jira/browse/MAPREDUCE-4437) | Race in MR ApplicationMaster can cause reducers to never be scheduled |  Critical | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [HADOOP-8599](https://issues.apache.org/jira/browse/HADOOP-8599) | Non empty response from FileSystem.getFileBlockLocations when asking for data beyond the end of file |  Major | fs | Andrey Klochkov | Andrey Klochkov |
| [MAPREDUCE-4449](https://issues.apache.org/jira/browse/MAPREDUCE-4449) | Incorrect MR\_HISTORY\_STORAGE property name in JHAdminConfig |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4448](https://issues.apache.org/jira/browse/MAPREDUCE-4448) | Nodemanager crashes upon application cleanup if aggregation failed to start |  Critical | mrv2, nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-3646](https://issues.apache.org/jira/browse/HDFS-3646) | LeaseRenewer can hold reference to inactive DFSClient instances forever |  Critical | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3577](https://issues.apache.org/jira/browse/HDFS-3577) | WebHdfsFileSystem can not read files larger than 24KB |  Blocker | webhdfs | Alejandro Abdelnur | Tsz Wo Nicholas Sze |
| [HDFS-3597](https://issues.apache.org/jira/browse/HDFS-3597) | SNN can fail to start on upgrade |  Minor | namenode | Andy Isaacson | Andy Isaacson |
| [HDFS-3688](https://issues.apache.org/jira/browse/HDFS-3688) | Namenode loses datanode hostname if datanode re-registers |  Major | datanode | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3893](https://issues.apache.org/jira/browse/MAPREDUCE-3893) | allow capacity scheduler configs maximum-applications and maximum-am-resource-percent configurable on a per queue basis |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [HADOOP-8606](https://issues.apache.org/jira/browse/HADOOP-8606) | FileSystem.get may return the wrong filesystem |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4467](https://issues.apache.org/jira/browse/MAPREDUCE-4467) | IndexCache failures due to missing synchronization |  Critical | nodemanager | Andrey Klochkov | Kihwal Lee |
| [MAPREDUCE-4423](https://issues.apache.org/jira/browse/MAPREDUCE-4423) | Potential infinite fetching of map output |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3696](https://issues.apache.org/jira/browse/HDFS-3696) | Create files with WebHdfsFileSystem goes OOM when file size is big |  Critical | webhdfs | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HADOOP-8613](https://issues.apache.org/jira/browse/HADOOP-8613) | AbstractDelegationTokenIdentifier#getUser() should set token auth type |  Critical | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8627](https://issues.apache.org/jira/browse/HADOOP-8627) | FS deleteOnExit may delete the wrong path |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8634](https://issues.apache.org/jira/browse/HADOOP-8634) | Ensure FileSystem#close doesn't squawk for deleteOnExit paths |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8550](https://issues.apache.org/jira/browse/HADOOP-8550) | hadoop fs -touchz automatically created parent directories |  Major | fs | Robert Joseph Evans | John George |
| [MAPREDUCE-4456](https://issues.apache.org/jira/browse/MAPREDUCE-4456) | LocalDistributedCacheManager can get an ArrayIndexOutOfBounds when creating symlinks |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4496](https://issues.apache.org/jira/browse/MAPREDUCE-4496) | AM logs link is missing user name |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [HADOOP-8637](https://issues.apache.org/jira/browse/HADOOP-8637) | FilterFileSystem#setWriteChecksum is broken |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4493](https://issues.apache.org/jira/browse/MAPREDUCE-4493) | Distibuted Cache Compatability Issues |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4492](https://issues.apache.org/jira/browse/MAPREDUCE-4492) | Configuring total queue capacity between 100.5 and 99.5 at perticular level is sucessfull |  Minor | mrv2 | Nishan Shetty | Mayank Bansal |
| [HADOOP-8370](https://issues.apache.org/jira/browse/HADOOP-8370) | Native build failure: javah: class file for org.apache.hadoop.classification.InterfaceAudience not found |  Major | native | Trevor Robinson | Trevor Robinson |
| [MAPREDUCE-4457](https://issues.apache.org/jira/browse/MAPREDUCE-4457) | mr job invalid transition TA\_TOO\_MANY\_FETCH\_FAILURE at FAILED |  Critical | mrv2 | Thomas Graves | Robert Joseph Evans |
| [MAPREDUCE-4444](https://issues.apache.org/jira/browse/MAPREDUCE-4444) | nodemanager fails to start when one of the local-dirs is bad |  Blocker | nodemanager | Nathan Roberts | Jason Lowe |
| [HADOOP-8633](https://issues.apache.org/jira/browse/HADOOP-8633) | Interrupted FsShell copies may leave tmp files |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4504](https://issues.apache.org/jira/browse/MAPREDUCE-4504) | SortValidator writes to wrong directory |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3782](https://issues.apache.org/jira/browse/MAPREDUCE-3782) | teragen terasort jobs fail when using webhdfs:// |  Critical | mrv2 | Arpit Gupta | Jason Lowe |
| [YARN-14](https://issues.apache.org/jira/browse/YARN-14) | Symlinks to peer distributed cache files no longer work |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4053](https://issues.apache.org/jira/browse/MAPREDUCE-4053) | Counters group names deprecation is wrong, iterating over group names deprecated names don't show up |  Major | mrv2 | Alejandro Abdelnur | Robert Joseph Evans |
| [HDFS-3718](https://issues.apache.org/jira/browse/HDFS-3718) | Datanode won't shutdown because of runaway DataBlockScanner thread |  Critical | datanode | Kihwal Lee | Kihwal Lee |
| [HDFS-3794](https://issues.apache.org/jira/browse/HDFS-3794) | WebHDFS Open used with Offset returns the original (and incorrect) Content Length in the HTTP Header. |  Major | webhdfs | Ravi Prakash | Ravi Prakash |
| [HADOOP-8703](https://issues.apache.org/jira/browse/HADOOP-8703) | distcpV2: turn CRC checking off for 0 byte size |  Major | . | Dave Thompson | Dave Thompson |
| [MAPREDUCE-4562](https://issues.apache.org/jira/browse/MAPREDUCE-4562) | Support for "FileSystemCounter" legacy counter group name for compatibility reasons is creating incorrect counter name |  Major | . | Jarek Jarcec Cecho | Jarek Jarcec Cecho |
| [HADOOP-8390](https://issues.apache.org/jira/browse/HADOOP-8390) | TestFileSystemCanonicalization fails with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HDFS-3788](https://issues.apache.org/jira/browse/HDFS-3788) | distcp can't copy large files using webhdfs due to missing Content-Length header |  Critical | webhdfs | Eli Collins | Tsz Wo Nicholas Sze |
| [HADOOP-8692](https://issues.apache.org/jira/browse/HADOOP-8692) | TestLocalDirAllocator fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8693](https://issues.apache.org/jira/browse/HADOOP-8693) | TestSecurityUtil fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8697](https://issues.apache.org/jira/browse/HADOOP-8697) | TestWritableName fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [YARN-25](https://issues.apache.org/jira/browse/YARN-25) | remove old aggregated logs |  Major | . | Thomas Graves | Robert Joseph Evans |
| [HADOOP-8695](https://issues.apache.org/jira/browse/HADOOP-8695) | TestPathData fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-7967](https://issues.apache.org/jira/browse/HADOOP-7967) | Need generalized multi-token filesystem support |  Critical | fs, security | Daryn Sharp | Daryn Sharp |
| [YARN-27](https://issues.apache.org/jira/browse/YARN-27) | Failed refreshQueues due to misconfiguration prevents further refreshing of queues |  Major | . | Ramya Sunil | Arun C Murthy |
| [YARN-58](https://issues.apache.org/jira/browse/YARN-58) | NM leaks filesystems |  Critical | nodemanager | Daryn Sharp | Jason Lowe |
| [HADOOP-8611](https://issues.apache.org/jira/browse/HADOOP-8611) | Allow fall-back to the shell-based implementation when JNI-based users-group mapping fails |  Major | security | Kihwal Lee | Robert Parker |
| [MAPREDUCE-3506](https://issues.apache.org/jira/browse/MAPREDUCE-3506) | Calling getPriority on JobInfo after parsing a history log with JobHistoryParser throws a NullPointerException |  Minor | client, mrv2 | Ratandeep Ratti | Jason Lowe |
| [MAPREDUCE-4570](https://issues.apache.org/jira/browse/MAPREDUCE-4570) | ProcfsBasedProcessTree#constructProcessInfo() prints a warning if procfsDir/\<pid\>/stat is not found. |  Minor | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4068](https://issues.apache.org/jira/browse/MAPREDUCE-4068) | Jars in lib subdirectory of the submittable JAR are not added to the classpath |  Blocker | mrv2 | Ahmed Radwan | Robert Kanter |
| [HDFS-3841](https://issues.apache.org/jira/browse/HDFS-3841) | Port HDFS-3835 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8225](https://issues.apache.org/jira/browse/HADOOP-8225) | DistCp fails when invoked by Oozie |  Blocker | security | Mithun Radhakrishnan | Daryn Sharp |
| [MAPREDUCE-2374](https://issues.apache.org/jira/browse/MAPREDUCE-2374) | "Text File Busy" errors launching MR tasks |  Major | . | Todd Lipcon | Andy Isaacson |
| [HADOOP-8709](https://issues.apache.org/jira/browse/HADOOP-8709) | globStatus changed behavior from 0.20/1.x |  Critical | fs | Jason Lowe | Jason Lowe |
| [HADOOP-8725](https://issues.apache.org/jira/browse/HADOOP-8725) | MR is broken when security is off |  Blocker | security | Daryn Sharp | Daryn Sharp |
| [HDFS-3177](https://issues.apache.org/jira/browse/HDFS-3177) | Allow DFSClient to find out and use the CRC type being used for a file. |  Major | datanode, hdfs-client | Kihwal Lee | Kihwal Lee |
| [HADOOP-8060](https://issues.apache.org/jira/browse/HADOOP-8060) | Add a capability to discover and set checksum types per file. |  Major | fs, util | Kihwal Lee | Kihwal Lee |
| [YARN-31](https://issues.apache.org/jira/browse/YARN-31) | TestDelegationTokenRenewer fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4600](https://issues.apache.org/jira/browse/MAPREDUCE-4600) | TestTokenCache.java from MRV1 no longer compiles |  Critical | . | Robert Joseph Evans | Daryn Sharp |
| [HADOOP-8614](https://issues.apache.org/jira/browse/HADOOP-8614) | IOUtils#skipFully hangs forever on EOF |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-3861](https://issues.apache.org/jira/browse/HDFS-3861) | Deadlock in DFSClient |  Blocker | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HADOOP-8726](https://issues.apache.org/jira/browse/HADOOP-8726) | The Secrets in Credentials are not available to MR tasks |  Major | security | Benoy Antony | Daryn Sharp |
| [MAPREDUCE-4569](https://issues.apache.org/jira/browse/MAPREDUCE-4569) | TestHsWebServicesJobsQuery fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [YARN-63](https://issues.apache.org/jira/browse/YARN-63) | RMNodeImpl is missing valid transitions from the UNHEALTHY state |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HADOOP-8727](https://issues.apache.org/jira/browse/HADOOP-8727) | Gracefully deprecate dfs.umaskmode in 2.x onwards |  Major | conf | Harsh J | Harsh J |
| [YARN-66](https://issues.apache.org/jira/browse/YARN-66) | aggregated logs permissions not set properly |  Critical | nodemanager | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4612](https://issues.apache.org/jira/browse/MAPREDUCE-4612) | job summary file permissions not set when its created |  Critical | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4611](https://issues.apache.org/jira/browse/MAPREDUCE-4611) | MR AM dies badly when Node is decomissioned |  Critical | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4604](https://issues.apache.org/jira/browse/MAPREDUCE-4604) | In mapred-default, mapreduce.map.maxattempts & mapreduce.reduce.maxattempts defaults are set to 4 as well as mapreduce.job.maxtaskfailures.per.tracker. |  Critical | mrv2 | Ravi Prakash | Ravi Prakash |
| [HDFS-3873](https://issues.apache.org/jira/browse/HDFS-3873) | Hftp assumes security is disabled if token fetch fails |  Major | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3852](https://issues.apache.org/jira/browse/HDFS-3852) | TestHftpDelegationToken is broken after HADOOP-8225 |  Major | hdfs-client, security | Aaron T. Myers | Daryn Sharp |
| [YARN-68](https://issues.apache.org/jira/browse/YARN-68) | NodeManager will refuse to shutdown indefinitely due to container log aggregation |  Major | nodemanager | patrick white | Daryn Sharp |
| [YARN-87](https://issues.apache.org/jira/browse/YARN-87) | NM ResourceLocalizationService does not set permissions of local cache directories |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-3890](https://issues.apache.org/jira/browse/HDFS-3890) | filecontext mkdirs doesn't apply umask as expected |  Critical | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4633](https://issues.apache.org/jira/browse/MAPREDUCE-4633) | history server doesn't set permissions on all subdirs |  Critical | jobhistoryserver | Thomas Graves | oss.wakayama |
| [MAPREDUCE-4641](https://issues.apache.org/jira/browse/MAPREDUCE-4641) | Exception in commitJob marks job as successful in job history |  Major | mrv2 | Jason Lowe | Jason Lowe |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2505](https://issues.apache.org/jira/browse/HDFS-2505) | Add a test to verify getFileChecksum works with ViewFS |  Minor | test | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3983](https://issues.apache.org/jira/browse/MAPREDUCE-3983) | TestTTResourceReporting can fail, and should just be deleted |  Major | mrv1 | Robert Joseph Evans | Ravi Prakash |
| [HADOOP-8283](https://issues.apache.org/jira/browse/HADOOP-8283) | Allow tests to control token service value |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-3258](https://issues.apache.org/jira/browse/HDFS-3258) | Test for HADOOP-8144 (pseudoSortByDistance in NetworkTopology for first rack local node) |  Major | test | Eli Collins | Junping Du |
| [MAPREDUCE-4212](https://issues.apache.org/jira/browse/MAPREDUCE-4212) | TestJobClientGetJob sometimes fails |  Major | test | Daryn Sharp | Daryn Sharp |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-3168](https://issues.apache.org/jira/browse/HDFS-3168) | Clean up FSNamesystem and BlockManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3942](https://issues.apache.org/jira/browse/MAPREDUCE-3942) | Randomize master key generation for ApplicationTokenSecretManager and roll it every so often |  Major | mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3972](https://issues.apache.org/jira/browse/MAPREDUCE-3972) | Locking and exception issues in JobHistory Server. |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4161](https://issues.apache.org/jira/browse/MAPREDUCE-4161) | create sockets consistently |  Major | client, mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3613](https://issues.apache.org/jira/browse/MAPREDUCE-3613) | web service calls header contains 2 content types |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4163](https://issues.apache.org/jira/browse/MAPREDUCE-4163) | consistently set the bind address |  Major | mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4162](https://issues.apache.org/jira/browse/MAPREDUCE-4162) | Correctly set token service |  Major | client, mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3940](https://issues.apache.org/jira/browse/MAPREDUCE-3940) | ContainerTokens should have an expiry interval |  Major | mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-39](https://issues.apache.org/jira/browse/YARN-39) | RM-NM secret-keys should be randomly generated and rolled every so often |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-60](https://issues.apache.org/jira/browse/YARN-60) | NMs rejects all container tokens after secret key rolls |  Blocker | nodemanager | Daryn Sharp | Vinod Kumar Vavilapalli |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4051](https://issues.apache.org/jira/browse/MAPREDUCE-4051) | Remove the empty hadoop-mapreduce-project/assembly/all.xml file |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4134](https://issues.apache.org/jira/browse/MAPREDUCE-4134) | Remove references of mapred.child.ulimit etc. since they are not being used any more |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [YARN-1](https://issues.apache.org/jira/browse/YARN-1) | Move YARN out of hadoop-mapreduce |  Major | . | Arun C Murthy | Arun C Murthy |


