
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

## Release 3.1.3 - 2019-09-12

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15922](https://issues.apache.org/jira/browse/HADOOP-15922) | DelegationTokenAuthenticationFilter get wrong doAsUser since it does not decode URL |  Major | common, kms | He Xiaoqiao | He Xiaoqiao |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15950](https://issues.apache.org/jira/browse/HADOOP-15950) | Failover for LdapGroupsMapping |  Major | common, security | Lukas Majercak | Lukas Majercak |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15481](https://issues.apache.org/jira/browse/HADOOP-15481) | Emit FairCallQueue stats as metrics |  Major | metrics, rpc-server | Erik Krogen | Christopher Gregorian |
| [HDFS-14213](https://issues.apache.org/jira/browse/HDFS-14213) | Remove Jansson from BUILDING.txt |  Minor | documentation | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14221](https://issues.apache.org/jira/browse/HDFS-14221) | Replace Guava Optional with Java Optional |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-14222](https://issues.apache.org/jira/browse/HDFS-14222) | Make ThrottledAsyncChecker constructor public |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-16089](https://issues.apache.org/jira/browse/HADOOP-16089) | AliyunOSS: update oss-sdk version to 3.4.1 |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14231](https://issues.apache.org/jira/browse/HDFS-14231) | DataXceiver#run() should not log exceptions caused by InvalidToken exception as an error |  Major | hdfs | Kitti Nanasi | Kitti Nanasi |
| [YARN-7171](https://issues.apache.org/jira/browse/YARN-7171) | RM UI should sort memory / cores numerically |  Major | . | Eric Maynard | Ahmed Hussein |
| [YARN-9282](https://issues.apache.org/jira/browse/YARN-9282) | Typo in javadoc of class LinuxContainerExecutor: hadoop.security.authetication should be 'authentication' |  Trivial | . | Szilard Nemeth | Charan Hebri |
| [HADOOP-16108](https://issues.apache.org/jira/browse/HADOOP-16108) | Tail Follow Interval Should Allow To Specify The Sleep Interval To Save Unnecessary RPC's |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [YARN-8295](https://issues.apache.org/jira/browse/YARN-8295) | [UI2] Improve "Resource Usage" tab error message when there are no data available. |  Minor | yarn-ui-v2 | Gergely Novák | Charan Hebri |
| [YARN-7824](https://issues.apache.org/jira/browse/YARN-7824) | [UI2] Yarn Component Instance page should include link to container logs |  Major | yarn-ui-v2 | Yesha Vora | Akhil PB |
| [HADOOP-15281](https://issues.apache.org/jira/browse/HADOOP-15281) | Distcp to add no-rename copy option |  Major | tools/distcp | Steve Loughran | Andrew Olson |
| [YARN-9309](https://issues.apache.org/jira/browse/YARN-9309) | Improve graph text in SLS to avoid overlapping |  Minor | . | Bilwa S T | Bilwa S T |
| [YARN-9168](https://issues.apache.org/jira/browse/YARN-9168) | DistributedShell client timeout should be -1 by default |  Minor | . | Zhankun Tang | Zhankun Tang |
| [YARN-9087](https://issues.apache.org/jira/browse/YARN-9087) | Improve logging for initialization of Resource plugins |  Major | yarn | Szilard Nemeth | Szilard Nemeth |
| [YARN-9121](https://issues.apache.org/jira/browse/YARN-9121) | Replace GpuDiscoverer.getInstance() to a readable object for easy access control |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9139](https://issues.apache.org/jira/browse/YARN-9139) | Simplify initializer code of GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-14247](https://issues.apache.org/jira/browse/HDFS-14247) | Repeat adding node description into network topology |  Minor | datanode | HuangTao | HuangTao |
| [YARN-9138](https://issues.apache.org/jira/browse/YARN-9138) | Improve test coverage for nvidia-smi binary execution of GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [MAPREDUCE-7191](https://issues.apache.org/jira/browse/MAPREDUCE-7191) | JobHistoryServer should log exception when loading/parsing history file failed |  Minor | mrv2 | Jiandan Yang | Jiandan Yang |
| [HDFS-14346](https://issues.apache.org/jira/browse/HDFS-14346) | Better time precision in getTimeDuration |  Minor | namenode | Chao Sun | Chao Sun |
| [HDFS-14366](https://issues.apache.org/jira/browse/HDFS-14366) | Improve HDFS append performance |  Major | hdfs | Chao Sun | Chao Sun |
| [MAPREDUCE-7190](https://issues.apache.org/jira/browse/MAPREDUCE-7190) | Add SleepJob additional parameter to make parallel runs distinguishable |  Major | . | Adam Antal | Adam Antal |
| [HADOOP-16208](https://issues.apache.org/jira/browse/HADOOP-16208) | Do Not Log InterruptedException in Client |  Minor | common | David Mollitor | David Mollitor |
| [YARN-9463](https://issues.apache.org/jira/browse/YARN-9463) | Add queueName info when failing with queue capacity sanity check |  Trivial | capacity scheduler | Aihua Xu | Aihua Xu |
| [HADOOP-16227](https://issues.apache.org/jira/browse/HADOOP-16227) | Upgrade checkstyle to 8.19 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14432](https://issues.apache.org/jira/browse/HDFS-14432) | dfs.datanode.shared.file.descriptor.paths duplicated in hdfs-default.xml |  Minor | hdfs | puleya7 | puleya7 |
| [HDFS-14463](https://issues.apache.org/jira/browse/HDFS-14463) | Add Log Level link under NameNode and DataNode Web UI Utilities dropdown |  Trivial | webhdfs | Siyao Meng | Siyao Meng |
| [YARN-9529](https://issues.apache.org/jira/browse/YARN-9529) | Log correct cpu controller path on error while initializing CGroups. |  Major | nodemanager | Jonathan Hung | Jonathan Hung |
| [HADOOP-16289](https://issues.apache.org/jira/browse/HADOOP-16289) | Allow extra jsvc startup option in hadoop\_start\_secure\_daemon in hadoop-functions.sh |  Major | scripts | Siyao Meng | Siyao Meng |
| [HADOOP-16307](https://issues.apache.org/jira/browse/HADOOP-16307) | Intern User Name and Group Name in FileStatus |  Major | fs | David Mollitor | David Mollitor |
| [HDFS-14507](https://issues.apache.org/jira/browse/HDFS-14507) | Document -blockingDecommission option for hdfs dfsadmin -listOpenFiles |  Minor | documentation | Siyao Meng | Siyao Meng |
| [HDFS-14451](https://issues.apache.org/jira/browse/HDFS-14451) | Incorrect header or version mismatch log message |  Minor | ipc | David Mollitor | Shweta |
| [HDFS-14502](https://issues.apache.org/jira/browse/HDFS-14502) | keepResults option in NNThroughputBenchmark should call saveNamespace() |  Major | benchmarks, hdfs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16323](https://issues.apache.org/jira/browse/HADOOP-16323) | https everywhere in Maven settings |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9563](https://issues.apache.org/jira/browse/YARN-9563) | Resource report REST API could return NaN or Inf |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-9545](https://issues.apache.org/jira/browse/YARN-9545) | Create healthcheck REST endpoint for ATSv2 |  Major | ATSv2 | Zoltan Siegl | Zoltan Siegl |
| [HDFS-10659](https://issues.apache.org/jira/browse/HDFS-10659) | Namenode crashes after Journalnode re-installation in an HA cluster due to missing paxos directory |  Major | ha, journal-node | Amit Anand | star |
| [HDFS-14513](https://issues.apache.org/jira/browse/HDFS-14513) | FSImage which is saving should be clean while NameNode shutdown |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [YARN-9543](https://issues.apache.org/jira/browse/YARN-9543) | [UI2] Handle ATSv2 server down or failures cases gracefully in YARN UI v2 |  Major | ATSv2, yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [HADOOP-16369](https://issues.apache.org/jira/browse/HADOOP-16369) | Fix zstandard shortname misspelled as zts |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-14560](https://issues.apache.org/jira/browse/HDFS-14560) | Allow block replication parameters to be refreshable |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-12770](https://issues.apache.org/jira/browse/HDFS-12770) | Add doc about how to disable client socket cache |  Trivial | hdfs-client | Weiwei Yang | Weiwei Yang |
| [HADOOP-9157](https://issues.apache.org/jira/browse/HADOOP-9157) | Better option for curl in hadoop-auth-examples |  Minor | documentation | Jingguo Yao | Andras Bokor |
| [HDFS-14340](https://issues.apache.org/jira/browse/HDFS-14340) | Lower the log level when can't get postOpAttr |  Minor | nfs | Anuhan Torgonshar | Anuhan Torgonshar |
| [HADOOP-15914](https://issues.apache.org/jira/browse/HADOOP-15914) | hadoop jar command has no help argument |  Major | common | Adam Antal | Adam Antal |
| [HADOOP-16156](https://issues.apache.org/jira/browse/HADOOP-16156) | [Clean-up] Remove NULL check before instanceof and fix checkstyle in InnerNodeImpl |  Minor | . | Shweta | Shweta |
| [HADOOP-14385](https://issues.apache.org/jira/browse/HADOOP-14385) | HttpExceptionUtils#validateResponse swallows exceptions |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-12564](https://issues.apache.org/jira/browse/HDFS-12564) | Add the documents of swebhdfs configurations on the client side |  Major | documentation, webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14403](https://issues.apache.org/jira/browse/HDFS-14403) | Cost-Based RPC FairCallQueue |  Major | ipc, namenode | Erik Krogen | Christopher Gregorian |
| [HADOOP-16266](https://issues.apache.org/jira/browse/HADOOP-16266) | Add more fine-grained processing time metrics to the RPC layer |  Minor | ipc | Christopher Gregorian | Erik Krogen |
| [YARN-9629](https://issues.apache.org/jira/browse/YARN-9629) | Support configurable MIN\_LOG\_ROLLING\_INTERVAL |  Minor | log-aggregation, nodemanager, yarn | Adam Antal | Adam Antal |
| [HDFS-13694](https://issues.apache.org/jira/browse/HDFS-13694) | Making md5 computing being in parallel with image loading |  Major | . | zhouyingchao | Lisheng Sun |
| [HDFS-14632](https://issues.apache.org/jira/browse/HDFS-14632) | Reduce useless #getNumLiveDataNodes call in SafeModeMonitor |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [YARN-9573](https://issues.apache.org/jira/browse/YARN-9573) | DistributedShell cannot specify LogAggregationContext |  Major | distributed-shell, log-aggregation, yarn | Adam Antal | Adam Antal |
| [YARN-9337](https://issues.apache.org/jira/browse/YARN-9337) | GPU auto-discovery script runs even when the resource is given by hand |  Major | yarn | Adam Antal | Adam Antal |
| [YARN-9127](https://issues.apache.org/jira/browse/YARN-9127) | Create more tests to verify GpuDeviceInformationParser |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HDFS-14547](https://issues.apache.org/jira/browse/HDFS-14547) | DirectoryWithQuotaFeature.quota costs additional memory even the storage type quota is not set. |  Major | . | Jinglun | Jinglun |
| [HDFS-14697](https://issues.apache.org/jira/browse/HDFS-14697) | Backport HDFS-14513 to branch-2 |  Minor | namenode | He Xiaoqiao | He Xiaoqiao |
| [YARN-8045](https://issues.apache.org/jira/browse/YARN-8045) | Reduce log output from container status calls |  Major | . | Shane Kumpf | Craig Condit |
| [HDFS-14693](https://issues.apache.org/jira/browse/HDFS-14693) | NameNode should log a warning when EditLog IPC logger's pending size exceeds limit. |  Minor | namenode | Xudong Cao | Xudong Cao |
| [YARN-9094](https://issues.apache.org/jira/browse/YARN-9094) | Remove unused interface method: NodeResourceUpdaterPlugin#handleUpdatedResourceFromRM |  Trivial | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9096](https://issues.apache.org/jira/browse/YARN-9096) | Some GpuResourcePlugin and ResourcePluginManager methods are synchronized unnecessarily |  Major | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9092](https://issues.apache.org/jira/browse/YARN-9092) | Create an object for cgroups mount enable and cgroups mount path as they belong together |  Minor | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9124](https://issues.apache.org/jira/browse/YARN-9124) | Resolve contradiction in ResourceUtils: addMandatoryResources / checkMandatoryResources work differently |  Minor | . | Szilard Nemeth | Adam Antal |
| [YARN-8199](https://issues.apache.org/jira/browse/YARN-8199) | Logging fileSize of log files under NM Local Dir |  Major | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [YARN-9729](https://issues.apache.org/jira/browse/YARN-9729) | [UI2] Fix error message for logs when ATSv2 is offline |  Major | yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [YARN-9135](https://issues.apache.org/jira/browse/YARN-9135) | NM State store ResourceMappings serialization are tested with Strings instead of real Device objects |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HDFS-14370](https://issues.apache.org/jira/browse/HDFS-14370) | Edit log tailing fast-path should allow for backoff |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-9442](https://issues.apache.org/jira/browse/YARN-9442) | container working directory has group read permissions |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-16459](https://issues.apache.org/jira/browse/HADOOP-16459) | Backport [HADOOP-16266] "Add more fine-grained processing time metrics to the RPC layer" to branch-2 |  Major | . | Erik Krogen | Erik Krogen |
| [HDFS-14491](https://issues.apache.org/jira/browse/HDFS-14491) | More Clarity on Namenode UI Around Blocks and Replicas |  Minor | . | Alan Jackoway | Siyao Meng |
| [YARN-9140](https://issues.apache.org/jira/browse/YARN-9140) | Code cleanup in ResourcePluginManager.initialize and in TestResourcePluginManager |  Trivial | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9488](https://issues.apache.org/jira/browse/YARN-9488) | Skip YARNFeatureNotEnabledException from ClientRMService |  Minor | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-8586](https://issues.apache.org/jira/browse/YARN-8586) | Extract log aggregation related fields and methods from RMAppImpl |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9100](https://issues.apache.org/jira/browse/YARN-9100) | Add tests for GpuResourceAllocator and do minor code cleanup |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HADOOP-15246](https://issues.apache.org/jira/browse/HADOOP-15246) | SpanReceiverInfo - Prefer ArrayList over LinkedList |  Trivial | common | David Mollitor | David Mollitor |
| [HADOOP-16158](https://issues.apache.org/jira/browse/HADOOP-16158) | DistCp to support checksum validation when copy blocks in parallel |  Major | tools/distcp | Kai Xie | Kai Xie |
| [HDFS-14746](https://issues.apache.org/jira/browse/HDFS-14746) | Trivial test code update after HDFS-14687 |  Trivial | ec | Wei-Chiu Chuang | kevin su |
| [HDFS-13709](https://issues.apache.org/jira/browse/HDFS-13709) | Report bad block to NN when transfer block encounter EIO exception |  Major | datanode | Chen Zhang | Chen Zhang |
| [HDFS-14665](https://issues.apache.org/jira/browse/HDFS-14665) | HttpFS: LISTSTATUS response is missing HDFS-specific fields |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HDFS-14276](https://issues.apache.org/jira/browse/HDFS-14276) | [SBN read] Reduce tailing overhead |  Major | ha, namenode | Wei-Chiu Chuang | Ayush Saxena |
| [HDFS-14748](https://issues.apache.org/jira/browse/HDFS-14748) | Make DataNodePeerMetrics#minOutlierDetectionSamples configurable |  Major | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-15998](https://issues.apache.org/jira/browse/HADOOP-15998) | Ensure jar validation works on Windows. |  Blocker | build | Brian Grunkemeyer | Brian Grunkemeyer |
| [HDFS-14633](https://issues.apache.org/jira/browse/HDFS-14633) | The StorageType quota and consume in QuotaFeature is not handled for rename |  Major | . | Jinglun | Jinglun |
| [YARN-9795](https://issues.apache.org/jira/browse/YARN-9795) | ClusterMetrics to include AM allocation delay |  Minor | . | Fengnan Li | Fengnan Li |
| [YARN-8995](https://issues.apache.org/jira/browse/YARN-8995) | Log events info in AsyncDispatcher when event queue size cumulatively reaches a certain number every time. |  Major | metrics, nodemanager, resourcemanager | zhuqi | zhuqi |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13642](https://issues.apache.org/jira/browse/HDFS-13642) | Creating a file with block size smaller than EC policy's cell size should fail |  Major | erasure-coding | Xiao Chen | Xiao Chen |
| [HADOOP-15948](https://issues.apache.org/jira/browse/HADOOP-15948) | Inconsistency in get and put syntax if filename/dirname contains space |  Minor | fs | vivek kumar | Ayush Saxena |
| [HDFS-13816](https://issues.apache.org/jira/browse/HDFS-13816) | dfs.getQuotaUsage() throws NPE on non-existent dir instead of FileNotFoundException |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-15966](https://issues.apache.org/jira/browse/HADOOP-15966) | Hadoop Kerberos broken on macos as java.security.krb5.realm is reset: Null realm name (601) |  Major | scripts | Steve Loughran | Steve Loughran |
| [HADOOP-16028](https://issues.apache.org/jira/browse/HADOOP-16028) | Fix NetworkTopology chooseRandom function to support excluded nodes |  Major | . | Sihai Ke | Sihai Ke |
| [YARN-9162](https://issues.apache.org/jira/browse/YARN-9162) | Fix TestRMAdminCLI#testHelp |  Major | resourcemanager, test | Ayush Saxena | Ayush Saxena |
| [HADOOP-16031](https://issues.apache.org/jira/browse/HADOOP-16031) | TestSecureLogins#testValidKerberosName fails |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16016](https://issues.apache.org/jira/browse/HADOOP-16016) | TestSSLFactory#testServerWeakCiphers sporadically fails in precommit builds |  Major | security, test | Jason Lowe | Akira Ajisaka |
| [HDFS-14198](https://issues.apache.org/jira/browse/HDFS-14198) | Upload and Create button doesn't get enabled after getting reset. |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9203](https://issues.apache.org/jira/browse/YARN-9203) | Fix typos in yarn-default.xml |  Trivial | documentation | Rahul Padmanabhan | Rahul Padmanabhan |
| [HDFS-14207](https://issues.apache.org/jira/browse/HDFS-14207) | ZKFC should catch exception when ha configuration missing |  Major | hdfs | Fei Hui | Fei Hui |
| [HDFS-14218](https://issues.apache.org/jira/browse/HDFS-14218) | EC: Ls -e throw NPE when directory ec policy is disabled |  Major | . | Surendra Singh Lilhore | Ayush Saxena |
| [YARN-9210](https://issues.apache.org/jira/browse/YARN-9210) | RM nodes web page can not display node info |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-8961](https://issues.apache.org/jira/browse/YARN-8961) | [UI2] Flow Run End Time shows 'Invalid date' |  Major | . | Charan Hebri | Akhil PB |
| [YARN-7088](https://issues.apache.org/jira/browse/YARN-7088) | Add application launch time to Resource Manager REST API |  Major | . | Abdullah Yousufi | Kanwaljeet Sachdev |
| [YARN-9222](https://issues.apache.org/jira/browse/YARN-9222) | Print launchTime in ApplicationSummary |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-8901](https://issues.apache.org/jira/browse/YARN-8901) | Restart "NEVER" policy does not work with component dependency |  Critical | . | Yesha Vora | Suma Shivaprasad |
| [YARN-9237](https://issues.apache.org/jira/browse/YARN-9237) | NM should ignore sending finished apps to RM during RM fail-over |  Major | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-6616](https://issues.apache.org/jira/browse/YARN-6616) | YARN AHS shows submitTime for jobs same as startTime |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9099](https://issues.apache.org/jira/browse/YARN-9099) | GpuResourceAllocator#getReleasingGpus calculates number of GPUs in a wrong way |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16086](https://issues.apache.org/jira/browse/HADOOP-16086) | Backport HADOOP-15549 to branch-3.1 |  Major | metrics | Yuming Wang | Todd Lipcon |
| [YARN-9206](https://issues.apache.org/jira/browse/YARN-9206) | RMServerUtils does not count SHUTDOWN as an accepted state |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-16096](https://issues.apache.org/jira/browse/HADOOP-16096) | HADOOP-15281/distcp -Xdirect needs to use commons-logging on 3.1 |  Critical | . | Eric Payne | Steve Loughran |
| [HDFS-14140](https://issues.apache.org/jira/browse/HDFS-14140) | JournalNodeSyncer authentication is failing in secure cluster |  Major | journal-node, security | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-9257](https://issues.apache.org/jira/browse/YARN-9257) | Distributed Shell client throws a NPE for a non-existent queue |  Major | distributed-shell | Charan Hebri | Charan Hebri |
| [YARN-8761](https://issues.apache.org/jira/browse/YARN-8761) | Service AM support for decommissioning component instances |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-14266](https://issues.apache.org/jira/browse/HDFS-14266) | EC : Fsck -blockId shows null for EC Blocks if One Block Is Not Available. |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14274](https://issues.apache.org/jira/browse/HDFS-14274) | EC: NPE While Listing EC Policy For A Directory Following Replication Policy. |  Major | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-14263](https://issues.apache.org/jira/browse/HDFS-14263) | Remove unnecessary block file exists check from FsDatasetImpl#getBlockInputStream() |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-7761](https://issues.apache.org/jira/browse/YARN-7761) | [UI2] Clicking 'master container log' or 'Link' next to 'log' under application's appAttempt goes to Old UI's Log link |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [YARN-9295](https://issues.apache.org/jira/browse/YARN-9295) | [UI2] Fix label typo in Cluster Overview page |  Trivial | yarn-ui-v2 | Charan Hebri | Charan Hebri |
| [YARN-9284](https://issues.apache.org/jira/browse/YARN-9284) | Fix the unit of yarn.service.am-resource.memory in the document |  Minor | documentation, yarn-native-services | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-9283](https://issues.apache.org/jira/browse/YARN-9283) | Javadoc of LinuxContainerExecutor#addSchedPriorityCommand has a wrong property name as reference |  Minor | documentation | Szilard Nemeth | Adam Antal |
| [YARN-9286](https://issues.apache.org/jira/browse/YARN-9286) | [Timeline Server] Sorting based on FinalStatus shows pop-up message |  Minor | timelineserver | Nallasivan | Bilwa S T |
| [HDFS-14081](https://issues.apache.org/jira/browse/HDFS-14081) | hdfs dfsadmin -metasave metasave\_test results NPE |  Major | hdfs | Shweta | Shweta |
| [HADOOP-15813](https://issues.apache.org/jira/browse/HADOOP-15813) | Enable more reliable SSL connection reuse |  Major | common | Daryn Sharp | Daryn Sharp |
| [HADOOP-16105](https://issues.apache.org/jira/browse/HADOOP-16105) | WASB in secure mode does not set connectingUsingSAS |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-9238](https://issues.apache.org/jira/browse/YARN-9238) | Avoid allocating opportunistic containers to previous/removed/non-exist application attempt |  Critical | . | lujie | lujie |
| [YARN-9118](https://issues.apache.org/jira/browse/YARN-9118) | Handle exceptions with parsing user defined GPU devices in GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9317](https://issues.apache.org/jira/browse/YARN-9317) | Avoid repeated YarnConfiguration#timelineServiceV2Enabled check |  Major | . | Bibin A Chundatt | Prabhu Joseph |
| [YARN-9213](https://issues.apache.org/jira/browse/YARN-9213) | RM Web UI v1 does not show custom resource allocations for containers page |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9248](https://issues.apache.org/jira/browse/YARN-9248) | RMContainerImpl:Invalid event: ACQUIRED at KILLED |  Major | . | lujie | lujie |
| [HADOOP-16018](https://issues.apache.org/jira/browse/HADOOP-16018) | DistCp won't reassemble chunks when blocks per chunk \> 0 |  Major | tools/distcp | Kai Xie | Kai Xie |
| [YARN-9334](https://issues.apache.org/jira/browse/YARN-9334) | YARN Service Client does not work with SPNEGO when knox is configured |  Major | yarn-native-services | Tarun Parimi | Billie Rinaldi |
| [HDFS-14305](https://issues.apache.org/jira/browse/HDFS-14305) | Serial number in BlockTokenSecretManager could overlap between different namenodes |  Major | namenode, security | Chao Sun | He Xiaoqiao |
| [HDFS-14314](https://issues.apache.org/jira/browse/HDFS-14314) | fullBlockReportLeaseId should be reset after registering to NN |  Critical | datanode | star | star |
| [YARN-8803](https://issues.apache.org/jira/browse/YARN-8803) | [UI2] Show flow runs in the order of recently created time in graph widgets |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-16114](https://issues.apache.org/jira/browse/HADOOP-16114) | NetUtils#canonicalizeHost gives different value for same host |  Minor | net | Praveen Krishna | Praveen Krishna |
| [HDFS-14317](https://issues.apache.org/jira/browse/HDFS-14317) | Standby does not trigger edit log rolling when in-progress edit log tailing is enabled |  Critical | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-14333](https://issues.apache.org/jira/browse/HDFS-14333) | Datanode fails to start if any disk has errors during Namenode registration |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16192](https://issues.apache.org/jira/browse/HADOOP-16192) | CallQueue backoff bug fixes: doesn't perform backoff when add() is used, and doesn't update backoff when refreshed |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14037](https://issues.apache.org/jira/browse/HDFS-14037) | Fix SSLFactory truststore reloader thread leak in URLConnectionFactory |  Major | hdfs-client, webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16225](https://issues.apache.org/jira/browse/HADOOP-16225) | Fix links to the developer mailing lists in DownstreamDev.md |  Minor | documentation | Akira Ajisaka | Wanqiang Ji |
| [HADOOP-16232](https://issues.apache.org/jira/browse/HADOOP-16232) | Fix errors in the checkstyle configration xmls |  Major | build | Akira Ajisaka | Wanqiang Ji |
| [HDFS-14389](https://issues.apache.org/jira/browse/HDFS-14389) | getAclStatus returns incorrect permissions and owner when an iNodeAttributeProvider is configured |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14407](https://issues.apache.org/jira/browse/HDFS-14407) | Fix misuse of SLF4j logging API in DatasetVolumeChecker#checkAllVolumes |  Minor | . | Wanqiang Ji | Wanqiang Ji |
| [YARN-9413](https://issues.apache.org/jira/browse/YARN-9413) | Queue resource leak after app fail for CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-14544](https://issues.apache.org/jira/browse/HADOOP-14544) | DistCp documentation for command line options is misaligned. |  Minor | documentation | Chris Nauroth | Masatake Iwasaki |
| [HDFS-10477](https://issues.apache.org/jira/browse/HDFS-10477) | Stop decommission a rack of DataNodes caused NameNode fail over to standby |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [YARN-6695](https://issues.apache.org/jira/browse/YARN-6695) | Race condition in RM for publishing container events vs appFinished events causes NPE |  Critical | . | Rohith Sharma K S | Prabhu Joseph |
| [YARN-8622](https://issues.apache.org/jira/browse/YARN-8622) | NodeManager native build fails due to getgrouplist not found on macOS |  Major | nodemanager | Ewan Higgs | Siyao Meng |
| [HADOOP-16265](https://issues.apache.org/jira/browse/HADOOP-16265) | Configuration#getTimeDuration is not consistent between default value and manual settings. |  Major | . | star | star |
| [YARN-9307](https://issues.apache.org/jira/browse/YARN-9307) | node\_partitions constraint does not work |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-13677](https://issues.apache.org/jira/browse/HDFS-13677) | Dynamic refresh Disk configuration results in overwriting VolumeMap |  Blocker | . | xuzq | xuzq |
| [YARN-9285](https://issues.apache.org/jira/browse/YARN-9285) | RM UI progress column is of wrong type |  Minor | yarn | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16278](https://issues.apache.org/jira/browse/HADOOP-16278) | With S3A Filesystem, Long Running services End up Doing lot of GC and eventually die |  Major | common, hadoop-aws, metrics | Rajat Khandelwal | Rajat Khandelwal |
| [YARN-9504](https://issues.apache.org/jira/browse/YARN-9504) | [UI2] Fair scheduler queue view page does not show actual capacity |  Major | fairscheduler, yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [YARN-9519](https://issues.apache.org/jira/browse/YARN-9519) | TFile log aggregation file format is not working for yarn.log-aggregation.TFile.remote-app-log-dir config |  Major | log-aggregation | Adam Antal | Adam Antal |
| [HADOOP-16247](https://issues.apache.org/jira/browse/HADOOP-16247) | NPE in FsUrlConnection |  Major | hdfs-client | Karthik Palanisamy | Karthik Palanisamy |
| [HADOOP-16248](https://issues.apache.org/jira/browse/HADOOP-16248) | MutableQuantiles leak memory under heavy load |  Major | metrics | Alexis Daboville | Alexis Daboville |
| [HDFS-14323](https://issues.apache.org/jira/browse/HDFS-14323) | Distcp fails in Hadoop 3.x when 2.x source webhdfs url has special characters in hdfs file path |  Major | webhdfs | Srinivasu Majeti | Srinivasu Majeti |
| [MAPREDUCE-7205](https://issues.apache.org/jira/browse/MAPREDUCE-7205) | Treat container scheduler kill exit code as a task attempt killing event |  Major | applicationmaster, mr-am, mrv2 | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14500](https://issues.apache.org/jira/browse/HDFS-14500) | NameNode StartupProgress continues to report edit log segments after the LOADING\_EDITS phase is finished |  Major | namenode | Erik Krogen | Erik Krogen |
| [HADOOP-16331](https://issues.apache.org/jira/browse/HADOOP-16331) | Fix ASF License check in pom.xml |  Major | . | Wanqiang Ji | Akira Ajisaka |
| [YARN-9542](https://issues.apache.org/jira/browse/YARN-9542) | Fix LogsCLI guessAppOwner ignores custom file format suffix |  Minor | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14512](https://issues.apache.org/jira/browse/HDFS-14512) | ONE\_SSD policy will be violated while write data with DistributedFileSystem.create(....favoredNodes) |  Major | . | Shen Yinjie | Ayush Saxena |
| [HADOOP-16334](https://issues.apache.org/jira/browse/HADOOP-16334) | Fix yetus-wrapper not working when HADOOP\_YETUS\_VERSION \>= 0.9.0 |  Major | yetus | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14521](https://issues.apache.org/jira/browse/HDFS-14521) | Suppress setReplication logging. |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-9507](https://issues.apache.org/jira/browse/YARN-9507) | Fix NPE in NodeManager#serviceStop on startup failure |  Minor | . | Bilwa S T | Bilwa S T |
| [YARN-8947](https://issues.apache.org/jira/browse/YARN-8947) | [UI2] Active User info missing from UI2 |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8906](https://issues.apache.org/jira/browse/YARN-8906) | [UI2] NM hostnames not displayed correctly in Node Heatmap Chart |  Major | . | Charan Hebri | Akhil PB |
| [YARN-8625](https://issues.apache.org/jira/browse/YARN-8625) | Aggregate Resource Allocation for each job is not present in ATS |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16345](https://issues.apache.org/jira/browse/HADOOP-16345) | Potential NPE when instantiating FairCallQueue metrics |  Major | ipc | Erik Krogen | Erik Krogen |
| [YARN-9594](https://issues.apache.org/jira/browse/YARN-9594) | Fix missing break statement in ContainerScheduler#handle |  Major | . | lujie | lujie |
| [YARN-9565](https://issues.apache.org/jira/browse/YARN-9565) | RMAppImpl#ranNodes not cleared on FinalTransition |  Major | . | Bibin A Chundatt | Bilwa S T |
| [YARN-9547](https://issues.apache.org/jira/browse/YARN-9547) | ContainerStatusPBImpl default execution type is not returned |  Major | . | Bibin A Chundatt | Bilwa S T |
| [HDFS-13231](https://issues.apache.org/jira/browse/HDFS-13231) | Extend visualization for Decommissioning, Maintenance Mode under Datanode tab in the NameNode UI |  Major | datanode, namenode | Haibo Yan | Stephen O'Donnell |
| [YARN-9621](https://issues.apache.org/jira/browse/YARN-9621) | FIX TestDSWithMultipleNodeManager.testDistributedShellWithPlacementConstraint on branch-3.1 |  Major | distributed-shell, test | Peter Bacsko | Prabhu Joseph |
| [HDFS-14535](https://issues.apache.org/jira/browse/HDFS-14535) | The default 8KB buffer in requestFileDescriptors#BufferedOutputStream is causing lots of heap allocation in HBase when using short-circut read |  Major | hdfs-client | Zheng Hu | Zheng Hu |
| [HDFS-13730](https://issues.apache.org/jira/browse/HDFS-13730) | BlockReaderRemote.sendReadResult throws NPE |  Major | hdfs-client | Wei-Chiu Chuang | Yuanbo Liu |
| [YARN-9584](https://issues.apache.org/jira/browse/YARN-9584) | Should put initializeProcessTrees method call before get pid |  Critical | nodemanager | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14010](https://issues.apache.org/jira/browse/HDFS-14010) | Pass correct DF usage to ReservedSpaceCalculator builder |  Minor | . | Lukas Majercak | Lukas Majercak |
| [HDFS-14078](https://issues.apache.org/jira/browse/HDFS-14078) | Admin helper fails to prettify NullPointerExceptions |  Major | . | Elek, Marton | Elek, Marton |
| [HDFS-14101](https://issues.apache.org/jira/browse/HDFS-14101) | Random failure of testListCorruptFilesCorruptedBlock |  Major | test | Kihwal Lee | Zsolt Venczel |
| [HDFS-14465](https://issues.apache.org/jira/browse/HDFS-14465) | When the Block expected replications is larger than the number of DataNodes, entering maintenance will never exit. |  Major | . | Yicong Cai | Yicong Cai |
| [HDFS-12487](https://issues.apache.org/jira/browse/HDFS-12487) | FsDatasetSpi.isValidBlock() lacks null pointer check inside and neither do the callers |  Major | balancer & mover, diskbalancer | liumi | liumi |
| [HDFS-14074](https://issues.apache.org/jira/browse/HDFS-14074) | DataNode runs async disk checks  maybe  throws NullPointerException, and DataNode failed to register to NameSpace. |  Major | hdfs | guangyi lu | guangyi lu |
| [HDFS-14541](https://issues.apache.org/jira/browse/HDFS-14541) |  When evictableMmapped or evictable size is zero, do not throw NoSuchElementException |  Major | hdfs-client, performance | Zheng Hu | Lisheng Sun |
| [HDFS-14598](https://issues.apache.org/jira/browse/HDFS-14598) | Findbugs warning caused by HDFS-12487 |  Minor | diskbalancer | Wei-Chiu Chuang | He Xiaoqiao |
| [YARN-9639](https://issues.apache.org/jira/browse/YARN-9639) | DecommissioningNodesWatcher cause memory leak |  Blocker | . | Bibin A Chundatt | Bilwa S T |
| [YARN-9327](https://issues.apache.org/jira/browse/YARN-9327) | Improve synchronisation in ProtoUtils#convertToProtoFormat block |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-9655](https://issues.apache.org/jira/browse/YARN-9655) | AllocateResponse in FederationInterceptor lost  applicationPriority |  Major | federation | hunshenshi | hunshenshi |
| [HADOOP-16385](https://issues.apache.org/jira/browse/HADOOP-16385) | Namenode crashes with "RedundancyMonitor thread received Runtime exception" |  Major | . | krishna reddy | Ayush Saxena |
| [YARN-9644](https://issues.apache.org/jira/browse/YARN-9644) | First RMContext object is always leaked during switch over |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-14629](https://issues.apache.org/jira/browse/HDFS-14629) | Property value Hard Coded in DNConf.java |  Trivial | . | hemanthboyina | hemanthboyina |
| [YARN-9557](https://issues.apache.org/jira/browse/YARN-9557) | Application fails in diskchecker when ReadWriteDiskValidator is configured. |  Critical | nodemanager | Anuruddh Nayak | Bilwa S T |
| [HDFS-12703](https://issues.apache.org/jira/browse/HDFS-12703) | Exceptions are fatal to decommissioning monitor |  Critical | namenode | Daryn Sharp | He Xiaoqiao |
| [HDFS-12748](https://issues.apache.org/jira/browse/HDFS-12748) | NameNode memory leak when accessing webhdfs GETHOMEDIRECTORY |  Major | hdfs | Jiandan Yang | Weiwei Yang |
| [YARN-9625](https://issues.apache.org/jira/browse/YARN-9625) | UI2 - No link to a queue on the Queues page for Fair Scheduler |  Major | . | Charan Hebri | Zoltan Siegl |
| [HDFS-14466](https://issues.apache.org/jira/browse/HDFS-14466) | Add a regression test for HDFS-14323 |  Minor | fs, test, webhdfs | Yuya Ebihara | Masatake Iwasaki |
| [YARN-9235](https://issues.apache.org/jira/browse/YARN-9235) | If linux container executor is not set for a GPU cluster GpuResourceHandlerImpl is not initialized and NPE is thrown |  Major | yarn | Antal Bálint Steinbach | Adam Antal |
| [YARN-9626](https://issues.apache.org/jira/browse/YARN-9626) | UI2 - Fair scheduler queue apps page issues |  Major | . | Charan Hebri | Zoltan Siegl |
| [YARN-9682](https://issues.apache.org/jira/browse/YARN-9682) | Wrong log message when finalizing the upgrade |  Trivial | . | kyungwan nam | kyungwan nam |
| [HADOOP-16440](https://issues.apache.org/jira/browse/HADOOP-16440) | Distcp can not preserve timestamp with -delete  option |  Major | . | ludun | ludun |
| [MAPREDUCE-7076](https://issues.apache.org/jira/browse/MAPREDUCE-7076) | TestNNBench#testNNBenchCreateReadAndDelete failing in our internal build |  Minor | test | Rushabh S Shah | kevin su |
| [YARN-9668](https://issues.apache.org/jira/browse/YARN-9668) | UGI conf doesn't read user overridden configurations on RM and NM startup |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-9844](https://issues.apache.org/jira/browse/HADOOP-9844) | NPE when trying to create an error message response of SASL RPC |  Major | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-16245](https://issues.apache.org/jira/browse/HADOOP-16245) | Enabling SSL within LdapGroupsMapping can break system SSL configs |  Major | common, security | Erik Krogen | Erik Krogen |
| [HDFS-14429](https://issues.apache.org/jira/browse/HDFS-14429) | Block remain in COMMITTED but not COMPLETE caused by Decommission |  Major | . | Yicong Cai | Yicong Cai |
| [HADOOP-16435](https://issues.apache.org/jira/browse/HADOOP-16435) | RpcMetrics should not be retained forever |  Critical | rpc-server | Zoltan Haindrich | Zoltan Haindrich |
| [YARN-9596](https://issues.apache.org/jira/browse/YARN-9596) | QueueMetrics has incorrect metrics when labelled partitions are involved |  Major | capacity scheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [MAPREDUCE-7225](https://issues.apache.org/jira/browse/MAPREDUCE-7225) | Fix broken current folder expansion during MR job start |  Major | mrv2 | Adam Antal | Peter Bacsko |
| [HDFS-13529](https://issues.apache.org/jira/browse/HDFS-13529) | Fix default trash policy emptier trigger time correctly |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HADOOP-15681](https://issues.apache.org/jira/browse/HADOOP-15681) | AuthenticationFilter should generate valid date format for Set-Cookie header regardless of default Locale |  Minor | security | Cao Manh Dat | Cao Manh Dat |
| [HDFS-14685](https://issues.apache.org/jira/browse/HDFS-14685) | DefaultAuditLogger doesn't print CallerContext |  Major | hdfs | xuzq | xuzq |
| [HDFS-14462](https://issues.apache.org/jira/browse/HDFS-14462) | WebHDFS throws "Error writing request body to server" instead of DSQuotaExceededException |  Major | webhdfs | Erik Krogen | Simbarashe Dzinamarira |
| [HDFS-14557](https://issues.apache.org/jira/browse/HDFS-14557) | JournalNode error: Can't scan a pre-transactional edit log |  Major | ha | Wei-Chiu Chuang | Stephen O'Donnell |
| [HDFS-14692](https://issues.apache.org/jira/browse/HDFS-14692) | Upload button should not encode complete url |  Major | . | Lokesh Jain | Lokesh Jain |
| [HDFS-14631](https://issues.apache.org/jira/browse/HDFS-14631) | The DirectoryScanner doesn't fix the wrongly placed replica. |  Major | . | Jinglun | Jinglun |
| [YARN-9685](https://issues.apache.org/jira/browse/YARN-9685) | NPE when rendering the info table of leaf queue in non-accessible partitions |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-14459](https://issues.apache.org/jira/browse/HDFS-14459) | ClosedChannelException silently ignored in FsVolumeList.addBlockPool() |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13359](https://issues.apache.org/jira/browse/HDFS-13359) | DataXceiver hung due to the lock in FsDatasetImpl#getBlockInputStream |  Major | datanode | Yiqun Lin | Yiqun Lin |
| [YARN-9451](https://issues.apache.org/jira/browse/YARN-9451) | AggregatedLogsBlock shows wrong NM http port |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9723](https://issues.apache.org/jira/browse/YARN-9723) | ApplicationPlacementContext is not required for terminated jobs during recovery |  Major | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12914](https://issues.apache.org/jira/browse/HDFS-12914) | Block report leases cause missing blocks until next report |  Critical | namenode | Daryn Sharp | Santosh Marella |
| [HDFS-14148](https://issues.apache.org/jira/browse/HDFS-14148) | HDFS OIV ReverseXML SnapshotSection parser throws exception when there are more than one snapshottable directory |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-14595](https://issues.apache.org/jira/browse/HDFS-14595) | HDFS-11848 breaks API compatibility |  Blocker | . | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14423](https://issues.apache.org/jira/browse/HDFS-14423) | Percent (%) and plus (+) characters no longer work in WebHDFS |  Major | webhdfs | Jing Wang | Masatake Iwasaki |
| [MAPREDUCE-7230](https://issues.apache.org/jira/browse/MAPREDUCE-7230) | TestHSWebApp.testLogsViewSingle fails |  Major | jobhistoryserver, test | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14687](https://issues.apache.org/jira/browse/HDFS-14687) | Standby Namenode never come out of safemode when EC files are being written. |  Critical | ec, namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-13101](https://issues.apache.org/jira/browse/HDFS-13101) | Yet another fsimage corruption related to snapshot |  Major | snapshots | Yongjun Zhang | Shashikant Banerjee |
| [HDFS-13201](https://issues.apache.org/jira/browse/HDFS-13201) | Fix prompt message in testPolicyAndStateCantBeNull |  Minor | . | chencan | chencan |
| [HDFS-14311](https://issues.apache.org/jira/browse/HDFS-14311) | Multi-threading conflict at layoutVersion when loading block pool storage |  Major | rolling upgrades | Yicong Cai | Yicong Cai |
| [HDFS-14582](https://issues.apache.org/jira/browse/HDFS-14582) | Failed to start DN with ArithmeticException when NULL checksum used |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16494](https://issues.apache.org/jira/browse/HADOOP-16494) | Add SHA-256 or SHA-512 checksum to release artifacts to comply with the release distribution policy |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9774](https://issues.apache.org/jira/browse/YARN-9774) | Fix order of arguments for assertEquals in TestSLSUtils |  Minor | test | Nikhil Navadiya | Nikhil Navadiya |
| [HDFS-13596](https://issues.apache.org/jira/browse/HDFS-13596) | NN restart fails after RollingUpgrade from 2.x to 3.x |  Blocker | hdfs | Hanisha Koneru | Fei Hui |
| [HDFS-14396](https://issues.apache.org/jira/browse/HDFS-14396) | Failed to load image from FSImageFile when downgrade from 3.x to 2.x |  Blocker | rolling upgrades | Fei Hui | Fei Hui |
| [YARN-9642](https://issues.apache.org/jira/browse/YARN-9642) | Fix Memory Leak in AbstractYarnScheduler caused by timer |  Blocker | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-13977](https://issues.apache.org/jira/browse/HDFS-13977) | NameNode can kill itself if it tries to send too many txns to a QJM simultaneously |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-9438](https://issues.apache.org/jira/browse/YARN-9438) | launchTime not written to state store for running applications |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-12212](https://issues.apache.org/jira/browse/HDFS-12212) | Options.Rename.To\_TRASH is considered even when Options.Rename.NONE is specified |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-8178](https://issues.apache.org/jira/browse/HDFS-8178) | QJM doesn't move aside stale inprogress edits files |  Major | qjm | Zhe Zhang | Istvan Fajth |
| [HDFS-14706](https://issues.apache.org/jira/browse/HDFS-14706) | Checksums are not checked if block meta file is less than 7 bytes |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9797](https://issues.apache.org/jira/browse/YARN-9797) | LeafQueue#activateApplications should use resourceCalculator#fitsIn |  Blocker | . | Bibin A Chundatt | Bilwa S T |
| [YARN-9785](https://issues.apache.org/jira/browse/YARN-9785) | Fix DominantResourceCalculator when one resource is zero |  Blocker | . | Bilwa S T | Bilwa S T |
| [YARN-9817](https://issues.apache.org/jira/browse/YARN-9817) | Fix failing testcases due to not initialized AsyncDispatcher -  ArithmeticException: / by zero |  Major | test | Prabhu Joseph | Prabhu Joseph |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-9315](https://issues.apache.org/jira/browse/YARN-9315) | TestCapacitySchedulerMetrics fails intermittently |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9316](https://issues.apache.org/jira/browse/YARN-9316) | TestPlacementConstraintsUtil#testInterAppConstraintsByAppID fails intermittently |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9325](https://issues.apache.org/jira/browse/YARN-9325) | TestQueueManagementDynamicEditPolicy fails intermittent |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-11950](https://issues.apache.org/jira/browse/HDFS-11950) | Disable libhdfs zerocopy test on Mac |  Minor | libhdfs | John Zhuge | Akira Ajisaka |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16045](https://issues.apache.org/jira/browse/HADOOP-16045) | Don't run TestDU on Windows |  Trivial | common, test | Lukas Majercak | Lukas Majercak |
| [HADOOP-16079](https://issues.apache.org/jira/browse/HADOOP-16079) | Token.toString faulting if any token listed can't load. |  Blocker | security | Steve Loughran | Steve Loughran |
| [YARN-9253](https://issues.apache.org/jira/browse/YARN-9253) | Add UT to verify Placement Constraint in Distributed Shell |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9293](https://issues.apache.org/jira/browse/YARN-9293) | Optimize MockAMLauncher event handling |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-16109](https://issues.apache.org/jira/browse/HADOOP-16109) | Parquet reading S3AFileSystem causes EOF |  Blocker | fs/s3 | Dave Christianson | Steve Loughran |
| [HADOOP-16191](https://issues.apache.org/jira/browse/HADOOP-16191) | AliyunOSS: improvements for copyFile/copyDirectory and logging |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9391](https://issues.apache.org/jira/browse/YARN-9391) | Disable PATH variable to be passed to Docker container |  Major | . | Eric Yang | Jim Brennan |
| [HADOOP-16220](https://issues.apache.org/jira/browse/HADOOP-16220) | Add findbugs ignores for unjustified issues during update to guava to 27.0-jre in hadoop-project |  Major | . | Gabor Bota | Gabor Bota |
| [HADOOP-16233](https://issues.apache.org/jira/browse/HADOOP-16233) | S3AFileStatus to declare that isEncrypted() is always true |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16306](https://issues.apache.org/jira/browse/HADOOP-16306) | AliyunOSS: Remove temporary files when upload small files to OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14553](https://issues.apache.org/jira/browse/HDFS-14553) | Make queue size of BlockReportProcessingThread configurable |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HDFS-14034](https://issues.apache.org/jira/browse/HDFS-14034) | Support getQuotaUsage API in WebHDFS |  Major | fs, webhdfs | Erik Krogen | Chao Sun |
| [YARN-9765](https://issues.apache.org/jira/browse/YARN-9765) | SLS runner crashes when run with metrics turned off. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-14674](https://issues.apache.org/jira/browse/HDFS-14674) | [SBN read] Got an unexpected txid when tail editlog |  Blocker | . | wangzhaohui | wangzhaohui |
| [YARN-9775](https://issues.apache.org/jira/browse/YARN-9775) | RMWebServices /scheduler-conf GET returns all hadoop configurations for ZKConfigurationStore |  Major | restapi | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14779](https://issues.apache.org/jira/browse/HDFS-14779) | Fix logging error in TestEditLog#testMultiStreamsLoadEditWithConfMaxTxns |  Major | . | Jonathan Hung | Jonathan Hung |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16025](https://issues.apache.org/jira/browse/HADOOP-16025) | Update the year to 2019 |  Major | build | Ayush Saxena | Ayush Saxena |
| [HDFS-12729](https://issues.apache.org/jira/browse/HDFS-12729) | Document special paths in HDFS |  Major | documentation | Chris Douglas | Masatake Iwasaki |
| [YARN-9191](https://issues.apache.org/jira/browse/YARN-9191) | Add cli option in DS to support enforceExecutionType in resource requests. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16263](https://issues.apache.org/jira/browse/HADOOP-16263) | Update BUILDING.txt with macOS native build instructions |  Minor | . | Siyao Meng | Siyao Meng |
| [YARN-9559](https://issues.apache.org/jira/browse/YARN-9559) | Create AbstractContainersLauncher for pluggable ContainersLauncher logic |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16551](https://issues.apache.org/jira/browse/HADOOP-16551) | The changelog\*.md seems not generated when create-release |  Blocker | . | Zhankun Tang |  |


