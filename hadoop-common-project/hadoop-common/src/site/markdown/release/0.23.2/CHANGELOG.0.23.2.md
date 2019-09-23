
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

## Release 0.23.2 - Unreleased (as of 2018-09-01)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2887](https://issues.apache.org/jira/browse/HDFS-2887) | Define a FSVolume interface |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-8131](https://issues.apache.org/jira/browse/HADOOP-8131) | FsShell put doesn't correctly handle a non-existent dir |  Critical | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8164](https://issues.apache.org/jira/browse/HADOOP-8164) | Handle paths using back slash as path separator for windows only |  Major | fs | Suresh Srinivas | Daryn Sharp |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2943](https://issues.apache.org/jira/browse/HDFS-2943) | Expose last checkpoint time and transaction stats as JMX metrics |  Major | namenode | Aaron T. Myers | Aaron T. Myers |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1217](https://issues.apache.org/jira/browse/HDFS-1217) | Some methods in the NameNdoe should not be public |  Major | namenode | Tsz Wo Nicholas Sze | Laxman |
| [HADOOP-8048](https://issues.apache.org/jira/browse/HADOOP-8048) | Allow merging of Credentials |  Major | util | Daryn Sharp | Daryn Sharp |
| [HDFS-2506](https://issues.apache.org/jira/browse/HDFS-2506) | Umbrella jira for tracking separation of wire protocol datatypes from the implementation types |  Major | datanode, namenode | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-8071](https://issues.apache.org/jira/browse/HADOOP-8071) | Avoid an extra packet in client code when nagling is disabled |  Minor | ipc | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3864](https://issues.apache.org/jira/browse/MAPREDUCE-3864) | Fix cluster setup docs for correct SNN HTTPS parameters |  Minor | documentation, security | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3849](https://issues.apache.org/jira/browse/MAPREDUCE-3849) | Change TokenCache's reading of the binary token file |  Major | security | Daryn Sharp | Daryn Sharp |
| [HDFS-2655](https://issues.apache.org/jira/browse/HDFS-2655) | BlockReaderLocal#skip performs unnecessary IO |  Major | datanode | Eli Collins | Brandon Li |
| [HDFS-2907](https://issues.apache.org/jira/browse/HDFS-2907) | Make FSDataset in Datanode Pluggable |  Minor | . | Sanjay Radia | Tsz Wo Nicholas Sze |
| [HDFS-2985](https://issues.apache.org/jira/browse/HDFS-2985) | Improve logging when replicas are marked as corrupt |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2981](https://issues.apache.org/jira/browse/HDFS-2981) | The default value of dfs.client.block.write.replace-datanode-on-failure.enable should be true |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3730](https://issues.apache.org/jira/browse/MAPREDUCE-3730) | Allow restarted NM to rejoin cluster before RM expires it |  Minor | mrv2, resourcemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3901](https://issues.apache.org/jira/browse/MAPREDUCE-3901) | lazy load JobHistory Task and TaskAttempt details |  Major | jobhistoryserver, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3922](https://issues.apache.org/jira/browse/MAPREDUCE-3922) | Fix the potential problem compiling 32 bit binaries on a x86\_64 host. |  Minor | build, mrv2 | Eugene Koontz | Hitesh Shah |
| [HDFS-3024](https://issues.apache.org/jira/browse/HDFS-3024) | Improve performance of stringification in addStoredBlock |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3066](https://issues.apache.org/jira/browse/HDFS-3066) | cap space usage of default log4j rolling policy (hdfs specific changes) |  Major | scripts | Patrick Hunt | Patrick Hunt |
| [MAPREDUCE-3989](https://issues.apache.org/jira/browse/MAPREDUCE-3989) | cap space usage of default log4j rolling policy (mr specific changes) |  Major | . | Patrick Hunt | Patrick Hunt |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7874](https://issues.apache.org/jira/browse/HADOOP-7874) | native libs should be under lib/native/ dir |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8042](https://issues.apache.org/jira/browse/HADOOP-8042) | When copying a file out of HDFS, modifying it, and uploading it back into HDFS, the put fails due to a CRC mismatch |  Critical | fs | Kevin J. Price | Daryn Sharp |
| [HADOOP-8035](https://issues.apache.org/jira/browse/HADOOP-8035) | Hadoop Maven site is inefficient and runs phases redundantly |  Minor | build | Andrew Bayer | Andrew Bayer |
| [HDFS-2764](https://issues.apache.org/jira/browse/HDFS-2764) | TestBackupNode is racy |  Major | namenode, test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-3680](https://issues.apache.org/jira/browse/MAPREDUCE-3680) | FifoScheduler web service rest API can print out invalid JSON |  Major | mrv2 | Thomas Graves |  |
| [HADOOP-8051](https://issues.apache.org/jira/browse/HADOOP-8051) | HttpFS documentation it is not wired to the generated site |  Major | documentation | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3852](https://issues.apache.org/jira/browse/MAPREDUCE-3852) | test TestLinuxResourceCalculatorPlugin failing |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-776](https://issues.apache.org/jira/browse/HDFS-776) | Fix exception handling in Balancer |  Critical | balancer & mover | Owen O'Malley | Uma Maheswara Rao G |
| [HADOOP-6502](https://issues.apache.org/jira/browse/HADOOP-6502) | DistributedFileSystem#listStatus is very slow when listing a directory with a size of 1300 |  Critical | util | Hairong Kuang | Sharad Agarwal |
| [HDFS-2950](https://issues.apache.org/jira/browse/HDFS-2950) | Secondary NN HTTPS address should be listed as a NAMESERVICE\_SPECIFIC\_KEY |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2525](https://issues.apache.org/jira/browse/HDFS-2525) | Race between BlockPoolSliceScanner and append |  Critical | datanode | Todd Lipcon | Brandon Li |
| [HDFS-2938](https://issues.apache.org/jira/browse/HDFS-2938) | Recursive delete of a large directory makes namenode unresponsive |  Major | namenode | Suresh Srinivas | Hari Mankude |
| [HADOOP-8074](https://issues.apache.org/jira/browse/HADOOP-8074) | Small bug in hadoop error message for unknown commands |  Trivial | scripts | Eli Collins | Colin P. McCabe |
| [HADOOP-8082](https://issues.apache.org/jira/browse/HADOOP-8082) | add hadoop-client and hadoop-minicluster to the dependency-management section |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8066](https://issues.apache.org/jira/browse/HADOOP-8066) | The full docs build intermittently fails |  Major | build | Aaron T. Myers | Andrew Bayer |
| [HADOOP-8083](https://issues.apache.org/jira/browse/HADOOP-8083) | javadoc generation for some modules is not done under target/ |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8036](https://issues.apache.org/jira/browse/HADOOP-8036) | TestViewFsTrash assumes the user's home directory is 2 levels deep |  Major | fs, test | Eli Collins | Colin P. McCabe |
| [MAPREDUCE-3862](https://issues.apache.org/jira/browse/MAPREDUCE-3862) | Nodemanager can appear to hang on shutdown due to lingering DeletionService threads |  Major | mrv2, nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-2969](https://issues.apache.org/jira/browse/HDFS-2969) | ExtendedBlock.equals is incorrectly implemented |  Critical | datanode | Todd Lipcon | Todd Lipcon |
| [HADOOP-8046](https://issues.apache.org/jira/browse/HADOOP-8046) | Revert StaticMapping semantics to the existing ones, add DNS mapping diagnostics in progress |  Minor | . | Steve Loughran | Steve Loughran |
| [HDFS-2725](https://issues.apache.org/jira/browse/HDFS-2725) | hdfs script usage information is missing the information about "dfs" command |  Major | hdfs-client | Prashant Sharma |  |
| [HADOOP-8057](https://issues.apache.org/jira/browse/HADOOP-8057) | hadoop-setup-conf.sh not working because of some extra spaces. |  Major | scripts | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-3634](https://issues.apache.org/jira/browse/MAPREDUCE-3634) | All daemons should crash instead of hanging around when their EventHandlers get exceptions |  Major | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-8050](https://issues.apache.org/jira/browse/HADOOP-8050) | Deadlock in metrics |  Major | metrics | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-3884](https://issues.apache.org/jira/browse/MAPREDUCE-3884) | PWD should be first in the classpath of MR tasks |  Critical | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7660](https://issues.apache.org/jira/browse/HADOOP-7660) | Maven generated .classpath doesnot includes "target/generated-test-source/java" as source directory. |  Minor | build | Laxman | Laxman |
| [HDFS-2944](https://issues.apache.org/jira/browse/HDFS-2944) | Typo in hdfs-default.xml causes dfs.client.block.write.replace-datanode-on-failure.enable to be mistakenly disabled |  Major | hdfs-client | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-3878](https://issues.apache.org/jira/browse/MAPREDUCE-3878) | Null user on filtered jobhistory job page |  Critical | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3583](https://issues.apache.org/jira/browse/MAPREDUCE-3583) | ProcfsBasedProcessTree#constructProcessInfo() may throw NumberFormatException |  Critical | . | Ted Yu | Ted Yu |
| [MAPREDUCE-3738](https://issues.apache.org/jira/browse/MAPREDUCE-3738) | NM can hang during shutdown if AppLogAggregatorImpl thread dies unexpectedly |  Critical | mrv2, nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-3008](https://issues.apache.org/jira/browse/HDFS-3008) | Negative caching of local addrs doesn't work |  Major | hdfs-client | Eli Collins | Eli Collins |
| [MAPREDUCE-3866](https://issues.apache.org/jira/browse/MAPREDUCE-3866) | bin/yarn prints the command line unnecessarily |  Minor | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3904](https://issues.apache.org/jira/browse/MAPREDUCE-3904) | [NPE] Job history produced with mapreduce.cluster.acls.enabled false can not be viewed with mapreduce.cluster.acls.enabled true |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3918](https://issues.apache.org/jira/browse/MAPREDUCE-3918) | proc\_historyserver no longer in command line arguments for HistoryServer |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [HDFS-3006](https://issues.apache.org/jira/browse/HDFS-3006) | Webhdfs "SETOWNER" call returns incorrect content-type |  Major | webhdfs | bc Wong | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2793](https://issues.apache.org/jira/browse/MAPREDUCE-2793) | [MR-279] Maintain consistency in naming appIDs, jobIDs and attemptIDs |  Critical | mrv2 | Ramya Sunil | Bikas Saha |
| [MAPREDUCE-3910](https://issues.apache.org/jira/browse/MAPREDUCE-3910) | user not allowed to submit jobs even though queue -showacls shows it allows |  Blocker | mrv2 | John George | John George |
| [MAPREDUCE-3686](https://issues.apache.org/jira/browse/MAPREDUCE-3686) | history server web ui - job counter values for map/reduce not shown properly |  Critical | mrv2 | Thomas Graves | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3913](https://issues.apache.org/jira/browse/MAPREDUCE-3913) | RM application webpage is unresponsive after 2000 jobs |  Critical | mrv2, webapps | Jason Lowe | Jason Lowe |
| [MAPREDUCE-2855](https://issues.apache.org/jira/browse/MAPREDUCE-2855) | ResourceBundle lookup during counter name resolution takes a lot of time |  Major | . | Todd Lipcon | Siddharth Seth |
| [MAPREDUCE-3790](https://issues.apache.org/jira/browse/MAPREDUCE-3790) | Broken pipe on streaming job can lead to truncated output for a successful job |  Major | contrib/streaming, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3816](https://issues.apache.org/jira/browse/MAPREDUCE-3816) | capacity scheduler web ui bar graphs for used capacity wrong |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3930](https://issues.apache.org/jira/browse/MAPREDUCE-3930) | The AM page for a Reducer that has not been launched causes an NPE |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3931](https://issues.apache.org/jira/browse/MAPREDUCE-3931) | MR tasks failing due to changing timestamps on Resources to download |  Major | mrv2 | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-3687](https://issues.apache.org/jira/browse/MAPREDUCE-3687) | If AM dies before it returns new tracking URL, proxy redirects to http://N/A/ and doesn't return error code |  Major | mrv2 | David Capwell | Ravi Prakash |
| [MAPREDUCE-3920](https://issues.apache.org/jira/browse/MAPREDUCE-3920) | Revise yarn default port number selection |  Major | nodemanager, resourcemanager | Dave Thompson | Dave Thompson |
| [MAPREDUCE-3903](https://issues.apache.org/jira/browse/MAPREDUCE-3903) | no admin override to view jobs on mr app master and job history server |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-3012](https://issues.apache.org/jira/browse/HDFS-3012) | Exception while renewing delegation token |  Critical | . | Ramya Sunil | Robert Joseph Evans |
| [MAPREDUCE-3706](https://issues.apache.org/jira/browse/MAPREDUCE-3706) | HTTP Circular redirect error on the job attempts page |  Critical | mrv2 | Thomas Graves | Robert Joseph Evans |
| [MAPREDUCE-3896](https://issues.apache.org/jira/browse/MAPREDUCE-3896) | pig job through oozie hangs |  Blocker | jobhistoryserver, mrv2 | John George | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3792](https://issues.apache.org/jira/browse/MAPREDUCE-3792) | job -list displays only the jobs submitted by a particular user |  Critical | mrv2 | Ramya Sunil | Jason Lowe |
| [MAPREDUCE-3614](https://issues.apache.org/jira/browse/MAPREDUCE-3614) |  finalState UNDEFINED if AM is killed by hand |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3929](https://issues.apache.org/jira/browse/MAPREDUCE-3929) | output of mapred -showacl is not clear |  Major | mrv2 | John George | John George |
| [HADOOP-8123](https://issues.apache.org/jira/browse/HADOOP-8123) | hadoop-project invalid pom warnings prevent transitive dependency resolution |  Critical | build | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3960](https://issues.apache.org/jira/browse/MAPREDUCE-3960) | web proxy doesn't forward request to AM with configured hostname/IP |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3897](https://issues.apache.org/jira/browse/MAPREDUCE-3897) | capacity scheduler - maxActiveApplicationsPerUser calculation can be wrong |  Critical | mrv2 | Thomas Graves | Eric Payne |
| [MAPREDUCE-3497](https://issues.apache.org/jira/browse/MAPREDUCE-3497) | missing documentation for yarn cli and subcommands - similar to commands\_manual.html |  Major | documentation, mrv2 | Thomas Graves | Thomas Graves |
| [HADOOP-8137](https://issues.apache.org/jira/browse/HADOOP-8137) | Site side links for commands manual (MAPREDUCE-3497) |  Major | documentation | Vinod Kumar Vavilapalli | Thomas Graves |
| [MAPREDUCE-3009](https://issues.apache.org/jira/browse/MAPREDUCE-3009) | RM UI -\> Applications -\> Application(Job History) -\> Map Tasks -\> Task ID -\> Node link is not working |  Major | jobhistoryserver, mrv2 | chackaravarthy | chackaravarthy |
| [MAPREDUCE-3954](https://issues.apache.org/jira/browse/MAPREDUCE-3954) | Clean up passing HEAPSIZE to yarn and mapred commands. |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3964](https://issues.apache.org/jira/browse/MAPREDUCE-3964) | ResourceManager does not have JVM metrics |  Critical | mrv2, resourcemanager | Jason Lowe | Jason Lowe |
| [HADOOP-8064](https://issues.apache.org/jira/browse/HADOOP-8064) | Remove unnecessary dependency on w3c.org in document processing |  Major | build | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-3034](https://issues.apache.org/jira/browse/MAPREDUCE-3034) | NM should act on a REBOOT command from RM |  Critical | mrv2, nodemanager | Vinod Kumar Vavilapalli | Devaraj K |
| [HDFS-3032](https://issues.apache.org/jira/browse/HDFS-3032) | Lease renewer tries forever even if renewal is not possible |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-3976](https://issues.apache.org/jira/browse/MAPREDUCE-3976) | TestRMContainerAllocator failing |  Major | mrv2 | Bikas Saha | Jason Lowe |
| [HADOOP-8140](https://issues.apache.org/jira/browse/HADOOP-8140) | dfs -getmerge  should process its argments better |  Major | . | arkady borkovsky | Daryn Sharp |
| [MAPREDUCE-3961](https://issues.apache.org/jira/browse/MAPREDUCE-3961) | Map/ReduceSlotMillis computation incorrect |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3977](https://issues.apache.org/jira/browse/MAPREDUCE-3977) | LogAggregationService leaks log aggregator objects |  Critical | mrv2, nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3975](https://issues.apache.org/jira/browse/MAPREDUCE-3975) | Default value not set for Configuration parameter mapreduce.job.local.dir |  Blocker | mrv2 | Eric Payne | Eric Payne |
| [HADOOP-8146](https://issues.apache.org/jira/browse/HADOOP-8146) | FsShell commands cannot be interrupted |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3982](https://issues.apache.org/jira/browse/MAPREDUCE-3982) | TestEmptyJob fails with FileNotFound |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3101](https://issues.apache.org/jira/browse/HDFS-3101) | cannot read empty file using webhdfs |  Major | webhdfs | Zhanwei Wang | Tsz Wo Nicholas Sze |
| [HADOOP-8176](https://issues.apache.org/jira/browse/HADOOP-8176) | Disambiguate the destination of FsShell copies |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4005](https://issues.apache.org/jira/browse/MAPREDUCE-4005) | AM container logs URL is broken for completed apps when log aggregation is enabled |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4006](https://issues.apache.org/jira/browse/MAPREDUCE-4006) | history server container log web UI sometimes combines stderr/stdout/syslog contents together |  Major | jobhistoryserver, mrv2 | Jason Lowe | Siddharth Seth |
| [MAPREDUCE-4025](https://issues.apache.org/jira/browse/MAPREDUCE-4025) | AM can crash if task attempt reports bogus progress value |  Blocker | mr-am, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4034](https://issues.apache.org/jira/browse/MAPREDUCE-4034) | Unable to view task logs on history server with mapreduce.job.acl-view-job=\* |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4043](https://issues.apache.org/jira/browse/MAPREDUCE-4043) | Secret keys set in Credentials are not seen by tasks |  Blocker | mrv2, security | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4061](https://issues.apache.org/jira/browse/MAPREDUCE-4061) | RM only has 1 AM launcher thread |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-3160](https://issues.apache.org/jira/browse/HDFS-3160) | httpfs should exec catalina instead of forking it |  Major | scripts | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-3853](https://issues.apache.org/jira/browse/HDFS-3853) | Port MiniDFSCluster enableManagedDfsDirsRedundancy option to branch-2 |  Minor | namenode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-2815](https://issues.apache.org/jira/browse/HDFS-2815) | Namenode is not coming out of safemode when we perform ( NN crash + restart ) .  Also FSCK report shows blocks missed. |  Critical | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-3877](https://issues.apache.org/jira/browse/MAPREDUCE-3877) | Add a test to formalise the current state transitions of the yarn lifecycle |  Minor | mrv2 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-3798](https://issues.apache.org/jira/browse/MAPREDUCE-3798) | TestJobCleanup testCustomCleanup is failing |  Major | test | Ravi Prakash | Ravi Prakash |
| [HDFS-3060](https://issues.apache.org/jira/browse/HDFS-3060) | Bump TestDistributedUpgrade#testDistributedUpgrade timeout |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-2038](https://issues.apache.org/jira/browse/HDFS-2038) | Update test to handle relative paths with globs |  Critical | test | Daryn Sharp | Kihwal Lee |
| [HDFS-3098](https://issues.apache.org/jira/browse/HDFS-3098) | Update FsShell tests for quoted metachars |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-3104](https://issues.apache.org/jira/browse/HDFS-3104) | Add tests for mkdir -p |  Major | test | Daryn Sharp | Daryn Sharp |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-3944](https://issues.apache.org/jira/browse/MAPREDUCE-3944) | JobHistory web services are slower then the UI and can easly overload the JH |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8173](https://issues.apache.org/jira/browse/HADOOP-8173) | FsShell needs to handle quoted metachars |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8175](https://issues.apache.org/jira/browse/HADOOP-8175) | Add mkdir -p flag |  Major | fs | Daryn Sharp | Daryn Sharp |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8032](https://issues.apache.org/jira/browse/HADOOP-8032) | mvn site:stage-deploy should be able to use the scp protocol to stage documents |  Major | build, documentation | Ravi Prakash | Ravi Prakash |
| [HDFS-2931](https://issues.apache.org/jira/browse/HDFS-2931) | Switch the DataNode's BlockVolumeChoosingPolicy to be a private-audience interface |  Minor | datanode | Harsh J | Harsh J |


