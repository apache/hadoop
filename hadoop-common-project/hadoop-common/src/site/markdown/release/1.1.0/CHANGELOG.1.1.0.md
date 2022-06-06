
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

## Release 1.1.0 - 2012-10-13

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8552](https://issues.apache.org/jira/browse/HADOOP-8552) | Conflict: Same security.log.file for multiple users. |  Major | conf, security | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-8365](https://issues.apache.org/jira/browse/HADOOP-8365) | Add flag to disable durable sync |  Blocker | . | Eli Collins | Eli Collins |
| [HADOOP-8314](https://issues.apache.org/jira/browse/HADOOP-8314) | HttpServer#hasAdminAccess should return false if authorization is enabled but user is not authenticated |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8230](https://issues.apache.org/jira/browse/HADOOP-8230) | Enable sync by default and disable append |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-8154](https://issues.apache.org/jira/browse/HADOOP-8154) | DNS#getIPs shouldn't silently return the local host IP for bogus interface names |  Major | conf | Eli Collins | Eli Collins |
| [HADOOP-5464](https://issues.apache.org/jira/browse/HADOOP-5464) | DFSClient does not treat write timeout of 0 properly |  Major | . | Raghu Angadi | Raghu Angadi |
| [HDFS-3522](https://issues.apache.org/jira/browse/HDFS-3522) | If NN is in safemode, it should throw SafeModeException when getBlockLocations has zero locations |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-3044](https://issues.apache.org/jira/browse/HDFS-3044) | fsck move should be non-destructive by default |  Major | namenode | Eli Collins | Colin Patrick McCabe |
| [HDFS-2617](https://issues.apache.org/jira/browse/HDFS-2617) | Replaced Kerberized SSL for image transfer and fsck with SPNEGO-based solution |  Major | security | Jakob Homan | Jakob Homan |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7823](https://issues.apache.org/jira/browse/HADOOP-7823) | port HADOOP-4012 to branch-1 (splitting support for bzip2) |  Major | . | Tim Broberg | Andrew Purtell |
| [HADOOP-7806](https://issues.apache.org/jira/browse/HADOOP-7806) | Support binding to sub-interfaces |  Major | util | Harsh J | Harsh J |
| [HDFS-3150](https://issues.apache.org/jira/browse/HDFS-3150) | Add option for clients to contact DNs via hostname |  Major | datanode, hdfs-client | Eli Collins | Eli Collins |
| [HDFS-3148](https://issues.apache.org/jira/browse/HDFS-3148) | The client should be able to use multiple local interfaces for data transfer |  Major | hdfs-client, performance | Eli Collins | Eli Collins |
| [HDFS-3055](https://issues.apache.org/jira/browse/HDFS-3055) | Implement recovery mode for branch-1 |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [MAPREDUCE-3837](https://issues.apache.org/jira/browse/MAPREDUCE-3837) | Job tracker is not able to recover job in case of crash and after that no user can submit job. |  Major | . | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-3118](https://issues.apache.org/jira/browse/MAPREDUCE-3118) | Backport Gridmix and Rumen features from trunk to Hadoop 0.20 security branch |  Major | contrib/gridmix, tools/rumen | Ravi Gummadi | Ravi Gummadi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8748](https://issues.apache.org/jira/browse/HADOOP-8748) | Move dfsclient retry to a util class |  Minor | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-8656](https://issues.apache.org/jira/browse/HADOOP-8656) | backport forced daemon shutdown of HADOOP-8353 into branch-1 |  Minor | bin | Steve Loughran | Roman Shaposhnik |
| [HADOOP-8430](https://issues.apache.org/jira/browse/HADOOP-8430) | Backport new FileSystem methods introduced by HADOOP-8014 to branch-1 |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-8209](https://issues.apache.org/jira/browse/HADOOP-8209) | Add option to relax build-version check for branch-1 |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-7509](https://issues.apache.org/jira/browse/HADOOP-7509) | Improve message when Authentication is required |  Trivial | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-6995](https://issues.apache.org/jira/browse/HADOOP-6995) | Allow wildcards to be used in ProxyUsers configurations |  Minor | security | Todd Lipcon | Todd Lipcon |
| [HDFS-3871](https://issues.apache.org/jira/browse/HDFS-3871) | Change NameNodeProxies to use HADOOP-8748 |  Minor | hdfs-client | Arun C Murthy | Arun C Murthy |
| [HDFS-3814](https://issues.apache.org/jira/browse/HDFS-3814) | Make the replication monitor multipliers configurable in 1.x |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [HDFS-3703](https://issues.apache.org/jira/browse/HDFS-3703) | Decrease the datanode failure detection time |  Major | datanode, namenode | Nicolas Liochon | Jing Zhao |
| [HDFS-3667](https://issues.apache.org/jira/browse/HDFS-3667) | Add retry support to WebHdfsFileSystem |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3617](https://issues.apache.org/jira/browse/HDFS-3617) | Port HDFS-96 to branch-1 (support blocks greater than 2GB) |  Major | . | Matt Foley | Harsh J |
| [HDFS-3596](https://issues.apache.org/jira/browse/HDFS-3596) | Improve FSEditLog pre-allocation in branch-1 |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3516](https://issues.apache.org/jira/browse/HDFS-3516) | Check content-type in WebHdfsFileSystem |  Major | hdfs-client, webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3504](https://issues.apache.org/jira/browse/HDFS-3504) | Configurable retry in DFSClient |  Major | hdfs-client | Siddharth Seth | Tsz Wo Nicholas Sze |
| [HDFS-3131](https://issues.apache.org/jira/browse/HDFS-3131) | Improve TestStorageRestore |  Minor | . | Tsz Wo Nicholas Sze | Brandon Li |
| [HDFS-3094](https://issues.apache.org/jira/browse/HDFS-3094) | add -nonInteractive and -force option to namenode -format command |  Major | . | Arpit Gupta | Arpit Gupta |
| [HDFS-2872](https://issues.apache.org/jira/browse/HDFS-2872) | Add sanity checks during edits loading that generation stamps are non-decreasing |  Major | namenode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-2654](https://issues.apache.org/jira/browse/HDFS-2654) | Make BlockReaderLocal not extend RemoteBlockReader2 |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2653](https://issues.apache.org/jira/browse/HDFS-2653) | DFSClient should cache whether addrs are non-local when short-circuiting is enabled |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2638](https://issues.apache.org/jira/browse/HDFS-2638) | Improve a block recovery log |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-2465](https://issues.apache.org/jira/browse/HDFS-2465) | Add HDFS support for fadvise readahead and drop-behind |  Major | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-1378](https://issues.apache.org/jira/browse/HDFS-1378) | Edit log replay should track and report file offsets in case of errors |  Major | namenode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-496](https://issues.apache.org/jira/browse/HDFS-496) | Use PureJavaCrc32 in HDFS |  Minor | datanode, hdfs-client, performance | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4603](https://issues.apache.org/jira/browse/MAPREDUCE-4603) | Allow JobClient to retry job-submission when JT is in safemode |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4511](https://issues.apache.org/jira/browse/MAPREDUCE-4511) | Add IFile readahead |  Major | mrv1, mrv2, performance | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4328](https://issues.apache.org/jira/browse/MAPREDUCE-4328) | Add the option to quiesce the JobTracker |  Major | mrv1 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4001](https://issues.apache.org/jira/browse/MAPREDUCE-4001) | Improve MAPREDUCE-3789's fix logic by looking at job's slot demands instead |  Minor | capacity-sched | Harsh J | Harsh J |
| [MAPREDUCE-3597](https://issues.apache.org/jira/browse/MAPREDUCE-3597) | Provide a way to access other info of history file from Rumentool |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3395](https://issues.apache.org/jira/browse/MAPREDUCE-3395) | Add mapred.disk.healthChecker.interval to mapred-default.xml |  Trivial | documentation | Eli Collins | Eli Collins |
| [MAPREDUCE-3394](https://issues.apache.org/jira/browse/MAPREDUCE-3394) | Add log guard for a debug message in ReduceTask |  Trivial | task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3365](https://issues.apache.org/jira/browse/MAPREDUCE-3365) | Uncomment eventlog settings from the documentation |  Trivial | contrib/fair-share | Sho Shimauchi | Sho Shimauchi |
| [MAPREDUCE-3289](https://issues.apache.org/jira/browse/MAPREDUCE-3289) | Make use of fadvise in the NM's shuffle handler |  Major | mrv2, nodemanager, performance | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3278](https://issues.apache.org/jira/browse/MAPREDUCE-3278) | 0.20: avoid a busy-loop in ReduceTask scheduling |  Major | mrv1, performance, task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2919](https://issues.apache.org/jira/browse/MAPREDUCE-2919) | The JT web UI should show job start times |  Minor | jobtracker | Eli Collins | Harsh J |
| [MAPREDUCE-2836](https://issues.apache.org/jira/browse/MAPREDUCE-2836) | Provide option to fail jobs when submitted to non-existent pools. |  Minor | contrib/fair-share | Jeff Bean | Ahmed Radwan |
| [MAPREDUCE-2835](https://issues.apache.org/jira/browse/MAPREDUCE-2835) | Make per-job counter limits configurable |  Major | . | Tom White | Tom White |
| [MAPREDUCE-2103](https://issues.apache.org/jira/browse/MAPREDUCE-2103) | task-controller shouldn't require o-r permissions |  Trivial | task-controller | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1906](https://issues.apache.org/jira/browse/MAPREDUCE-1906) | Lower default minimum heartbeat interval for tasktracker \> Jobtracker |  Major | jobtracker, performance, tasktracker | Scott Carey | Todd Lipcon |
| [MAPREDUCE-782](https://issues.apache.org/jira/browse/MAPREDUCE-782) | Use PureJavaCrc32 in mapreduce spills |  Minor | performance | Todd Lipcon | Todd Lipcon |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8617](https://issues.apache.org/jira/browse/HADOOP-8617) | backport pure Java CRC32 calculator changes to branch-1 |  Major | performance | Brandon Li | Brandon Li |
| [HADOOP-8445](https://issues.apache.org/jira/browse/HADOOP-8445) | Token should not print the password in toString |  Major | security | Ravi Prakash | Ravi Prakash |
| [HADOOP-8417](https://issues.apache.org/jira/browse/HADOOP-8417) | HADOOP-6963 didn't update hadoop-core-pom-template.xml |  Major | . | Ted Yu | Ted Yu |
| [HADOOP-8399](https://issues.apache.org/jira/browse/HADOOP-8399) | Remove JDK5 dependency from Hadoop 1.0+ line |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-8329](https://issues.apache.org/jira/browse/HADOOP-8329) | Build fails with Java 7 |  Major | build | Kumar Ravi | Eli Collins |
| [HADOOP-8269](https://issues.apache.org/jira/browse/HADOOP-8269) | Fix some javadoc warnings on branch-1 |  Trivial | documentation | Eli Collins | Eli Collins |
| [HADOOP-8159](https://issues.apache.org/jira/browse/HADOOP-8159) | NetworkTopology: getLeaf should check for invalid topologies |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-7988](https://issues.apache.org/jira/browse/HADOOP-7988) | Upper case in hostname part of the principals doesn't work with kerberos. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7982](https://issues.apache.org/jira/browse/HADOOP-7982) | UserGroupInformation fails to login if thread's context classloader can't load HadoopLoginModule |  Major | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-7908](https://issues.apache.org/jira/browse/HADOOP-7908) | Fix three javadoc warnings on branch-1 |  Trivial | documentation | Eli Collins | Eli Collins |
| [HADOOP-7898](https://issues.apache.org/jira/browse/HADOOP-7898) | Fix javadoc warnings in AuthenticationToken.java |  Minor | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7879](https://issues.apache.org/jira/browse/HADOOP-7879) | DistributedFileSystem#createNonRecursive should also incrementWriteOps statistics. |  Trivial | . | Jonathan Hsieh | Jonathan Hsieh |
| [HADOOP-7870](https://issues.apache.org/jira/browse/HADOOP-7870) | fix SequenceFile#createWriter with boolean createParent arg to respect createParent. |  Major | . | Jonathan Hsieh | Jonathan Hsieh |
| [HADOOP-7745](https://issues.apache.org/jira/browse/HADOOP-7745) | I switched variable names in HADOOP-7509 |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-7666](https://issues.apache.org/jira/browse/HADOOP-7666) | branch-0.20-security doesn't include o.a.h.security.TestAuthenticationFilter |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7665](https://issues.apache.org/jira/browse/HADOOP-7665) | branch-0.20-security doesn't include SPNEGO settings in core-default.xml |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7653](https://issues.apache.org/jira/browse/HADOOP-7653) | tarball doesn't include .eclipse.templates |  Minor | build | Jonathan Natkins | Jonathan Natkins |
| [HADOOP-7634](https://issues.apache.org/jira/browse/HADOOP-7634) | Cluster setup docs specify wrong owner for task-controller.cfg |  Minor | documentation, security | Eli Collins | Eli Collins |
| [HADOOP-7629](https://issues.apache.org/jira/browse/HADOOP-7629) | regression with MAPREDUCE-2289 - setPermission passed immutable FsPermission (rpc failure) |  Major | . | Patrick Hunt | Todd Lipcon |
| [HADOOP-7621](https://issues.apache.org/jira/browse/HADOOP-7621) | alfredo config should be in a file not readable by users |  Critical | security | Alejandro Abdelnur | Aaron T. Myers |
| [HADOOP-7297](https://issues.apache.org/jira/browse/HADOOP-7297) | Error in the documentation regarding Checkpoint/Backup Node |  Trivial | documentation | arnaud p | Harsh J |
| [HADOOP-6947](https://issues.apache.org/jira/browse/HADOOP-6947) | Kerberos relogin should set refreshKrb5Config to true |  Major | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-6546](https://issues.apache.org/jira/browse/HADOOP-6546) | BloomMapFile can return false negatives |  Major | io | Clark Jefcoat | Clark Jefcoat |
| [HADOOP-6527](https://issues.apache.org/jira/browse/HADOOP-6527) | UserGroupInformation::createUserForTesting clobbers already defined group mappings |  Major | security | Jakob Homan | Ivan Mitic |
| [HADOOP-5836](https://issues.apache.org/jira/browse/HADOOP-5836) | Bug in S3N handling of directory markers using an object with a trailing "/" causes jobs to fail |  Major | fs/s3 | Ian Nowland | Ian Nowland |
| [HDFS-3966](https://issues.apache.org/jira/browse/HDFS-3966) | For branch-1, TestFileCreation should use JUnit4 to make assumeTrue work |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-3701](https://issues.apache.org/jira/browse/HDFS-3701) | HDFS may miss the final block when reading a file opened for writing if one of the datanode is dead |  Critical | hdfs-client | Nicolas Liochon | Nicolas Liochon |
| [HDFS-3698](https://issues.apache.org/jira/browse/HDFS-3698) | TestHftpFileSystem is failing in branch-1 due to changed default secure port |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3696](https://issues.apache.org/jira/browse/HDFS-3696) | Create files with WebHdfsFileSystem goes OOM when file size is big |  Critical | webhdfs | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HDFS-3551](https://issues.apache.org/jira/browse/HDFS-3551) | WebHDFS CREATE does not use client location for redirection |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3518](https://issues.apache.org/jira/browse/HDFS-3518) | Provide API to check HDFS operational state |  Major | hdfs-client | Bikas Saha | Tsz Wo Nicholas Sze |
| [HDFS-3466](https://issues.apache.org/jira/browse/HDFS-3466) | The SPNEGO filter for the NameNode should come out of the web keytab file |  Major | namenode, security | Owen O'Malley | Owen O'Malley |
| [HDFS-3461](https://issues.apache.org/jira/browse/HDFS-3461) | HFTP should use the same port & protocol for getting the delegation token |  Major | . | Owen O'Malley | Owen O'Malley |
| [HDFS-3453](https://issues.apache.org/jira/browse/HDFS-3453) | HDFS does not use ClientProtocol in a backward-compatible way |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3330](https://issues.apache.org/jira/browse/HDFS-3330) | If GetImageServlet throws an Error or RTE, response has HTTP "OK" status |  Critical | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3176](https://issues.apache.org/jira/browse/HDFS-3176) | JsonUtil should not parse the MD5MD5CRC32FileChecksum bytes on its own. |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3078](https://issues.apache.org/jira/browse/HDFS-3078) | 2NN https port setting is broken |  Major | . | Eli Collins | Eli Collins |
| [HDFS-3008](https://issues.apache.org/jira/browse/HDFS-3008) | Negative caching of local addrs doesn't work |  Major | hdfs-client | Eli Collins | Eli Collins |
| [HDFS-2877](https://issues.apache.org/jira/browse/HDFS-2877) | If locking of a storage dir fails, it will remove the other NN's lock file on exit |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2869](https://issues.apache.org/jira/browse/HDFS-2869) | Error in Webhdfs documentation for mkdir |  Minor | webhdfs | Harsh J | Harsh J |
| [HDFS-2790](https://issues.apache.org/jira/browse/HDFS-2790) | FSNamesystem.setTimes throws exception with wrong configuration name in the message |  Minor | . | Arpit Gupta | Arpit Gupta |
| [HDFS-2751](https://issues.apache.org/jira/browse/HDFS-2751) | Datanode drops OS cache behind reads even for short reads |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2741](https://issues.apache.org/jira/browse/HDFS-2741) | dfs.datanode.max.xcievers missing in 0.20.205.0 |  Minor | . | Markus Jelsma |  |
| [HDFS-2728](https://issues.apache.org/jira/browse/HDFS-2728) | Remove dfsadmin -printTopology from branch-1 docs since it does not exist |  Minor | namenode | Harsh J | Harsh J |
| [HDFS-2637](https://issues.apache.org/jira/browse/HDFS-2637) | The rpc timeout for block recovery is too low |  Major | hdfs-client | Eli Collins | Eli Collins |
| [HDFS-2547](https://issues.apache.org/jira/browse/HDFS-2547) | ReplicationTargetChooser has incorrect block placement comments |  Trivial | namenode | Harsh J | Harsh J |
| [HDFS-2541](https://issues.apache.org/jira/browse/HDFS-2541) | For a sufficiently large value of blocks, the DN Scanner may request a random number with a negative seed value. |  Major | datanode | Harsh J | Harsh J |
| [HDFS-2305](https://issues.apache.org/jira/browse/HDFS-2305) | Running multiple 2NNs can result in corrupt file system |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1910](https://issues.apache.org/jira/browse/HDFS-1910) | when dfs.name.dir and dfs.name.edits.dir are same fsimage will be saved twice every time |  Minor | namenode | Gokul |  |
| [MAPREDUCE-4698](https://issues.apache.org/jira/browse/MAPREDUCE-4698) | TestJobHistoryConfig throws Exception in testJobHistoryLogging |  Minor | . | Gopal V | Gopal V |
| [MAPREDUCE-4675](https://issues.apache.org/jira/browse/MAPREDUCE-4675) | TestKillSubProcesses fails as the process is still alive after the job is done |  Major | test | Arpit Gupta | Bikas Saha |
| [MAPREDUCE-4673](https://issues.apache.org/jira/browse/MAPREDUCE-4673) | make TestRawHistoryFile and TestJobHistoryServer more robust |  Major | test | Arpit Gupta | Arpit Gupta |
| [MAPREDUCE-4558](https://issues.apache.org/jira/browse/MAPREDUCE-4558) | TestJobTrackerSafeMode is failing |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-4400](https://issues.apache.org/jira/browse/MAPREDUCE-4400) | Fix performance regression for small jobs/workflows |  Major | performance, task | Luke Lu | Luke Lu |
| [MAPREDUCE-4241](https://issues.apache.org/jira/browse/MAPREDUCE-4241) | Pipes examples do not compile on Ubuntu 12.04 |  Major | build, examples | Andrew Bayer | Andrew Bayer |
| [MAPREDUCE-4095](https://issues.apache.org/jira/browse/MAPREDUCE-4095) | TestJobInProgress#testLocality uses a bogus topology |  Major | . | Eli Collins | Colin Patrick McCabe |
| [MAPREDUCE-4088](https://issues.apache.org/jira/browse/MAPREDUCE-4088) | Task stuck in JobLocalizer prevented other tasks on the same node from committing |  Critical | mrv1 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4087](https://issues.apache.org/jira/browse/MAPREDUCE-4087) | [Gridmix] GenerateDistCacheData job of Gridmix can become slow in some cases |  Major | . | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3789](https://issues.apache.org/jira/browse/MAPREDUCE-3789) | CapacityTaskScheduler may perform unnecessary reservations in heterogenous tracker environments |  Critical | capacity-sched, scheduler | Harsh J | Harsh J |
| [MAPREDUCE-3674](https://issues.apache.org/jira/browse/MAPREDUCE-3674) | If invoked with no queueName request param, jobqueue\_details.jsp injects a null queue name into schedulers. |  Critical | jobtracker | Harsh J | Harsh J |
| [MAPREDUCE-3419](https://issues.apache.org/jira/browse/MAPREDUCE-3419) | Don't mark exited TT threads as dead in MiniMRCluster |  Major | tasktracker, test | Eli Collins | Eli Collins |
| [MAPREDUCE-3405](https://issues.apache.org/jira/browse/MAPREDUCE-3405) | MAPREDUCE-3015 broke compilation of contrib scheduler tests |  Critical | capacity-sched, contrib/fair-share | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2932](https://issues.apache.org/jira/browse/MAPREDUCE-2932) | Missing instrumentation plugin class shouldn't crash the TT startup per design |  Trivial | tasktracker | Harsh J | Harsh J |
| [MAPREDUCE-2905](https://issues.apache.org/jira/browse/MAPREDUCE-2905) | CapBasedLoadManager incorrectly allows assignment when assignMultiple is true (was: assignmultiple per job) |  Major | contrib/fair-share | Jeff Bean | Jeff Bean |
| [MAPREDUCE-2903](https://issues.apache.org/jira/browse/MAPREDUCE-2903) | Map Tasks graph is throwing XML Parse error when Job is executed with 0 maps |  Major | jobtracker | Devaraj K | Devaraj K |
| [MAPREDUCE-2806](https://issues.apache.org/jira/browse/MAPREDUCE-2806) | [Gridmix] Load job fails with timeout errors when resource emulation is turned on |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-2377](https://issues.apache.org/jira/browse/MAPREDUCE-2377) | task-controller fails to parse configuration if it doesn't end in \n |  Major | task-controller | Todd Lipcon | Benoy Antony |
| [MAPREDUCE-2129](https://issues.apache.org/jira/browse/MAPREDUCE-2129) | Job may hang if mapreduce.job.committer.setup.cleanup.needed=false and mapreduce.map/reduce.failures.maxpercent\>0 |  Major | jobtracker | Kang Xiao | Subroto Sanyal |
| [MAPREDUCE-1740](https://issues.apache.org/jira/browse/MAPREDUCE-1740) | NPE in getMatchingLevelForNodes when node locations are variable depth |  Major | jobtracker | Todd Lipcon | Ahmed Radwan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7942](https://issues.apache.org/jira/browse/HADOOP-7942) | enabling clover coverage reports fails hadoop unit test compilation |  Major | . | Giridharan Kesavan | Jitendra Nath Pandey |
| [HDFS-3129](https://issues.apache.org/jira/browse/HDFS-3129) | NetworkTopology: add test that getLeaf should check for invalid topologies |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-2332](https://issues.apache.org/jira/browse/HDFS-2332) | Add test for HADOOP-7629: using an immutable FsPermission as an IPC parameter |  Major | test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2073](https://issues.apache.org/jira/browse/MAPREDUCE-2073) | TestTrackerDistributedCacheManager should be up-front about requirements on build environment |  Trivial | distributed-cache, test | Todd Lipcon | Todd Lipcon |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7753](https://issues.apache.org/jira/browse/HADOOP-7753) | Support fadvise and sync\_data\_range in NativeIO, add ReadaheadPool class |  Major | io, native, performance | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3424](https://issues.apache.org/jira/browse/MAPREDUCE-3424) | Some LinuxTaskController cleanup |  Minor | tasktracker | Eli Collins | Eli Collins |
| [MAPREDUCE-3015](https://issues.apache.org/jira/browse/MAPREDUCE-3015) | Add local dir failure info to metrics and the web UI |  Major | tasktracker | Eli Collins | Eli Collins |
| [MAPREDUCE-3008](https://issues.apache.org/jira/browse/MAPREDUCE-3008) | [Gridmix] Improve cumulative CPU usage emulation for short running tasks |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-2957](https://issues.apache.org/jira/browse/MAPREDUCE-2957) | The TT should not re-init if it has no good local dirs |  Major | tasktracker | Eli Collins | Eli Collins |
| [MAPREDUCE-2850](https://issues.apache.org/jira/browse/MAPREDUCE-2850) | Add test for TaskTracker disk failure handling (MR-2413) |  Major | tasktracker | Eli Collins | Ravi Gummadi |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-2517](https://issues.apache.org/jira/browse/MAPREDUCE-2517) | Porting Gridmix v3 system tests into trunk branch. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |


