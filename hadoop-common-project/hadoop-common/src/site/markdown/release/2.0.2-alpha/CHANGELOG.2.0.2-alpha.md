
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

## Release 2.0.2-alpha - 2012-10-09

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8794](https://issues.apache.org/jira/browse/HADOOP-8794) | Modifiy bin/hadoop to point to HADOOP\_YARN\_HOME |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-8710](https://issues.apache.org/jira/browse/HADOOP-8710) | Remove ability for users to easily run the trash emptier |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-8689](https://issues.apache.org/jira/browse/HADOOP-8689) | Make trash a server side configuration option |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-8551](https://issues.apache.org/jira/browse/HADOOP-8551) | fs -mkdir creates parent directories without the -p option |  Major | fs | Robert Joseph Evans | John George |
| [HADOOP-8458](https://issues.apache.org/jira/browse/HADOOP-8458) | Add management hook to AuthenticationHandler to enable delegation token operations support |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8388](https://issues.apache.org/jira/browse/HADOOP-8388) | Remove unused BlockLocation serialization |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8368](https://issues.apache.org/jira/browse/HADOOP-8368) | Use CMake rather than autotools to build native code |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3675](https://issues.apache.org/jira/browse/HDFS-3675) | libhdfs: follow documented return codes |  Minor | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3522](https://issues.apache.org/jira/browse/HDFS-3522) | If NN is in safemode, it should throw SafeModeException when getBlockLocations has zero locations |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-3446](https://issues.apache.org/jira/browse/HDFS-3446) | HostsFileReader silently ignores bad includes/excludes |  Major | namenode | Matthew Jacobs | Matthew Jacobs |
| [HDFS-3318](https://issues.apache.org/jira/browse/HDFS-3318) | Hftp hangs on transfers \>2GB |  Blocker | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-2727](https://issues.apache.org/jira/browse/HDFS-2727) | libhdfs should get the default block size from the server |  Minor | libhdfs | Sho Shimauchi | Colin Patrick McCabe |
| [HDFS-2686](https://issues.apache.org/jira/browse/HDFS-2686) | Remove DistributedUpgrade related code |  Major | datanode, namenode | Todd Lipcon | Suresh Srinivas |
| [HDFS-2617](https://issues.apache.org/jira/browse/HDFS-2617) | Replaced Kerberized SSL for image transfer and fsck with SPNEGO-based solution |  Major | security | Jakob Homan | Jakob Homan |
| [MAPREDUCE-4629](https://issues.apache.org/jira/browse/MAPREDUCE-4629) | Remove JobHistory.DEBUG\_MODE |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4311](https://issues.apache.org/jira/browse/MAPREDUCE-4311) | Capacity scheduler.xml does not accept decimal values for capacity and maximum-capacity settings |  Major | capacity-sched, mrv2 | Thomas Graves | Karthik Kambatla |
| [MAPREDUCE-4072](https://issues.apache.org/jira/browse/MAPREDUCE-4072) | User set java.library.path seems to overwrite default creating problems native lib loading |  Major | mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-3812](https://issues.apache.org/jira/browse/MAPREDUCE-3812) | Lower default allocation sizes, fix allocation configurations and document them |  Major | mrv2, performance | Vinod Kumar Vavilapalli | Harsh J |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8644](https://issues.apache.org/jira/browse/HADOOP-8644) | AuthenticatedURL should be able to use SSLFactory |  Critical | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8581](https://issues.apache.org/jira/browse/HADOOP-8581) | add support for HTTPS to the web UIs |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8465](https://issues.apache.org/jira/browse/HADOOP-8465) | hadoop-auth should support ephemeral authentication |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8135](https://issues.apache.org/jira/browse/HADOOP-8135) | Add ByteBufferReadable interface to FSDataInputStream |  Major | fs | Henry Robinson | Henry Robinson |
| [HDFS-3637](https://issues.apache.org/jira/browse/HDFS-3637) | Add support for encrypting the DataTransferProtocol |  Major | datanode, hdfs-client, security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3535](https://issues.apache.org/jira/browse/HDFS-3535) | Audit logging should log denied accesses |  Major | namenode | Andy Isaacson | Andy Isaacson |
| [HDFS-3150](https://issues.apache.org/jira/browse/HDFS-3150) | Add option for clients to contact DNs via hostname |  Major | datanode, hdfs-client | Eli Collins | Eli Collins |
| [HDFS-3113](https://issues.apache.org/jira/browse/HDFS-3113) | httpfs does not support delegation tokens |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3042](https://issues.apache.org/jira/browse/HDFS-3042) | Automatic failover support for NN HA |  Major | auto-failover, ha | Todd Lipcon | Todd Lipcon |
| [HDFS-2978](https://issues.apache.org/jira/browse/HDFS-2978) | The NameNode should expose name dir statuses via JMX |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-2793](https://issues.apache.org/jira/browse/HDFS-2793) | Add an admin command to trigger an edit log roll |  Major | namenode | Aaron T. Myers | Todd Lipcon |
| [HDFS-744](https://issues.apache.org/jira/browse/HDFS-744) | Support hsync in HDFS |  Major | datanode, hdfs-client | Hairong Kuang | Lars Hofhansl |
| [MAPREDUCE-4417](https://issues.apache.org/jira/browse/MAPREDUCE-4417) | add support for encrypted shuffle |  Major | mrv2, security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4355](https://issues.apache.org/jira/browse/MAPREDUCE-4355) | Add RunningJob.getJobStatus() |  Major | mrv1, mrv2 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-3773](https://issues.apache.org/jira/browse/MAPREDUCE-3773) | Add queue metrics with buckets for job run times |  Major | jobtracker | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-3451](https://issues.apache.org/jira/browse/MAPREDUCE-3451) | Port Fair Scheduler to MR2 |  Major | mrv2, scheduler | NO NAME | NO NAME |
| [MAPREDUCE-987](https://issues.apache.org/jira/browse/MAPREDUCE-987) | Exposing MiniDFS and MiniMR clusters as a single process command-line |  Minor | build, test | Philip Zeyliger | Ahmed Radwan |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8754](https://issues.apache.org/jira/browse/HADOOP-8754) | Deprecate all the RPC.getServer() variants |  Minor | ipc | Brandon Li | Brandon Li |
| [HADOOP-8748](https://issues.apache.org/jira/browse/HADOOP-8748) | Move dfsclient retry to a util class |  Minor | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-8700](https://issues.apache.org/jira/browse/HADOOP-8700) | Move the checksum type constants to an enum |  Minor | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-8687](https://issues.apache.org/jira/browse/HADOOP-8687) | Upgrade log4j to 1.2.17 |  Minor | . | Eli Collins | Eli Collins |
| [HADOOP-8635](https://issues.apache.org/jira/browse/HADOOP-8635) | Cannot cancel paths registered deleteOnExit |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8624](https://issues.apache.org/jira/browse/HADOOP-8624) | ProtobufRpcEngine should log all RPCs if TRACE logging is enabled |  Minor | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-8623](https://issues.apache.org/jira/browse/HADOOP-8623) | hadoop jar command should respect HADOOP\_OPTS |  Minor | scripts | Steven Willis | Steven Willis |
| [HADOOP-8620](https://issues.apache.org/jira/browse/HADOOP-8620) | Add -Drequire.fuse and -Drequire.snappy |  Minor | build | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8609](https://issues.apache.org/jira/browse/HADOOP-8609) | IPC server logs a useless message when shutting down socket |  Major | . | Todd Lipcon | Jon Zuanich |
| [HADOOP-8541](https://issues.apache.org/jira/browse/HADOOP-8541) | Better high-percentile latency metrics |  Major | metrics | Andrew Wang | Andrew Wang |
| [HADOOP-8535](https://issues.apache.org/jira/browse/HADOOP-8535) | Cut hadoop build times in half (upgrade maven-compiler-plugin to 2.5.1) |  Major | build | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-8533](https://issues.apache.org/jira/browse/HADOOP-8533) | Remove Parallel Call in IPC |  Major | ipc | Suresh Srinivas | Brandon Li |
| [HADOOP-8531](https://issues.apache.org/jira/browse/HADOOP-8531) | SequenceFile Writer can throw out a better error if a serializer or deserializer isn't available |  Trivial | io | Harsh J | madhukara phatak |
| [HADOOP-8525](https://issues.apache.org/jira/browse/HADOOP-8525) | Provide Improved Traceability for Configuration |  Trivial | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8524](https://issues.apache.org/jira/browse/HADOOP-8524) | Allow users to get source of a Configuration parameter |  Trivial | conf | Harsh J | Harsh J |
| [HADOOP-8463](https://issues.apache.org/jira/browse/HADOOP-8463) | hadoop.security.auth\_to\_local needs a key definition and doc |  Major | security | Eli Collins | madhukara phatak |
| [HADOOP-8398](https://issues.apache.org/jira/browse/HADOOP-8398) | Cleanup BlockLocation |  Minor | . | Eli Collins | Eli Collins |
| [HADOOP-8373](https://issues.apache.org/jira/browse/HADOOP-8373) | Port RPC.getServerAddress to 0.23 |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-8367](https://issues.apache.org/jira/browse/HADOOP-8367) | Improve documentation of declaringClassProtocolName in rpc headers |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-8362](https://issues.apache.org/jira/browse/HADOOP-8362) | Improve exception message when Configuration.set() is called with a null key or value |  Trivial | conf | Todd Lipcon | madhukara phatak |
| [HADOOP-8361](https://issues.apache.org/jira/browse/HADOOP-8361) | Avoid out-of-memory problems when deserializing strings |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8358](https://issues.apache.org/jira/browse/HADOOP-8358) | Config-related WARN for dfs.web.ugi can be avoided. |  Trivial | conf | Harsh J | Harsh J |
| [HADOOP-8340](https://issues.apache.org/jira/browse/HADOOP-8340) | SNAPSHOT build versions should compare as less than their eventual final release |  Minor | util | Todd Lipcon | Todd Lipcon |
| [HADOOP-8335](https://issues.apache.org/jira/browse/HADOOP-8335) | Improve Configuration's address handling |  Major | util | Daryn Sharp | Daryn Sharp |
| [HADOOP-8323](https://issues.apache.org/jira/browse/HADOOP-8323) | Revert HADOOP-7940 and improve javadocs and test for Text.clear() |  Critical | io | Harsh J | Harsh J |
| [HADOOP-8286](https://issues.apache.org/jira/browse/HADOOP-8286) | Simplify getting a socket address from conf |  Major | conf | Daryn Sharp | Daryn Sharp |
| [HADOOP-8278](https://issues.apache.org/jira/browse/HADOOP-8278) | Make sure components declare correct set of dependencies |  Major | build | Tom White | Tom White |
| [HADOOP-8244](https://issues.apache.org/jira/browse/HADOOP-8244) | Improve comments on ByteBufferReadable.read |  Major | . | Henry Robinson | Henry Robinson |
| [HADOOP-8242](https://issues.apache.org/jira/browse/HADOOP-8242) | AbstractDelegationTokenIdentifier: add getter methods for owner and realuser |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8240](https://issues.apache.org/jira/browse/HADOOP-8240) | Allow users to specify a checksum type on create() |  Major | fs | Kihwal Lee | Kihwal Lee |
| [HADOOP-8239](https://issues.apache.org/jira/browse/HADOOP-8239) | Extend MD5MD5CRC32FileChecksum to show the actual checksum type being used |  Major | fs | Kihwal Lee | Kihwal Lee |
| [HADOOP-8227](https://issues.apache.org/jira/browse/HADOOP-8227) | Allow RPC to limit ephemeral port range. |  Blocker | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8224](https://issues.apache.org/jira/browse/HADOOP-8224) | Don't hardcode hdfs.audit.logger in the scripts |  Major | conf | Eli Collins | Tomohiko Kinebuchi |
| [HADOOP-8075](https://issues.apache.org/jira/browse/HADOOP-8075) | Lower native-hadoop library log from info to debug |  Major | native | Eli Collins | Hızır Sefa İrken |
| [HADOOP-7510](https://issues.apache.org/jira/browse/HADOOP-7510) | Tokens should use original hostname provided instead of ip |  Major | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-6802](https://issues.apache.org/jira/browse/HADOOP-6802) | Remove FS\_CLIENT\_BUFFER\_DIR\_KEY = "fs.client.buffer.dir" from CommonConfigurationKeys.java (not used, deprecated) |  Major | conf, fs | Erik Steffl | Sho Shimauchi |
| [HADOOP-3450](https://issues.apache.org/jira/browse/HADOOP-3450) | Add tests to Local Directory Allocator for asserting their URI-returning capability |  Minor | fs | Ari Rabkin | Sho Shimauchi |
| [HDFS-3907](https://issues.apache.org/jira/browse/HDFS-3907) | Allow multiple users for local block readers |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-3887](https://issues.apache.org/jira/browse/HDFS-3887) | Remove redundant chooseTarget methods in BlockPlacementPolicy.java |  Trivial | namenode | Jing Zhao | Jing Zhao |
| [HDFS-3871](https://issues.apache.org/jira/browse/HDFS-3871) | Change NameNodeProxies to use HADOOP-8748 |  Minor | hdfs-client | Arun C Murthy | Arun C Murthy |
| [HDFS-3866](https://issues.apache.org/jira/browse/HDFS-3866) | HttpFS POM should have property where to download tomcat from |  Minor | build | Ryan Hennig | Plamen Jeliazkov |
| [HDFS-3844](https://issues.apache.org/jira/browse/HDFS-3844) | Add @Override where necessary and remove unnecessary {@inheritdoc} and imports |  Trivial | . | Jing Zhao | Jing Zhao |
| [HDFS-3819](https://issues.apache.org/jira/browse/HDFS-3819) | Should check whether invalidate work percentage default value is not greater than 1.0f |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-3802](https://issues.apache.org/jira/browse/HDFS-3802) | StartupOption.name in HdfsServerConstants should be final |  Trivial | . | Jing Zhao | Jing Zhao |
| [HDFS-3796](https://issues.apache.org/jira/browse/HDFS-3796) | Speed up edit log tests by avoiding fsync() |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HDFS-3765](https://issues.apache.org/jira/browse/HDFS-3765) | Namenode INITIALIZESHAREDEDITS should be able to initialize all shared storages |  Major | ha | Vinayakumar B | Vinayakumar B |
| [HDFS-3723](https://issues.apache.org/jira/browse/HDFS-3723) | All commands should support meaningful --help |  Major | scripts, tools | E. Sammer | Jing Zhao |
| [HDFS-3711](https://issues.apache.org/jira/browse/HDFS-3711) | Manually convert remaining tests to JUnit4 |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-3697](https://issues.apache.org/jira/browse/HDFS-3697) | Enable fadvise readahead by default |  Minor | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-3672](https://issues.apache.org/jira/browse/HDFS-3672) | Expose disk-location information for blocks to enable better scheduling |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-3666](https://issues.apache.org/jira/browse/HDFS-3666) | Plumb more exception messages to terminate |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3663](https://issues.apache.org/jira/browse/HDFS-3663) | MiniDFSCluster should capture the code path that led to the first ExitException |  Major | test | Eli Collins | Eli Collins |
| [HDFS-3659](https://issues.apache.org/jira/browse/HDFS-3659) | Add missing @Override to methods across the hadoop-hdfs project |  Minor | documentation | Brandon Li | Brandon Li |
| [HDFS-3650](https://issues.apache.org/jira/browse/HDFS-3650) | Use MutableQuantiles to provide latency histograms for various operations |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-3641](https://issues.apache.org/jira/browse/HDFS-3641) | Move server Util time methods to common and use now instead of System#currentTimeMillis |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3613](https://issues.apache.org/jira/browse/HDFS-3613) | GSet prints some INFO level values, which aren't really very useful to all |  Trivial | namenode | Harsh J | Andrew Wang |
| [HDFS-3612](https://issues.apache.org/jira/browse/HDFS-3612) | Single namenode image directory config warning can be improved |  Trivial | namenode | Harsh J | Andy Isaacson |
| [HDFS-3610](https://issues.apache.org/jira/browse/HDFS-3610) | fuse\_dfs: Provide a way to use the default (configured) NN URI |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3607](https://issues.apache.org/jira/browse/HDFS-3607) | log a message when fuse\_dfs is not built |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3604](https://issues.apache.org/jira/browse/HDFS-3604) | Add dfs.webhdfs.enabled to hdfs-default.xml |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3583](https://issues.apache.org/jira/browse/HDFS-3583) | Convert remaining tests to Junit4 |  Major | test | Eli Collins | Andrew Wang |
| [HDFS-3582](https://issues.apache.org/jira/browse/HDFS-3582) | Hook daemon process exit for testing |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-3568](https://issues.apache.org/jira/browse/HDFS-3568) | fuse\_dfs: add support for security |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3537](https://issues.apache.org/jira/browse/HDFS-3537) | Move libhdfs and fuse-dfs source to native subdirectories |  Minor | fuse-dfs, libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3520](https://issues.apache.org/jira/browse/HDFS-3520) | Add transfer rate logging to TransferFsImage |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-3516](https://issues.apache.org/jira/browse/HDFS-3516) | Check content-type in WebHdfsFileSystem |  Major | hdfs-client, webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3514](https://issues.apache.org/jira/browse/HDFS-3514) | Add missing TestParallelLocalRead |  Major | test | Henry Robinson | Henry Robinson |
| [HDFS-3513](https://issues.apache.org/jira/browse/HDFS-3513) | HttpFS should cache filesystems |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3504](https://issues.apache.org/jira/browse/HDFS-3504) | Configurable retry in DFSClient |  Major | hdfs-client | Siddharth Seth | Tsz Wo Nicholas Sze |
| [HDFS-3481](https://issues.apache.org/jira/browse/HDFS-3481) | Refactor HttpFS handling of JAX-RS query string parameters |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3475](https://issues.apache.org/jira/browse/HDFS-3475) | Make the replication and invalidation rates configurable |  Trivial | . | Harsh J | Harsh J |
| [HDFS-3454](https://issues.apache.org/jira/browse/HDFS-3454) | Balancer unconditionally logs InterruptedException at INFO level on shutdown if security is enabled |  Minor | balancer & mover | Eli Collins | Eli Collins |
| [HDFS-3438](https://issues.apache.org/jira/browse/HDFS-3438) | BootstrapStandby should not require a rollEdits on active node |  Major | ha | Todd Lipcon | Todd Lipcon |
| [HDFS-3419](https://issues.apache.org/jira/browse/HDFS-3419) | Cleanup LocatedBlock |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3417](https://issues.apache.org/jira/browse/HDFS-3417) | Rename BalancerDatanode#getName to getDisplayName to be consistent with Datanode |  Minor | datanode | Eli Collins | Eli Collins |
| [HDFS-3416](https://issues.apache.org/jira/browse/HDFS-3416) | Cleanup DatanodeID and DatanodeRegistration constructors used by testing |  Minor | datanode | Eli Collins | Eli Collins |
| [HDFS-3404](https://issues.apache.org/jira/browse/HDFS-3404) | Make putImage in GetImageServlet infer remote address to fetch from request |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3401](https://issues.apache.org/jira/browse/HDFS-3401) | Cleanup DatanodeDescriptor creation in the tests |  Major | datanode, test | Eli Collins | Eli Collins |
| [HDFS-3400](https://issues.apache.org/jira/browse/HDFS-3400) | DNs should be able start with jsvc even if security is disabled |  Major | datanode, scripts | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3394](https://issues.apache.org/jira/browse/HDFS-3394) | Do not use generic in INodeFile.getLastBlock() |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3390](https://issues.apache.org/jira/browse/HDFS-3390) | DFSAdmin should print full stack traces of errors when DEBUG logging is enabled |  Minor | hdfs-client | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3372](https://issues.apache.org/jira/browse/HDFS-3372) | offlineEditsViewer should be able to read a binary edits file with recovery mode |  Minor | tools | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3343](https://issues.apache.org/jira/browse/HDFS-3343) | Improve metrics for DN read latency |  Major | datanode | Todd Lipcon | Andrew Wang |
| [HDFS-3341](https://issues.apache.org/jira/browse/HDFS-3341) | Change minimum RPC versions to 2.0.0-SNAPSHOT instead of 2.0.0 |  Minor | . | Todd Lipcon | Todd Lipcon |
| [HDFS-3335](https://issues.apache.org/jira/browse/HDFS-3335) | check for edit log corruption at the end of the log |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3276](https://issues.apache.org/jira/browse/HDFS-3276) | initializeSharedEdits should have a -nonInteractive flag |  Minor | ha, namenode | Vinithra Varadharajan | Todd Lipcon |
| [HDFS-3230](https://issues.apache.org/jira/browse/HDFS-3230) | Cleanup DatanodeID creation in the tests |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-3227](https://issues.apache.org/jira/browse/HDFS-3227) | Mavenise libhdfs tests |  Major | libhdfs | Henry Robinson |  |
| [HDFS-3170](https://issues.apache.org/jira/browse/HDFS-3170) | Add more useful metrics for write latency |  Major | datanode | Todd Lipcon | Matthew Jacobs |
| [HDFS-3134](https://issues.apache.org/jira/browse/HDFS-3134) | Harden edit log loader against malformed or malicious input |  Major | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3110](https://issues.apache.org/jira/browse/HDFS-3110) | libhdfs implementation of direct read API |  Major | libhdfs, performance | Henry Robinson | Henry Robinson |
| [HDFS-3040](https://issues.apache.org/jira/browse/HDFS-3040) | TestMulitipleNNDataBlockScanner is misspelled |  Trivial | test | Aaron T. Myers | madhukara phatak |
| [HDFS-3002](https://issues.apache.org/jira/browse/HDFS-3002) | TestNameNodeMetrics need not wait for metrics update with new metrics framework |  Trivial | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2988](https://issues.apache.org/jira/browse/HDFS-2988) | Improve error message when storage directory lock fails |  Minor | namenode | Todd Lipcon | Miomir Boljanovic |
| [HDFS-2885](https://issues.apache.org/jira/browse/HDFS-2885) | Remove "federation" from the nameservice config options |  Major | namenode | Eli Collins | Tsz Wo Nicholas Sze |
| [HDFS-2834](https://issues.apache.org/jira/browse/HDFS-2834) | ByteBuffer-based read API for DFSInputStream |  Major | hdfs-client, performance | Henry Robinson | Henry Robinson |
| [HDFS-2652](https://issues.apache.org/jira/browse/HDFS-2652) | Port token service changes from 205 |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-2421](https://issues.apache.org/jira/browse/HDFS-2421) | Improve the concurrency of  SerialNumberMap in NameNode |  Major | namenode | Hairong Kuang | Jing Zhao |
| [HDFS-2391](https://issues.apache.org/jira/browse/HDFS-2391) | Newly set BalancerBandwidth value is not displayed anywhere |  Major | balancer & mover | Rajit Saha | Harsh J |
| [HDFS-1013](https://issues.apache.org/jira/browse/HDFS-1013) | Miscellaneous improvements to HTML markup for web UIs |  Minor | . | Todd Lipcon | Eugene Koontz |
| [HDFS-799](https://issues.apache.org/jira/browse/HDFS-799) | libhdfs must call DetachCurrentThread when a thread is destroyed |  Major | . | Christian Kunz | Colin Patrick McCabe |
| [MAPREDUCE-4638](https://issues.apache.org/jira/browse/MAPREDUCE-4638) | MR AppMaster shouldn't rely on YARN\_APPLICATION\_CLASSPATH providing MR jars |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4614](https://issues.apache.org/jira/browse/MAPREDUCE-4614) | Simplify debugging a job's tokens |  Major | client, task | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4511](https://issues.apache.org/jira/browse/MAPREDUCE-4511) | Add IFile readahead |  Major | mrv1, mrv2, performance | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4422](https://issues.apache.org/jira/browse/MAPREDUCE-4422) | YARN\_APPLICATION\_CLASSPATH needs a documented default value in YarnConfiguration |  Major | nodemanager | Arun C Murthy | Ahmed Radwan |
| [MAPREDUCE-4408](https://issues.apache.org/jira/browse/MAPREDUCE-4408) | allow jobs to set a JAR that is in the distributed cached |  Major | mrv1, mrv2 | Alejandro Abdelnur | Robert Kanter |
| [MAPREDUCE-4375](https://issues.apache.org/jira/browse/MAPREDUCE-4375) | Show Configuration Tracability in MR UI |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4301](https://issues.apache.org/jira/browse/MAPREDUCE-4301) | Dedupe some strings in MRAM for memory savings |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4283](https://issues.apache.org/jira/browse/MAPREDUCE-4283) | Display tail of aggregated logs by default |  Major | jobhistoryserver, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4274](https://issues.apache.org/jira/browse/MAPREDUCE-4274) | MapOutputBuffer should use native byte order for kvmeta |  Minor | performance, task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4210](https://issues.apache.org/jira/browse/MAPREDUCE-4210) | Expose listener address for WebApp |  Major | webapps | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4205](https://issues.apache.org/jira/browse/MAPREDUCE-4205) | retrofit all JVM shutdown hooks to use ShutdownHookManager |  Major | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4190](https://issues.apache.org/jira/browse/MAPREDUCE-4190) |  Improve web UI for task attempts userlog link |  Major | mrv2, webapps | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4157](https://issues.apache.org/jira/browse/MAPREDUCE-4157) | ResourceManager should not kill apps that are well behaved |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4151](https://issues.apache.org/jira/browse/MAPREDUCE-4151) | RM scheduler web page should filter apps to those that are relevant to scheduling |  Major | mrv2, webapps | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4146](https://issues.apache.org/jira/browse/MAPREDUCE-4146) | Support limits on task status string length and number of block locations in branch-2 |  Major | . | Tom White | Ahmed Radwan |
| [MAPREDUCE-4079](https://issues.apache.org/jira/browse/MAPREDUCE-4079) | Allow MR AppMaster to limit ephemeral port range. |  Blocker | mr-am, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4059](https://issues.apache.org/jira/browse/MAPREDUCE-4059) | The history server should have a separate pluggable storage/query interface |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4017](https://issues.apache.org/jira/browse/MAPREDUCE-4017) | Add jobname to jobsummary log |  Trivial | jobhistoryserver, jobtracker | Koji Noguchi | Thomas Graves |
| [MAPREDUCE-3921](https://issues.apache.org/jira/browse/MAPREDUCE-3921) | MR AM should act on the nodes liveliness information when nodes go up/down/unhealthy |  Major | mr-am, mrv2 | Vinod Kumar Vavilapalli | Bikas Saha |
| [MAPREDUCE-3907](https://issues.apache.org/jira/browse/MAPREDUCE-3907) | Document entries mapred-default.xml for the jobhistory server. |  Minor | documentation | Eugene Koontz | Eugene Koontz |
| [MAPREDUCE-3906](https://issues.apache.org/jira/browse/MAPREDUCE-3906) | Fix inconsistency in documentation regarding mapreduce.jobhistory.principal |  Trivial | documentation | Eugene Koontz | Eugene Koontz |
| [MAPREDUCE-3871](https://issues.apache.org/jira/browse/MAPREDUCE-3871) | Allow symlinking in LocalJobRunner DistributedCache |  Major | distributed-cache | Tom White | Tom White |
| [MAPREDUCE-3850](https://issues.apache.org/jira/browse/MAPREDUCE-3850) | Avoid redundant calls for tokens in TokenCache |  Major | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3842](https://issues.apache.org/jira/browse/MAPREDUCE-3842) | stop webpages from automatic refreshing |  Critical | mrv2, webapps | Alejandro Abdelnur | Thomas Graves |
| [MAPREDUCE-3659](https://issues.apache.org/jira/browse/MAPREDUCE-3659) | Host-based token support |  Major | security | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3289](https://issues.apache.org/jira/browse/MAPREDUCE-3289) | Make use of fadvise in the NM's shuffle handler |  Major | mrv2, nodemanager, performance | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2786](https://issues.apache.org/jira/browse/MAPREDUCE-2786) | TestDFSIO should also test compression reading/writing from command-line. |  Minor | benchmarks | Plamen Jeliazkov | Plamen Jeliazkov |
| [YARN-420](https://issues.apache.org/jira/browse/YARN-420) | Enable the RM to work with AM's that are not managed by it |  Major | applications/unmanaged-AM-launcher | Bikas Saha | Bikas Saha |
| [YARN-419](https://issues.apache.org/jira/browse/YARN-419) | Add client side for Unmanaged-AMs |  Major | applications/unmanaged-AM-launcher | Bikas Saha | Bikas Saha |
| [YARN-137](https://issues.apache.org/jira/browse/YARN-137) | Change the default scheduler to the CapacityScheduler |  Major | scheduler | Siddharth Seth | Siddharth Seth |
| [YARN-80](https://issues.apache.org/jira/browse/YARN-80) | Support delay scheduling for node locality in MR2's capacity scheduler |  Major | capacityscheduler | Todd Lipcon | Arun C Murthy |
| [YARN-10](https://issues.apache.org/jira/browse/YARN-10) | dist-shell shouldn't have a (test) dependency on hadoop-mapreduce-client-core |  Major | . | Arun C Murthy | Hitesh Shah |
| [YARN-9](https://issues.apache.org/jira/browse/YARN-9) | Rename YARN\_HOME to HADOOP\_YARN\_HOME |  Major | . | Arun C Murthy | Vinod Kumar Vavilapalli |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8801](https://issues.apache.org/jira/browse/HADOOP-8801) | ExitUtil#terminate should capture the exception stack trace |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-8781](https://issues.apache.org/jira/browse/HADOOP-8781) | hadoop-config.sh should add JAVA\_LIBRARY\_PATH to LD\_LIBRARY\_PATH |  Major | scripts | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8775](https://issues.apache.org/jira/browse/HADOOP-8775) | MR2 distcp permits non-positive value to -bandwidth option which causes job never to complete |  Major | . | Sandy Ryza | Sandy Ryza |
| [HADOOP-8770](https://issues.apache.org/jira/browse/HADOOP-8770) | NN should not RPC to self to find trash defaults (causes deadlock) |  Blocker | trash | Todd Lipcon | Eli Collins |
| [HADOOP-8766](https://issues.apache.org/jira/browse/HADOOP-8766) | FileContextMainOperationsBaseTest should randomize the root dir |  Major | test | Eli Collins | Colin Patrick McCabe |
| [HADOOP-8764](https://issues.apache.org/jira/browse/HADOOP-8764) | CMake: HADOOP-8737 broke ARM build |  Major | build | Trevor Robinson | Trevor Robinson |
| [HADOOP-8749](https://issues.apache.org/jira/browse/HADOOP-8749) | HADOOP-8031 changed the way in which relative xincludes are handled in Configuration. |  Major | conf | Ahmed Radwan | Ahmed Radwan |
| [HADOOP-8747](https://issues.apache.org/jira/browse/HADOOP-8747) | Syntax error on cmake version 2.6 patch 2 in JNIFlags.cmake |  Major | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8737](https://issues.apache.org/jira/browse/HADOOP-8737) | cmake: always use JAVA\_HOME to find libjvm.so, jni.h, jni\_md.h |  Minor | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8727](https://issues.apache.org/jira/browse/HADOOP-8727) | Gracefully deprecate dfs.umaskmode in 2.x onwards |  Major | conf | Harsh J | Harsh J |
| [HADOOP-8726](https://issues.apache.org/jira/browse/HADOOP-8726) | The Secrets in Credentials are not available to MR tasks |  Major | security | Benoy Antony | Daryn Sharp |
| [HADOOP-8725](https://issues.apache.org/jira/browse/HADOOP-8725) | MR is broken when security is off |  Blocker | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-8722](https://issues.apache.org/jira/browse/HADOOP-8722) | Update BUILDING.txt with latest snappy info |  Minor | documentation | Eli Collins | Colin Patrick McCabe |
| [HADOOP-8721](https://issues.apache.org/jira/browse/HADOOP-8721) | ZKFC should not retry 45 times when attempting a graceful fence during a failover |  Critical | auto-failover, ha | suja s | Vinayakumar B |
| [HADOOP-8720](https://issues.apache.org/jira/browse/HADOOP-8720) | TestLocalFileSystem should use test root subdirectory |  Trivial | test | Vlad Rozov | Vlad Rozov |
| [HADOOP-8709](https://issues.apache.org/jira/browse/HADOOP-8709) | globStatus changed behavior from 0.20/1.x |  Critical | fs | Jason Lowe | Jason Lowe |
| [HADOOP-8703](https://issues.apache.org/jira/browse/HADOOP-8703) | distcpV2: turn CRC checking off for 0 byte size |  Major | . | Dave Thompson | Dave Thompson |
| [HADOOP-8699](https://issues.apache.org/jira/browse/HADOOP-8699) | some common testcases create core-site.xml in test-classes making other testcases to fail |  Critical | test | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8697](https://issues.apache.org/jira/browse/HADOOP-8697) | TestWritableName fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8695](https://issues.apache.org/jira/browse/HADOOP-8695) | TestPathData fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8693](https://issues.apache.org/jira/browse/HADOOP-8693) | TestSecurityUtil fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8692](https://issues.apache.org/jira/browse/HADOOP-8692) | TestLocalDirAllocator fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8686](https://issues.apache.org/jira/browse/HADOOP-8686) | Fix warnings in native code |  Minor | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8660](https://issues.apache.org/jira/browse/HADOOP-8660) | TestPseudoAuthenticator failing with NPE |  Major | . | Eli Collins | Alejandro Abdelnur |
| [HADOOP-8659](https://issues.apache.org/jira/browse/HADOOP-8659) | Native libraries must build with soft-float ABI for Oracle JVM on ARM |  Major | native | Trevor Robinson | Colin Patrick McCabe |
| [HADOOP-8655](https://issues.apache.org/jira/browse/HADOOP-8655) | In TextInputFormat, while specifying textinputformat.record.delimiter the character/character sequences in data file similar to starting character/starting character sequence in delimiter were found missing in certain cases in the Map Output |  Major | util | Arun A K |  |
| [HADOOP-8654](https://issues.apache.org/jira/browse/HADOOP-8654) | TextInputFormat delimiter  bug:- Input Text portion ends with & Delimiter starts with same char/char sequence |  Major | util | Gelesh |  |
| [HADOOP-8648](https://issues.apache.org/jira/browse/HADOOP-8648) | libhadoop:  native CRC32 validation crashes when io.bytes.per.checksum=1 |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8637](https://issues.apache.org/jira/browse/HADOOP-8637) | FilterFileSystem#setWriteChecksum is broken |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8634](https://issues.apache.org/jira/browse/HADOOP-8634) | Ensure FileSystem#close doesn't squawk for deleteOnExit paths |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8633](https://issues.apache.org/jira/browse/HADOOP-8633) | Interrupted FsShell copies may leave tmp files |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8632](https://issues.apache.org/jira/browse/HADOOP-8632) | Configuration leaking class-loaders |  Major | conf | Costin Leau | Costin Leau |
| [HADOOP-8627](https://issues.apache.org/jira/browse/HADOOP-8627) | FS deleteOnExit may delete the wrong path |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8626](https://issues.apache.org/jira/browse/HADOOP-8626) | Typo in default setting for hadoop.security.group.mapping.ldap.search.filter.user |  Major | security | Jonathan Natkins | Jonathan Natkins |
| [HADOOP-8614](https://issues.apache.org/jira/browse/HADOOP-8614) | IOUtils#skipFully hangs forever on EOF |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8613](https://issues.apache.org/jira/browse/HADOOP-8613) | AbstractDelegationTokenIdentifier#getUser() should set token auth type |  Critical | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8611](https://issues.apache.org/jira/browse/HADOOP-8611) | Allow fall-back to the shell-based implementation when JNI-based users-group mapping fails |  Major | security | Kihwal Lee | Robert Parker |
| [HADOOP-8606](https://issues.apache.org/jira/browse/HADOOP-8606) | FileSystem.get may return the wrong filesystem |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8599](https://issues.apache.org/jira/browse/HADOOP-8599) | Non empty response from FileSystem.getFileBlockLocations when asking for data beyond the end of file |  Major | fs | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-8587](https://issues.apache.org/jira/browse/HADOOP-8587) | HarFileSystem access of harMetaCache isn't threadsafe |  Minor | fs | Eli Collins | Eli Collins |
| [HADOOP-8586](https://issues.apache.org/jira/browse/HADOOP-8586) | Fixup a bunch of SPNEGO misspellings |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-8585](https://issues.apache.org/jira/browse/HADOOP-8585) | Fix initialization circularity between UserGroupInformation and HadoopConfiguration |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8573](https://issues.apache.org/jira/browse/HADOOP-8573) | Configuration tries to read from an inputstream resource multiple times. |  Major | conf | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8566](https://issues.apache.org/jira/browse/HADOOP-8566) | AvroReflectSerializer.accept(Class) throws a NPE if the class has no package (primitive types and arrays) |  Major | io | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8563](https://issues.apache.org/jira/browse/HADOOP-8563) | don't package hadoop-pipes examples/bin |  Minor | build | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8550](https://issues.apache.org/jira/browse/HADOOP-8550) | hadoop fs -touchz automatically created parent directories |  Major | fs | Robert Joseph Evans | John George |
| [HADOOP-8547](https://issues.apache.org/jira/browse/HADOOP-8547) | Package hadoop-pipes examples/bin directory (again) |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8543](https://issues.apache.org/jira/browse/HADOOP-8543) | Invalid pom.xml files on 0.23 branch |  Major | build | Radim Kolar | Radim Kolar |
| [HADOOP-8538](https://issues.apache.org/jira/browse/HADOOP-8538) | CMake builds fail on ARM |  Major | native | Trevor Robinson | Trevor Robinson |
| [HADOOP-8537](https://issues.apache.org/jira/browse/HADOOP-8537) | Two TFile tests failing recently |  Major | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-8512](https://issues.apache.org/jira/browse/HADOOP-8512) | AuthenticatedURL should reset the Token when the server returns other than OK on authentication |  Minor | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8509](https://issues.apache.org/jira/browse/HADOOP-8509) | JarFinder duplicate entry: META-INF/MANIFEST.MF exception |  Minor | util | Matteo Bertozzi | Alejandro Abdelnur |
| [HADOOP-8507](https://issues.apache.org/jira/browse/HADOOP-8507) | Avoid OOM while deserializing DelegationTokenIdentifer |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8501](https://issues.apache.org/jira/browse/HADOOP-8501) | Gridmix fails to compile on OpenJDK7u4 |  Major | benchmarks | Radim Kolar | Radim Kolar |
| [HADOOP-8499](https://issues.apache.org/jira/browse/HADOOP-8499) | Lower min.user.id to 500 for the tests |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8495](https://issues.apache.org/jira/browse/HADOOP-8495) | Update Netty to avoid leaking file descriptors during shuffle |  Critical | build | Jason Lowe | Jason Lowe |
| [HADOOP-8488](https://issues.apache.org/jira/browse/HADOOP-8488) | test-patch.sh gives +1 even if the native build fails. |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8485](https://issues.apache.org/jira/browse/HADOOP-8485) | Don't hardcode "Apache Hadoop 0.23" in the docs |  Minor | documentation | Eli Collins | Eli Collins |
| [HADOOP-8481](https://issues.apache.org/jira/browse/HADOOP-8481) | update BUILDING.txt to talk about cmake rather than autotools |  Trivial | documentation | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8480](https://issues.apache.org/jira/browse/HADOOP-8480) | The native build should honor -DskipTests |  Trivial | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8466](https://issues.apache.org/jira/browse/HADOOP-8466) | hadoop-client POM incorrectly excludes avro |  Major | build | Bruno Mahé | Bruno Mahé |
| [HADOOP-8460](https://issues.apache.org/jira/browse/HADOOP-8460) | Document proper setting of HADOOP\_PID\_DIR and HADOOP\_SECURE\_DN\_PID\_DIR |  Major | documentation | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8452](https://issues.apache.org/jira/browse/HADOOP-8452) | DN logs backtrace when running under jsvc and /jmx is loaded |  Minor | . | Andy Isaacson | Andy Isaacson |
| [HADOOP-8450](https://issues.apache.org/jira/browse/HADOOP-8450) | Remove src/test/system |  Trivial | test | Colin Patrick McCabe | Eli Collins |
| [HADOOP-8449](https://issues.apache.org/jira/browse/HADOOP-8449) | hadoop fs -text fails with compressed sequence files with the codec file extension |  Minor | . | Joey Echeverria | Harsh J |
| [HADOOP-8444](https://issues.apache.org/jira/browse/HADOOP-8444) | Fix the tests FSMainOperationsBaseTest.java and F ileContextMainOperationsBaseTest.java to avoid potential test failure |  Major | fs, test | Mariappan Asokan | madhukara phatak |
| [HADOOP-8438](https://issues.apache.org/jira/browse/HADOOP-8438) | hadoop-validate-setup.sh refers to examples jar file which doesn't exist |  Major | . | Devaraj K | Devaraj K |
| [HADOOP-8433](https://issues.apache.org/jira/browse/HADOOP-8433) | Don't set HADOOP\_LOG\_DIR in hadoop-env.sh |  Major | scripts | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-8431](https://issues.apache.org/jira/browse/HADOOP-8431) | Running distcp wo args throws IllegalArgumentException |  Major | . | Eli Collins | Sandy Ryza |
| [HADOOP-8423](https://issues.apache.org/jira/browse/HADOOP-8423) | MapFile.Reader.get() crashes jvm or throws EOFException on Snappy or LZO block-compressed data |  Major | io | Jason B | Todd Lipcon |
| [HADOOP-8422](https://issues.apache.org/jira/browse/HADOOP-8422) | Deprecate FileSystem#getDefault\* and getServerDefault methods that don't take a Path argument |  Minor | fs | Eli Collins | Eli Collins |
| [HADOOP-8408](https://issues.apache.org/jira/browse/HADOOP-8408) | MR doesn't work with a non-default ViewFS mount table and security enabled |  Major | viewfs | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-8406](https://issues.apache.org/jira/browse/HADOOP-8406) | CompressionCodecFactory.CODEC\_PROVIDERS iteration is thread-unsafe |  Major | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-8400](https://issues.apache.org/jira/browse/HADOOP-8400) | All commands warn "Kerberos krb5 configuration not found" when security is not enabled |  Major | security | Eli Collins | Alejandro Abdelnur |
| [HADOOP-8393](https://issues.apache.org/jira/browse/HADOOP-8393) | hadoop-config.sh missing variable exports, causes Yarn jobs to fail with ClassNotFoundException MRAppMaster |  Major | scripts | Patrick Hunt | Patrick Hunt |
| [HADOOP-8390](https://issues.apache.org/jira/browse/HADOOP-8390) | TestFileSystemCanonicalization fails with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8372](https://issues.apache.org/jira/browse/HADOOP-8372) | normalizeHostName() in NetUtils is not working properly in resolving a hostname start with numeric character |  Major | io, util | Junping Du | Junping Du |
| [HADOOP-8370](https://issues.apache.org/jira/browse/HADOOP-8370) | Native build failure: javah: class file for org.apache.hadoop.classification.InterfaceAudience not found |  Major | native | Trevor Robinson | Trevor Robinson |
| [HADOOP-8342](https://issues.apache.org/jira/browse/HADOOP-8342) | HDFS command fails with exception following merge of HADOOP-8325 |  Major | fs | Randy Clayton | Alejandro Abdelnur |
| [HADOOP-8341](https://issues.apache.org/jira/browse/HADOOP-8341) | Fix or filter findbugs issues in hadoop-tools |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8334](https://issues.apache.org/jira/browse/HADOOP-8334) | HttpServer sometimes returns incorrect port |  Major | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8330](https://issues.apache.org/jira/browse/HADOOP-8330) | TestSequenceFile.testCreateUsesFsArg() is broken |  Minor | test | John George | John George |
| [HADOOP-8329](https://issues.apache.org/jira/browse/HADOOP-8329) | Build fails with Java 7 |  Major | build | Kumar Ravi | Eli Collins |
| [HADOOP-8328](https://issues.apache.org/jira/browse/HADOOP-8328) | Duplicate FileSystem Statistics object for 'file' scheme |  Major | fs | Tom White | Tom White |
| [HADOOP-8327](https://issues.apache.org/jira/browse/HADOOP-8327) | distcpv2 and distcpv1 jars should not coexist |  Major | . | Dave Thompson | Dave Thompson |
| [HADOOP-8325](https://issues.apache.org/jira/browse/HADOOP-8325) | Add a ShutdownHookManager to be used by different components instead of the JVM shutdownhook |  Critical | fs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8317](https://issues.apache.org/jira/browse/HADOOP-8317) | Update maven-assembly-plugin to 2.3 - fix build on FreeBSD |  Major | build | Radim Kolar |  |
| [HADOOP-8316](https://issues.apache.org/jira/browse/HADOOP-8316) | Audit logging should be disabled by default |  Major | conf | Eli Collins | Eli Collins |
| [HADOOP-8305](https://issues.apache.org/jira/browse/HADOOP-8305) | distcp over viewfs is broken |  Major | viewfs | John George | John George |
| [HADOOP-8288](https://issues.apache.org/jira/browse/HADOOP-8288) | Remove references of mapred.child.ulimit etc. since they are not being used any more |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-8287](https://issues.apache.org/jira/browse/HADOOP-8287) | etc/hadoop is missing hadoop-env.sh |  Major | conf | Eli Collins | Eli Collins |
| [HADOOP-8268](https://issues.apache.org/jira/browse/HADOOP-8268) | A few pom.xml across Hadoop project may fail XML validation |  Major | build | Radim Kolar | Radim Kolar |
| [HADOOP-8249](https://issues.apache.org/jira/browse/HADOOP-8249) | invalid hadoop-auth cookies should trigger authentication if info is avail before returning HTTP 401 |  Major | security | bc Wong | Alejandro Abdelnur |
| [HADOOP-8225](https://issues.apache.org/jira/browse/HADOOP-8225) | DistCp fails when invoked by Oozie |  Blocker | security | Mithun Radhakrishnan | Daryn Sharp |
| [HADOOP-8197](https://issues.apache.org/jira/browse/HADOOP-8197) | Configuration logs WARNs on every use of a deprecated key |  Critical | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8180](https://issues.apache.org/jira/browse/HADOOP-8180) | Remove hsqldb since its not needed from pom.xml |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-8179](https://issues.apache.org/jira/browse/HADOOP-8179) | risk of NPE in CopyCommands processArguments() |  Minor | fs | Steve Loughran | Daryn Sharp |
| [HADOOP-8172](https://issues.apache.org/jira/browse/HADOOP-8172) | Configuration no longer sets all keys in a deprecated key list. |  Critical | conf | Robert Joseph Evans | Anupam Seth |
| [HADOOP-8168](https://issues.apache.org/jira/browse/HADOOP-8168) | empty-string owners or groups causes {{MissingFormatWidthException}} in o.a.h.fs.shell.Ls.ProcessPath() |  Major | fs | Eugene Koontz | Eugene Koontz |
| [HADOOP-8167](https://issues.apache.org/jira/browse/HADOOP-8167) | Configuration deprecation logic breaks backwards compatibility |  Blocker | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8144](https://issues.apache.org/jira/browse/HADOOP-8144) | pseudoSortByDistance in NetworkTopology doesn't work properly if no local node and first node is local rack node |  Minor | io | Junping Du | Junping Du |
| [HADOOP-8129](https://issues.apache.org/jira/browse/HADOOP-8129) | ViewFileSystemTestSetup setupForViewFileSystem is erring when the user's home directory is somewhere other than /home (eg. /User) etc. |  Major | fs, test | Ravi Prakash | Ahmed Radwan |
| [HADOOP-8110](https://issues.apache.org/jira/browse/HADOOP-8110) | TestViewFsTrash occasionally fails |  Major | fs | Tsz Wo Nicholas Sze | Jason Lowe |
| [HADOOP-8104](https://issues.apache.org/jira/browse/HADOOP-8104) | Inconsistent Jackson versions |  Major | . | Colin Patrick McCabe | Alejandro Abdelnur |
| [HADOOP-8088](https://issues.apache.org/jira/browse/HADOOP-8088) | User-group mapping cache incorrectly does negative caching on transient failures |  Major | security | Kihwal Lee | Kihwal Lee |
| [HADOOP-8060](https://issues.apache.org/jira/browse/HADOOP-8060) | Add a capability to discover and set checksum types per file. |  Major | fs, util | Kihwal Lee | Kihwal Lee |
| [HADOOP-8031](https://issues.apache.org/jira/browse/HADOOP-8031) | Configuration class fails to find embedded .jar resources; should use URL.openStream() |  Major | conf | Elias Ross | Elias Ross |
| [HADOOP-8014](https://issues.apache.org/jira/browse/HADOOP-8014) | ViewFileSystem does not correctly implement getDefaultBlockSize, getDefaultReplication, getContentSummary |  Major | fs | Daryn Sharp | John George |
| [HADOOP-8005](https://issues.apache.org/jira/browse/HADOOP-8005) | Multiple SLF4J binding message in .out file for all daemons |  Major | scripts | Joe Crobak | Jason Lowe |
| [HADOOP-7967](https://issues.apache.org/jira/browse/HADOOP-7967) | Need generalized multi-token filesystem support |  Critical | fs, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-7868](https://issues.apache.org/jira/browse/HADOOP-7868) | Hadoop native fails to compile when default linker option is -Wl,--as-needed |  Major | native | James Page | Trevor Robinson |
| [HADOOP-7818](https://issues.apache.org/jira/browse/HADOOP-7818) | DiskChecker#checkDir should fail if the directory is not executable |  Minor | util | Eli Collins | madhukara phatak |
| [HADOOP-7703](https://issues.apache.org/jira/browse/HADOOP-7703) | WebAppContext should also be stopped and cleared |  Major | . | Devaraj K | Devaraj K |
| [HADOOP-6963](https://issues.apache.org/jira/browse/HADOOP-6963) | Fix FileUtil.getDU. It should not include the size of the directory or follow symbolic links |  Critical | fs | Owen O'Malley | Ravi Prakash |
| [HADOOP-3886](https://issues.apache.org/jira/browse/HADOOP-3886) | Error in javadoc of Reporter, Mapper and Progressable |  Minor | documentation | brien colwell | Jingguo Yao |
| [HDFS-3972](https://issues.apache.org/jira/browse/HDFS-3972) | Trash emptier fails in secure HA cluster |  Critical | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3928](https://issues.apache.org/jira/browse/HDFS-3928) | MiniDFSCluster should reset the first ExitException on shutdown |  Major | test | Eli Collins | Eli Collins |
| [HDFS-3902](https://issues.apache.org/jira/browse/HDFS-3902) | TestDatanodeBlockScanner#testBlockCorruptionPolicy is broken |  Minor | . | Andy Isaacson | Andy Isaacson |
| [HDFS-3895](https://issues.apache.org/jira/browse/HDFS-3895) | hadoop-client must include commons-cli |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3890](https://issues.apache.org/jira/browse/HDFS-3890) | filecontext mkdirs doesn't apply umask as expected |  Critical | . | Thomas Graves | Thomas Graves |
| [HDFS-3888](https://issues.apache.org/jira/browse/HDFS-3888) | BlockPlacementPolicyDefault code cleanup |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-3879](https://issues.apache.org/jira/browse/HDFS-3879) | Fix findbugs warning in TransferFsImage on branch-2 |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-3873](https://issues.apache.org/jira/browse/HDFS-3873) | Hftp assumes security is disabled if token fetch fails |  Major | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3864](https://issues.apache.org/jira/browse/HDFS-3864) | NN does not update internal file mtime for OP\_CLOSE when reading from the edit log |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3861](https://issues.apache.org/jira/browse/HDFS-3861) | Deadlock in DFSClient |  Blocker | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3860](https://issues.apache.org/jira/browse/HDFS-3860) | HeartbeatManager#Monitor may wrongly hold the writelock of namesystem |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3856](https://issues.apache.org/jira/browse/HDFS-3856) | TestHDFSServerPorts failure is causing surefire fork failure |  Blocker | test | Thomas Graves | Eli Collins |
| [HDFS-3853](https://issues.apache.org/jira/browse/HDFS-3853) | Port MiniDFSCluster enableManagedDfsDirsRedundancy option to branch-2 |  Minor | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3852](https://issues.apache.org/jira/browse/HDFS-3852) | TestHftpDelegationToken is broken after HADOOP-8225 |  Major | hdfs-client, security | Aaron T. Myers | Daryn Sharp |
| [HDFS-3849](https://issues.apache.org/jira/browse/HDFS-3849) | When re-loading the FSImage, we should clear the existing genStamp and leases. |  Critical | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3841](https://issues.apache.org/jira/browse/HDFS-3841) | Port HDFS-3835 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3837](https://issues.apache.org/jira/browse/HDFS-3837) | Fix DataNode.recoverBlock findbugs warning |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-3835](https://issues.apache.org/jira/browse/HDFS-3835) | Long-lived 2NN cannot perform a checkpoint if security is enabled and the NN restarts with outstanding delegation tokens |  Major | namenode, security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3833](https://issues.apache.org/jira/browse/HDFS-3833) | TestDFSShell fails on Windows due to file concurrent read write |  Major | test | Brandon Li | Brandon Li |
| [HDFS-3832](https://issues.apache.org/jira/browse/HDFS-3832) | Remove protocol methods related to DistributedUpgrade |  Major | datanode, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-3830](https://issues.apache.org/jira/browse/HDFS-3830) | test\_libhdfs\_threaded: use forceNewInstance |  Minor | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3816](https://issues.apache.org/jira/browse/HDFS-3816) | Invalidate work percentage default value should be 0.32f instead of 32 |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-3808](https://issues.apache.org/jira/browse/HDFS-3808) | fuse\_dfs: postpone libhdfs intialization until after fork |  Critical | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3803](https://issues.apache.org/jira/browse/HDFS-3803) | BlockPoolSliceScanner new work period notice is very chatty at INFO level |  Minor | datanode | Andrew Purtell |  |
| [HDFS-3794](https://issues.apache.org/jira/browse/HDFS-3794) | WebHDFS Open used with Offset returns the original (and incorrect) Content Length in the HTTP Header. |  Major | webhdfs | Ravi Prakash | Ravi Prakash |
| [HDFS-3790](https://issues.apache.org/jira/browse/HDFS-3790) | test\_fuse\_dfs.c doesn't compile on centos 5 |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3788](https://issues.apache.org/jira/browse/HDFS-3788) | distcp can't copy large files using webhdfs due to missing Content-Length header |  Critical | webhdfs | Eli Collins | Tsz Wo Nicholas Sze |
| [HDFS-3760](https://issues.apache.org/jira/browse/HDFS-3760) | primitiveCreate is a write, not a read |  Minor | hdfs-client | Andy Isaacson | Andy Isaacson |
| [HDFS-3758](https://issues.apache.org/jira/browse/HDFS-3758) | TestFuseDFS test failing |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3756](https://issues.apache.org/jira/browse/HDFS-3756) | DelegationTokenFetcher creates 2 HTTP connections, the second one not properly configured |  Critical | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3755](https://issues.apache.org/jira/browse/HDFS-3755) | Creating an already-open-for-write file with overwrite=true fails |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3754](https://issues.apache.org/jira/browse/HDFS-3754) | BlockSender doesn't shutdown ReadaheadPool threads |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-3738](https://issues.apache.org/jira/browse/HDFS-3738) | TestDFSClientRetries#testFailuresArePerOperation sets incorrect timeout config |  Minor | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3733](https://issues.apache.org/jira/browse/HDFS-3733) | Audit logs should include WebHDFS access |  Major | webhdfs | Andy Isaacson | Andy Isaacson |
| [HDFS-3732](https://issues.apache.org/jira/browse/HDFS-3732) | fuse\_dfs: incorrect configuration value checked for connection expiry timer period |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3731](https://issues.apache.org/jira/browse/HDFS-3731) | 2.0 release upgrade must handle blocks being written from 1.0 |  Blocker | datanode | Suresh Srinivas | Kihwal Lee |
| [HDFS-3724](https://issues.apache.org/jira/browse/HDFS-3724) | add InterfaceAudience annotations to HttpFS classes and making inner enum static |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3721](https://issues.apache.org/jira/browse/HDFS-3721) | hsync support broke wire compatibility |  Critical | datanode, hdfs-client | Todd Lipcon | Aaron T. Myers |
| [HDFS-3720](https://issues.apache.org/jira/browse/HDFS-3720) | hdfs.h must get packaged |  Major | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3718](https://issues.apache.org/jira/browse/HDFS-3718) | Datanode won't shutdown because of runaway DataBlockScanner thread |  Critical | datanode | Kihwal Lee | Kihwal Lee |
| [HDFS-3715](https://issues.apache.org/jira/browse/HDFS-3715) | Fix TestFileCreation#testFileCreationNamenodeRestart |  Major | test | Eli Collins | Andrew Wang |
| [HDFS-3710](https://issues.apache.org/jira/browse/HDFS-3710) | libhdfs misuses O\_RDONLY/WRONLY/RDWR |  Minor | libhdfs | Andy Isaacson | Andy Isaacson |
| [HDFS-3707](https://issues.apache.org/jira/browse/HDFS-3707) | TestFSInputChecker: improper use of skip |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3696](https://issues.apache.org/jira/browse/HDFS-3696) | Create files with WebHdfsFileSystem goes OOM when file size is big |  Critical | webhdfs | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HDFS-3690](https://issues.apache.org/jira/browse/HDFS-3690) | BlockPlacementPolicyDefault incorrectly casts LOG |  Major | . | Eli Collins | Eli Collins |
| [HDFS-3688](https://issues.apache.org/jira/browse/HDFS-3688) | Namenode loses datanode hostname if datanode re-registers |  Major | datanode | Jason Lowe | Jason Lowe |
| [HDFS-3683](https://issues.apache.org/jira/browse/HDFS-3683) | Edit log replay progress indicator shows \>100% complete |  Minor | namenode | Todd Lipcon | Plamen Jeliazkov |
| [HDFS-3679](https://issues.apache.org/jira/browse/HDFS-3679) | fuse\_dfs notrash option sets usetrash |  Minor | fuse-dfs | Conrad Meyer | Conrad Meyer |
| [HDFS-3673](https://issues.apache.org/jira/browse/HDFS-3673) | libhdfs: fix some compiler warnings |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3664](https://issues.apache.org/jira/browse/HDFS-3664) | BlockManager race when stopping active services |  Major | test | Eli Collins | Colin Patrick McCabe |
| [HDFS-3658](https://issues.apache.org/jira/browse/HDFS-3658) | TestDFSClientRetries#testNamenodeRestart failed |  Major | . | Eli Collins | Tsz Wo Nicholas Sze |
| [HDFS-3646](https://issues.apache.org/jira/browse/HDFS-3646) | LeaseRenewer can hold reference to inactive DFSClient instances forever |  Critical | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3633](https://issues.apache.org/jira/browse/HDFS-3633) | libhdfs: hdfsDelete should pass JNI\_FALSE or JNI\_TRUE |  Minor | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3629](https://issues.apache.org/jira/browse/HDFS-3629) | fix the typo in the error message about inconsistent storage layout version |  Trivial | namenode | Brandon Li | Brandon Li |
| [HDFS-3622](https://issues.apache.org/jira/browse/HDFS-3622) | Backport HDFS-3541 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3615](https://issues.apache.org/jira/browse/HDFS-3615) | Two BlockTokenSecretManager findbugs warnings |  Major | security | Eli Collins | Aaron T. Myers |
| [HDFS-3611](https://issues.apache.org/jira/browse/HDFS-3611) | NameNode prints unnecessary WARNs about edit log normally skipping a few bytes |  Trivial | namenode | Harsh J | Colin Patrick McCabe |
| [HDFS-3609](https://issues.apache.org/jira/browse/HDFS-3609) | libhdfs: don't force the URI to look like hdfs://hostname:port |  Major | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3608](https://issues.apache.org/jira/browse/HDFS-3608) | fuse\_dfs: detect changes in UID ticket cache |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3605](https://issues.apache.org/jira/browse/HDFS-3605) | Block mistakenly marked corrupt during edit log catchup phase of failover |  Major | ha, namenode | Brahma Reddy Battula | Todd Lipcon |
| [HDFS-3603](https://issues.apache.org/jira/browse/HDFS-3603) | Decouple TestHDFSTrash from TestTrash |  Major | test | Jason Lowe | Jason Lowe |
| [HDFS-3597](https://issues.apache.org/jira/browse/HDFS-3597) | SNN can fail to start on upgrade |  Minor | namenode | Andy Isaacson | Andy Isaacson |
| [HDFS-3591](https://issues.apache.org/jira/browse/HDFS-3591) | Backport HDFS-3357 to branch-0.23 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3581](https://issues.apache.org/jira/browse/HDFS-3581) | FSPermissionChecker#checkPermission sticky bit check missing range check |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-3580](https://issues.apache.org/jira/browse/HDFS-3580) | incompatible types; no instance(s) of type variable(s) V exist so that V conforms to boolean compiling HttpFSServer.java with OpenJDK |  Minor | . | Andy Isaacson | Andy Isaacson |
| [HDFS-3579](https://issues.apache.org/jira/browse/HDFS-3579) | libhdfs: fix exception handling |  Major | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3577](https://issues.apache.org/jira/browse/HDFS-3577) | WebHdfsFileSystem can not read files larger than 24KB |  Blocker | webhdfs | Alejandro Abdelnur | Tsz Wo Nicholas Sze |
| [HDFS-3575](https://issues.apache.org/jira/browse/HDFS-3575) | HttpFS does not log Exception Stacktraces |  Minor | . | Brock Noland | Brock Noland |
| [HDFS-3574](https://issues.apache.org/jira/browse/HDFS-3574) | Fix small race and do some cleanup in GetImageServlet |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3572](https://issues.apache.org/jira/browse/HDFS-3572) | Cleanup code which inits SPNEGO in HttpServer |  Minor | namenode, security | Todd Lipcon | Todd Lipcon |
| [HDFS-3559](https://issues.apache.org/jira/browse/HDFS-3559) | DFSTestUtil: use Builder class to construct DFSTestUtil instances |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3555](https://issues.apache.org/jira/browse/HDFS-3555) | idle client socket triggers DN ERROR log (should be INFO or DEBUG) |  Major | datanode, hdfs-client | Jeff Lord | Andy Isaacson |
| [HDFS-3551](https://issues.apache.org/jira/browse/HDFS-3551) | WebHDFS CREATE does not use client location for redirection |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3548](https://issues.apache.org/jira/browse/HDFS-3548) | NamenodeFsck.copyBlock fails to create a Block Reader |  Critical | namenode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-3541](https://issues.apache.org/jira/browse/HDFS-3541) | Deadlock between recovery, xceiver and packet responder |  Major | datanode | suja s | Vinayakumar B |
| [HDFS-3539](https://issues.apache.org/jira/browse/HDFS-3539) | libhdfs code cleanups |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3531](https://issues.apache.org/jira/browse/HDFS-3531) | EditLogFileOutputStream#preallocate should check for incomplete writes |  Minor | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3524](https://issues.apache.org/jira/browse/HDFS-3524) | TestFileLengthOnClusterRestart failed due to error message change |  Major | test | Eli Collins | Brandon Li |
| [HDFS-3518](https://issues.apache.org/jira/browse/HDFS-3518) | Provide API to check HDFS operational state |  Major | hdfs-client | Bikas Saha | Tsz Wo Nicholas Sze |
| [HDFS-3517](https://issues.apache.org/jira/browse/HDFS-3517) | TestStartup should bind ephemeral ports |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-3505](https://issues.apache.org/jira/browse/HDFS-3505) | DirectoryScanner does not join all threads in shutdown |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3501](https://issues.apache.org/jira/browse/HDFS-3501) | Checkpointing with security enabled will stop working after ticket lifetime expires |  Major | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3491](https://issues.apache.org/jira/browse/HDFS-3491) | HttpFs does not set permissions correctly |  Major | . | Romain Rigaux | Alejandro Abdelnur |
| [HDFS-3490](https://issues.apache.org/jira/browse/HDFS-3490) | DN WebHDFS methods throw NPE if Namenode RPC address param not specified |  Minor | webhdfs | Todd Lipcon | Tsz Wo Nicholas Sze |
| [HDFS-3487](https://issues.apache.org/jira/browse/HDFS-3487) | offlineimageviewer should give byte offset information when it encounters an exception |  Minor | tools | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3486](https://issues.apache.org/jira/browse/HDFS-3486) | offlineimageviewer can't read fsimage files that contain persistent delegation tokens |  Minor | security, tools | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3485](https://issues.apache.org/jira/browse/HDFS-3485) | DataTransferThrottler will over-throttle when currentTimeMillis jumps |  Minor | . | Andy Isaacson | Andy Isaacson |
| [HDFS-3484](https://issues.apache.org/jira/browse/HDFS-3484) | hdfs fsck doesn't work if NN HTTP address is set to 0.0.0.0 even if NN RPC address is configured |  Minor | hdfs-client | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3480](https://issues.apache.org/jira/browse/HDFS-3480) | Multiple SLF4J binding warning |  Major | build | Eli Collins | Vinayakumar B |
| [HDFS-3469](https://issues.apache.org/jira/browse/HDFS-3469) | start-dfs.sh will start zkfc, but stop-dfs.sh will not stop zkfc similarly. |  Minor | auto-failover | Vinayakumar B | Vinayakumar B |
| [HDFS-3466](https://issues.apache.org/jira/browse/HDFS-3466) | The SPNEGO filter for the NameNode should come out of the web keytab file |  Major | namenode, security | Owen O'Malley | Owen O'Malley |
| [HDFS-3460](https://issues.apache.org/jira/browse/HDFS-3460) | HttpFS proxyuser validation with Kerberos ON uses full principal name |  Critical | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3444](https://issues.apache.org/jira/browse/HDFS-3444) | hdfs groups command doesn't work with security enabled |  Major | hdfs-client | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3442](https://issues.apache.org/jira/browse/HDFS-3442) | Incorrect count for Missing Replicas in FSCK report |  Minor | . | suja s | Andrew Wang |
| [HDFS-3440](https://issues.apache.org/jira/browse/HDFS-3440) | should more effectively limit stream memory consumption when reading corrupt edit logs |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3436](https://issues.apache.org/jira/browse/HDFS-3436) | adding new datanode to existing  pipeline fails in case of Append/Recovery |  Major | datanode | Brahma Reddy Battula | Vinayakumar B |
| [HDFS-3433](https://issues.apache.org/jira/browse/HDFS-3433) | GetImageServlet should allow administrative requestors when security is enabled |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3428](https://issues.apache.org/jira/browse/HDFS-3428) | move DelegationTokenRenewer to common |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3422](https://issues.apache.org/jira/browse/HDFS-3422) | TestStandbyIsHot timeouts too aggressive |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HDFS-3415](https://issues.apache.org/jira/browse/HDFS-3415) | During NameNode starting up, it may pick wrong storage directory inspector when the layout versions of the storage directories are different |  Major | namenode | Brahma Reddy Battula | Brandon Li |
| [HDFS-3414](https://issues.apache.org/jira/browse/HDFS-3414) | Balancer does not find NameNode if rpc-address or servicerpc-address are not set in client configs |  Minor | balancer & mover | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3413](https://issues.apache.org/jira/browse/HDFS-3413) | TestFailureToReadEdits timing out |  Critical | ha, test | Todd Lipcon | Aaron T. Myers |
| [HDFS-3398](https://issues.apache.org/jira/browse/HDFS-3398) | Client will not retry when primaryDN is down once it's just got pipeline |  Minor | hdfs-client | Brahma Reddy Battula | amith |
| [HDFS-3391](https://issues.apache.org/jira/browse/HDFS-3391) | TestPipelinesFailover#testLeaseRecoveryAfterFailover is failing |  Critical | . | Arun C Murthy | Todd Lipcon |
| [HDFS-3385](https://issues.apache.org/jira/browse/HDFS-3385) | ClassCastException when trying to append a file |  Major | namenode | Brahma Reddy Battula | Tsz Wo Nicholas Sze |
| [HDFS-3368](https://issues.apache.org/jira/browse/HDFS-3368) | Missing blocks due to bad DataNodes coming up and down. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-3359](https://issues.apache.org/jira/browse/HDFS-3359) | DFSClient.close should close cached sockets |  Critical | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-3334](https://issues.apache.org/jira/browse/HDFS-3334) | ByteRangeInputStream leaks streams |  Major | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3331](https://issues.apache.org/jira/browse/HDFS-3331) | setBalancerBandwidth do not checkSuperuserPrivilege |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3321](https://issues.apache.org/jira/browse/HDFS-3321) | Error message for insufficient data nodes to come out of safemode is wrong. |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-3312](https://issues.apache.org/jira/browse/HDFS-3312) | Hftp selects wrong token service |  Blocker | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3308](https://issues.apache.org/jira/browse/HDFS-3308) | hftp/webhdfs can't get tokens if authority has no port |  Critical | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-3306](https://issues.apache.org/jira/browse/HDFS-3306) | fuse\_dfs: don't lock release operations |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3275](https://issues.apache.org/jira/browse/HDFS-3275) | Format command overwrites contents of non-empty shared edits dir if name dirs are empty without any prompting |  Major | ha, namenode | Vinithra Varadharajan | amith |
| [HDFS-3266](https://issues.apache.org/jira/browse/HDFS-3266) | DFSTestUtil#waitCorruptReplicas doesn't sleep between checks |  Minor | . | Aaron T. Myers | madhukara phatak |
| [HDFS-3243](https://issues.apache.org/jira/browse/HDFS-3243) | TestParallelRead timing out on jenkins |  Major | hdfs-client, test | Todd Lipcon | Henry Robinson |
| [HDFS-3235](https://issues.apache.org/jira/browse/HDFS-3235) | MiniDFSClusterManager doesn't correctly support -format option |  Minor | . | Henry Robinson | Henry Robinson |
| [HDFS-3194](https://issues.apache.org/jira/browse/HDFS-3194) | DataNode block scanner is running too frequently |  Major | datanode | suja s | Andy Isaacson |
| [HDFS-3177](https://issues.apache.org/jira/browse/HDFS-3177) | Allow DFSClient to find out and use the CRC type being used for a file. |  Major | datanode, hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3176](https://issues.apache.org/jira/browse/HDFS-3176) | JsonUtil should not parse the MD5MD5CRC32FileChecksum bytes on its own. |  Major | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-3166](https://issues.apache.org/jira/browse/HDFS-3166) | Hftp connections do not have a timeout |  Critical | hdfs-client | Daryn Sharp | Daryn Sharp |
| [HDFS-3157](https://issues.apache.org/jira/browse/HDFS-3157) | Error in deleting block is keep on coming from DN even after the block report and directory scanning has happened |  Major | namenode | J.Andreina | Ashish Singhi |
| [HDFS-3136](https://issues.apache.org/jira/browse/HDFS-3136) | Multiple SLF4J binding warning |  Major | build | Jason Lowe | Jason Lowe |
| [HDFS-3067](https://issues.apache.org/jira/browse/HDFS-3067) | NPE in DFSInputStream.readBuffer if read is repeated on corrupted block |  Major | hdfs-client | Henry Robinson | Henry Robinson |
| [HDFS-3054](https://issues.apache.org/jira/browse/HDFS-3054) | distcp -skipcrccheck has no effect |  Major | tools | patrick white | Colin Patrick McCabe |
| [HDFS-3048](https://issues.apache.org/jira/browse/HDFS-3048) | Small race in BlockManager#close |  Major | namenode | Eli Collins | Andy Isaacson |
| [HDFS-3037](https://issues.apache.org/jira/browse/HDFS-3037) | TestMulitipleNNDataBlockScanner#testBlockScannerAfterRestart is racy |  Minor | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3031](https://issues.apache.org/jira/browse/HDFS-3031) | HA: Fix complete() and getAdditionalBlock() RPCs to be idempotent. |  Major | ha | Stephen Chu | Todd Lipcon |
| [HDFS-2982](https://issues.apache.org/jira/browse/HDFS-2982) | Startup performance suffers when there are many edit log segments |  Critical | namenode | Todd Lipcon | Colin Patrick McCabe |
| [HDFS-2966](https://issues.apache.org/jira/browse/HDFS-2966) | TestNameNodeMetrics tests can fail under load |  Minor | test | Steve Loughran | Steve Loughran |
| [HDFS-2963](https://issues.apache.org/jira/browse/HDFS-2963) | Console Output is confusing while executing metasave (dfsadmin command) |  Minor | . | J.Andreina | Andrew Wang |
| [HDFS-2914](https://issues.apache.org/jira/browse/HDFS-2914) | HA: Standby should not enter safemode when resources are low |  Major | ha, namenode | Hari Mankude | Vinayakumar B |
| [HDFS-2800](https://issues.apache.org/jira/browse/HDFS-2800) | HA: TestStandbyCheckpoints.testCheckpointCancellation is racy |  Major | ha, test | Aaron T. Myers | Todd Lipcon |
| [HDFS-2797](https://issues.apache.org/jira/browse/HDFS-2797) | Fix misuses of InputStream#skip in the edit log code |  Major | ha, namenode | Aaron T. Myers | Colin Patrick McCabe |
| [HDFS-2759](https://issues.apache.org/jira/browse/HDFS-2759) | Pre-allocate HDFS edit log files after writing version number |  Major | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-2757](https://issues.apache.org/jira/browse/HDFS-2757) | Cannot read a local block that's being written to when using the local read short circuit |  Major | . | Jean-Daniel Cryans | Jean-Daniel Cryans |
| [HDFS-2619](https://issues.apache.org/jira/browse/HDFS-2619) | Remove my personal email address from the libhdfs build file. |  Major | build | Owen O'Malley | Owen O'Malley |
| [HDFS-2285](https://issues.apache.org/jira/browse/HDFS-2285) | BackupNode should reject requests trying to modify namespace |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2025](https://issues.apache.org/jira/browse/HDFS-2025) | Go Back to File View link is not working in tail.jsp |  Minor | datanode | sravankorumilli | Ashish Singhi |
| [HDFS-1490](https://issues.apache.org/jira/browse/HDFS-1490) | TransferFSImage should timeout |  Minor | namenode | Dmytro Molkov | Vinayakumar B |
| [HDFS-1249](https://issues.apache.org/jira/browse/HDFS-1249) | with fuse-dfs, chown which only has owner (or only group) argument fails with Input/output error. |  Minor | fuse-dfs | matsusaka kentaro | Colin Patrick McCabe |
| [HDFS-1153](https://issues.apache.org/jira/browse/HDFS-1153) | dfsnodelist.jsp should handle invalid input parameters |  Minor | datanode | Ravi Phulari | Ravi Phulari |
| [HDFS-766](https://issues.apache.org/jira/browse/HDFS-766) | Error message not clear for set space quota out of boundary  values. |  Minor | . | Ravi Phulari | Jon Zuanich |
| [HDFS-711](https://issues.apache.org/jira/browse/HDFS-711) | hdfsUtime does not handle atime = 0 or mtime = 0 correctly |  Major | documentation | freestyler | Colin Patrick McCabe |
| [HDFS-470](https://issues.apache.org/jira/browse/HDFS-470) | libhdfs should handle 0-length reads from FSInputStream correctly |  Minor | . | Pete Wyckoff | Colin Patrick McCabe |
| [MAPREDUCE-4691](https://issues.apache.org/jira/browse/MAPREDUCE-4691) | Historyserver can report "Unknown job" after RM says job has completed |  Critical | jobhistoryserver, mrv2 | Jason Lowe | Robert Joseph Evans |
| [MAPREDUCE-4689](https://issues.apache.org/jira/browse/MAPREDUCE-4689) | JobClient.getMapTaskReports on failed job results in NPE |  Major | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4649](https://issues.apache.org/jira/browse/MAPREDUCE-4649) | mr-jobhistory-daemon.sh needs to be updated post YARN-1 |  Major | jobhistoryserver | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4647](https://issues.apache.org/jira/browse/MAPREDUCE-4647) | We should only unjar jobjar if there is a lib directory in it. |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4646](https://issues.apache.org/jira/browse/MAPREDUCE-4646) | client does not receive job diagnostics for failed jobs |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4642](https://issues.apache.org/jira/browse/MAPREDUCE-4642) | MiniMRClientClusterFactory should not use job.setJar() |  Major | test | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4641](https://issues.apache.org/jira/browse/MAPREDUCE-4641) | Exception in commitJob marks job as successful in job history |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4635](https://issues.apache.org/jira/browse/MAPREDUCE-4635) | MR side of YARN-83. Changing package of YarnClient |  Major | . | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4633](https://issues.apache.org/jira/browse/MAPREDUCE-4633) | history server doesn't set permissions on all subdirs |  Critical | jobhistoryserver | Thomas Graves | oss.wakayama |
| [MAPREDUCE-4612](https://issues.apache.org/jira/browse/MAPREDUCE-4612) | job summary file permissions not set when its created |  Critical | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4611](https://issues.apache.org/jira/browse/MAPREDUCE-4611) | MR AM dies badly when Node is decomissioned |  Critical | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4610](https://issues.apache.org/jira/browse/MAPREDUCE-4610) | Support deprecated mapreduce.job.counters.limit property in MR2 |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-4608](https://issues.apache.org/jira/browse/MAPREDUCE-4608) | hadoop-mapreduce-client is missing some dependencies |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4604](https://issues.apache.org/jira/browse/MAPREDUCE-4604) | In mapred-default, mapreduce.map.maxattempts & mapreduce.reduce.maxattempts defaults are set to 4 as well as mapreduce.job.maxtaskfailures.per.tracker. |  Critical | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4600](https://issues.apache.org/jira/browse/MAPREDUCE-4600) | TestTokenCache.java from MRV1 no longer compiles |  Critical | . | Robert Joseph Evans | Daryn Sharp |
| [MAPREDUCE-4580](https://issues.apache.org/jira/browse/MAPREDUCE-4580) | Change MapReduce to use the yarn-client module |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4579](https://issues.apache.org/jira/browse/MAPREDUCE-4579) | TestTaskAttempt fails jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4577](https://issues.apache.org/jira/browse/MAPREDUCE-4577) | HDFS-3672 broke TestCombineFileInputFormat.testMissingBlocks() test |  Minor | test | Alejandro Abdelnur | Aaron T. Myers |
| [MAPREDUCE-4572](https://issues.apache.org/jira/browse/MAPREDUCE-4572) | Can not access user logs - Jetty is not configured by default to serve aliases/symlinks |  Major | tasktracker, webapps | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4570](https://issues.apache.org/jira/browse/MAPREDUCE-4570) | ProcfsBasedProcessTree#constructProcessInfo() prints a warning if procfsDir/\<pid\>/stat is not found. |  Minor | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4569](https://issues.apache.org/jira/browse/MAPREDUCE-4569) | TestHsWebServicesJobsQuery fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4562](https://issues.apache.org/jira/browse/MAPREDUCE-4562) | Support for "FileSystemCounter" legacy counter group name for compatibility reasons is creating incorrect counter name |  Major | . | Jarek Jarcec Cecho | Jarek Jarcec Cecho |
| [MAPREDUCE-4504](https://issues.apache.org/jira/browse/MAPREDUCE-4504) | SortValidator writes to wrong directory |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4503](https://issues.apache.org/jira/browse/MAPREDUCE-4503) | Should throw InvalidJobConfException if duplicates found in cacheArchives or cacheFiles |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4498](https://issues.apache.org/jira/browse/MAPREDUCE-4498) | Remove hsqldb jar from Hadoop runtime classpath |  Critical | build, examples | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4496](https://issues.apache.org/jira/browse/MAPREDUCE-4496) | AM logs link is missing user name |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4494](https://issues.apache.org/jira/browse/MAPREDUCE-4494) | TestFifoScheduler failing with Metrics source QueueMetrics,q0=default already exists! |  Major | mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4493](https://issues.apache.org/jira/browse/MAPREDUCE-4493) | Distibuted Cache Compatability Issues |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4492](https://issues.apache.org/jira/browse/MAPREDUCE-4492) | Configuring total queue capacity between 100.5 and 99.5 at perticular level is sucessfull |  Minor | mrv2 | Nishan Shetty | Mayank Bansal |
| [MAPREDUCE-4484](https://issues.apache.org/jira/browse/MAPREDUCE-4484) | Incorrect IS\_MINI\_YARN\_CLUSTER property name in YarnConfiguration |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4483](https://issues.apache.org/jira/browse/MAPREDUCE-4483) | 2.0 build does not work |  Major | . | John George | John George |
| [MAPREDUCE-4470](https://issues.apache.org/jira/browse/MAPREDUCE-4470) | Fix TestCombineFileInputFormat.testForEmptyFile |  Major | test | Kihwal Lee | Ilya Katsov |
| [MAPREDUCE-4467](https://issues.apache.org/jira/browse/MAPREDUCE-4467) | IndexCache failures due to missing synchronization |  Critical | nodemanager | Andrey Klochkov | Kihwal Lee |
| [MAPREDUCE-4465](https://issues.apache.org/jira/browse/MAPREDUCE-4465) | Update description of yarn.nodemanager.address property |  Trivial | . | Bo Wang | Bo Wang |
| [MAPREDUCE-4457](https://issues.apache.org/jira/browse/MAPREDUCE-4457) | mr job invalid transition TA\_TOO\_MANY\_FETCH\_FAILURE at FAILED |  Critical | mrv2 | Thomas Graves | Robert Joseph Evans |
| [MAPREDUCE-4456](https://issues.apache.org/jira/browse/MAPREDUCE-4456) | LocalDistributedCacheManager can get an ArrayIndexOutOfBounds when creating symlinks |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4449](https://issues.apache.org/jira/browse/MAPREDUCE-4449) | Incorrect MR\_HISTORY\_STORAGE property name in JHAdminConfig |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4448](https://issues.apache.org/jira/browse/MAPREDUCE-4448) | Nodemanager crashes upon application cleanup if aggregation failed to start |  Critical | mrv2, nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4447](https://issues.apache.org/jira/browse/MAPREDUCE-4447) | Remove aop from cruft from the ant build |  Major | build | Eli Collins | Eli Collins |
| [MAPREDUCE-4444](https://issues.apache.org/jira/browse/MAPREDUCE-4444) | nodemanager fails to start when one of the local-dirs is bad |  Blocker | nodemanager | Nathan Roberts | Jason Lowe |
| [MAPREDUCE-4441](https://issues.apache.org/jira/browse/MAPREDUCE-4441) | Fix build issue caused by MR-3451 |  Blocker | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4440](https://issues.apache.org/jira/browse/MAPREDUCE-4440) | Change SchedulerApp & SchedulerNode to be a minimal interface |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4437](https://issues.apache.org/jira/browse/MAPREDUCE-4437) | Race in MR ApplicationMaster can cause reducers to never be scheduled |  Critical | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4432](https://issues.apache.org/jira/browse/MAPREDUCE-4432) | Confusing warning message when GenericOptionsParser is not used |  Trivial | . | Gabriel Reid |  |
| [MAPREDUCE-4423](https://issues.apache.org/jira/browse/MAPREDUCE-4423) | Potential infinite fetching of map output |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4419](https://issues.apache.org/jira/browse/MAPREDUCE-4419) | ./mapred queue -info \<queuename\> -showJobs displays all the jobs irrespective of \<queuename\> |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-4416](https://issues.apache.org/jira/browse/MAPREDUCE-4416) | Some tests fail if Clover is enabled |  Critical | client, mrv2 | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4407](https://issues.apache.org/jira/browse/MAPREDUCE-4407) | Add hadoop-yarn-server-tests-\<version\>-tests.jar to hadoop dist package |  Major | build, mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4406](https://issues.apache.org/jira/browse/MAPREDUCE-4406) | Users should be able to specify the MiniCluster ResourceManager and JobHistoryServer ports |  Major | mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4402](https://issues.apache.org/jira/browse/MAPREDUCE-4402) | TestFileInputFormat fails intermittently |  Major | test | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4395](https://issues.apache.org/jira/browse/MAPREDUCE-4395) | Possible NPE at ClientDistributedCacheManager#determineTimestamps |  Critical | distributed-cache, job submission, mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4392](https://issues.apache.org/jira/browse/MAPREDUCE-4392) | Counters.makeCompactString() changed behavior from 0.20 |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4387](https://issues.apache.org/jira/browse/MAPREDUCE-4387) | RM gets fatal error and exits during TestRM |  Major | resourcemanager | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4384](https://issues.apache.org/jira/browse/MAPREDUCE-4384) | Race conditions in IndexCache |  Major | nodemanager | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4383](https://issues.apache.org/jira/browse/MAPREDUCE-4383) | HadoopPipes.cc needs to include unistd.h |  Minor | pipes | Andy Isaacson | Andy Isaacson |
| [MAPREDUCE-4380](https://issues.apache.org/jira/browse/MAPREDUCE-4380) | Empty Userlogs directory is getting created under logs directory |  Minor | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4379](https://issues.apache.org/jira/browse/MAPREDUCE-4379) | Node Manager throws java.lang.OutOfMemoryError: Java heap space due to org.apache.hadoop.fs.LocalDirAllocator.contexts |  Blocker | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4376](https://issues.apache.org/jira/browse/MAPREDUCE-4376) | TestClusterMRNotification times out |  Major | mrv2, test | Jason Lowe | Kihwal Lee |
| [MAPREDUCE-4372](https://issues.apache.org/jira/browse/MAPREDUCE-4372) | Deadlock in Resource Manager between SchedulerEventDispatcher.EventProcessor and Shutdown hook manager |  Major | mrv2, resourcemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4361](https://issues.apache.org/jira/browse/MAPREDUCE-4361) | Fix detailed metrics for protobuf-based RPC on 0.23 |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4342](https://issues.apache.org/jira/browse/MAPREDUCE-4342) | Distributed Cache gives inconsistent result if cache files get deleted from task tracker |  Major | . | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-4341](https://issues.apache.org/jira/browse/MAPREDUCE-4341) | add types to capacity scheduler properties documentation |  Major | capacity-sched, mrv2 | Thomas Graves | Karthik Kambatla |
| [MAPREDUCE-4336](https://issues.apache.org/jira/browse/MAPREDUCE-4336) | Distributed Shell fails when used with the CapacityScheduler |  Major | mrv2 | Siddharth Seth | Ahmed Radwan |
| [MAPREDUCE-4320](https://issues.apache.org/jira/browse/MAPREDUCE-4320) | gridmix mainClass wrong in pom.xml |  Major | contrib/gridmix | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4313](https://issues.apache.org/jira/browse/MAPREDUCE-4313) | TestTokenCache doesn't compile due TokenCache.getDelegationToken compilation error |  Blocker | build, test | Eli Collins | Robert Joseph Evans |
| [MAPREDUCE-4307](https://issues.apache.org/jira/browse/MAPREDUCE-4307) | TeraInputFormat calls FileSystem.getDefaultBlockSize() without a Path - Failure when using ViewFileSystem |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4306](https://issues.apache.org/jira/browse/MAPREDUCE-4306) | Problem running Distributed Shell applications as a user other than the one started the daemons |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4302](https://issues.apache.org/jira/browse/MAPREDUCE-4302) | NM goes down if error encountered during log aggregation |  Critical | nodemanager | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4300](https://issues.apache.org/jira/browse/MAPREDUCE-4300) | OOM in AM can turn it into a zombie. |  Major | applicationmaster | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4299](https://issues.apache.org/jira/browse/MAPREDUCE-4299) | Terasort hangs with MR2 FifoScheduler |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-4297](https://issues.apache.org/jira/browse/MAPREDUCE-4297) | Usersmap file in gridmix should not fail on empty lines |  Major | contrib/gridmix | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4295](https://issues.apache.org/jira/browse/MAPREDUCE-4295) | RM crashes due to DNS issue |  Critical | mrv2, resourcemanager | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4290](https://issues.apache.org/jira/browse/MAPREDUCE-4290) | JobStatus.getState() API is giving ambiguous values |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-4276](https://issues.apache.org/jira/browse/MAPREDUCE-4276) | Allow setting yarn.nodemanager.delete.debug-delay-sec property to "-1" for easier container debugging. |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4270](https://issues.apache.org/jira/browse/MAPREDUCE-4270) | data\_join test classes are in the wrong packge |  Major | mrv2 | Brock Noland | Thomas Graves |
| [MAPREDUCE-4269](https://issues.apache.org/jira/browse/MAPREDUCE-4269) | documentation: Gridmix has javadoc warnings in StressJobFactory |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4267](https://issues.apache.org/jira/browse/MAPREDUCE-4267) | mavenize pipes |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4264](https://issues.apache.org/jira/browse/MAPREDUCE-4264) | Got ClassCastException when using mapreduce.history.server.delegationtoken.required=true |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4262](https://issues.apache.org/jira/browse/MAPREDUCE-4262) | NM gives wrong log message saying "Connected to ResourceManager" before trying to connect |  Minor | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4252](https://issues.apache.org/jira/browse/MAPREDUCE-4252) | MR2 job never completes with 1 pending task |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-4250](https://issues.apache.org/jira/browse/MAPREDUCE-4250) | hadoop-config.sh missing variable exports, causes Yarn jobs to fail with ClassNotFoundException MRAppMaster |  Major | nodemanager | Patrick Hunt | Patrick Hunt |
| [MAPREDUCE-4238](https://issues.apache.org/jira/browse/MAPREDUCE-4238) | mavenize data\_join |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4237](https://issues.apache.org/jira/browse/MAPREDUCE-4237) | TestNodeStatusUpdater can fail if localhost has a domain associated with it |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4233](https://issues.apache.org/jira/browse/MAPREDUCE-4233) | NPE can happen in RMNMNodeInfo. |  Critical | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4228](https://issues.apache.org/jira/browse/MAPREDUCE-4228) | mapreduce.job.reduce.slowstart.completedmaps is not working properly to delay the scheduling of the reduce tasks |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4226](https://issues.apache.org/jira/browse/MAPREDUCE-4226) | ConcurrentModificationException in FileSystemCounterGroup |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-4224](https://issues.apache.org/jira/browse/MAPREDUCE-4224) | TestFifoScheduler throws org.apache.hadoop.metrics2.MetricsException |  Major | mrv2, scheduler, test | Devaraj K | Devaraj K |
| [MAPREDUCE-4220](https://issues.apache.org/jira/browse/MAPREDUCE-4220) | RM apps page starttime/endtime sorts are incorrect |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4215](https://issues.apache.org/jira/browse/MAPREDUCE-4215) | RM app page shows 500 error on appid parse error |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4211](https://issues.apache.org/jira/browse/MAPREDUCE-4211) | Error conditions (missing appid, appid not found) are masked in the RM app page |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4209](https://issues.apache.org/jira/browse/MAPREDUCE-4209) | junit dependency in hadoop-mapreduce-client is missing scope test |  Major | build | Radim Kolar |  |
| [MAPREDUCE-4206](https://issues.apache.org/jira/browse/MAPREDUCE-4206) | Sorting by Last Health-Update on the RM nodes page sorts does not work correctly |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4197](https://issues.apache.org/jira/browse/MAPREDUCE-4197) | Include the hsqldb jar in the hadoop-mapreduce tar file |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4194](https://issues.apache.org/jira/browse/MAPREDUCE-4194) | ConcurrentModificationError in DirectoryCollection |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4189](https://issues.apache.org/jira/browse/MAPREDUCE-4189) | TestContainerManagerSecurity is failing |  Critical | mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-4169](https://issues.apache.org/jira/browse/MAPREDUCE-4169) | Container Logs appear in unsorted order |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4165](https://issues.apache.org/jira/browse/MAPREDUCE-4165) | Committing is misspelled as commiting in task logs |  Trivial | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4160](https://issues.apache.org/jira/browse/MAPREDUCE-4160) | some mrv1 ant tests fail with timeout - due to 4156 |  Major | test | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4159](https://issues.apache.org/jira/browse/MAPREDUCE-4159) | Job is running in Uber mode after setting "mapreduce.job.ubertask.maxreduces" to zero |  Major | mrv2 | Nishan Shetty | Devaraj K |
| [MAPREDUCE-4156](https://issues.apache.org/jira/browse/MAPREDUCE-4156) | ant build fails compiling JobInProgress |  Major | build | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4152](https://issues.apache.org/jira/browse/MAPREDUCE-4152) | map task left hanging after AM dies trying to connect to RM |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4148](https://issues.apache.org/jira/browse/MAPREDUCE-4148) | MapReduce should not have a compile-time dependency on HDFS |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-4144](https://issues.apache.org/jira/browse/MAPREDUCE-4144) | ResourceManager NPE while handling NODE\_UPDATE |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4140](https://issues.apache.org/jira/browse/MAPREDUCE-4140) | mapreduce classes incorrectly importing "clover.org.apache.\*" classes |  Major | client, mrv2 | Patrick Hunt | Patrick Hunt |
| [MAPREDUCE-4139](https://issues.apache.org/jira/browse/MAPREDUCE-4139) | Potential ResourceManager deadlock when SchedulerEventDispatcher is stopped |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4133](https://issues.apache.org/jira/browse/MAPREDUCE-4133) | MR over viewfs is broken |  Major | . | John George | John George |
| [MAPREDUCE-4129](https://issues.apache.org/jira/browse/MAPREDUCE-4129) | Lots of unneeded counters log messages |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4128](https://issues.apache.org/jira/browse/MAPREDUCE-4128) | AM Recovery expects all attempts of a completed task to also be completed. |  Major | mrv2 | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4117](https://issues.apache.org/jira/browse/MAPREDUCE-4117) | mapred job -status throws NullPointerException |  Critical | client, mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-4102](https://issues.apache.org/jira/browse/MAPREDUCE-4102) | job counters not available in Jobhistory webui for killed jobs |  Major | webapps | Thomas Graves | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4099](https://issues.apache.org/jira/browse/MAPREDUCE-4099) | ApplicationMaster may fail to remove staging directory |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4097](https://issues.apache.org/jira/browse/MAPREDUCE-4097) | tools testcases fail because missing mrapp-generated-classpath file in classpath |  Major | build | Alejandro Abdelnur | Roman Shaposhnik |
| [MAPREDUCE-4092](https://issues.apache.org/jira/browse/MAPREDUCE-4092) | commitJob Exception does not fail job (regression in 0.23 vs 0.20) |  Blocker | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-4091](https://issues.apache.org/jira/browse/MAPREDUCE-4091) | tools testcases failing because of MAPREDUCE-4082 |  Critical | build, test | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4089](https://issues.apache.org/jira/browse/MAPREDUCE-4089) | Hung Tasks never time out. |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4082](https://issues.apache.org/jira/browse/MAPREDUCE-4082) | hadoop-mapreduce-client-app's mrapp-generated-classpath file should not be in the module JAR |  Critical | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4074](https://issues.apache.org/jira/browse/MAPREDUCE-4074) | Client continuously retries to RM When RM goes down before launching Application Master |  Major | . | Devaraj K | xieguiming |
| [MAPREDUCE-4073](https://issues.apache.org/jira/browse/MAPREDUCE-4073) | CS assigns multiple off-switch containers when using multi-level-queues |  Critical | mrv2, scheduler | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-4068](https://issues.apache.org/jira/browse/MAPREDUCE-4068) | Jars in lib subdirectory of the submittable JAR are not added to the classpath |  Blocker | mrv2 | Ahmed Radwan | Robert Kanter |
| [MAPREDUCE-4062](https://issues.apache.org/jira/browse/MAPREDUCE-4062) | AM Launcher thread can hang forever |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4060](https://issues.apache.org/jira/browse/MAPREDUCE-4060) | Multiple SLF4J binding warning |  Major | build | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4053](https://issues.apache.org/jira/browse/MAPREDUCE-4053) | Counters group names deprecation is wrong, iterating over group names deprecated names don't show up |  Major | mrv2 | Alejandro Abdelnur | Robert Joseph Evans |
| [MAPREDUCE-4050](https://issues.apache.org/jira/browse/MAPREDUCE-4050) | Invalid node link |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4048](https://issues.apache.org/jira/browse/MAPREDUCE-4048) | NullPointerException exception while accessing the Application Master UI |  Major | mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-4040](https://issues.apache.org/jira/browse/MAPREDUCE-4040) | History links should use hostname rather than IP address. |  Minor | jobhistoryserver, mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-4031](https://issues.apache.org/jira/browse/MAPREDUCE-4031) | Node Manager hangs on shut down |  Critical | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-4024](https://issues.apache.org/jira/browse/MAPREDUCE-4024) | RM webservices can't query on finalStatus |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4020](https://issues.apache.org/jira/browse/MAPREDUCE-4020) | Web services returns incorrect JSON for deep queue tree |  Major | mrv2, webapps | Jason Lowe | Anupam Seth |
| [MAPREDUCE-4012](https://issues.apache.org/jira/browse/MAPREDUCE-4012) | Hadoop Job setup error leaves no useful info to users (when LinuxTaskController is used) |  Minor | . | Koji Noguchi | Thomas Graves |
| [MAPREDUCE-4010](https://issues.apache.org/jira/browse/MAPREDUCE-4010) | TestWritableJobConf fails on trunk |  Critical | mrv2 | Jason Lowe | Alejandro Abdelnur |
| [MAPREDUCE-4002](https://issues.apache.org/jira/browse/MAPREDUCE-4002) | MultiFileWordCount job fails if the input path is not from default file system |  Major | examples | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3999](https://issues.apache.org/jira/browse/MAPREDUCE-3999) | Tracking link gives an error if the AppMaster hasn't started yet |  Major | mrv2, webapps | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3993](https://issues.apache.org/jira/browse/MAPREDUCE-3993) | Graceful handling of codec errors during decompression |  Major | mrv1, mrv2 | Todd Lipcon | Karthik Kambatla |
| [MAPREDUCE-3992](https://issues.apache.org/jira/browse/MAPREDUCE-3992) | Reduce fetcher doesn't verify HTTP status code of response |  Major | mrv1 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3988](https://issues.apache.org/jira/browse/MAPREDUCE-3988) | mapreduce.job.local.dir doesn't point to a single directory on a node. |  Major | mrv2 | Vinod Kumar Vavilapalli | Eric Payne |
| [MAPREDUCE-3947](https://issues.apache.org/jira/browse/MAPREDUCE-3947) | yarn.app.mapreduce.am.resource.mb not documented |  Minor | . | Todd Lipcon | Devaraj K |
| [MAPREDUCE-3932](https://issues.apache.org/jira/browse/MAPREDUCE-3932) | MR tasks failing and crashing the AM when available-resources/headRoom becomes zero |  Critical | mr-am, mrv2 | Vinod Kumar Vavilapalli | Robert Joseph Evans |
| [MAPREDUCE-3927](https://issues.apache.org/jira/browse/MAPREDUCE-3927) | Shuffle hang when set map.failures.percent |  Critical | mrv2 | MengWang | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3893](https://issues.apache.org/jira/browse/MAPREDUCE-3893) | allow capacity scheduler configs maximum-applications and maximum-am-resource-percent configurable on a per queue basis |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3889](https://issues.apache.org/jira/browse/MAPREDUCE-3889) | job client tries to use /tasklog interface, but that doesn't exist anymore |  Critical | mrv2 | Thomas Graves | Devaraj K |
| [MAPREDUCE-3873](https://issues.apache.org/jira/browse/MAPREDUCE-3873) | Nodemanager is not getting decommisioned if the absolute ip is given in exclude file. |  Minor | mrv2, nodemanager | Nishan Shetty | xieguiming |
| [MAPREDUCE-3870](https://issues.apache.org/jira/browse/MAPREDUCE-3870) | Invalid App Metrics |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3782](https://issues.apache.org/jira/browse/MAPREDUCE-3782) | teragen terasort jobs fail when using webhdfs:// |  Critical | mrv2 | Arpit Gupta | Jason Lowe |
| [MAPREDUCE-3728](https://issues.apache.org/jira/browse/MAPREDUCE-3728) | ShuffleHandler can't access results when configured in a secure mode |  Critical | mrv2, nodemanager | Roman Shaposhnik | Ding Yuan |
| [MAPREDUCE-3682](https://issues.apache.org/jira/browse/MAPREDUCE-3682) | Tracker URL says AM tasks run on localhost |  Major | mrv2 | David Capwell | Ravi Prakash |
| [MAPREDUCE-3672](https://issues.apache.org/jira/browse/MAPREDUCE-3672) | Killed maps shouldn't be counted towards JobCounter.NUM\_FAILED\_MAPS |  Major | mr-am, mrv2 | Vinod Kumar Vavilapalli | Anupam Seth |
| [MAPREDUCE-3650](https://issues.apache.org/jira/browse/MAPREDUCE-3650) | testGetTokensForHftpFS() fails |  Blocker | mrv2 | Thomas Graves | Ravi Prakash |
| [MAPREDUCE-3621](https://issues.apache.org/jira/browse/MAPREDUCE-3621) | TestDBJob and TestDataDrivenDBInputFormat ant tests fail |  Major | mrv2 | Thomas Graves | Ravi Prakash |
| [MAPREDUCE-3543](https://issues.apache.org/jira/browse/MAPREDUCE-3543) | Mavenize Gridmix. |  Critical | mrv2 | Mahadev konar | Thomas Graves |
| [MAPREDUCE-3506](https://issues.apache.org/jira/browse/MAPREDUCE-3506) | Calling getPriority on JobInfo after parsing a history log with JobHistoryParser throws a NullPointerException |  Minor | client, mrv2 | Ratandeep Ratti | Jason Lowe |
| [MAPREDUCE-3493](https://issues.apache.org/jira/browse/MAPREDUCE-3493) | Add the default mapreduce.shuffle.port property to mapred-default.xml |  Minor | mrv2 | Ahmed Radwan |  |
| [MAPREDUCE-3350](https://issues.apache.org/jira/browse/MAPREDUCE-3350) | Per-app RM page should have the list of application-attempts like on the app JHS page |  Critical | mrv2, webapps | Vinod Kumar Vavilapalli | Jonathan Eagles |
| [MAPREDUCE-3348](https://issues.apache.org/jira/browse/MAPREDUCE-3348) | mapred job -status fails to give info even if the job is present in History |  Major | mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-3082](https://issues.apache.org/jira/browse/MAPREDUCE-3082) | archive command take wrong path for input file with current directory |  Major | harchive | Rajit Saha | John George |
| [MAPREDUCE-2739](https://issues.apache.org/jira/browse/MAPREDUCE-2739) | MR-279: Update installation docs (remove YarnClientFactory) |  Minor | mrv2 | Ahmed Radwan | Bo Wang |
| [MAPREDUCE-2374](https://issues.apache.org/jira/browse/MAPREDUCE-2374) | "Text File Busy" errors launching MR tasks |  Major | . | Todd Lipcon | Andy Isaacson |
| [MAPREDUCE-2289](https://issues.apache.org/jira/browse/MAPREDUCE-2289) | Permissions race can make getStagingDir fail on local filesystem |  Major | job submission | Todd Lipcon | Ahmed Radwan |
| [MAPREDUCE-2220](https://issues.apache.org/jira/browse/MAPREDUCE-2220) | Fix new API FileOutputFormat-related typos in mapred-default.xml |  Minor | documentation | Rui KUBO | Rui KUBO |
| [YARN-174](https://issues.apache.org/jira/browse/YARN-174) | TestNodeStatusUpdater is failing in trunk |  Major | nodemanager | Robert Joseph Evans | Vinod Kumar Vavilapalli |
| [YARN-138](https://issues.apache.org/jira/browse/YARN-138) | Improve default config values for YARN |  Major | resourcemanager, scheduler | Arun C Murthy | Harsh J |
| [YARN-108](https://issues.apache.org/jira/browse/YARN-108) | FSDownload can create cache directories with the wrong permissions |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-106](https://issues.apache.org/jira/browse/YARN-106) | Nodemanager needs to set permissions of local directories |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-88](https://issues.apache.org/jira/browse/YARN-88) | DefaultContainerExecutor can fail to set proper permissions |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-87](https://issues.apache.org/jira/browse/YARN-87) | NM ResourceLocalizationService does not set permissions of local cache directories |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-83](https://issues.apache.org/jira/browse/YARN-83) | Change package of YarnClient to include apache |  Major | client | Bikas Saha | Bikas Saha |
| [YARN-79](https://issues.apache.org/jira/browse/YARN-79) | Calling YarnClientImpl.close throws Exception |  Major | client | Bikas Saha | Vinod Kumar Vavilapalli |
| [YARN-75](https://issues.apache.org/jira/browse/YARN-75) | RMContainer should handle a RELEASE event while RUNNING |  Major | . | Siddharth Seth | Siddharth Seth |
| [YARN-68](https://issues.apache.org/jira/browse/YARN-68) | NodeManager will refuse to shutdown indefinitely due to container log aggregation |  Major | nodemanager | patrick white | Daryn Sharp |
| [YARN-66](https://issues.apache.org/jira/browse/YARN-66) | aggregated logs permissions not set properly |  Critical | nodemanager | Thomas Graves | Thomas Graves |
| [YARN-63](https://issues.apache.org/jira/browse/YARN-63) | RMNodeImpl is missing valid transitions from the UNHEALTHY state |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-58](https://issues.apache.org/jira/browse/YARN-58) | NM leaks filesystems |  Critical | nodemanager | Daryn Sharp | Jason Lowe |
| [YARN-42](https://issues.apache.org/jira/browse/YARN-42) | Node Manager throws NPE on startup |  Major | nodemanager | Devaraj K | Devaraj K |
| [YARN-37](https://issues.apache.org/jira/browse/YARN-37) | TestRMAppTransitions.testAppSubmittedKilled passes for the wrong reason |  Minor | resourcemanager | Jason Lowe | Mayank Bansal |
| [YARN-36](https://issues.apache.org/jira/browse/YARN-36) | branch-2.1.0-alpha doesn't build |  Blocker | . | Eli Collins | Radim Kolar |
| [YARN-31](https://issues.apache.org/jira/browse/YARN-31) | TestDelegationTokenRenewer fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [YARN-27](https://issues.apache.org/jira/browse/YARN-27) | Failed refreshQueues due to misconfiguration prevents further refreshing of queues |  Major | . | Ramya Sunil | Arun C Murthy |
| [YARN-25](https://issues.apache.org/jira/browse/YARN-25) | remove old aggregated logs |  Major | . | Thomas Graves | Robert Joseph Evans |
| [YARN-22](https://issues.apache.org/jira/browse/YARN-22) | Using URI for yarn.nodemanager log dirs fails |  Minor | . | Eli Collins | Mayank Bansal |
| [YARN-15](https://issues.apache.org/jira/browse/YARN-15) | YarnConfiguration DEFAULT\_YARN\_APPLICATION\_CLASSPATH should be updated |  Critical | nodemanager | Alejandro Abdelnur | Arun C Murthy |
| [YARN-14](https://issues.apache.org/jira/browse/YARN-14) | Symlinks to peer distributed cache files no longer work |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-13](https://issues.apache.org/jira/browse/YARN-13) | Merge of yarn reorg into branch-2 copied trunk tree |  Critical | . | Todd Lipcon |  |
| [YARN-12](https://issues.apache.org/jira/browse/YARN-12) | Several Findbugs issues with new FairScheduler in YARN |  Major | scheduler | Junping Du | Junping Du |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8283](https://issues.apache.org/jira/browse/HADOOP-8283) | Allow tests to control token service value |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-3709](https://issues.apache.org/jira/browse/HDFS-3709) | TestStartup tests still binding to the ephemeral port |  Major | test | Eli Collins | Eli Collins |
| [HDFS-3665](https://issues.apache.org/jira/browse/HDFS-3665) | Add a test for renaming across file systems via a symlink |  Major | test | Eli Collins | Eli Collins |
| [HDFS-3634](https://issues.apache.org/jira/browse/HDFS-3634) | Add self-contained, mavenized fuse\_dfs test |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3606](https://issues.apache.org/jira/browse/HDFS-3606) | libhdfs: create self-contained unit test |  Minor | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3291](https://issues.apache.org/jira/browse/HDFS-3291) | add test that covers HttpFS working w/ a non-HDFS Hadoop filesystem |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3258](https://issues.apache.org/jira/browse/HDFS-3258) | Test for HADOOP-8144 (pseudoSortByDistance in NetworkTopology for first rack local node) |  Major | test | Eli Collins | Junping Du |
| [MAPREDUCE-4212](https://issues.apache.org/jira/browse/MAPREDUCE-4212) | TestJobClientGetJob sometimes fails |  Major | test | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3983](https://issues.apache.org/jira/browse/MAPREDUCE-3983) | TestTTResourceReporting can fail, and should just be deleted |  Major | mrv1 | Robert Joseph Evans | Ravi Prakash |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7754](https://issues.apache.org/jira/browse/HADOOP-7754) | Expose file descriptors from Hadoop-wrapped local FileSystems |  Major | native, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-3502](https://issues.apache.org/jira/browse/HDFS-3502) | Change INodeFile and INodeFileUnderConstruction to package private |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3474](https://issues.apache.org/jira/browse/HDFS-3474) | Cleanup Exception handling in BookKeeper journal manager |  Major | . | Ivan Kelly | Ivan Kelly |
| [HDFS-3468](https://issues.apache.org/jira/browse/HDFS-3468) | Make BKJM-ZK session timeout configurable. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-3452](https://issues.apache.org/jira/browse/HDFS-3452) | BKJM:Switch from standby to active fails and NN gets shut down due to delay in clearing of lock |  Blocker | . | suja s | Uma Maheswara Rao G |
| [HDFS-3441](https://issues.apache.org/jira/browse/HDFS-3441) | Race condition between rolling logs at active NN and purging at standby |  Major | . | suja s | Rakesh R |
| [HDFS-3423](https://issues.apache.org/jira/browse/HDFS-3423) | BKJM: NN startup is failing, when tries to recoverUnfinalizedSegments() a bad inProgress\_ ZNodes |  Major | . | Rakesh R | Ivan Kelly |
| [HDFS-3408](https://issues.apache.org/jira/browse/HDFS-3408) | BKJM : Namenode format fails, if there is no BK root |  Minor | namenode | Rakesh R | Rakesh R |
| [HDFS-3389](https://issues.apache.org/jira/browse/HDFS-3389) | Document the BKJM usage in Namenode HA. |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-3369](https://issues.apache.org/jira/browse/HDFS-3369) | change variable names referring to inode in blockmanagement to more appropriate |  Minor | namenode | John George | John George |
| [HDFS-3190](https://issues.apache.org/jira/browse/HDFS-3190) | Simple refactors in existing NN code to assist QuorumJournalManager extension |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3168](https://issues.apache.org/jira/browse/HDFS-3168) | Clean up FSNamesystem and BlockManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3058](https://issues.apache.org/jira/browse/HDFS-3058) | HA: Bring BookKeeperJournalManager up to date with HA changes |  Major | . | Ivan Kelly | Ivan Kelly |
| [HDFS-2717](https://issues.apache.org/jira/browse/HDFS-2717) | BookKeeper Journal output stream doesn't check addComplete rc |  Major | . | Ivan Kelly | Ivan Kelly |
| [HDFS-2330](https://issues.apache.org/jira/browse/HDFS-2330) | In NNStorage.java, IOExceptions of stream closures  can mask root exceptions. |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-4163](https://issues.apache.org/jira/browse/MAPREDUCE-4163) | consistently set the bind address |  Major | mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4162](https://issues.apache.org/jira/browse/MAPREDUCE-4162) | Correctly set token service |  Major | client, mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4161](https://issues.apache.org/jira/browse/MAPREDUCE-4161) | create sockets consistently |  Major | client, mrv2 | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-3972](https://issues.apache.org/jira/browse/MAPREDUCE-3972) | Locking and exception issues in JobHistory Server. |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3942](https://issues.apache.org/jira/browse/MAPREDUCE-3942) | Randomize master key generation for ApplicationTokenSecretManager and roll it every so often |  Major | mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3940](https://issues.apache.org/jira/browse/MAPREDUCE-3940) | ContainerTokens should have an expiry interval |  Major | mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3613](https://issues.apache.org/jira/browse/MAPREDUCE-3613) | web service calls header contains 2 content types |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [YARN-60](https://issues.apache.org/jira/browse/YARN-60) | NMs rejects all container tokens after secret key rolls |  Blocker | nodemanager | Daryn Sharp | Vinod Kumar Vavilapalli |
| [YARN-39](https://issues.apache.org/jira/browse/YARN-39) | RM-NM secret-keys should be randomly generated and rolled every so often |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-29](https://issues.apache.org/jira/browse/YARN-29) | Add a yarn-client module |  Major | client | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8403](https://issues.apache.org/jira/browse/HADOOP-8403) | bump up POMs version to 2.0.1-SNAPSHOT |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4134](https://issues.apache.org/jira/browse/MAPREDUCE-4134) | Remove references of mapred.child.ulimit etc. since they are not being used any more |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4051](https://issues.apache.org/jira/browse/MAPREDUCE-4051) | Remove the empty hadoop-mapreduce-project/assembly/all.xml file |  Major | . | Ravi Prakash | Ravi Prakash |
| [YARN-154](https://issues.apache.org/jira/browse/YARN-154) | Create Yarn trunk and commit jobs |  Major | . | Eli Collins | Robert Joseph Evans |
| [YARN-1](https://issues.apache.org/jira/browse/YARN-1) | Move YARN out of hadoop-mapreduce |  Major | . | Arun C Murthy | Arun C Murthy |


