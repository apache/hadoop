
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

## Release 0.23.1 - 2012-02-17

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8013](https://issues.apache.org/jira/browse/HADOOP-8013) | ViewFileSystem does not honor setVerifyChecksum |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7470](https://issues.apache.org/jira/browse/HADOOP-7470) | move up to Jackson 1.8.8 |  Minor | util | Steve Loughran | Enis Soztutar |
| [HADOOP-7348](https://issues.apache.org/jira/browse/HADOOP-7348) | Modify the option of FsShell getmerge from [addnl] to [-nl] for consistency |  Major | fs | XieXianshan | XieXianshan |
| [MAPREDUCE-3720](https://issues.apache.org/jira/browse/MAPREDUCE-3720) | Command line listJobs should not visit each AM |  Major | client, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7808](https://issues.apache.org/jira/browse/HADOOP-7808) | Port token service changes from 205 |  Major | fs, security | Daryn Sharp | Daryn Sharp |
| [HDFS-2316](https://issues.apache.org/jira/browse/HDFS-2316) | [umbrella] WebHDFS: a complete FileSystem implementation for accessing HDFS over HTTP |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2765](https://issues.apache.org/jira/browse/MAPREDUCE-2765) | DistCp Rewrite |  Major | distcp, mrv2 | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [MAPREDUCE-778](https://issues.apache.org/jira/browse/MAPREDUCE-778) | [Rumen] Need a standalone JobHistory log anonymizer |  Major | tools/rumen | Hong Tang | Amar Kamat |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8027](https://issues.apache.org/jira/browse/HADOOP-8027) | Visiting /jmx on the daemon web interfaces may print unnecessary error in logs |  Minor | metrics | Harsh J | Aaron T. Myers |
| [HADOOP-8015](https://issues.apache.org/jira/browse/HADOOP-8015) | ChRootFileSystem should extend FilterFileSystem |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8009](https://issues.apache.org/jira/browse/HADOOP-8009) | Create hadoop-client and hadoop-minicluster artifacts for downstream projects |  Critical | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7987](https://issues.apache.org/jira/browse/HADOOP-7987) | Support setting the run-as user in unsecure mode |  Major | security | Devaraj Das | Jitendra Nath Pandey |
| [HADOOP-7939](https://issues.apache.org/jira/browse/HADOOP-7939) | Improve Hadoop subcomponent integration in Hadoop 0.23 |  Major | build, conf, documentation, scripts | Roman Shaposhnik | Roman Shaposhnik |
| [HADOOP-7934](https://issues.apache.org/jira/browse/HADOOP-7934) | Normalize dependencies versions across all modules |  Critical | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7919](https://issues.apache.org/jira/browse/HADOOP-7919) | [Doc] Remove hadoop.logfile.\* properties. |  Trivial | documentation | Harsh J | Harsh J |
| [HADOOP-7910](https://issues.apache.org/jira/browse/HADOOP-7910) | add configuration methods to handle human readable size values |  Minor | conf | Sho Shimauchi | Sho Shimauchi |
| [HADOOP-7890](https://issues.apache.org/jira/browse/HADOOP-7890) | Redirect hadoop script's deprecation message to stderr |  Trivial | scripts | Koji Noguchi | Koji Noguchi |
| [HADOOP-7858](https://issues.apache.org/jira/browse/HADOOP-7858) | Drop some info logging to DEBUG level in IPC, metrics, and HTTP |  Trivial | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7841](https://issues.apache.org/jira/browse/HADOOP-7841) | Run tests with non-secure random |  Trivial | build | Todd Lipcon | Todd Lipcon |
| [HADOOP-7804](https://issues.apache.org/jira/browse/HADOOP-7804) | enable hadoop config generator to set dfs.block.local-path-access.user to enable short circuit read |  Major | conf | Arpit Gupta | Arpit Gupta |
| [HADOOP-7792](https://issues.apache.org/jira/browse/HADOOP-7792) | Common component for HDFS-2416: Add verifyToken method to AbstractDelegationTokenSecretManager |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7777](https://issues.apache.org/jira/browse/HADOOP-7777) | Implement a base class for DNSToSwitchMapping implementations that can offer extra topology information |  Major | util | Steve Loughran | Steve Loughran |
| [HADOOP-7761](https://issues.apache.org/jira/browse/HADOOP-7761) | Improve performance of raw comparisons |  Major | io, performance, util | Todd Lipcon | Todd Lipcon |
| [HADOOP-7758](https://issues.apache.org/jira/browse/HADOOP-7758) | Make GlobFilter class public |  Major | fs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7736](https://issues.apache.org/jira/browse/HADOOP-7736) | Remove duplicate call of Path#normalizePath during initialization. |  Trivial | fs | Harsh J | Harsh J |
| [HADOOP-7657](https://issues.apache.org/jira/browse/HADOOP-7657) | Add support for LZ4 compression |  Major | . | Bert Sanders | Binglin Chang |
| [HADOOP-7574](https://issues.apache.org/jira/browse/HADOOP-7574) | Improvement for FSshell -stat |  Trivial | fs | XieXianshan | XieXianshan |
| [HADOOP-7504](https://issues.apache.org/jira/browse/HADOOP-7504) | hadoop-metrics.properties missing some Ganglia31 options |  Trivial | metrics | Eli Collins | Harsh J |
| [HADOOP-7424](https://issues.apache.org/jira/browse/HADOOP-7424) | Log an error if the topology script doesn't handle multiple args |  Major | . | Eli Collins | Uma Maheswara Rao G |
| [HADOOP-6886](https://issues.apache.org/jira/browse/HADOOP-6886) | LocalFileSystem Needs createNonRecursive API |  Minor | fs | Nicolas Spiegelberg | Nicolas Spiegelberg |
| [HADOOP-6840](https://issues.apache.org/jira/browse/HADOOP-6840) | Support non-recursive create() in FileSystem & SequenceFile.Writer |  Minor | fs, io | Nicolas Spiegelberg | Nicolas Spiegelberg |
| [HADOOP-6614](https://issues.apache.org/jira/browse/HADOOP-6614) | RunJar should provide more diags when it can't create a temp file |  Minor | util | Steve Loughran | Jonathan Hsieh |
| [HADOOP-4515](https://issues.apache.org/jira/browse/HADOOP-4515) | conf.getBoolean must be case insensitive |  Minor | . | Abhijit Bagri | Sho Shimauchi |
| [HDFS-3139](https://issues.apache.org/jira/browse/HDFS-3139) | Minor Datanode logging improvement |  Minor | datanode | Eli Collins | Eli Collins |
| [HDFS-2868](https://issues.apache.org/jira/browse/HDFS-2868) | Add number of active transfer threads to the DataNode status |  Minor | datanode | Harsh J | Harsh J |
| [HDFS-2826](https://issues.apache.org/jira/browse/HDFS-2826) | Test case for HDFS-1476 (safemode can initialize repl queues before exiting) |  Minor | namenode, test | Todd Lipcon | Todd Lipcon |
| [HDFS-2825](https://issues.apache.org/jira/browse/HDFS-2825) | Add test hook to turn off the writer preferring its local DN |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2817](https://issues.apache.org/jira/browse/HDFS-2817) | Combine the two TestSafeMode test suites |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HDFS-2814](https://issues.apache.org/jira/browse/HDFS-2814) | NamenodeMXBean does not account for svn revision in the version information |  Minor | . | Hitesh Shah | Hitesh Shah |
| [HDFS-2803](https://issues.apache.org/jira/browse/HDFS-2803) | Adding logging to LeaseRenewer for better lease expiration triage. |  Minor | namenode | Jimmy Xiang | Jimmy Xiang |
| [HDFS-2788](https://issues.apache.org/jira/browse/HDFS-2788) | HdfsServerConstants#DN\_KEEPALIVE\_TIMEOUT is dead code |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2761](https://issues.apache.org/jira/browse/HDFS-2761) | Improve Hadoop subcomponent integration in Hadoop 0.23 |  Major | build, hdfs-client, scripts | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-2729](https://issues.apache.org/jira/browse/HDFS-2729) | Update BlockManager's comments regarding the invalid block set |  Minor | namenode | Harsh J | Harsh J |
| [HDFS-2726](https://issues.apache.org/jira/browse/HDFS-2726) | "Exception in createBlockOutputStream" shouldn't delete exception stack trace |  Major | . | Michael Bieniosek | Harsh J |
| [HDFS-2675](https://issues.apache.org/jira/browse/HDFS-2675) | Reduce verbosity when double-closing edit logs |  Trivial | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2654](https://issues.apache.org/jira/browse/HDFS-2654) | Make BlockReaderLocal not extend RemoteBlockReader2 |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2653](https://issues.apache.org/jira/browse/HDFS-2653) | DFSClient should cache whether addrs are non-local when short-circuiting is enabled |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2604](https://issues.apache.org/jira/browse/HDFS-2604) | Add a log message to show if WebHDFS is enabled |  Minor | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2570](https://issues.apache.org/jira/browse/HDFS-2570) | Add descriptions for dfs.\*.https.address in hdfs-default.xml |  Trivial | documentation | Eli Collins | Eli Collins |
| [HDFS-2568](https://issues.apache.org/jira/browse/HDFS-2568) | Use a set to manage child sockets in XceiverServer |  Trivial | datanode | Harsh J | Harsh J |
| [HDFS-2566](https://issues.apache.org/jira/browse/HDFS-2566) | Move BPOfferService to be a non-inner class |  Minor | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2563](https://issues.apache.org/jira/browse/HDFS-2563) | Some cleanup in BPOfferService |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2562](https://issues.apache.org/jira/browse/HDFS-2562) | Refactor DN configuration variables out of DataNode class |  Minor | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2560](https://issues.apache.org/jira/browse/HDFS-2560) | Refactor BPOfferService to be a static inner class |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2536](https://issues.apache.org/jira/browse/HDFS-2536) | Remove unused imports |  Trivial | namenode | Aaron T. Myers | Harsh J |
| [HDFS-2533](https://issues.apache.org/jira/browse/HDFS-2533) | Remove needless synchronization on FSDataSet.getBlockFile |  Minor | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-2511](https://issues.apache.org/jira/browse/HDFS-2511) | Add dev script to generate HDFS protobufs |  Minor | build | Todd Lipcon | Alejandro Abdelnur |
| [HDFS-2502](https://issues.apache.org/jira/browse/HDFS-2502) | hdfs-default.xml should include dfs.name.dir.restore |  Minor | documentation | Eli Collins | Harsh J |
| [HDFS-2454](https://issues.apache.org/jira/browse/HDFS-2454) | Move maxXceiverCount check to before starting the thread in dataXceiver |  Minor | datanode | Uma Maheswara Rao G | Harsh J |
| [HDFS-2397](https://issues.apache.org/jira/browse/HDFS-2397) | Undeprecate SecondaryNameNode |  Major | namenode | Todd Lipcon | Eli Collins |
| [HDFS-2349](https://issues.apache.org/jira/browse/HDFS-2349) | DN should log a WARN, not an INFO when it detects a corruption during block transfer |  Trivial | datanode | Harsh J | Harsh J |
| [HDFS-2335](https://issues.apache.org/jira/browse/HDFS-2335) | DataNodeCluster and NNStorage always pull fresh entropy |  Major | datanode, namenode | Eli Collins | Uma Maheswara Rao G |
| [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | Shortcut a local client reads to a Datanodes files directly |  Major | . | Sanjay Radia | Jitendra Nath Pandey |
| [HDFS-2178](https://issues.apache.org/jira/browse/HDFS-2178) | HttpFS - a read/write Hadoop file system proxy |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2080](https://issues.apache.org/jira/browse/HDFS-2080) | Speed up DFS read path by lessening checksum overhead |  Major | hdfs-client, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-554](https://issues.apache.org/jira/browse/HDFS-554) | BlockInfo.ensureCapacity may get a speedup from System.arraycopy() |  Minor | namenode | Steve Loughran | Harsh J |
| [HDFS-362](https://issues.apache.org/jira/browse/HDFS-362) | FSEditLog should not writes long and short as UTF8 and should not use ArrayWritable for writing non-array items |  Major | namenode | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [MAPREDUCE-3771](https://issues.apache.org/jira/browse/MAPREDUCE-3771) | Port MAPREDUCE-1735 to trunk/0.23 |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3756](https://issues.apache.org/jira/browse/MAPREDUCE-3756) | Make single shuffle limit configurable |  Major | mrv2 | Arun C Murthy | Hitesh Shah |
| [MAPREDUCE-3693](https://issues.apache.org/jira/browse/MAPREDUCE-3693) | Add admin env to mapred-default.xml |  Minor | mrv2 | Roman Shaposhnik | Roman Shaposhnik |
| [MAPREDUCE-3692](https://issues.apache.org/jira/browse/MAPREDUCE-3692) | yarn-resourcemanager out and log files can get big |  Blocker | mrv2 | Eli Collins | Eli Collins |
| [MAPREDUCE-3679](https://issues.apache.org/jira/browse/MAPREDUCE-3679) | AM logs and others should not automatically refresh after every 1 second. |  Major | mrv2 | Mahadev konar | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3610](https://issues.apache.org/jira/browse/MAPREDUCE-3610) | Some parts in MR use old property dfs.block.size |  Minor | . | Sho Shimauchi | Sho Shimauchi |
| [MAPREDUCE-3597](https://issues.apache.org/jira/browse/MAPREDUCE-3597) | Provide a way to access other info of history file from Rumentool |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3481](https://issues.apache.org/jira/browse/MAPREDUCE-3481) | [Gridmix] Improve STRESS mode locking |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-3415](https://issues.apache.org/jira/browse/MAPREDUCE-3415) | improve MiniMRYarnCluster & DistributedShell JAR resolution |  Major | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3411](https://issues.apache.org/jira/browse/MAPREDUCE-3411) | Performance Upgrade for jQuery |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3371](https://issues.apache.org/jira/browse/MAPREDUCE-3371) | Review and improve the yarn-api javadocs. |  Minor | documentation, mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3369](https://issues.apache.org/jira/browse/MAPREDUCE-3369) | Migrate MR1 tests to run on MR2 using the new interfaces introduced in MAPREDUCE-3169 |  Major | mrv1, mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-3360](https://issues.apache.org/jira/browse/MAPREDUCE-3360) | Provide information about lost nodes in the UI. |  Critical | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3341](https://issues.apache.org/jira/browse/MAPREDUCE-3341) | Enhance logging of initalized queue limit values |  Major | mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-3331](https://issues.apache.org/jira/browse/MAPREDUCE-3331) | Improvement to single node cluster setup documentation for 0.23 |  Minor | mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-3325](https://issues.apache.org/jira/browse/MAPREDUCE-3325) | Improvements to CapacityScheduler doc |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3299](https://issues.apache.org/jira/browse/MAPREDUCE-3299) | Add AMInfo table to the AM job page |  Minor | mrv2 | Siddharth Seth | Jonathan Eagles |
| [MAPREDUCE-3265](https://issues.apache.org/jira/browse/MAPREDUCE-3265) | Reduce log level on MR2 IPC construction, etc |  Blocker | mrv2 | Todd Lipcon | Arun C Murthy |
| [MAPREDUCE-3238](https://issues.apache.org/jira/browse/MAPREDUCE-3238) | Small cleanup in SchedulerApp |  Trivial | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3169](https://issues.apache.org/jira/browse/MAPREDUCE-3169) | Create a new MiniMRCluster equivalent which only provides client APIs cross MR1 and MR2 |  Major | mrv1, mrv2, test | Todd Lipcon | Ahmed Radwan |
| [MAPREDUCE-3147](https://issues.apache.org/jira/browse/MAPREDUCE-3147) | Handle leaf queues with the same name properly |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2863](https://issues.apache.org/jira/browse/MAPREDUCE-2863) | Support web-services for RM & NM |  Blocker | mrv2, nodemanager, resourcemanager | Arun C Murthy | Thomas Graves |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8055](https://issues.apache.org/jira/browse/HADOOP-8055) | Distribution tar.gz does not contain etc/hadoop/core-site.xml |  Major | build | Eric Charles | Harsh J |
| [HADOOP-8054](https://issues.apache.org/jira/browse/HADOOP-8054) | NPE with FilterFileSystem |  Critical | fs | Amareshwari Sriramadasu | Daryn Sharp |
| [HADOOP-8052](https://issues.apache.org/jira/browse/HADOOP-8052) | Hadoop Metrics2 should emit Float.MAX\_VALUE (instead of Double.MAX\_VALUE) to avoid making Ganglia's gmetad core |  Major | metrics | Varun Kapoor | Varun Kapoor |
| [HADOOP-8018](https://issues.apache.org/jira/browse/HADOOP-8018) | Hudson auto test for HDFS has started throwing javadoc: warning - Error fetching URL: http://java.sun.com/javase/6/docs/api/package-list |  Major | build, test | Matt Foley | Jonathan Eagles |
| [HADOOP-8012](https://issues.apache.org/jira/browse/HADOOP-8012) | hadoop-daemon.sh and yarn-daemon.sh are trying to mkdir and chow log/pid dirs which can fail |  Minor | scripts | Roman Shaposhnik | Roman Shaposhnik |
| [HADOOP-8006](https://issues.apache.org/jira/browse/HADOOP-8006) | TestFSInputChecker is failing in trunk. |  Major | fs | Uma Maheswara Rao G | Daryn Sharp |
| [HADOOP-8002](https://issues.apache.org/jira/browse/HADOOP-8002) | SecurityUtil acquired token message should be a debug rather than info |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8001](https://issues.apache.org/jira/browse/HADOOP-8001) | ChecksumFileSystem's rename doesn't correctly handle checksum files |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8000](https://issues.apache.org/jira/browse/HADOOP-8000) | fetchdt command not available in bin/hadoop |  Critical | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-7999](https://issues.apache.org/jira/browse/HADOOP-7999) | "hadoop archive" fails with ClassNotFoundException |  Critical | scripts | Jason Lowe | Jason Lowe |
| [HADOOP-7998](https://issues.apache.org/jira/browse/HADOOP-7998) | CheckFileSystem does not correctly honor setVerifyChecksum |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7993](https://issues.apache.org/jira/browse/HADOOP-7993) | Hadoop ignores old-style config options for enabling compressed output |  Major | conf | Anupam Seth | Anupam Seth |
| [HADOOP-7988](https://issues.apache.org/jira/browse/HADOOP-7988) | Upper case in hostname part of the principals doesn't work with kerberos. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7986](https://issues.apache.org/jira/browse/HADOOP-7986) | Add config for History Server protocol in hadoop-policy for service level authorization. |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-7982](https://issues.apache.org/jira/browse/HADOOP-7982) | UserGroupInformation fails to login if thread's context classloader can't load HadoopLoginModule |  Major | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-7981](https://issues.apache.org/jira/browse/HADOOP-7981) | Improve documentation for org.apache.hadoop.io.compress.Decompressor.getRemaining |  Major | io | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7975](https://issues.apache.org/jira/browse/HADOOP-7975) | Add entry to XML defaults for new LZ4 codec |  Minor | . | Harsh J | Harsh J |
| [HADOOP-7974](https://issues.apache.org/jira/browse/HADOOP-7974) | TestViewFsTrash incorrectly determines the user's home directory |  Major | fs, test | Eli Collins | Harsh J |
| [HADOOP-7971](https://issues.apache.org/jira/browse/HADOOP-7971) | hadoop \<job/queue/pipes\> removed - should be added back, but deprecated |  Blocker | . | Thomas Graves | Prashant Sharma |
| [HADOOP-7964](https://issues.apache.org/jira/browse/HADOOP-7964) | Deadlock in class init. |  Blocker | security, util | Kihwal Lee | Daryn Sharp |
| [HADOOP-7963](https://issues.apache.org/jira/browse/HADOOP-7963) | test failures: TestViewFileSystemWithAuthorityLocalFileSystem and TestViewFileSystemLocalFileSystem |  Blocker | . | Thomas Graves | Siddharth Seth |
| [HADOOP-7949](https://issues.apache.org/jira/browse/HADOOP-7949) | Updated maxIdleTime default in the code to match core-default.xml |  Trivial | ipc | Eli Collins | Eli Collins |
| [HADOOP-7948](https://issues.apache.org/jira/browse/HADOOP-7948) | Shell scripts created by hadoop-dist/pom.xml to build tar do not properly propagate failure |  Minor | build | Michajlo Matijkiw | Michajlo Matijkiw |
| [HADOOP-7936](https://issues.apache.org/jira/browse/HADOOP-7936) | There's a Hoop README in the root dir of the tarball |  Major | build | Eli Collins | Alejandro Abdelnur |
| [HADOOP-7933](https://issues.apache.org/jira/browse/HADOOP-7933) | Viewfs changes for MAPREDUCE-3529 |  Critical | viewfs | Siddharth Seth | Siddharth Seth |
| [HADOOP-7917](https://issues.apache.org/jira/browse/HADOOP-7917) | compilation of protobuf files fails in windows/cygwin |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7914](https://issues.apache.org/jira/browse/HADOOP-7914) | duplicate declaration of hadoop-hdfs test-jar |  Major | build | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7912](https://issues.apache.org/jira/browse/HADOOP-7912) | test-patch should run eclipse:eclipse to verify that it does not break again |  Major | build | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-7907](https://issues.apache.org/jira/browse/HADOOP-7907) | hadoop-tools JARs are not part of the distro |  Blocker | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7902](https://issues.apache.org/jira/browse/HADOOP-7902) | skipping name rules setting (if already set) should be done on UGI initialization only |  Major | . | Tsz Wo Nicholas Sze | Alejandro Abdelnur |
| [HADOOP-7898](https://issues.apache.org/jira/browse/HADOOP-7898) | Fix javadoc warnings in AuthenticationToken.java |  Minor | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7887](https://issues.apache.org/jira/browse/HADOOP-7887) | KerberosAuthenticatorHandler is not setting KerberosName name rules from configuration |  Critical | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7878](https://issues.apache.org/jira/browse/HADOOP-7878) | Regression HADOOP-7777 switch changes break HDFS tests when the isSingleSwitch() predicate is used |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-7870](https://issues.apache.org/jira/browse/HADOOP-7870) | fix SequenceFile#createWriter with boolean createParent arg to respect createParent. |  Major | . | Jonathan Hsieh | Jonathan Hsieh |
| [HADOOP-7864](https://issues.apache.org/jira/browse/HADOOP-7864) | Building mvn site with Maven \< 3.0.2 causes OOM errors |  Major | build | Andrew Bayer | Andrew Bayer |
| [HADOOP-7859](https://issues.apache.org/jira/browse/HADOOP-7859) | TestViewFsHdfs.testgetFileLinkStatus is failing an assert |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-7854](https://issues.apache.org/jira/browse/HADOOP-7854) | UGI getCurrentUser is not synchronized |  Critical | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-7853](https://issues.apache.org/jira/browse/HADOOP-7853) | multiple javax security configurations cause conflicts |  Blocker | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-7851](https://issues.apache.org/jira/browse/HADOOP-7851) | Configuration.getClasses() never returns the default value. |  Major | conf | Amar Kamat | Uma Maheswara Rao G |
| [HADOOP-7843](https://issues.apache.org/jira/browse/HADOOP-7843) | compilation failing because workDir not initialized in RunJar.java |  Major | . | John George | John George |
| [HADOOP-7837](https://issues.apache.org/jira/browse/HADOOP-7837) | no NullAppender in the log4j config |  Major | conf | Steve Loughran | Eli Collins |
| [HADOOP-7813](https://issues.apache.org/jira/browse/HADOOP-7813) | test-patch +1 patches that introduce javadoc and findbugs warnings in some cases |  Major | build, test | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7811](https://issues.apache.org/jira/browse/HADOOP-7811) | TestUserGroupInformation#testGetServerSideGroups test fails in chroot |  Major | security, test | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7810](https://issues.apache.org/jira/browse/HADOOP-7810) | move hadoop archive to core from tools |  Blocker | . | John George | John George |
| [HADOOP-7802](https://issues.apache.org/jira/browse/HADOOP-7802) | Hadoop scripts unconditionally source "$bin"/../libexec/hadoop-config.sh. |  Major | . | Bruno Mahé | Bruno Mahé |
| [HADOOP-7801](https://issues.apache.org/jira/browse/HADOOP-7801) | HADOOP\_PREFIX cannot be overriden |  Major | build | Bruno Mahé | Bruno Mahé |
| [HADOOP-7787](https://issues.apache.org/jira/browse/HADOOP-7787) | Make source tarball use conventional name. |  Major | build | Bruno Mahé | Bruno Mahé |
| [HADOOP-6490](https://issues.apache.org/jira/browse/HADOOP-6490) | Path.normalize should use StringUtils.replace in favor of String.replace |  Minor | fs | Zheng Shao | Uma Maheswara Rao G |
| [HDFS-2923](https://issues.apache.org/jira/browse/HDFS-2923) | Namenode IPC handler count uses the wrong configuration key |  Critical | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2893](https://issues.apache.org/jira/browse/HDFS-2893) | The start/stop scripts don't start/stop the 2NN when using the default configuration |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-2889](https://issues.apache.org/jira/browse/HDFS-2889) | getNumCurrentReplicas is package private but should be public on 0.23 (see HDFS-2408) |  Major | hdfs-client | Gregory Chanan | Gregory Chanan |
| [HDFS-2869](https://issues.apache.org/jira/browse/HDFS-2869) | Error in Webhdfs documentation for mkdir |  Minor | webhdfs | Harsh J | Harsh J |
| [HDFS-2840](https://issues.apache.org/jira/browse/HDFS-2840) | TestHostnameFilter should work with localhost or localhost.localdomain |  Major | test | Eli Collins | Alejandro Abdelnur |
| [HDFS-2837](https://issues.apache.org/jira/browse/HDFS-2837) | mvn javadoc:javadoc not seeing LimitedPrivate class |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2836](https://issues.apache.org/jira/browse/HDFS-2836) | HttpFSServer still has 2 javadoc warnings in trunk |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2835](https://issues.apache.org/jira/browse/HDFS-2835) | Fix org.apache.hadoop.hdfs.tools.GetConf$Command Findbug issue |  Major | tools | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2827](https://issues.apache.org/jira/browse/HDFS-2827) | Cannot save namespace after renaming a directory above a file with an open lease |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2822](https://issues.apache.org/jira/browse/HDFS-2822) | processMisReplicatedBlock incorrectly identifies under-construction blocks as under-replicated |  Major | ha, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2818](https://issues.apache.org/jira/browse/HDFS-2818) | dfshealth.jsp missing space between role and node name |  Trivial | namenode | Todd Lipcon | Devaraj K |
| [HDFS-2816](https://issues.apache.org/jira/browse/HDFS-2816) | Fix missing license header in hadoop-hdfs-project/hadoop-hdfs-httpfs/dev-support/findbugsExcludeFile.xml |  Trivial | . | Hitesh Shah | Hitesh Shah |
| [HDFS-2810](https://issues.apache.org/jira/browse/HDFS-2810) | Leases not properly getting renewed by clients |  Critical | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-2791](https://issues.apache.org/jira/browse/HDFS-2791) | If block report races with closing of file, replica is incorrectly marked corrupt |  Major | datanode, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2790](https://issues.apache.org/jira/browse/HDFS-2790) | FSNamesystem.setTimes throws exception with wrong configuration name in the message |  Minor | . | Arpit Gupta | Arpit Gupta |
| [HDFS-2751](https://issues.apache.org/jira/browse/HDFS-2751) | Datanode drops OS cache behind reads even for short reads |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2722](https://issues.apache.org/jira/browse/HDFS-2722) | HttpFs shouldn't be using an int for block size |  Major | hdfs-client | Harsh J | Harsh J |
| [HDFS-2710](https://issues.apache.org/jira/browse/HDFS-2710) | HDFS part of MAPREDUCE-3529, HADOOP-7933 |  Critical | . | Siddharth Seth |  |
| [HDFS-2707](https://issues.apache.org/jira/browse/HDFS-2707) | HttpFS should read the hadoop-auth secret from a file instead inline from the configuration |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2706](https://issues.apache.org/jira/browse/HDFS-2706) | Use configuration for blockInvalidateLimit if it is set |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2705](https://issues.apache.org/jira/browse/HDFS-2705) | HttpFS server should check that upload requests have correct content-type |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2658](https://issues.apache.org/jira/browse/HDFS-2658) | HttpFS introduced 70 javadoc warnings |  Major | . | Eli Collins | Alejandro Abdelnur |
| [HDFS-2657](https://issues.apache.org/jira/browse/HDFS-2657) | TestHttpFSServer and TestServerWebApp are failing on trunk |  Major | . | Eli Collins | Alejandro Abdelnur |
| [HDFS-2649](https://issues.apache.org/jira/browse/HDFS-2649) | eclipse:eclipse build fails for hadoop-hdfs-httpfs |  Major | build | Jason Lowe | Jason Lowe |
| [HDFS-2646](https://issues.apache.org/jira/browse/HDFS-2646) | Hadoop HttpFS introduced 4 findbug warnings. |  Major | . | Uma Maheswara Rao G | Alejandro Abdelnur |
| [HDFS-2640](https://issues.apache.org/jira/browse/HDFS-2640) | Javadoc generation hangs |  Major | . | Tom White | Tom White |
| [HDFS-2614](https://issues.apache.org/jira/browse/HDFS-2614) | hadoop dist tarball is missing hdfs headers |  Major | build | Bruno Mahé | Alejandro Abdelnur |
| [HDFS-2606](https://issues.apache.org/jira/browse/HDFS-2606) | webhdfs client filesystem impl must set the content-type header for create/append |  Critical | webhdfs | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2596](https://issues.apache.org/jira/browse/HDFS-2596) | TestDirectoryScanner doesn't test parallel scans |  Major | datanode, test | Eli Collins | Eli Collins |
| [HDFS-2594](https://issues.apache.org/jira/browse/HDFS-2594) | webhdfs HTTP API should implement getDelegationTokens() instead getDelegationToken() |  Critical | webhdfs | Alejandro Abdelnur | Tsz Wo Nicholas Sze |
| [HDFS-2590](https://issues.apache.org/jira/browse/HDFS-2590) | Some links in WebHDFS forrest doc do not work |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2588](https://issues.apache.org/jira/browse/HDFS-2588) | hdfs jsp pages missing DOCTYPE [post-split branches] |  Trivial | scripts | Dave Vronay | Dave Vronay |
| [HDFS-2575](https://issues.apache.org/jira/browse/HDFS-2575) | DFSTestUtil may create empty files |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HDFS-2567](https://issues.apache.org/jira/browse/HDFS-2567) | When 0 DNs are available, show a proper error when trying to browse DFS via web UI |  Major | namenode | Harsh J | Harsh J |
| [HDFS-2553](https://issues.apache.org/jira/browse/HDFS-2553) | BlockPoolSliceScanner spinning in loop |  Critical | datanode | Todd Lipcon | Uma Maheswara Rao G |
| [HDFS-2545](https://issues.apache.org/jira/browse/HDFS-2545) | Webhdfs: Support multiple namenodes in federation |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2544](https://issues.apache.org/jira/browse/HDFS-2544) | Hadoop scripts unconditionally source "$bin"/../libexec/hadoop-config.sh. |  Major | scripts | Bruno Mahé | Bruno Mahé |
| [HDFS-2543](https://issues.apache.org/jira/browse/HDFS-2543) | HADOOP\_PREFIX cannot be overriden |  Major | scripts | Bruno Mahé | Bruno Mahé |
| [HDFS-2541](https://issues.apache.org/jira/browse/HDFS-2541) | For a sufficiently large value of blocks, the DN Scanner may request a random number with a negative seed value. |  Major | datanode | Harsh J | Harsh J |
| [HDFS-1314](https://issues.apache.org/jira/browse/HDFS-1314) | dfs.blocksize accepts only absolute value |  Minor | . | Karim Saadah | Sho Shimauchi |
| [HDFS-442](https://issues.apache.org/jira/browse/HDFS-442) | dfsthroughput in test.jar throws NPE |  Minor | test | Ramya Sunil | Harsh J |
| [HDFS-69](https://issues.apache.org/jira/browse/HDFS-69) | Improve dfsadmin command line help |  Minor | . | Ravi Phulari | Harsh J |
| [MAPREDUCE-3880](https://issues.apache.org/jira/browse/MAPREDUCE-3880) | Allow for 32-bit container-executor |  Blocker | build, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3858](https://issues.apache.org/jira/browse/MAPREDUCE-3858) | Task attempt failure during commit results in task never completing |  Critical | mrv2 | Tom White | Tom White |
| [MAPREDUCE-3856](https://issues.apache.org/jira/browse/MAPREDUCE-3856) | Instances of RunningJob class givs incorrect job tracking urls when mutiple jobs are submitted from same client jvm. |  Critical | mrv2 | Eric Payne | Eric Payne |
| [MAPREDUCE-3843](https://issues.apache.org/jira/browse/MAPREDUCE-3843) | Job summary log file found missing on the RM host |  Critical | jobhistoryserver, mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-3840](https://issues.apache.org/jira/browse/MAPREDUCE-3840) | JobEndNotifier doesn't use the proxyToUse during connecting |  Blocker | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3834](https://issues.apache.org/jira/browse/MAPREDUCE-3834) | If multiple hosts for a split belong to the same rack, the rack is added multiple times in the AM request table |  Critical | mr-am, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3833](https://issues.apache.org/jira/browse/MAPREDUCE-3833) | Capacity scheduler queue refresh doesn't recompute queue capacities properly |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3828](https://issues.apache.org/jira/browse/MAPREDUCE-3828) | Broken urls: AM tracking url and jobhistory url in a single node setup. |  Major | mrv2 | Ahmed Radwan | Siddharth Seth |
| [MAPREDUCE-3826](https://issues.apache.org/jira/browse/MAPREDUCE-3826) | RM UI when loaded throws a message stating Data Tables warning and then the column sorting stops working |  Major | mrv2 | Arpit Gupta | Jonathan Eagles |
| [MAPREDUCE-3822](https://issues.apache.org/jira/browse/MAPREDUCE-3822) | TestJobCounters is failing intermittently on trunk and 0.23. |  Critical | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3817](https://issues.apache.org/jira/browse/MAPREDUCE-3817) | bin/mapred command cannot run distcp and archive jobs |  Major | mrv2 | Arpit Gupta | Arpit Gupta |
| [MAPREDUCE-3814](https://issues.apache.org/jira/browse/MAPREDUCE-3814) | MR1 compile fails |  Major | mrv1, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3808](https://issues.apache.org/jira/browse/MAPREDUCE-3808) | NPE in FileOutputCommitter when running a 0 reduce job |  Blocker | mrv2 | Siddharth Seth | Robert Joseph Evans |
| [MAPREDUCE-3804](https://issues.apache.org/jira/browse/MAPREDUCE-3804) | yarn webapp interface vulnerable to cross scripting attacks |  Major | jobhistoryserver, mrv2, resourcemanager | Dave Thompson | Dave Thompson |
| [MAPREDUCE-3795](https://issues.apache.org/jira/browse/MAPREDUCE-3795) | "job -status" command line output is malformed |  Major | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3794](https://issues.apache.org/jira/browse/MAPREDUCE-3794) | Support mapred.Task.Counter and mapred.JobInProgress.Counter enums for compatibility |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-3791](https://issues.apache.org/jira/browse/MAPREDUCE-3791) | can't build site in hadoop-yarn-server-common |  Major | documentation, mrv2 | Roman Shaposhnik | Mahadev konar |
| [MAPREDUCE-3784](https://issues.apache.org/jira/browse/MAPREDUCE-3784) | maxActiveApplications(\|PerUser) per queue is too low for small clusters |  Major | mrv2 | Ramya Sunil | Arun C Murthy |
| [MAPREDUCE-3780](https://issues.apache.org/jira/browse/MAPREDUCE-3780) | RM assigns containers to killed applications |  Blocker | mrv2 | Ramya Sunil | Hitesh Shah |
| [MAPREDUCE-3775](https://issues.apache.org/jira/browse/MAPREDUCE-3775) | Change MiniYarnCluster to escape special chars in testname |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3774](https://issues.apache.org/jira/browse/MAPREDUCE-3774) | yarn-default.xml should be moved to hadoop-yarn-common. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3770](https://issues.apache.org/jira/browse/MAPREDUCE-3770) | [Rumen] Zombie.getJobConf() results into NPE |  Critical | tools/rumen | Amar Kamat | Amar Kamat |
| [MAPREDUCE-3765](https://issues.apache.org/jira/browse/MAPREDUCE-3765) | FifoScheduler does not respect yarn.scheduler.fifo.minimum-allocation-mb setting |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3764](https://issues.apache.org/jira/browse/MAPREDUCE-3764) | AllocatedGB etc metrics incorrect if min-allocation-mb isn't a multiple of 1GB |  Critical | mrv2 | Siddharth Seth | Arun C Murthy |
| [MAPREDUCE-3762](https://issues.apache.org/jira/browse/MAPREDUCE-3762) | Resource Manager fails to come up with default capacity scheduler configs. |  Critical | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3760](https://issues.apache.org/jira/browse/MAPREDUCE-3760) | Blacklisted NMs should not appear in Active nodes list |  Major | mrv2 | Ramya Sunil | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3759](https://issues.apache.org/jira/browse/MAPREDUCE-3759) | ClassCastException thrown in -list-active-trackers when there are a few unhealthy nodes |  Major | mrv2 | Ramya Sunil | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3754](https://issues.apache.org/jira/browse/MAPREDUCE-3754) | RM webapp should have pages filtered based on App-state |  Major | mrv2, webapps | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3752](https://issues.apache.org/jira/browse/MAPREDUCE-3752) | Headroom should be capped by queue max-cap |  Blocker | mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3749](https://issues.apache.org/jira/browse/MAPREDUCE-3749) | ConcurrentModificationException in counter groups |  Blocker | mrv2 | Tom White | Tom White |
| [MAPREDUCE-3748](https://issues.apache.org/jira/browse/MAPREDUCE-3748) | Move CS related nodeUpdate log messages to DEBUG |  Minor | mrv2 | Ramya Sunil | Ramya Sunil |
| [MAPREDUCE-3747](https://issues.apache.org/jira/browse/MAPREDUCE-3747) | Memory Total is not refreshed until an app is launched |  Major | mrv2 | Ramya Sunil | Arun C Murthy |
| [MAPREDUCE-3744](https://issues.apache.org/jira/browse/MAPREDUCE-3744) | Unable to retrieve application logs via "yarn logs" or "mapred job -logs" |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3742](https://issues.apache.org/jira/browse/MAPREDUCE-3742) | "yarn logs" command fails with ClassNotFoundException |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3737](https://issues.apache.org/jira/browse/MAPREDUCE-3737) | The Web Application Proxy's is not documented very well |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3735](https://issues.apache.org/jira/browse/MAPREDUCE-3735) | Add distcp jar to the distribution (tar) |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3733](https://issues.apache.org/jira/browse/MAPREDUCE-3733) | Add Apache License Header to hadoop-distcp/pom.xml |  Major | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3732](https://issues.apache.org/jira/browse/MAPREDUCE-3732) | CS should only use 'activeUsers with pending requests' for computing user-limits |  Blocker | mrv2, resourcemanager, scheduler | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3727](https://issues.apache.org/jira/browse/MAPREDUCE-3727) | jobtoken location property in jobconf refers to wrong jobtoken file |  Critical | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3723](https://issues.apache.org/jira/browse/MAPREDUCE-3723) | TestAMWebServicesJobs & TestHSWebServicesJobs incorrectly asserting tests |  Major | mrv2, test, webapps | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3721](https://issues.apache.org/jira/browse/MAPREDUCE-3721) | Race in shuffle can cause it to hang |  Blocker | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3717](https://issues.apache.org/jira/browse/MAPREDUCE-3717) | JobClient test jar has missing files to run all the test programs. |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3716](https://issues.apache.org/jira/browse/MAPREDUCE-3716) | java.io.File.createTempFile fails in map/reduce tasks |  Blocker | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3714](https://issues.apache.org/jira/browse/MAPREDUCE-3714) | Reduce hangs in a corner case |  Blocker | mrv2, task | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3713](https://issues.apache.org/jira/browse/MAPREDUCE-3713) | Incorrect headroom reported to jobs |  Blocker | mrv2, resourcemanager | Siddharth Seth | Arun C Murthy |
| [MAPREDUCE-3712](https://issues.apache.org/jira/browse/MAPREDUCE-3712) | The mapreduce tar does not contain the hadoop-mapreduce-client-jobclient-tests.jar. |  Blocker | mrv2 | Ravi Prakash | Mahadev konar |
| [MAPREDUCE-3710](https://issues.apache.org/jira/browse/MAPREDUCE-3710) | last split generated by FileInputFormat.getSplits may not have the best locality |  Major | mrv1, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3709](https://issues.apache.org/jira/browse/MAPREDUCE-3709) | TestDistributedShell is failing |  Major | mrv2, test | Eli Collins | Hitesh Shah |
| [MAPREDUCE-3708](https://issues.apache.org/jira/browse/MAPREDUCE-3708) | Metrics: Incorrect Apps Submitted Count |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3705](https://issues.apache.org/jira/browse/MAPREDUCE-3705) | ant build fails on 0.23 branch |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3703](https://issues.apache.org/jira/browse/MAPREDUCE-3703) | ResourceManager should provide node lists in JMX output |  Critical | mrv2, resourcemanager | Eric Payne | Eric Payne |
| [MAPREDUCE-3702](https://issues.apache.org/jira/browse/MAPREDUCE-3702) | internal server error trying access application master via proxy with filter enabled |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3701](https://issues.apache.org/jira/browse/MAPREDUCE-3701) | Delete HadoopYarnRPC from 0.23 branch. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3699](https://issues.apache.org/jira/browse/MAPREDUCE-3699) | Default RPC handlers are very low for YARN servers |  Major | mrv2 | Vinod Kumar Vavilapalli | Hitesh Shah |
| [MAPREDUCE-3697](https://issues.apache.org/jira/browse/MAPREDUCE-3697) | Hadoop Counters API limits Oozie's working across different hadoop versions |  Blocker | mrv2 | John George | Mahadev konar |
| [MAPREDUCE-3696](https://issues.apache.org/jira/browse/MAPREDUCE-3696) | MR job via oozie does not work on hadoop 23 |  Blocker | mrv2 | John George | John George |
| [MAPREDUCE-3691](https://issues.apache.org/jira/browse/MAPREDUCE-3691) | webservices add support to compress response |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3689](https://issues.apache.org/jira/browse/MAPREDUCE-3689) | RM web UI doesn't handle newline in job name |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3684](https://issues.apache.org/jira/browse/MAPREDUCE-3684) | LocalDistributedCacheManager does not shut down its thread pool |  Major | client | Tom White | Tom White |
| [MAPREDUCE-3683](https://issues.apache.org/jira/browse/MAPREDUCE-3683) | Capacity scheduler LeafQueues maximum capacity calculation issues |  Blocker | mrv2 | Thomas Graves | Arun C Murthy |
| [MAPREDUCE-3681](https://issues.apache.org/jira/browse/MAPREDUCE-3681) | capacity scheduler LeafQueues calculate used capacity wrong |  Critical | mrv2 | Thomas Graves | Arun C Murthy |
| [MAPREDUCE-3669](https://issues.apache.org/jira/browse/MAPREDUCE-3669) | Getting a lot of PriviledgedActionException / SaslException when running a job |  Blocker | mrv2 | Thomas Graves | Mahadev konar |
| [MAPREDUCE-3664](https://issues.apache.org/jira/browse/MAPREDUCE-3664) | HDFS Federation Documentation has incorrect configuration example |  Minor | documentation | praveen sripati | Brandon Li |
| [MAPREDUCE-3657](https://issues.apache.org/jira/browse/MAPREDUCE-3657) | State machine visualize build fails |  Minor | build, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-3656](https://issues.apache.org/jira/browse/MAPREDUCE-3656) | Sort job on 350 scale is consistently failing with latest MRV2 code |  Blocker | applicationmaster, mrv2, resourcemanager | Karam Singh | Siddharth Seth |
| [MAPREDUCE-3652](https://issues.apache.org/jira/browse/MAPREDUCE-3652) | org.apache.hadoop.mapred.TestWebUIAuthorization.testWebUIAuthorization fails |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3651](https://issues.apache.org/jira/browse/MAPREDUCE-3651) | TestQueueManagerRefresh fails |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3649](https://issues.apache.org/jira/browse/MAPREDUCE-3649) | Job End notification gives an error on calling back. |  Blocker | mrv2 | Mahadev konar | Ravi Prakash |
| [MAPREDUCE-3648](https://issues.apache.org/jira/browse/MAPREDUCE-3648) | TestJobConf failing |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3646](https://issues.apache.org/jira/browse/MAPREDUCE-3646) | Remove redundant URL info from "mapred job" output |  Major | client, mrv2 | Ramya Sunil | Jonathan Eagles |
| [MAPREDUCE-3645](https://issues.apache.org/jira/browse/MAPREDUCE-3645) | TestJobHistory fails |  Blocker | mrv1 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3639](https://issues.apache.org/jira/browse/MAPREDUCE-3639) | TokenCache likely broken for FileSystems which don't issue delegation tokens |  Blocker | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3625](https://issues.apache.org/jira/browse/MAPREDUCE-3625) | CapacityScheduler web-ui display of queue's used capacity is broken |  Critical | mrv2 | Arun C Murthy | Jason Lowe |
| [MAPREDUCE-3624](https://issues.apache.org/jira/browse/MAPREDUCE-3624) | bin/yarn script adds jdk tools.jar to the classpath. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3617](https://issues.apache.org/jira/browse/MAPREDUCE-3617) | Remove yarn default values for resource manager and nodemanager principal |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3615](https://issues.apache.org/jira/browse/MAPREDUCE-3615) | mapred ant test failures |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3608](https://issues.apache.org/jira/browse/MAPREDUCE-3608) | MAPREDUCE-3522 commit causes compilation to fail |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3604](https://issues.apache.org/jira/browse/MAPREDUCE-3604) | Streaming's check for local mode is broken |  Blocker | contrib/streaming | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3596](https://issues.apache.org/jira/browse/MAPREDUCE-3596) | Sort benchmark got hang after completion of 99% map phase |  Blocker | applicationmaster, mrv2 | Ravi Prakash | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3588](https://issues.apache.org/jira/browse/MAPREDUCE-3588) | bin/yarn broken after MAPREDUCE-3366 |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3586](https://issues.apache.org/jira/browse/MAPREDUCE-3586) | Lots of AMs hanging around in PIG testing |  Blocker | mr-am, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3582](https://issues.apache.org/jira/browse/MAPREDUCE-3582) | Move successfully passing MR1 tests to MR2 maven tree. |  Major | mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-3579](https://issues.apache.org/jira/browse/MAPREDUCE-3579) | ConverterUtils should not include a port in a path for a URL with no port |  Major | mrv2 | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-3564](https://issues.apache.org/jira/browse/MAPREDUCE-3564) | TestStagingCleanup and TestJobEndNotifier are failing on trunk. |  Blocker | mrv2 | Mahadev konar | Siddharth Seth |
| [MAPREDUCE-3563](https://issues.apache.org/jira/browse/MAPREDUCE-3563) | LocalJobRunner doesn't handle Jobs using o.a.h.mapreduce.OutputCommitter |  Major | mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3560](https://issues.apache.org/jira/browse/MAPREDUCE-3560) | TestRMNodeTransitions is failing on trunk |  Blocker | mrv2, resourcemanager, test | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-3557](https://issues.apache.org/jira/browse/MAPREDUCE-3557) | MR1 test fail to compile because of missing hadoop-archives dependency |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3549](https://issues.apache.org/jira/browse/MAPREDUCE-3549) | write api documentation for web service apis for RM, NM, mapreduce app master, and job history server |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3544](https://issues.apache.org/jira/browse/MAPREDUCE-3544) | gridmix build is broken, requires hadoop-archives to be added as ivy dependency |  Major | build, tools/rumen | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3542](https://issues.apache.org/jira/browse/MAPREDUCE-3542) | Support "FileSystemCounter" legacy counter group name for compatibility |  Major | . | Tom White | Tom White |
| [MAPREDUCE-3541](https://issues.apache.org/jira/browse/MAPREDUCE-3541) | Fix broken TestJobQueueClient test |  Blocker | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3537](https://issues.apache.org/jira/browse/MAPREDUCE-3537) | DefaultContainerExecutor has a race condn. with multiple concurrent containers |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3532](https://issues.apache.org/jira/browse/MAPREDUCE-3532) | When 0 is provided as port number in yarn.nodemanager.webapp.address, NMs webserver component picks up random port, NM keeps on Reporting 0 port to RM |  Critical | mrv2, nodemanager | Karam Singh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3531](https://issues.apache.org/jira/browse/MAPREDUCE-3531) | Sometimes java.lang.IllegalArgumentException: Invalid key to HMAC computation in NODE\_UPDATE also causing RM to stop scheduling |  Blocker | mrv2, resourcemanager, scheduler | Karam Singh | Robert Joseph Evans |
| [MAPREDUCE-3530](https://issues.apache.org/jira/browse/MAPREDUCE-3530) | Sometimes NODE\_UPDATE to the scheduler throws an NPE causing the scheduling to stop |  Blocker | mrv2, resourcemanager, scheduler | Karam Singh | Arun C Murthy |
| [MAPREDUCE-3529](https://issues.apache.org/jira/browse/MAPREDUCE-3529) | TokenCache does not cache viewfs credentials correctly |  Critical | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3528](https://issues.apache.org/jira/browse/MAPREDUCE-3528) | The task timeout check interval should be configurable independent of mapreduce.task.timeout |  Major | mr-am, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3527](https://issues.apache.org/jira/browse/MAPREDUCE-3527) | Fix minor API incompatibilities between 1.0 and 0.23 |  Major | . | Tom White | Tom White |
| [MAPREDUCE-3522](https://issues.apache.org/jira/browse/MAPREDUCE-3522) | Capacity Scheduler ACLs not inherited by default |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3521](https://issues.apache.org/jira/browse/MAPREDUCE-3521) | Hadoop Streaming ignores unknown parameters |  Minor | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3518](https://issues.apache.org/jira/browse/MAPREDUCE-3518) | mapred queue -info \<queue\> -showJobs throws NPE |  Critical | client, mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3513](https://issues.apache.org/jira/browse/MAPREDUCE-3513) | Capacity Scheduler web UI has a spelling mistake for Memory. |  Trivial | mrv2 | Mahadev konar | chackaravarthy |
| [MAPREDUCE-3510](https://issues.apache.org/jira/browse/MAPREDUCE-3510) | Capacity Scheduler inherited ACLs not displayed by mapred queue -showacls |  Major | capacity-sched, mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3505](https://issues.apache.org/jira/browse/MAPREDUCE-3505) | yarn APPLICATION\_CLASSPATH needs to be overridable |  Major | mrv2 | Bruno Mahé | Ahmed Radwan |
| [MAPREDUCE-3500](https://issues.apache.org/jira/browse/MAPREDUCE-3500) | MRJobConfig creates an LD\_LIBRARY\_PATH using the platform ARCH |  Major | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3499](https://issues.apache.org/jira/browse/MAPREDUCE-3499) | New MiniMR does not setup proxyuser configuration correctly, thus tests using doAs do not work |  Blocker | mrv2, test | Alejandro Abdelnur | John George |
| [MAPREDUCE-3496](https://issues.apache.org/jira/browse/MAPREDUCE-3496) | Yarn initializes ACL operations from capacity scheduler config in a non-deterministic order |  Major | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3490](https://issues.apache.org/jira/browse/MAPREDUCE-3490) | RMContainerAllocator counts failed maps towards Reduce ramp up |  Blocker | mr-am, mrv2 | Siddharth Seth | Sharad Agarwal |
| [MAPREDUCE-3488](https://issues.apache.org/jira/browse/MAPREDUCE-3488) | Streaming jobs are failing because the main class isnt set in the pom files. |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3487](https://issues.apache.org/jira/browse/MAPREDUCE-3487) | jobhistory web ui task counters no longer links to singletakecounter page |  Critical | mrv2 | Thomas Graves | Jason Lowe |
| [MAPREDUCE-3484](https://issues.apache.org/jira/browse/MAPREDUCE-3484) | JobEndNotifier is getting interrupted before completing all its retries. |  Major | mr-am, mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3479](https://issues.apache.org/jira/browse/MAPREDUCE-3479) | JobClient#getJob cannot find local jobs |  Major | client | Tom White | Tom White |
| [MAPREDUCE-3478](https://issues.apache.org/jira/browse/MAPREDUCE-3478) | Cannot build against ZooKeeper 3.4.0 |  Minor | mrv2 | Andrew Bayer | Tom White |
| [MAPREDUCE-3477](https://issues.apache.org/jira/browse/MAPREDUCE-3477) | Hadoop site documentation cannot be built anymore on trunk and branch-0.23 |  Major | documentation, mrv2 | Bruno Mahé | Jonathan Eagles |
| [MAPREDUCE-3465](https://issues.apache.org/jira/browse/MAPREDUCE-3465) | org.apache.hadoop.yarn.util.TestLinuxResourceCalculatorPlugin fails on 0.23 |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3464](https://issues.apache.org/jira/browse/MAPREDUCE-3464) | mapreduce jsp pages missing DOCTYPE [post-split branches] |  Trivial | . | Dave Vronay | Dave Vronay |
| [MAPREDUCE-3463](https://issues.apache.org/jira/browse/MAPREDUCE-3463) | Second AM fails to recover properly when first AM is killed with java.lang.IllegalArgumentException causing lost job |  Blocker | applicationmaster, mrv2 | Karam Singh | Siddharth Seth |
| [MAPREDUCE-3462](https://issues.apache.org/jira/browse/MAPREDUCE-3462) | Job submission failing in JUnit tests |  Blocker | mrv2, test | Amar Kamat | Ravi Prakash |
| [MAPREDUCE-3460](https://issues.apache.org/jira/browse/MAPREDUCE-3460) | MR AM can hang if containers are allocated on a node blacklisted by the AM |  Blocker | mr-am, mrv2 | Siddharth Seth | Robert Joseph Evans |
| [MAPREDUCE-3458](https://issues.apache.org/jira/browse/MAPREDUCE-3458) | Fix findbugs warnings in hadoop-examples |  Major | mrv2 | Arun C Murthy | Devaraj K |
| [MAPREDUCE-3456](https://issues.apache.org/jira/browse/MAPREDUCE-3456) | $HADOOP\_PREFIX/bin/yarn should set defaults for $HADOOP\_\*\_HOME |  Blocker | mrv2 | Eric Payne | Eric Payne |
| [MAPREDUCE-3454](https://issues.apache.org/jira/browse/MAPREDUCE-3454) | [Gridmix] TestDistCacheEmulation is broken |  Major | contrib/gridmix | Amar Kamat | Hitesh Shah |
| [MAPREDUCE-3453](https://issues.apache.org/jira/browse/MAPREDUCE-3453) | RM web ui application details page shows RM cluster about information |  Major | mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3452](https://issues.apache.org/jira/browse/MAPREDUCE-3452) | fifoscheduler web ui page always shows 0% used for the queue |  Major | mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3450](https://issues.apache.org/jira/browse/MAPREDUCE-3450) | NM port info no longer available in JobHistory |  Major | mr-am, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3448](https://issues.apache.org/jira/browse/MAPREDUCE-3448) | TestCombineOutputCollector javac unchecked warning on mocked generics |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3447](https://issues.apache.org/jira/browse/MAPREDUCE-3447) | mapreduce examples not working |  Blocker | mrv2 | Thomas Graves | Mahadev konar |
| [MAPREDUCE-3444](https://issues.apache.org/jira/browse/MAPREDUCE-3444) | trunk/0.23 builds broken |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3443](https://issues.apache.org/jira/browse/MAPREDUCE-3443) | Oozie jobs are running as oozie user even though they create the jobclient as doAs. |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3437](https://issues.apache.org/jira/browse/MAPREDUCE-3437) | Branch 23 fails to build with Failure to find org.apache.hadoop:hadoop-project:pom:0.24.0-SNAPSHOT |  Blocker | build, mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3436](https://issues.apache.org/jira/browse/MAPREDUCE-3436) | JobHistory webapp address should use the host from the jobhistory address |  Major | mrv2, webapps | Bruno Mahé | Ahmed Radwan |
| [MAPREDUCE-3434](https://issues.apache.org/jira/browse/MAPREDUCE-3434) | Nightly build broken |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3427](https://issues.apache.org/jira/browse/MAPREDUCE-3427) | streaming tests fail with MR2 |  Blocker | contrib/streaming, mrv2 | Alejandro Abdelnur | Hitesh Shah |
| [MAPREDUCE-3422](https://issues.apache.org/jira/browse/MAPREDUCE-3422) | Counter display names are not being picked up |  Major | mrv2 | Tom White | Jonathan Eagles |
| [MAPREDUCE-3420](https://issues.apache.org/jira/browse/MAPREDUCE-3420) | [Umbrella ticket] Make uber jobs functional |  Major | mrv2 | Hitesh Shah |  |
| [MAPREDUCE-3417](https://issues.apache.org/jira/browse/MAPREDUCE-3417) | job access controls not working app master and job history UI's |  Blocker | mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3413](https://issues.apache.org/jira/browse/MAPREDUCE-3413) | RM web ui applications not sorted in any order by default |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3412](https://issues.apache.org/jira/browse/MAPREDUCE-3412) | 'ant docs' is broken |  Major | . | Amar Kamat | Amar Kamat |
| [MAPREDUCE-3408](https://issues.apache.org/jira/browse/MAPREDUCE-3408) | yarn-daemon.sh unconditionnaly sets yarn.root.logger |  Major | mrv2, nodemanager, resourcemanager | Bruno Mahé | Bruno Mahé |
| [MAPREDUCE-3407](https://issues.apache.org/jira/browse/MAPREDUCE-3407) | Wrong jar getting used in TestMR\*Jobs\* for MiniMRYarnCluster |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3404](https://issues.apache.org/jira/browse/MAPREDUCE-3404) | Speculative Execution: speculative map tasks launched even if -Dmapreduce.map.speculative=false |  Critical | job submission, mrv2 | patrick white | Eric Payne |
| [MAPREDUCE-3398](https://issues.apache.org/jira/browse/MAPREDUCE-3398) | Log Aggregation broken in Secure Mode |  Blocker | mrv2, nodemanager | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3391](https://issues.apache.org/jira/browse/MAPREDUCE-3391) | Connecting to CM is logged as Connecting to RM |  Minor | applicationmaster | Subroto Sanyal | Subroto Sanyal |
| [MAPREDUCE-3389](https://issues.apache.org/jira/browse/MAPREDUCE-3389) | MRApps loads the 'mrapp-generated-classpath' file with classpath from the build machine |  Critical | mrv2 | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3387](https://issues.apache.org/jira/browse/MAPREDUCE-3387) | A tracking URL of N/A before the app master is launched breaks oozie |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3382](https://issues.apache.org/jira/browse/MAPREDUCE-3382) | Network ACLs can prevent AMs to ping the Job-end notification URL |  Critical | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Ravi Prakash |
| [MAPREDUCE-3379](https://issues.apache.org/jira/browse/MAPREDUCE-3379) | LocalResourceTracker should not tracking deleted cache entries |  Major | mrv2, nodemanager | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3376](https://issues.apache.org/jira/browse/MAPREDUCE-3376) | Old mapred API combiner uses NULL reporter |  Major | mrv1, mrv2 | Robert Joseph Evans | Subroto Sanyal |
| [MAPREDUCE-3373](https://issues.apache.org/jira/browse/MAPREDUCE-3373) | Hadoop scripts unconditionally source "$bin"/../libexec/hadoop-config.sh. |  Major | . | Bruno Mahé | Bruno Mahé |
| [MAPREDUCE-3372](https://issues.apache.org/jira/browse/MAPREDUCE-3372) | HADOOP\_PREFIX cannot be overriden |  Major | . | Bruno Mahé | Bruno Mahé |
| [MAPREDUCE-3370](https://issues.apache.org/jira/browse/MAPREDUCE-3370) | MiniMRYarnCluster uses a hard coded path location for the MapReduce application jar |  Major | mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-3368](https://issues.apache.org/jira/browse/MAPREDUCE-3368) | compile-mapred-test fails |  Critical | build, mrv2 | Ramya Sunil | Hitesh Shah |
| [MAPREDUCE-3366](https://issues.apache.org/jira/browse/MAPREDUCE-3366) | Mapreduce component should use consistent directory structure layout as HDFS/common |  Major | mrv2 | Eric Yang | Eric Yang |
| [MAPREDUCE-3355](https://issues.apache.org/jira/browse/MAPREDUCE-3355) | AM scheduling hangs frequently with sort job on 350 nodes |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3354](https://issues.apache.org/jira/browse/MAPREDUCE-3354) | JobHistoryServer should be started by bin/mapred and not by bin/yarn |  Blocker | jobhistoryserver, mrv2 | Vinod Kumar Vavilapalli | Jonathan Eagles |
| [MAPREDUCE-3349](https://issues.apache.org/jira/browse/MAPREDUCE-3349) | No rack-name logged in JobHistory for unsuccessful tasks |  Blocker | mrv2 | Vinod Kumar Vavilapalli | Amar Kamat |
| [MAPREDUCE-3346](https://issues.apache.org/jira/browse/MAPREDUCE-3346) | Rumen LoggedTaskAttempt  getHostName call returns hostname as null |  Blocker | tools/rumen | Karam Singh | Amar Kamat |
| [MAPREDUCE-3345](https://issues.apache.org/jira/browse/MAPREDUCE-3345) | Race condition in ResourceManager causing TestContainerManagerSecurity to fail sometimes |  Major | mrv2, resourcemanager | Vinod Kumar Vavilapalli | Hitesh Shah |
| [MAPREDUCE-3344](https://issues.apache.org/jira/browse/MAPREDUCE-3344) | o.a.h.mapreduce.Reducer since 0.21 blindly casts to ReduceContext.ValueIterator |  Major | . | Brock Noland | Brock Noland |
| [MAPREDUCE-3342](https://issues.apache.org/jira/browse/MAPREDUCE-3342) | JobHistoryServer doesn't show job queue |  Critical | jobhistoryserver, mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3339](https://issues.apache.org/jira/browse/MAPREDUCE-3339) | Job is getting hanged indefinitely,if the child processes are killed on the NM.  KILL\_CONTAINER eventtype is continuosly sent to the containers that are not existing |  Blocker | mrv2 | Ramgopal N | Siddharth Seth |
| [MAPREDUCE-3336](https://issues.apache.org/jira/browse/MAPREDUCE-3336) | com.google.inject.internal.Preconditions not public api - shouldn't be using it |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3333](https://issues.apache.org/jira/browse/MAPREDUCE-3333) | MR AM for sort-job going out of memory |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3329](https://issues.apache.org/jira/browse/MAPREDUCE-3329) | capacity schedule maximum-capacity allowed to be less then capacity |  Blocker | mrv2 | Thomas Graves | Arun C Murthy |
| [MAPREDUCE-3328](https://issues.apache.org/jira/browse/MAPREDUCE-3328) | mapred queue -list output inconsistent and missing child queues |  Critical | mrv2 | Thomas Graves | Ravi Prakash |
| [MAPREDUCE-3327](https://issues.apache.org/jira/browse/MAPREDUCE-3327) | RM web ui scheduler link doesn't show correct max value for queues |  Critical | mrv2 | Thomas Graves | Anupam Seth |
| [MAPREDUCE-3326](https://issues.apache.org/jira/browse/MAPREDUCE-3326) | RM web UI scheduler link not as useful as should be |  Critical | mrv2 | Thomas Graves | Jason Lowe |
| [MAPREDUCE-3324](https://issues.apache.org/jira/browse/MAPREDUCE-3324) | Not All HttpServer tools links (stacks,logs,config,metrics) are accessible through all UI servers |  Critical | jobhistoryserver, mrv2, nodemanager | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3312](https://issues.apache.org/jira/browse/MAPREDUCE-3312) | Make MR AM not send a stopContainer w/o corresponding start container |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3291](https://issues.apache.org/jira/browse/MAPREDUCE-3291) | App fail to launch due to delegation token not found in cache |  Blocker | mrv2 | Ramya Sunil | Robert Joseph Evans |
| [MAPREDUCE-3280](https://issues.apache.org/jira/browse/MAPREDUCE-3280) | MR AM should not read the username from configuration |  Major | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3243](https://issues.apache.org/jira/browse/MAPREDUCE-3243) | Invalid tracking URL for streaming jobs |  Major | contrib/streaming, mrv2 | Ramya Sunil | Jonathan Eagles |
| [MAPREDUCE-3194](https://issues.apache.org/jira/browse/MAPREDUCE-3194) | "mapred mradmin" command is broken in mrv2 |  Major | mrv2 | Siddharth Seth | Jason Lowe |
| [MAPREDUCE-3121](https://issues.apache.org/jira/browse/MAPREDUCE-3121) | DFIP aka 'NodeManager should handle Disk-Failures In Place' |  Blocker | mrv2, nodemanager | Vinod Kumar Vavilapalli | Ravi Gummadi |
| [MAPREDUCE-3045](https://issues.apache.org/jira/browse/MAPREDUCE-3045) | Elapsed time filter on jobhistory server displays incorrect table entries |  Minor | jobhistoryserver, mrv2 | Ramya Sunil | Jonathan Eagles |
| [MAPREDUCE-2950](https://issues.apache.org/jira/browse/MAPREDUCE-2950) | [Gridmix] TestUserResolve fails in trunk |  Major | contrib/gridmix | Amar Kamat | Ravi Gummadi |
| [MAPREDUCE-2784](https://issues.apache.org/jira/browse/MAPREDUCE-2784) | [Gridmix] TestGridmixSummary fails with NPE when run in DEBUG mode. |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-2450](https://issues.apache.org/jira/browse/MAPREDUCE-2450) | Calls from running tasks to TaskTracker methods sometimes fail and incur a 60s timeout |  Major | . | Matei Zaharia | Rajesh Balamohan |
| [MAPREDUCE-1744](https://issues.apache.org/jira/browse/MAPREDUCE-1744) | DistributedCache creates its own FileSytem instance when adding a file/archive to the path |  Major | . | Dick King | Dick King |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-3854](https://issues.apache.org/jira/browse/MAPREDUCE-3854) | Reinstate environment variable tests in TestMiniMRChildTask |  Major | mrv2 | Tom White | Tom White |
| [MAPREDUCE-3803](https://issues.apache.org/jira/browse/MAPREDUCE-3803) | HDFS-2864 broke ant compilation |  Major | build | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3595](https://issues.apache.org/jira/browse/MAPREDUCE-3595) | Add missing TestCounters#testCounterValue test from branch 1 to 0.23 |  Major | test | Tom White | Tom White |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7590](https://issues.apache.org/jira/browse/HADOOP-7590) | Mavenize streaming and MR examples |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2879](https://issues.apache.org/jira/browse/HDFS-2879) | Change FSDataset to package private |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2864](https://issues.apache.org/jira/browse/HDFS-2864) | Remove redundant methods and a constant from FSDataset |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2786](https://issues.apache.org/jira/browse/HDFS-2786) | Fix host-based token incompatibilities in DFSUtil |  Major | namenode, security | Daryn Sharp | Kihwal Lee |
| [HDFS-2785](https://issues.apache.org/jira/browse/HDFS-2785) | Update webhdfs and httpfs for host-based token support |  Major | webhdfs | Daryn Sharp | Robert Joseph Evans |
| [HDFS-2784](https://issues.apache.org/jira/browse/HDFS-2784) | Update hftp and hdfs for host-based token support |  Major | hdfs-client, namenode, security | Daryn Sharp | Kihwal Lee |
| [HDFS-2130](https://issues.apache.org/jira/browse/HDFS-2130) | Switch default checksum to CRC32C |  Major | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-2129](https://issues.apache.org/jira/browse/HDFS-2129) | Simplify BlockReader to not inherit from FSInputChecker |  Major | hdfs-client, performance | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3846](https://issues.apache.org/jira/browse/MAPREDUCE-3846) | Restarted+Recovered AM hangs in some corner cases |  Critical | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3827](https://issues.apache.org/jira/browse/MAPREDUCE-3827) | Counters aggregation slowed down significantly after MAPREDUCE-3749 |  Blocker | mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3823](https://issues.apache.org/jira/browse/MAPREDUCE-3823) | Counters are getting calculated twice at job-finish and delaying clients. |  Major | mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3815](https://issues.apache.org/jira/browse/MAPREDUCE-3815) | Data Locality suffers if the AM asks for containers using IPs instead of hostnames |  Critical | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3813](https://issues.apache.org/jira/browse/MAPREDUCE-3813) | RackResolver should maintain a cache to avoid repetitive lookups. |  Major | mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3810](https://issues.apache.org/jira/browse/MAPREDUCE-3810) | MR AM's ContainerAllocator is assigning the allocated containers very slowly |  Blocker | mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3809](https://issues.apache.org/jira/browse/MAPREDUCE-3809) | Tasks may take upto 3 seconds to exit after completion |  Blocker | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3802](https://issues.apache.org/jira/browse/MAPREDUCE-3802) | If an MR AM dies twice  it looks like the process freezes |  Critical | applicationmaster, mrv2 | Robert Joseph Evans | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3718](https://issues.apache.org/jira/browse/MAPREDUCE-3718) | Default AM heartbeat interval should be one second |  Major | mrv2, performance | Vinod Kumar Vavilapalli | Hitesh Shah |
| [MAPREDUCE-3711](https://issues.apache.org/jira/browse/MAPREDUCE-3711) | AppMaster recovery for Medium to large jobs take long time |  Blocker | mrv2 | Siddharth Seth | Robert Joseph Evans |
| [MAPREDUCE-3698](https://issues.apache.org/jira/browse/MAPREDUCE-3698) | Client cannot talk to the history server in secure mode |  Blocker | mrv2 | Siddharth Seth | Mahadev konar |
| [MAPREDUCE-3641](https://issues.apache.org/jira/browse/MAPREDUCE-3641) | CapacityScheduler should be more conservative assigning off-switch requests |  Blocker | mrv2, scheduler | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3640](https://issues.apache.org/jira/browse/MAPREDUCE-3640) | AMRecovery should pick completed task form partial JobHistory files |  Blocker | mrv2 | Siddharth Seth | Arun C Murthy |
| [MAPREDUCE-3618](https://issues.apache.org/jira/browse/MAPREDUCE-3618) | TaskHeartbeatHandler holds a global lock for all task-updates |  Major | mrv2, performance | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3616](https://issues.apache.org/jira/browse/MAPREDUCE-3616) | Thread pool for launching containers in MR AM not expanding as expected |  Major | mr-am, mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3572](https://issues.apache.org/jira/browse/MAPREDUCE-3572) | MR AM's dispatcher is blocked by heartbeats to ResourceManager |  Critical | mr-am, mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3569](https://issues.apache.org/jira/browse/MAPREDUCE-3569) | TaskAttemptListener holds a global lock for all task-updates |  Critical | mr-am, mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3568](https://issues.apache.org/jira/browse/MAPREDUCE-3568) | Optimize Job's progress calculations in MR AM |  Critical | mr-am, mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3567](https://issues.apache.org/jira/browse/MAPREDUCE-3567) | Extraneous JobConf objects in AM heap |  Major | mr-am, mrv2, performance | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3566](https://issues.apache.org/jira/browse/MAPREDUCE-3566) | MR AM slows down due to repeatedly constructing ContainerLaunchContext |  Critical | mr-am, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3553](https://issues.apache.org/jira/browse/MAPREDUCE-3553) | Add support for data returned when exceptions thrown from web service apis to be in either xml or in JSON |  Minor | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3548](https://issues.apache.org/jira/browse/MAPREDUCE-3548) | write unit tests for web services for mapreduce app master and job history server |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3547](https://issues.apache.org/jira/browse/MAPREDUCE-3547) | finish unit tests for web services for RM and NM |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3534](https://issues.apache.org/jira/browse/MAPREDUCE-3534) | Compression benchmark run-time increased by 13% in 0.23 |  Blocker | mrv2 | Vinay Kumar Thota | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3525](https://issues.apache.org/jira/browse/MAPREDUCE-3525) | Shuffle benchmark is nearly 1.5x slower in 0.23 |  Blocker | mrv2 | Karam Singh | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3519](https://issues.apache.org/jira/browse/MAPREDUCE-3519) | Deadlock in LocalDirsHandlerService and ShuffleHandler |  Blocker | mrv2, nodemanager | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3512](https://issues.apache.org/jira/browse/MAPREDUCE-3512) | Batch jobHistory disk flushes |  Blocker | mr-am, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3511](https://issues.apache.org/jira/browse/MAPREDUCE-3511) | Counters occupy a good part of AM heap |  Blocker | mr-am, mrv2 | Siddharth Seth | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3485](https://issues.apache.org/jira/browse/MAPREDUCE-3485) | DISKS\_FAILED -101 error code should be defined in same location as ABORTED\_CONTAINER\_EXIT\_STATUS |  Major | mrv2 | Hitesh Shah | Ravi Gummadi |
| [MAPREDUCE-3433](https://issues.apache.org/jira/browse/MAPREDUCE-3433) | Finding counters by legacy group name returns empty counters |  Major | client, mrv2 | Tom White | Tom White |
| [MAPREDUCE-3426](https://issues.apache.org/jira/browse/MAPREDUCE-3426) | uber-jobs tried to write outputs into wrong dir |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3402](https://issues.apache.org/jira/browse/MAPREDUCE-3402) | AMScalability test of Sleep job with 100K 1-sec maps regressed into running very slowly |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3399](https://issues.apache.org/jira/browse/MAPREDUCE-3399) | ContainerLocalizer should request new resources after completing the current one |  Blocker | mrv2, nodemanager | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3392](https://issues.apache.org/jira/browse/MAPREDUCE-3392) | Cluster.getDelegationToken() throws NPE if client.getDelegationToken() returns null. |  Blocker | . | John George | John George |
| [MAPREDUCE-3380](https://issues.apache.org/jira/browse/MAPREDUCE-3380) | Token infrastructure for running clients which are not kerberos authenticated |  Blocker | mr-am, mrv2 | Alejandro Abdelnur | Mahadev konar |
| [MAPREDUCE-3221](https://issues.apache.org/jira/browse/MAPREDUCE-3221) | ant test TestSubmitJob failing on trunk |  Minor | mrv2, test | Hitesh Shah | Devaraj K |
| [MAPREDUCE-3219](https://issues.apache.org/jira/browse/MAPREDUCE-3219) | ant test TestDelegationToken failing on trunk |  Minor | mrv2, test | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3217](https://issues.apache.org/jira/browse/MAPREDUCE-3217) | ant test TestAuditLogger fails on trunk |  Minor | mrv2, test | Hitesh Shah | Devaraj K |
| [MAPREDUCE-3215](https://issues.apache.org/jira/browse/MAPREDUCE-3215) | org.apache.hadoop.mapreduce.TestNoJobSetupCleanup failing on trunk |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3102](https://issues.apache.org/jira/browse/MAPREDUCE-3102) | NodeManager should fail fast with wrong configuration or permissions for LinuxContainerExecutor |  Major | mrv2, security | Vinod Kumar Vavilapalli | Hitesh Shah |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7877](https://issues.apache.org/jira/browse/HADOOP-7877) | Federation: update Balancer documentation |  Major | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2587](https://issues.apache.org/jira/browse/HDFS-2587) | Add WebHDFS apt doc |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2574](https://issues.apache.org/jira/browse/HDFS-2574) | remove references to deprecated properties in hdfs-site.xml template and hdfs-default.xml |  Trivial | documentation | Joe Crobak | Joe Crobak |
| [HDFS-2552](https://issues.apache.org/jira/browse/HDFS-2552) | Add WebHdfs Forrest doc |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3811](https://issues.apache.org/jira/browse/MAPREDUCE-3811) | Make the Client-AM IPC retry count configurable |  Critical | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3630](https://issues.apache.org/jira/browse/MAPREDUCE-3630) | NullPointerException running teragen |  Critical | mrv2 | Amol Kekre | Mahadev konar |
| [MAPREDUCE-3468](https://issues.apache.org/jira/browse/MAPREDUCE-3468) | Change version to 0.23.1 for ant builds on the 23 branch |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3375](https://issues.apache.org/jira/browse/MAPREDUCE-3375) | Memory Emulation system tests. |  Major | . | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-3297](https://issues.apache.org/jira/browse/MAPREDUCE-3297) | Move Log Related components from yarn-server-nodemanager to yarn-common |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3251](https://issues.apache.org/jira/browse/MAPREDUCE-3251) | Network ACLs can prevent some clients to talk to MR ApplicationMaster |  Critical | mrv2 | Anupam Seth | Anupam Seth |
| [MAPREDUCE-2733](https://issues.apache.org/jira/browse/MAPREDUCE-2733) | Gridmix v3 cpu emulation system tests. |  Major | . | Vinay Kumar Thota | Vinay Kumar Thota |


