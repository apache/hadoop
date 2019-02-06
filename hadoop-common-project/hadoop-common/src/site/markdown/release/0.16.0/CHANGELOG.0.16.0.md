
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

## Release 0.16.0 - 2008-02-07



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2045](https://issues.apache.org/jira/browse/HADOOP-2045) | credits page should have more information |  Major | documentation | Doug Cutting | Doug Cutting |
| [HADOOP-1604](https://issues.apache.org/jira/browse/HADOOP-1604) | admins should be able to finalize namenode upgrades without running the cluster |  Critical | . | Owen O'Malley | Konstantin Shvachko |
| [HADOOP-1912](https://issues.apache.org/jira/browse/HADOOP-1912) | Datanode should support block replacement |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-2288](https://issues.apache.org/jira/browse/HADOOP-2288) | Change FileSystem API to support access control. |  Major | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2229](https://issues.apache.org/jira/browse/HADOOP-2229) | Provide a simple login implementation |  Major | fs | Tsz Wo Nicholas Sze | Hairong Kuang |
| [HADOOP-2184](https://issues.apache.org/jira/browse/HADOOP-2184) | RPC Support for user permissions and authentication. |  Major | ipc | Tsz Wo Nicholas Sze | Raghu Angadi |
| [HADOOP-1652](https://issues.apache.org/jira/browse/HADOOP-1652) | Rebalance data blocks when new data nodes added or data nodes become full |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-2145](https://issues.apache.org/jira/browse/HADOOP-2145) | need 'doc' target that runs forrest |  Major | build | Doug Cutting | Doug Cutting |
| [HADOOP-2085](https://issues.apache.org/jira/browse/HADOOP-2085) | Map-side joins on sorted, equally-partitioned datasets |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2381](https://issues.apache.org/jira/browse/HADOOP-2381) | Support permission information in FileStatus |  Major | fs | Tsz Wo Nicholas Sze | Raghu Angadi |
| [HADOOP-2336](https://issues.apache.org/jira/browse/HADOOP-2336) | Shell commands to access and modify file permissions |  Major | fs | Raghu Angadi | Raghu Angadi |
| [HADOOP-1301](https://issues.apache.org/jira/browse/HADOOP-1301) | resource management proviosioning for Hadoop |  Major | . | Pete Wyckoff | Hemanth Yamijala |
| [HADOOP-1298](https://issues.apache.org/jira/browse/HADOOP-1298) | adding user info to file |  Major | fs | Kurtis Heimerl | Tsz Wo Nicholas Sze |
| [HADOOP-2447](https://issues.apache.org/jira/browse/HADOOP-2447) | HDFS should be capable of limiting the total number of inodes in the system |  Major | . | Sameer Paranjpye | dhruba borthakur |
| [HADOOP-2487](https://issues.apache.org/jira/browse/HADOOP-2487) | Provide an option to get job status for all jobs run by or submitted to a job tracker |  Major | . | Hemanth Yamijala | Amareshwari Sriramadasu |
| [HADOOP-2398](https://issues.apache.org/jira/browse/HADOOP-2398) | Additional Instrumentation for NameNode, RPC Layer and JMX support |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-2514](https://issues.apache.org/jira/browse/HADOOP-2514) | Trash and permissions don't mix |  Major | . | Robert Chansler | Doug Cutting |
| [HADOOP-2012](https://issues.apache.org/jira/browse/HADOOP-2012) | Periodic verification at the Datanode |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2543](https://issues.apache.org/jira/browse/HADOOP-2543) | No-permission-checking mode for smooth transition to 0.16's permissions features. |  Major | . | Sanjay Radia | Hairong Kuang |
| [HADOOP-2603](https://issues.apache.org/jira/browse/HADOOP-2603) | SequenceFileAsBinaryInputFormat |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2367](https://issues.apache.org/jira/browse/HADOOP-2367) | Get representative hprof information from tasks |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2567](https://issues.apache.org/jira/browse/HADOOP-2567) | add FileSystem#getHomeDirectory() method |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-2529](https://issues.apache.org/jira/browse/HADOOP-2529) | DFS User Guide |  Major | documentation | Raghu Angadi | Raghu Angadi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2058](https://issues.apache.org/jira/browse/HADOOP-2058) | Allow adding additional datanodes to MiniDFSCluster |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-1855](https://issues.apache.org/jira/browse/HADOOP-1855) | fsck should verify block placement |  Major | . | dhruba borthakur | Konstantin Shvachko |
| [HADOOP-1839](https://issues.apache.org/jira/browse/HADOOP-1839) | Link-ify the Pending/Running/Complete/Killed tasks/task-attempts on jobdetails.jsp |  Major | . | Arun C Murthy | Amar Kamat |
| [HADOOP-1848](https://issues.apache.org/jira/browse/HADOOP-1848) | Redesign of Eclipse plug-in interface with Hadoop |  Major | . | Christophe Taton | Christophe Taton |
| [HADOOP-1857](https://issues.apache.org/jira/browse/HADOOP-1857) | Ability to run a script when a task fails to capture stack traces |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-1210](https://issues.apache.org/jira/browse/HADOOP-1210) | Log counters in job history |  Minor | . | Albert Chern | Owen O'Malley |
| [HADOOP-2105](https://issues.apache.org/jira/browse/HADOOP-2105) | Clarify requirements for Hadoop in overview.html |  Minor | . | Jim Kellerman | Jim Kellerman |
| [HADOOP-2086](https://issues.apache.org/jira/browse/HADOOP-2086) | ability to add dependencies to a job after construction |  Major | . | Adrian Woodhead | Adrian Woodhead |
| [HADOOP-1185](https://issues.apache.org/jira/browse/HADOOP-1185) | dynamically change log levels |  Major | util | dhruba borthakur | Tsz Wo Nicholas Sze |
| [HADOOP-2134](https://issues.apache.org/jira/browse/HADOOP-2134) | Remove developer-centric requirements from overview.html |  Major | documentation | Arun C Murthy | Jim Kellerman |
| [HADOOP-1274](https://issues.apache.org/jira/browse/HADOOP-1274) | Configuring different number of mappers and reducers per TaskTracker |  Major | . | Koji Noguchi | Amareshwari Sriramadasu |
| [HADOOP-2127](https://issues.apache.org/jira/browse/HADOOP-2127) | Add pipes sort example |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1843](https://issues.apache.org/jira/browse/HADOOP-1843) | Remove deprecated code in Configuration/JobConf |  Major | conf | Arun C Murthy | Arun C Murthy |
| [HADOOP-2113](https://issues.apache.org/jira/browse/HADOOP-2113) | Add "-text" command to FsShell to decode SequenceFile to stdout |  Minor | fs | Chris Douglas | Chris Douglas |
| [HADOOP-1900](https://issues.apache.org/jira/browse/HADOOP-1900) | the heartbeat and task event queries interval should be set dynamically by the JobTracker |  Major | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-1302](https://issues.apache.org/jira/browse/HADOOP-1302) | Remove deprecated contrib/abacus code |  Major | . | Doug Cutting | Enis Soztutar |
| [HADOOP-2349](https://issues.apache.org/jira/browse/HADOOP-2349) | FSEditLog.logEdit(byte op, Writable w1, Writable w2) should accept variable numbers of Writable, instead of two. |  Minor | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2185](https://issues.apache.org/jira/browse/HADOOP-2185) | Server ports: to roll or not to roll. |  Major | conf | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2401](https://issues.apache.org/jira/browse/HADOOP-2401) | Lease holder information should be passed in ClientProtocol.abandonBlock(...) |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-1841](https://issues.apache.org/jira/browse/HADOOP-1841) | IPC server should write repsonses asynchronously |  Major | ipc | Doug Cutting | dhruba borthakur |
| [HADOOP-2432](https://issues.apache.org/jira/browse/HADOOP-2432) | If HDFS is going to throw an exception "File does not exist" it should include the name of the file |  Minor | . | Jim Kellerman | Jim Kellerman |
| [HADOOP-2457](https://issues.apache.org/jira/browse/HADOOP-2457) | Add a 'forrest.home' property for the 'docs' target in build.xml |  Minor | documentation | Arun C Murthy | Arun C Murthy |
| [HADOOP-2149](https://issues.apache.org/jira/browse/HADOOP-2149) | Pure name-node benchmarks. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2466](https://issues.apache.org/jira/browse/HADOOP-2466) | FileInputFormat computeSplitSize() method, change visibility to protected and make it a member method |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-2425](https://issues.apache.org/jira/browse/HADOOP-2425) | TextOutputFormat should special case Text |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2208](https://issues.apache.org/jira/browse/HADOOP-2208) | Reduce frequency of Counter updates in the task tracker status |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-1719](https://issues.apache.org/jira/browse/HADOOP-1719) | Improve the utilization of shuffle copier threads |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-2390](https://issues.apache.org/jira/browse/HADOOP-2390) | Document the user-controls for intermediate/output compression via forrest |  Major | documentation | Arun C Murthy | Arun C Murthy |
| [HADOOP-1660](https://issues.apache.org/jira/browse/HADOOP-1660) | add support for native library toDistributedCache |  Major | . | Alejandro Abdelnur | Arun C Murthy |
| [HADOOP-2233](https://issues.apache.org/jira/browse/HADOOP-2233) | General example for modeling m/r load in Java |  Minor | test | Chris Douglas | Chris Douglas |
| [HADOOP-2547](https://issues.apache.org/jira/browse/HADOOP-2547) | remove use of 'magic number' in build.xml |  Trivial | build | Hrishikesh | Hrishikesh |
| [HADOOP-2268](https://issues.apache.org/jira/browse/HADOOP-2268) | JobControl classes should use interfaces rather than implemenations |  Minor | . | Adrian Woodhead | Adrian Woodhead |
| [HADOOP-2552](https://issues.apache.org/jira/browse/HADOOP-2552) | enable hdfs permission checking by default |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-2131](https://issues.apache.org/jira/browse/HADOOP-2131) | Speculative execution should be allowed for reducers only |  Critical | . | Srikanth Kakani | Amareshwari Sriramadasu |
| [HADOOP-1873](https://issues.apache.org/jira/browse/HADOOP-1873) | User permissions for Map/Reduce |  Major | . | Raghu Angadi | Hairong Kuang |
| [HADOOP-1965](https://issues.apache.org/jira/browse/HADOOP-1965) | Handle map output buffers better |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-2464](https://issues.apache.org/jira/browse/HADOOP-2464) | Test permissions related shell commands with DFS |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1876](https://issues.apache.org/jira/browse/HADOOP-1876) | Persisting completed jobs status |  Critical | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-2077](https://issues.apache.org/jira/browse/HADOOP-2077) | Logging version number (and compiled date) at STARTUP\_MSG |  Trivial | . | Koji Noguchi | Arun C Murthy |
| [HADOOP-1989](https://issues.apache.org/jira/browse/HADOOP-1989) | Add support for simulated Data Nodes  - helpful for testing and performance benchmarking of the Name Node without having a large cluster |  Minor | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-1742](https://issues.apache.org/jira/browse/HADOOP-1742) | FSNamesystem.startFile()  javadoc is inconsistent |  Minor | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1707](https://issues.apache.org/jira/browse/HADOOP-1707) | Remove the DFS Client disk-based cache |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2298](https://issues.apache.org/jira/browse/HADOOP-2298) | ant target without source and docs |  Major | build | Gautam Kowshik | Hrishikesh |
| [HADOOP-2469](https://issues.apache.org/jira/browse/HADOOP-2469) | WritableUtils.clone should take Configuration rather than JobConf |  Minor | io | stack | stack |
| [HADOOP-2596](https://issues.apache.org/jira/browse/HADOOP-2596) | add SequenceFile.createWriter() method that takes block size as parameter |  Minor | io | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-2645](https://issues.apache.org/jira/browse/HADOOP-2645) | Additional metrics  & jmx beans and cleanup to use the recent metrics libraries |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-2232](https://issues.apache.org/jira/browse/HADOOP-2232) | Add option to disable nagles algorithm in the IPC Server |  Major | ipc | Clint Morgan | Clint Morgan |
| [HADOOP-2566](https://issues.apache.org/jira/browse/HADOOP-2566) | need FileSystem#globStatus method |  Major | fs | Doug Cutting | Hairong Kuang |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2072](https://issues.apache.org/jira/browse/HADOOP-2072) | RawLocalFileStatus is causing Path problems |  Major | fs | Dennis Kubes |  |
| [HADOOP-1245](https://issues.apache.org/jira/browse/HADOOP-1245) | value for mapred.tasktracker.tasks.maximum taken from jobtracker, not tasktracker |  Major | . | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-1898](https://issues.apache.org/jira/browse/HADOOP-1898) | locking for the ReflectionUtils.logThreadInfo is too conservative |  Major | util | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-2100](https://issues.apache.org/jira/browse/HADOOP-2100) | hadoop-daemon.sh script fails if HADOOP\_PID\_DIR doesn't exist |  Major | scripts | Arun C Murthy | Michael Bieniosek |
| [HADOOP-2096](https://issues.apache.org/jira/browse/HADOOP-2096) | The file used to localize job.xml should be closed. |  Minor | . | Amar Kamat | Amar Kamat |
| [HADOOP-2098](https://issues.apache.org/jira/browse/HADOOP-2098) | File handles for log files are still open in case of jobs with 0 maps |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-2112](https://issues.apache.org/jira/browse/HADOOP-2112) | TestMiniMRMapRedDebugScript fails due to a missing file |  Blocker | . | Devaraj Das | Arun C Murthy |
| [HADOOP-2089](https://issues.apache.org/jira/browse/HADOOP-2089) | Multiple caheArchive does not work in Hadoop streaming |  Critical | . | Milind Bhandarkar | Lohit Vijayarenu |
| [HADOOP-2071](https://issues.apache.org/jira/browse/HADOOP-2071) | StreamXmlRecordReader throws java.io.IOException: Mark/reset exception in hadoop 0.14 |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-1348](https://issues.apache.org/jira/browse/HADOOP-1348) | Configuration XML bug: comments inside values |  Critical | conf | Eelco Lempsink | Rajagopal Natarajan |
| [HADOOP-1952](https://issues.apache.org/jira/browse/HADOOP-1952) | Streaming does not handle invalid -inputformat  (typo by users for example) |  Minor | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-2151](https://issues.apache.org/jira/browse/HADOOP-2151) | FileSyste.globPaths does not validate the return list of Paths |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-2121](https://issues.apache.org/jira/browse/HADOOP-2121) | Unexpected IOException in DFSOutputStream.close() |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1130](https://issues.apache.org/jira/browse/HADOOP-1130) | Remove unused ClientFinalizer in DFSClient |  Major | . | Philippe Gassmann | Chris Douglas |
| [HADOOP-2204](https://issues.apache.org/jira/browse/HADOOP-2204) | DFSTestUtil.waitReplication does not wait. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2078](https://issues.apache.org/jira/browse/HADOOP-2078) | Name-node should be able to close empty files. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1642](https://issues.apache.org/jira/browse/HADOOP-1642) | Jobs using LocalJobRunner + JobControl fails |  Critical | . | Johan Oskarsson | Doug Cutting |
| [HADOOP-2104](https://issues.apache.org/jira/browse/HADOOP-2104) | clover description attribute suppresses all other targets in -projecthelp |  Trivial | build | Chris Douglas | Chris Douglas |
| [HADOOP-2212](https://issues.apache.org/jira/browse/HADOOP-2212) | java.lang.ArithmeticException: / by zero in ChecksumFileSystem.open |  Critical | fs | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-2216](https://issues.apache.org/jira/browse/HADOOP-2216) | Job UI doesnot show running tasks and complete tasks correctly. |  Major | . | Amareshwari Sriramadasu | Amar Kamat |
| [HADOOP-2272](https://issues.apache.org/jira/browse/HADOOP-2272) | findbugs currently fails due to hadoop-streaming having moved |  Major | build | Adrian Woodhead | stack |
| [HADOOP-2244](https://issues.apache.org/jira/browse/HADOOP-2244) | MapWritable.readFields needs to clear internal hash else instance accumulates entries forever |  Major | io | stack | stack |
| [HADOOP-1984](https://issues.apache.org/jira/browse/HADOOP-1984) | some reducer stuck at copy phase and progress extremely slowly |  Critical | . | Runping Qi | Amar Kamat |
| [HADOOP-2245](https://issues.apache.org/jira/browse/HADOOP-2245) | TestRecordMR and TestAggregates fail once in a while |  Major | . | Devaraj Das | Adrian Woodhead |
| [HADOOP-2275](https://issues.apache.org/jira/browse/HADOOP-2275) | Erroneous detection of corrupted file when namenode fails to allocate any datanodes for newly allocated block |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2256](https://issues.apache.org/jira/browse/HADOOP-2256) | TestBlockReplacement unit test failed. |  Major | . | Raghu Angadi | Hairong Kuang |
| [HADOOP-2209](https://issues.apache.org/jira/browse/HADOOP-2209) | SecondaryNamenode process should exit if it encounters Runtime exceptions |  Major | . | dhruba borthakur |  |
| [HADOOP-2314](https://issues.apache.org/jira/browse/HADOOP-2314) | TestBlockReplacement occasionally get into an infinite loop |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-2300](https://issues.apache.org/jira/browse/HADOOP-2300) | mapred.tasktracker.tasks.maximum is completely ignored |  Blocker | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-2368](https://issues.apache.org/jira/browse/HADOOP-2368) | Lots of unit tests fail on Windows with exception: Login failed: CreateProcess |  Blocker | . | Mukund Madhugiri | Tsz Wo Nicholas Sze |
| [HADOOP-2363](https://issues.apache.org/jira/browse/HADOOP-2363) | Unit tests fail if there is another instance of Hadoop |  Major | test | Raghu Angadi | Konstantin Shvachko |
| [HADOOP-2271](https://issues.apache.org/jira/browse/HADOOP-2271) | chmod in ant package target fails |  Major | build | Adrian Woodhead | Adrian Woodhead |
| [HADOOP-2313](https://issues.apache.org/jira/browse/HADOOP-2313) | build does not fail when libhdfs build fails |  Minor | . | Nigel Daley | Nigel Daley |
| [HADOOP-2359](https://issues.apache.org/jira/browse/HADOOP-2359) | PendingReplicationMonitor thread received exception. java.lang.InterruptedException |  Major | . | Owen O'Malley | dhruba borthakur |
| [HADOOP-2365](https://issues.apache.org/jira/browse/HADOOP-2365) | Result of HashFunction.hash() contains all identical values |  Minor | . | Andrzej Bialecki | Jim Kellerman |
| [HADOOP-2323](https://issues.apache.org/jira/browse/HADOOP-2323) | JobTracker.close() prints stack traces for exceptions that are not errors |  Minor | . | Jim Kellerman | Jim Kellerman |
| [HADOOP-2376](https://issues.apache.org/jira/browse/HADOOP-2376) | The sort example shouldn't override the number of maps |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2248](https://issues.apache.org/jira/browse/HADOOP-2248) | Word count example is spending 24% of the time in incrCounter |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2228](https://issues.apache.org/jira/browse/HADOOP-2228) | Jobs fail because job.xml exists |  Major | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-2434](https://issues.apache.org/jira/browse/HADOOP-2434) | MapFile.get on HDFS in TRUNK is WAY!!! slower than 0.15.x |  Blocker | io | stack | stack |
| [HADOOP-2459](https://issues.apache.org/jira/browse/HADOOP-2459) | Running 'ant docs tar' includes src/docs/build in the resulting tar file |  Minor | build | Nigel Daley | Nigel Daley |
| [HADOOP-2453](https://issues.apache.org/jira/browse/HADOOP-2453) | wordcount-simple example gives ParseException with examples configuration file |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2215](https://issues.apache.org/jira/browse/HADOOP-2215) | Change documentation in cluster\_setup.html and mapred\_tutorial.html post HADOOP-1274 |  Major | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2352](https://issues.apache.org/jira/browse/HADOOP-2352) | Remove AC\_LIB\_CHECK from src/native/configure.ac to ensure libhadoop.so doesn't have a dependency on libz.so/liblzo.so |  Major | native | Arun C Murthy | Arun C Murthy |
| [HADOOP-2220](https://issues.apache.org/jira/browse/HADOOP-2220) | Reduce tasks fail too easily because of repeated fetch failures |  Blocker | . | Christian Kunz | Amar Kamat |
| [HADOOP-2247](https://issues.apache.org/jira/browse/HADOOP-2247) | Mappers fail easily due to repeated failures |  Blocker | . | Srikanth Kakani | Amar Kamat |
| [HADOOP-2452](https://issues.apache.org/jira/browse/HADOOP-2452) | Eclipse plug-in build.xml issue |  Trivial | build | Christophe Taton | Christophe Taton |
| [HADOOP-2476](https://issues.apache.org/jira/browse/HADOOP-2476) | Unit test fails on Windows: TestCopyFiles.testCopyFromLocalToLocal |  Blocker | fs | Mukund Madhugiri | Raghu Angadi |
| [HADOOP-2503](https://issues.apache.org/jira/browse/HADOOP-2503) | REST Insert / Select |  Critical | . | Billy Pearson | Bryan Duxbury |
| [HADOOP-2492](https://issues.apache.org/jira/browse/HADOOP-2492) | ConcurrentModificationException in org.apache.hadoop.ipc.Server.Responder |  Major | ipc | Devaraj Das | dhruba borthakur |
| [HADOOP-2344](https://issues.apache.org/jira/browse/HADOOP-2344) | Free up the buffers (input and error) while executing a shell command before waiting for it to finish. |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-2511](https://issues.apache.org/jira/browse/HADOOP-2511) | HADOOP-2344 introduced a javadoc warning |  Major | documentation | Arun C Murthy | Arun C Murthy |
| [HADOOP-2442](https://issues.apache.org/jira/browse/HADOOP-2442) | Unit test failed: org.apache.hadoop.fs.TestLocalFileSystemPermission.testLocalFSsetOwner |  Critical | fs | Mukund Madhugiri | Raghu Angadi |
| [HADOOP-2523](https://issues.apache.org/jira/browse/HADOOP-2523) | Unit test fails on Windows: TestDFSShell.testFilePermissions |  Blocker | . | Mukund Madhugiri | Raghu Angadi |
| [HADOOP-2535](https://issues.apache.org/jira/browse/HADOOP-2535) | Remove support for deprecated mapred.child.heap.size and indentation fix in TaskRunner.java |  Minor | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-2512](https://issues.apache.org/jira/browse/HADOOP-2512) | error stream handling in Shell executor |  Blocker | util | Raghu Angadi | Raghu Angadi |
| [HADOOP-2420](https://issues.apache.org/jira/browse/HADOOP-2420) | Use exit code to detect normal errors while excuting 'ls' in Local FS |  Blocker | fs | Raghu Angadi | Raghu Angadi |
| [HADOOP-2285](https://issues.apache.org/jira/browse/HADOOP-2285) | TextInputFormat is slow compared to reading files. |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2446](https://issues.apache.org/jira/browse/HADOOP-2446) | TestHDFSServerPorts fails. |  Major | test | Raghu Angadi | Nigel Daley |
| [HADOOP-2537](https://issues.apache.org/jira/browse/HADOOP-2537) | make build process compatible with Ant 1.7.0 |  Major | build | Nigel Daley | Hrishikesh |
| [HADOOP-1281](https://issues.apache.org/jira/browse/HADOOP-1281) | Speculative map tasks aren't getting killed although the TIP completed |  Critical | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-2571](https://issues.apache.org/jira/browse/HADOOP-2571) | javac generates a warning in test/o.a.h.io.FileBench |  Trivial | test | Chris Douglas | Chris Douglas |
| [HADOOP-2583](https://issues.apache.org/jira/browse/HADOOP-2583) | Potential Eclipse plug-in UI loop when editing location parameters |  Minor | contrib/eclipse-plugin | Christophe Taton | Christophe Taton |
| [HADOOP-2481](https://issues.apache.org/jira/browse/HADOOP-2481) | NNBench should periodically report its progress |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-2601](https://issues.apache.org/jira/browse/HADOOP-2601) | TestNNThroughput should not use a fixed namenode port |  Major | . | Hairong Kuang | Konstantin Shvachko |
| [HADOOP-2494](https://issues.apache.org/jira/browse/HADOOP-2494) | Set +x on contrib/\*/bin/\* in packaged tar bundle |  Major | scripts | stack | stack |
| [HADOOP-2605](https://issues.apache.org/jira/browse/HADOOP-2605) | leading slash in mapred.task.tracker.report.bindAddress |  Major | conf | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2620](https://issues.apache.org/jira/browse/HADOOP-2620) | 'bin/hadoop fs -help' does not list file permissions commands. |  Trivial | fs | Raghu Angadi | Raghu Angadi |
| [HADOOP-2614](https://issues.apache.org/jira/browse/HADOOP-2614) | dfs web interfaces should run as a configurable user account |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-290](https://issues.apache.org/jira/browse/HADOOP-290) | Fix Datanode transfer thread logging |  Minor | . | Dennis Kubes | dhruba borthakur |
| [HADOOP-2538](https://issues.apache.org/jira/browse/HADOOP-2538) | NPE in TaskLog.java |  Trivial | . | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-2582](https://issues.apache.org/jira/browse/HADOOP-2582) | hadoop dfs -copyToLocal creates zero byte files, when source file does not exists |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-2189](https://issues.apache.org/jira/browse/HADOOP-2189) | Incrementing user counters should count as progress |  Blocker | . | Owen O'Malley | Devaraj Das |
| [HADOOP-2284](https://issues.apache.org/jira/browse/HADOOP-2284) | BasicTypeSorterBase.compare calls progress on each compare |  Major | . | Owen O'Malley | Amar Kamat |
| [HADOOP-2649](https://issues.apache.org/jira/browse/HADOOP-2649) | The ReplicationMonitor sleep period should be configurable |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2549](https://issues.apache.org/jira/browse/HADOOP-2549) | hdfs does not honor dfs.du.reserved setting |  Critical | . | Joydeep Sen Sarma | Hairong Kuang |
| [HADOOP-2509](https://issues.apache.org/jira/browse/HADOOP-2509) | Add rat target to build |  Major | build | Nigel Daley | Hrishikesh |
| [HADOOP-2659](https://issues.apache.org/jira/browse/HADOOP-2659) | The commands in DFSAdmin should require admin privilege |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2633](https://issues.apache.org/jira/browse/HADOOP-2633) | Revert change to fsck made as part of permissions implementation |  Blocker | . | Robert Chansler | Tsz Wo Nicholas Sze |
| [HADOOP-2687](https://issues.apache.org/jira/browse/HADOOP-2687) | 1707 added errant INFO-level logging to DFSClient |  Blocker | . | stack | stack |
| [HADOOP-2402](https://issues.apache.org/jira/browse/HADOOP-2402) | Lzo compression compresses each write from TextOutputFormat |  Major | io, native | Chris Douglas | Chris Douglas |
| [HADOOP-2691](https://issues.apache.org/jira/browse/HADOOP-2691) | Some junit tests fail with the exception: All datanodes are bad. Aborting... |  Major | . | Hairong Kuang | dhruba borthakur |
| [HADOOP-1195](https://issues.apache.org/jira/browse/HADOOP-1195) | NullPointerException in FSNamesystem due to getDatanode() return value is not checked |  Major | . | Konstantin Shvachko | dhruba borthakur |
| [HADOOP-2640](https://issues.apache.org/jira/browse/HADOOP-2640) | MultiFileSplitInputFormat always returns 1 split when avgLengthPerSplit \> Integer.MAX\_VALUE |  Blocker | . | Frédéric Bertin | Enis Soztutar |
| [HADOOP-2626](https://issues.apache.org/jira/browse/HADOOP-2626) | RawLocalFileStatus is badly handling URIs |  Major | fs | Frédéric Bertin | Doug Cutting |
| [HADOOP-2646](https://issues.apache.org/jira/browse/HADOOP-2646) | SortValidator broken with fully-qualified working directories |  Blocker | test | Doug Cutting | Arun C Murthy |
| [HADOOP-2652](https://issues.apache.org/jira/browse/HADOOP-2652) | Fix permission issues for HftpFileSystem |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2092](https://issues.apache.org/jira/browse/HADOOP-2092) | Pipes C++ task does not die even if the Java tasks die |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-2714](https://issues.apache.org/jira/browse/HADOOP-2714) | Unit test fails on Windows: rg.apache.hadoop.dfs.TestDecommission |  Blocker | . | Mukund Madhugiri | dhruba borthakur |
| [HADOOP-2576](https://issues.apache.org/jira/browse/HADOOP-2576) | Namenode performance degradation over time |  Blocker | . | Christian Kunz | Raghu Angadi |
| [HADOOP-2720](https://issues.apache.org/jira/browse/HADOOP-2720) | Update HOD in Hadoop 0.16 |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2713](https://issues.apache.org/jira/browse/HADOOP-2713) | Unit test fails on Windows: org.apache.hadoop.dfs.TestDatanodeDeath |  Blocker | . | Mukund Madhugiri | dhruba borthakur |
| [HADOOP-2639](https://issues.apache.org/jira/browse/HADOOP-2639) | Reducers stuck in shuffle |  Blocker | . | Amareshwari Sriramadasu | Arun C Murthy |
| [HADOOP-2723](https://issues.apache.org/jira/browse/HADOOP-2723) | Hadoop 2367- Does not respect JobConf.getProfileEnabled() |  Blocker | . | Clint Morgan | Amareshwari Sriramadasu |
| [HADOOP-2734](https://issues.apache.org/jira/browse/HADOOP-2734) | docs link to lucene.apache.org |  Major | documentation | Doug Cutting | Doug Cutting |
| [HADOOP-2732](https://issues.apache.org/jira/browse/HADOOP-2732) | ab{5[6-9],[6-9][6-9]}.gz should not be treated as an illegal glob |  Blocker | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-2404](https://issues.apache.org/jira/browse/HADOOP-2404) | HADOOP-2185 breaks compatibility with hadoop-0.15.0 |  Blocker | conf | Arun C Murthy | Owen O'Malley |
| [HADOOP-2740](https://issues.apache.org/jira/browse/HADOOP-2740) | Modify HOD to work with changes mentioned in HADOOP-2404 |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-2755](https://issues.apache.org/jira/browse/HADOOP-2755) | dfs fsck extremely slow, dfs ls times out |  Blocker | . | Christian Kunz | Tsz Wo Nicholas Sze |
| [HADOOP-2768](https://issues.apache.org/jira/browse/HADOOP-2768) | DFSIO write performance benchmark shows a regression |  Blocker | . | Mukund Madhugiri | dhruba borthakur |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2000](https://issues.apache.org/jira/browse/HADOOP-2000) | Re-write NNBench to use MapReduce |  Major | test | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-2369](https://issues.apache.org/jira/browse/HADOOP-2369) | Representative mix of jobs for large cluster throughput benchmarking |  Major | test | Chris Douglas | Runping Qi |
| [HADOOP-2406](https://issues.apache.org/jira/browse/HADOOP-2406) | Micro-benchmark to measure read/write times through InputFormats |  Major | fs, test | Chris Douglas | Chris Douglas |
| [HADOOP-2449](https://issues.apache.org/jira/browse/HADOOP-2449) | Restore the  old NN Bench that was replaced by a MR NN Bench |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-2431](https://issues.apache.org/jira/browse/HADOOP-2431) | Test HDFS File Permissions |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-2342](https://issues.apache.org/jira/browse/HADOOP-2342) | create a micro-benchmark for measure local-file versus hdfs read |  Major | . | Owen O'Malley | Owen O'Malley |


