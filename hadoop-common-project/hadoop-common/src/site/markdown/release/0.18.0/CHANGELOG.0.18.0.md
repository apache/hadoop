
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

## Release 0.18.0 - 2008-08-22

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2585](https://issues.apache.org/jira/browse/HADOOP-2585) | Automatic namespace recovery from the secondary image. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2703](https://issues.apache.org/jira/browse/HADOOP-2703) | New files under lease (before close) still shows up as MISSING files/blocks in fsck |  Minor | . | Koji Noguchi | Lohit Vijayarenu |
| [HADOOP-2865](https://issues.apache.org/jira/browse/HADOOP-2865) | FsShell.ls() should print file attributes first then the path name. |  Major | . | Konstantin Shvachko | Edward J. Yoon |
| [HADOOP-3283](https://issues.apache.org/jira/browse/HADOOP-3283) | Need a mechanism for data nodes to update generation stamps. |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2797](https://issues.apache.org/jira/browse/HADOOP-2797) | Withdraw CRC upgrade from HDFS |  Critical | . | Robert Chansler | Raghu Angadi |
| [HADOOP-544](https://issues.apache.org/jira/browse/HADOOP-544) | Replace the job, tip and task ids with objects. |  Major | . | Owen O'Malley | Enis Soztutar |
| [HADOOP-2188](https://issues.apache.org/jira/browse/HADOOP-2188) | RPC should send a ping rather than use client timeouts |  Major | ipc | Owen O'Malley | Hairong Kuang |
| [HADOOP-2181](https://issues.apache.org/jira/browse/HADOOP-2181) | Input Split details for maps should be logged |  Minor | . | Lohit Vijayarenu | Amareshwari Sriramadasu |
| [HADOOP-3317](https://issues.apache.org/jira/browse/HADOOP-3317) | add default port for hdfs namenode |  Minor | . | Doug Cutting | Doug Cutting |
| [HADOOP-3226](https://issues.apache.org/jira/browse/HADOOP-3226) | Run combiner when merging spills from map output |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3329](https://issues.apache.org/jira/browse/HADOOP-3329) | DatanodeDescriptor objects stored in FSImage may be out dated. |  Major | . | Tsz Wo Nicholas Sze | dhruba borthakur |
| [HADOOP-2065](https://issues.apache.org/jira/browse/HADOOP-2065) | Replication policy for corrupted block |  Major | . | Koji Noguchi | Lohit Vijayarenu |
| [HADOOP-1702](https://issues.apache.org/jira/browse/HADOOP-1702) | Reduce buffer copies when data is written to DFS |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-2656](https://issues.apache.org/jira/browse/HADOOP-2656) | Support for upgrading existing cluster to facilitate appends to HDFS files |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3390](https://issues.apache.org/jira/browse/HADOOP-3390) | Remove deprecated ClientProtocol.abandonFileInProgress() |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3405](https://issues.apache.org/jira/browse/HADOOP-3405) | Make mapred internal classes package-local |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-3035](https://issues.apache.org/jira/browse/HADOOP-3035) | Data nodes should inform the name-node about block crc errors. |  Major | . | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-3265](https://issues.apache.org/jira/browse/HADOOP-3265) | Remove deprecated API getFileCacheHints |  Major | fs | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3310](https://issues.apache.org/jira/browse/HADOOP-3310) | Lease recovery for append |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3417](https://issues.apache.org/jira/browse/HADOOP-3417) | JobClient should not have a static configuration for cli parsing |  Major | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-2909](https://issues.apache.org/jira/browse/HADOOP-2909) | Improve IPC idle connection management |  Major | ipc | Hairong Kuang | Hairong Kuang |
| [HADOOP-3486](https://issues.apache.org/jira/browse/HADOOP-3486) | Change default for initial block report to 0 sec and document it in hadoop-defaults.xml |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-3459](https://issues.apache.org/jira/browse/HADOOP-3459) | Change dfs -ls listing to closely match format on Linux |  Major | . | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-3113](https://issues.apache.org/jira/browse/HADOOP-3113) | DFSOututStream.flush() should flush data to real block file on DataNode. |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3452](https://issues.apache.org/jira/browse/HADOOP-3452) | fsck exit code would be better if non-zero when FS corrupt |  Minor | . | Pete Wyckoff | Lohit Vijayarenu |
| [HADOOP-3095](https://issues.apache.org/jira/browse/HADOOP-3095) | Validating input paths and creating splits is slow on S3 |  Major | fs, fs/s3 | Tom White | Tom White |
| [HADOOP-3483](https://issues.apache.org/jira/browse/HADOOP-3483) | [HOD] Improvements with cluster directory handling |  Major | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3184](https://issues.apache.org/jira/browse/HADOOP-3184) | HOD gracefully exclude "bad" nodes during ring formation |  Major | contrib/hod | Marco Nicosia | Hemanth Yamijala |
| [HADOOP-3193](https://issues.apache.org/jira/browse/HADOOP-3193) | Discovery of corrupt block reported in name node log |  Minor | . | Robert Chansler | Chris Douglas |
| [HADOOP-3512](https://issues.apache.org/jira/browse/HADOOP-3512) | Split map/reduce tools into separate jars |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-3379](https://issues.apache.org/jira/browse/HADOOP-3379) | Document the "stream.non.zero.exit.status.is.failure" knob for streaming |  Blocker | documentation | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3569](https://issues.apache.org/jira/browse/HADOOP-3569) | KFS input stream read() returns 4 bytes instead of 1 |  Minor | . | Sriram Rao | Sriram Rao |
| [HADOOP-3598](https://issues.apache.org/jira/browse/HADOOP-3598) | Map-Reduce framework needlessly creates temporary \_${taskid} directories for Maps |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3610](https://issues.apache.org/jira/browse/HADOOP-3610) | [HOD] HOD does not automatically create a cluster directory for the script option |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3665](https://issues.apache.org/jira/browse/HADOOP-3665) | WritableComparator newKey() fails for NullWritable |  Minor | io | Lukas Vlcek | Chris Douglas |
| [HADOOP-3683](https://issues.apache.org/jira/browse/HADOOP-3683) | Hadoop dfs metric FilesListed shows number of files listed instead of operations |  Major | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3703](https://issues.apache.org/jira/browse/HADOOP-3703) | [HOD] logcondense needs to use the new pattern of output in hadoop dfs -lsr |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3808](https://issues.apache.org/jira/browse/HADOOP-3808) | [HOD] Include job tracker RPC in notes attribute after job submission |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3837](https://issues.apache.org/jira/browse/HADOOP-3837) | hadop streaming does not use progress reporting to detect hung tasks |  Major | . | dhruba borthakur | dhruba borthakur |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3074](https://issues.apache.org/jira/browse/HADOOP-3074) | URLStreamHandler for the DFS |  Major | util | Christophe Taton | Christophe Taton |
| [HADOOP-3061](https://issues.apache.org/jira/browse/HADOOP-3061) | Writable for single byte and double |  Major | io | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-3221](https://issues.apache.org/jira/browse/HADOOP-3221) | Need a "LineBasedTextInputFormat" |  Major | . | Milind Bhandarkar | Amareshwari Sriramadasu |
| [HADOOP-3336](https://issues.apache.org/jira/browse/HADOOP-3336) | Direct a subset of namenode RPC events for audit logging |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-1915](https://issues.apache.org/jira/browse/HADOOP-1915) | adding counters methods using String (as opposed to Enum) |  Minor | . | Alejandro Abdelnur | Tom White |
| [HADOOP-3246](https://issues.apache.org/jira/browse/HADOOP-3246) | FTP client over HDFS |  Major | util | Ankur | Ankur |
| [HADOOP-3250](https://issues.apache.org/jira/browse/HADOOP-3250) | Extend FileSystem API to allow appending to files |  Major | fs | dhruba borthakur | Tsz Wo Nicholas Sze |
| [HADOOP-1328](https://issues.apache.org/jira/browse/HADOOP-1328) | Hadoop Streaming needs to provide a way for the stream plugin to update global counters |  Major | . | Runping Qi | Tom White |
| [HADOOP-3187](https://issues.apache.org/jira/browse/HADOOP-3187) | Quotas for name space management |  Major | . | Robert Chansler | Hairong Kuang |
| [HADOOP-3307](https://issues.apache.org/jira/browse/HADOOP-3307) | Archives in Hadoop. |  Major | fs | Mahadev konar | Mahadev konar |
| [HADOOP-3460](https://issues.apache.org/jira/browse/HADOOP-3460) | SequenceFileAsBinaryOutputFormat |  Minor | . | Koji Noguchi | Koji Noguchi |
| [HADOOP-3230](https://issues.apache.org/jira/browse/HADOOP-3230) | Add command line access to named counters |  Major | scripts | Tom White | Tom White |
| [HADOOP-930](https://issues.apache.org/jira/browse/HADOOP-930) | Add support for reading regular (non-block-based) files from S3 in S3FileSystem |  Major | fs | Tom White | Tom White |
| [HADOOP-3022](https://issues.apache.org/jira/browse/HADOOP-3022) | Fast Cluster Restart |  Major | . | Robert Chansler | Konstantin Shvachko |
| [HADOOP-3502](https://issues.apache.org/jira/browse/HADOOP-3502) | Quota API needs documentation in Forrest |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3188](https://issues.apache.org/jira/browse/HADOOP-3188) | compaction utility for directories |  Major | . | Robert Chansler | Robert Chansler |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3254](https://issues.apache.org/jira/browse/HADOOP-3254) | FSNamesystem.gotHeartbeat(..., Object[] xferResults, Object[] deleteList) should not use Object[] as pass-by-reference parameters |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3274](https://issues.apache.org/jira/browse/HADOOP-3274) | The default constructor of BytesWritable should not create a 100-byte array. |  Minor | io | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2910](https://issues.apache.org/jira/browse/HADOOP-2910) | Throttle IPC Client/Server during bursts of requests or server slowdown |  Major | ipc | Hairong Kuang | Hairong Kuang |
| [HADOOP-3270](https://issues.apache.org/jira/browse/HADOOP-3270) | Constant DatanodeCommand should be stored in static fianl immutable variables. |  Minor | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3164](https://issues.apache.org/jira/browse/HADOOP-3164) | Use FileChannel.transferTo() when data is read from DataNode. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3295](https://issues.apache.org/jira/browse/HADOOP-3295) | Allow TextOutputFormat to use configurable separators |  Minor | io | Zheng Shao | Zheng Shao |
| [HADOOP-3308](https://issues.apache.org/jira/browse/HADOOP-3308) | Improve QuickSort by excluding values eq the pivot from the partition |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-2857](https://issues.apache.org/jira/browse/HADOOP-2857) | libhdfs: no way to set JVM args other than classpath |  Minor | . | Craig Macdonald | Craig Macdonald |
| [HADOOP-2461](https://issues.apache.org/jira/browse/HADOOP-2461) | Configuration should trim property names and accept decimal, hexadecimal, and octal numbers |  Minor | conf | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2799](https://issues.apache.org/jira/browse/HADOOP-2799) | Replace org.apache.hadoop.io.Closeable with java.io.Closeable |  Minor | io | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3345](https://issues.apache.org/jira/browse/HADOOP-3345) | Enhance the hudson-test-patch target |  Minor | build | Nigel Daley | Nigel Daley |
| [HADOOP-3144](https://issues.apache.org/jira/browse/HADOOP-3144) | better fault tolerance for corrupted text files |  Major | . | Joydeep Sen Sarma | Zheng Shao |
| [HADOOP-3334](https://issues.apache.org/jira/browse/HADOOP-3334) | Move lease handling codes out from FSNamesystem |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2019](https://issues.apache.org/jira/browse/HADOOP-2019) | DistributedFileCache should support .tgz files in addition to jars and zip files |  Major | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-3058](https://issues.apache.org/jira/browse/HADOOP-3058) | Hadoop DFS to report more replication metrics |  Minor | metrics | Marco Nicosia | Lohit Vijayarenu |
| [HADOOP-3297](https://issues.apache.org/jira/browse/HADOOP-3297) | The way in which ReduceTask/TaskTracker gets completion events during shuffle can be improved |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-3364](https://issues.apache.org/jira/browse/HADOOP-3364) | Faster image and log edits loading. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3369](https://issues.apache.org/jira/browse/HADOOP-3369) | Fast block processing during name-node startup. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2154](https://issues.apache.org/jira/browse/HADOOP-2154) | Non-interleaved checksums would optimize block transfers. |  Major | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-3332](https://issues.apache.org/jira/browse/HADOOP-3332) | improving the logging during shuffling |  Blocker | . | Runping Qi | Devaraj Das |
| [HADOOP-3355](https://issues.apache.org/jira/browse/HADOOP-3355) | Configuration should accept decimal and hexadecimal values |  Major | conf | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3350](https://issues.apache.org/jira/browse/HADOOP-3350) | distcp should permit users to limit the number of maps |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3013](https://issues.apache.org/jira/browse/HADOOP-3013) | fsck to show (checksum) corrupted files |  Major | . | Koji Noguchi | Lohit Vijayarenu |
| [HADOOP-3377](https://issues.apache.org/jira/browse/HADOOP-3377) | Use StringUtils#replaceAll instead of |  Trivial | . | Brice Arnould | Brice Arnould |
| [HADOOP-2661](https://issues.apache.org/jira/browse/HADOOP-2661) | Replicator log should include block id |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3398](https://issues.apache.org/jira/browse/HADOOP-3398) | ReduceTask::closestPowerOf2 is inefficient |  Trivial | . | Chris Douglas | Chris Douglas |
| [HADOOP-2867](https://issues.apache.org/jira/browse/HADOOP-2867) | Add a task's cwd to it's LD\_LIBRARY\_PATH |  Major | . | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3400](https://issues.apache.org/jira/browse/HADOOP-3400) | Facilitate creation of temporary files in HDFS |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-4](https://issues.apache.org/jira/browse/HADOOP-4) | tool to mount dfs on linux |  Major | . | John Xing | Pete Wyckoff |
| [HADOOP-3434](https://issues.apache.org/jira/browse/HADOOP-3434) | Retain cause of bind failure in Server.bind |  Major | . | Steve Loughran | Steve Loughran |
| [HADOOP-3429](https://issues.apache.org/jira/browse/HADOOP-3429) | Increase the buffersize for the streaming parent java process's streams |  Major | . | Devaraj Das | Amareshwari Sriramadasu |
| [HADOOP-3448](https://issues.apache.org/jira/browse/HADOOP-3448) | Add some more hints of the problem when datanode and namenode don't match |  Minor | . | Steve Loughran | Steve Loughran |
| [HADOOP-3177](https://issues.apache.org/jira/browse/HADOOP-3177) | Expose DFSOutputStream.fsync API though the FileSystem interface |  Major | . | dhruba borthakur | Tsz Wo Nicholas Sze |
| [HADOOP-3464](https://issues.apache.org/jira/browse/HADOOP-3464) | [HOD] HOD can improve error messages by reporting failures on compute nodes back to hod client |  Major | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-3455](https://issues.apache.org/jira/browse/HADOOP-3455) | IPC.Client synchronisation looks weak |  Major | ipc | Steve Loughran | Hairong Kuang |
| [HADOOP-3501](https://issues.apache.org/jira/browse/HADOOP-3501) | deprecate InMemoryFileSystem |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-3366](https://issues.apache.org/jira/browse/HADOOP-3366) | Shuffle/Merge improvements |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3492](https://issues.apache.org/jira/browse/HADOOP-3492) | add forrest documentation for user archives |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-3467](https://issues.apache.org/jira/browse/HADOOP-3467) | The javadoc for FileSystem.deleteOnExit should have more description |  Blocker | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3096](https://issues.apache.org/jira/browse/HADOOP-3096) | Improve documentation about the Task Execution Environment in the Map-Reduce tutorial |  Blocker | documentation | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3406](https://issues.apache.org/jira/browse/HADOOP-3406) | Document controls for profiling maps & reduces |  Blocker | documentation | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3277](https://issues.apache.org/jira/browse/HADOOP-3277) | hod should better errors message when deallocate is fired on non allocated directory. |  Minor | contrib/hod | Karam Singh |  |
| [HADOOP-2762](https://issues.apache.org/jira/browse/HADOOP-2762) | Better documentation of controls for memory limits on hadoop daemons and Map-Reduce tasks |  Blocker | documentation, scripts | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3535](https://issues.apache.org/jira/browse/HADOOP-3535) | IOUtils.close needs better documentation |  Blocker | io | Owen O'Malley | Owen O'Malley |
| [HADOOP-3599](https://issues.apache.org/jira/browse/HADOOP-3599) | The new setCombineOnceOnly shouldn't take a JobConf, since it is a method on JobConf |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-3547](https://issues.apache.org/jira/browse/HADOOP-3547) | Improve documentation about distributing native libraries via DistributedCache |  Blocker | documentation | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3532](https://issues.apache.org/jira/browse/HADOOP-3532) | Create build targets to create api change reports using jdiff |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-3572](https://issues.apache.org/jira/browse/HADOOP-3572) | setQuotas usage interface has some minor bugs. |  Minor | . | Mahadev konar | Hairong Kuang |
| [HADOOP-2987](https://issues.apache.org/jira/browse/HADOOP-2987) | Keep two generations of fsimage |  Major | . | Robert Chansler | Konstantin Shvachko |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2905](https://issues.apache.org/jira/browse/HADOOP-2905) | fsck -move triggers NPE in namenode |  Major | . | Michael Bieniosek | Lohit Vijayarenu |
| [HADOOP-2928](https://issues.apache.org/jira/browse/HADOOP-2928) | Remove deprecated methods getContentLength() in ClientProtocol, NameNode, FileSystem, DistributedFileSystem and DFSClient |  Blocker | . | Tsz Wo Nicholas Sze | Lohit Vijayarenu |
| [HADOOP-3176](https://issues.apache.org/jira/browse/HADOOP-3176) | Change lease record when a open-for-write-file gets renamed |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3130](https://issues.apache.org/jira/browse/HADOOP-3130) | Shuffling takes too long to get the last map output. |  Major | . | Runping Qi | Amar Kamat |
| [HADOOP-3160](https://issues.apache.org/jira/browse/HADOOP-3160) | remove exists() from ClientProtocol and NameNode |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3269](https://issues.apache.org/jira/browse/HADOOP-3269) | NameNode doesn't startup when restarted after running an MR job |  Blocker | . | Devaraj Das | Tsz Wo Nicholas Sze |
| [HADOOP-3282](https://issues.apache.org/jira/browse/HADOOP-3282) | TestCheckpoint occasionally fails because of the port issues. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3272](https://issues.apache.org/jira/browse/HADOOP-3272) | Reduce redundant copy of Block object in BlocksMap.map hash map |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3268](https://issues.apache.org/jira/browse/HADOOP-3268) | TestUrlStreamHandler.testFileUrls fails on Windows |  Major | test | Nigel Daley | Christophe Taton |
| [HADOOP-3127](https://issues.apache.org/jira/browse/HADOOP-3127) | rm /user/\<username\>/.Trash/\_\_\_\_ only moves it back to .Trash |  Minor | fs | Koji Noguchi | Brice Arnould |
| [HADOOP-3300](https://issues.apache.org/jira/browse/HADOOP-3300) | FindBugs warnings in NetworkTopology |  Major | . | Tom White | Tom White |
| [HADOOP-2793](https://issues.apache.org/jira/browse/HADOOP-2793) | Links for worst performing shuffle tasks are broken in Analyze Job. |  Minor | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-3313](https://issues.apache.org/jira/browse/HADOOP-3313) | RPC::Invoker makes unnecessary calls to System.currentTimeMillis |  Minor | . | Chris Douglas | Chris Douglas |
| [HADOOP-3318](https://issues.apache.org/jira/browse/HADOOP-3318) | Hadoop streaming doesn't recognize "Darwin" as an OS but Soylatte (OpenJDK port to Mac) reports that rather than "Mac OS X" |  Major | . | Sam Pullara | Sam Pullara |
| [HADOOP-3301](https://issues.apache.org/jira/browse/HADOOP-3301) | Misleading error message when S3 URI contains hostname containing an underscore |  Major | fs/s3 | Tom White | Tom White |
| [HADOOP-3109](https://issues.apache.org/jira/browse/HADOOP-3109) | RPC should accepted connections even when rpc queue is full (ie undo part of HADOOP-2910) |  Blocker | . | Sanjay Radia | Hairong Kuang |
| [HADOOP-3338](https://issues.apache.org/jira/browse/HADOOP-3338) | trunk doesn't compile after HADOOP-544 was committed |  Blocker | . | Nigel Daley | Christophe Taton |
| [HADOOP-3337](https://issues.apache.org/jira/browse/HADOOP-3337) | Name-node fails to start because DatanodeInfo format changed. |  Blocker | . | Konstantin Shvachko | Tsz Wo Nicholas Sze |
| [HADOOP-3101](https://issues.apache.org/jira/browse/HADOOP-3101) | 'bin/hadoop job' should display the help and silently exit |  Minor | . | Amar Kamat | Edward J. Yoon |
| [HADOOP-3119](https://issues.apache.org/jira/browse/HADOOP-3119) | Text.getBytes() |  Trivial | . | Andrew Gudkov | Tim Nelson |
| [HADOOP-2294](https://issues.apache.org/jira/browse/HADOOP-2294) | In hdfs.h , the comment says you release the result of a hdfsListDirectory with a freehdfsFileInfo, but should say hdfsFreeFileInfo |  Trivial | . | Dick King | Craig Macdonald |
| [HADOOP-3335](https://issues.apache.org/jira/browse/HADOOP-3335) | 'make clean' in src/c++/libhdfs does 'rm -rf /\*' |  Critical | build | Doug Cutting | Doug Cutting |
| [HADOOP-2930](https://issues.apache.org/jira/browse/HADOOP-2930) | make {start,stop}-balancer.sh work even if hadoop-daemon.sh isn't in the PATH |  Trivial | scripts | Spiros Papadimitriou | Spiros Papadimitriou |
| [HADOOP-3085](https://issues.apache.org/jira/browse/HADOOP-3085) | pushMetric() method of various metric util classes should catch exceptions |  Major | metrics | Runping Qi | Chris Douglas |
| [HADOOP-3248](https://issues.apache.org/jira/browse/HADOOP-3248) | Improve Namenode startup performance |  Major | . | girish vaitheeswaran | dhruba borthakur |
| [HADOOP-3299](https://issues.apache.org/jira/browse/HADOOP-3299) | org.apache.hadoop.mapred.join.CompositeInputFormat does not initialize  TextInput format files with the configuration resulting in an NullPointerException |  Major | io | Jason | Chris Douglas |
| [HADOOP-3309](https://issues.apache.org/jira/browse/HADOOP-3309) | Unit test fails on Windows: org.apache.hadoop.mapred.TestMiniMRDFSSort.unknown |  Major | . | Mukund Madhugiri | Lohit Vijayarenu |
| [HADOOP-3348](https://issues.apache.org/jira/browse/HADOOP-3348) | TestUrlStreamHandler hangs on LINUX |  Major | fs | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3371](https://issues.apache.org/jira/browse/HADOOP-3371) | MBeanUtil dumps stacktrace from registerMBean |  Minor | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3349](https://issues.apache.org/jira/browse/HADOOP-3349) | FSNamesystem.changeLease(src, dst) incorrectly updates the paths inside a lease |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3365](https://issues.apache.org/jira/browse/HADOOP-3365) | SequenceFile.Sorter.MergeQueue.next does an unnecessary copy of the key |  Major | io | Arun C Murthy | Devaraj Das |
| [HADOOP-3203](https://issues.apache.org/jira/browse/HADOOP-3203) | TaskTracker::localizeJob doesn't provide the correct size to LocalDirAllocator |  Major | . | Chris Douglas | Amareshwari Sriramadasu |
| [HADOOP-3388](https://issues.apache.org/jira/browse/HADOOP-3388) | TestDatanodeBlockScanner failed while trying to corrupt replicas |  Major | test | dhruba borthakur | dhruba borthakur |
| [HADOOP-3393](https://issues.apache.org/jira/browse/HADOOP-3393) | TestHDFSServerPorts fails on LINUX (NFS mounted directory) and on WINDOWS |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3391](https://issues.apache.org/jira/browse/HADOOP-3391) | HADOOP-3248 introduced a findbugs warning. |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3399](https://issues.apache.org/jira/browse/HADOOP-3399) | Debug log not removed in ipc client |  Trivial | ipc | Raghu Angadi | Raghu Angadi |
| [HADOOP-3339](https://issues.apache.org/jira/browse/HADOOP-3339) | DFS Write pipeline does not detect defective datanode correctly if it times out. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3396](https://issues.apache.org/jira/browse/HADOOP-3396) | Unit test TestDatanodeBlockScanner fails on Windows |  Critical | . | Mukund Madhugiri | Lohit Vijayarenu |
| [HADOOP-3409](https://issues.apache.org/jira/browse/HADOOP-3409) | NameNode should save the root inode into fsimage |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3296](https://issues.apache.org/jira/browse/HADOOP-3296) | Some levels are skipped while creating the task cache in JobInProgress |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-3375](https://issues.apache.org/jira/browse/HADOOP-3375) | Lease paths are sometimes not removed from LeaseManager.sortedLeasesByPath |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3424](https://issues.apache.org/jira/browse/HADOOP-3424) | the value returned from getPartition should be checked to make sure it is in the range 0..#reduces-1 |  Major | . | Owen O'Malley | Chris Douglas |
| [HADOOP-3408](https://issues.apache.org/jira/browse/HADOOP-3408) | Change FSNamesytem status metrics to IntValue |  Major | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3381](https://issues.apache.org/jira/browse/HADOOP-3381) | INode interlinks can multiply effect of memory leaks |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3403](https://issues.apache.org/jira/browse/HADOOP-3403) | Job tracker's ExpireTackers thread gets NullPointerException if a tasktracker is lost. |  Blocker | . | Amareshwari Sriramadasu | Arun C Murthy |
| [HADOOP-1318](https://issues.apache.org/jira/browse/HADOOP-1318) | Do not fail completed maps on lost tasktrackers if '-reducer NONE' is specified |  Minor | . | Arun C Murthy | Amareshwari Sriramadasu |
| [HADOOP-3351](https://issues.apache.org/jira/browse/HADOOP-3351) | Fix history viewer |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3419](https://issues.apache.org/jira/browse/HADOOP-3419) | TestFsck fails once in a while on WINDOWS/LINUX |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3259](https://issues.apache.org/jira/browse/HADOOP-3259) | Configuration.substituteVars() needs to handle security exceptions |  Trivial | conf | Steve Loughran | Edward J. Yoon |
| [HADOOP-3232](https://issues.apache.org/jira/browse/HADOOP-3232) | Datanodes time out |  Critical | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-3451](https://issues.apache.org/jira/browse/HADOOP-3451) | test-libhdfs fails on Linux |  Blocker | test | Mukund Madhugiri | Lohit Vijayarenu |
| [HADOOP-3401](https://issues.apache.org/jira/browse/HADOOP-3401) | Update FileBench to use the "work" directory for SequenceFileOutputFormat |  Major | test | Chris Douglas | Chris Douglas |
| [HADOOP-3468](https://issues.apache.org/jira/browse/HADOOP-3468) | Compile error: FTPFileSystem.java:26: cannot access org.apache.commons.net.ftp.FTP |  Blocker | fs | Tsz Wo Nicholas Sze | Ankur |
| [HADOOP-2669](https://issues.apache.org/jira/browse/HADOOP-2669) | DFS client lost lease during writing into DFS files |  Major | . | Runping Qi | dhruba borthakur |
| [HADOOP-3410](https://issues.apache.org/jira/browse/HADOOP-3410) | KFS implementation needs to return file modification time |  Minor | . | Sriram Rao | Sriram Rao |
| [HADOOP-3340](https://issues.apache.org/jira/browse/HADOOP-3340) | hadoop dfs metrics shows 0 |  Major | metrics | Eric Yang | Lohit Vijayarenu |
| [HADOOP-3435](https://issues.apache.org/jira/browse/HADOOP-3435) | test-patch fail if sh != bash |  Major | . | Brice Arnould | Brice Arnould |
| [HADOOP-3471](https://issues.apache.org/jira/browse/HADOOP-3471) | TestIndexedSort sometimes fails |  Major | test | Chris Douglas | Chris Douglas |
| [HADOOP-3443](https://issues.apache.org/jira/browse/HADOOP-3443) | map outputs should not be renamed between partitions |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-3454](https://issues.apache.org/jira/browse/HADOOP-3454) | Text.find incorrectly searches beyond the end of the buffer |  Major | . | Chad Whipkey | Chad Whipkey |
| [HADOOP-3376](https://issues.apache.org/jira/browse/HADOOP-3376) | [HOD] HOD should have a way to detect and deal with clusters that violate/exceed resource manager limits |  Major | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-2132](https://issues.apache.org/jira/browse/HADOOP-2132) | Killing successfully completed jobs moves them to failed |  Critical | . | Srikanth Kakani | Jothi Padmanabhan |
| [HADOOP-2961](https://issues.apache.org/jira/browse/HADOOP-2961) | [HOD] Hod expects port info though external host is not mentioned. |  Minor | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3151](https://issues.apache.org/jira/browse/HADOOP-3151) | Hod should have better error messages. |  Major | contrib/hod | Arkady Borkovsky | Vinod Kumar Vavilapalli |
| [HADOOP-3476](https://issues.apache.org/jira/browse/HADOOP-3476) | Code cleanup needed in fuse-dfs |  Major | . | Tsz Wo Nicholas Sze | Pete Wyckoff |
| [HADOOP-2095](https://issues.apache.org/jira/browse/HADOOP-2095) | Reducer failed due to Out ofMemory |  Major | . | Runping Qi | Arun C Murthy |
| [HADOOP-2427](https://issues.apache.org/jira/browse/HADOOP-2427) | Cleanup of mapred.local.dir after maptask is complete |  Major | . | Lohit Vijayarenu | Amareshwari Sriramadasu |
| [HADOOP-2565](https://issues.apache.org/jira/browse/HADOOP-2565) | DFSPath cache of FileStatus can become stale |  Major | . | Doug Cutting | Tsz Wo Nicholas Sze |
| [HADOOP-3326](https://issues.apache.org/jira/browse/HADOOP-3326) | ReduceTask should not sleep for 200 ms while waiting for merge to finish |  Major | . | Owen O'Malley | Sharad Agarwal |
| [HADOOP-3493](https://issues.apache.org/jira/browse/HADOOP-3493) | TestStreamingFailure fails. |  Major | . | Amareshwari Sriramadasu | Lohit Vijayarenu |
| [HADOOP-236](https://issues.apache.org/jira/browse/HADOOP-236) | job tracker should refuse connection from a task tracker with a different version number |  Major | . | Hairong Kuang | Sharad Agarwal |
| [HADOOP-3453](https://issues.apache.org/jira/browse/HADOOP-3453) | ipc.Client.close() throws NullPointerException |  Major | ipc | Tsz Wo Nicholas Sze | Hairong Kuang |
| [HADOOP-3427](https://issues.apache.org/jira/browse/HADOOP-3427) | In ReduceTask::fetchOutputs, wait for result can be improved slightly |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-3240](https://issues.apache.org/jira/browse/HADOOP-3240) | TestJobShell should not create files in the current directory |  Blocker | test | Tsz Wo Nicholas Sze | Mahadev konar |
| [HADOOP-3496](https://issues.apache.org/jira/browse/HADOOP-3496) | TestHarFileSystem.testArchives fails |  Blocker | fs | Amareshwari Sriramadasu | Tom White |
| [HADOOP-2393](https://issues.apache.org/jira/browse/HADOOP-2393) | TaskTracker locks up removing job files within a synchronized method |  Critical | . | Joydeep Sen Sarma | Amareshwari Sriramadasu |
| [HADOOP-3135](https://issues.apache.org/jira/browse/HADOOP-3135) | if the 'mapred.system.dir' in the client jobconf is different from the JobTracker's value job submission fails |  Critical | . | Alejandro Abdelnur | Subru Krishnan |
| [HADOOP-3503](https://issues.apache.org/jira/browse/HADOOP-3503) | Race condition when client and namenode start block recovery simultaneously |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3440](https://issues.apache.org/jira/browse/HADOOP-3440) | TaskRunner creates a symlink with name 'null' if a file is added to DistributedCache without fragment |  Minor | . | Abhijit Bagri | Devaraj Das |
| [HADOOP-3413](https://issues.apache.org/jira/browse/HADOOP-3413) | SequenceFile.Reader doesn't use the Serialization framework |  Critical | io | Arun C Murthy | Tom White |
| [HADOOP-3463](https://issues.apache.org/jira/browse/HADOOP-3463) | hadoop scripts don't change directory to hadoop\_home |  Critical | scripts | Owen O'Malley | Owen O'Malley |
| [HADOOP-3491](https://issues.apache.org/jira/browse/HADOOP-3491) | Name-node shutdown causes InterruptedException in ResolutionMonitor |  Major | . | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-3509](https://issues.apache.org/jira/browse/HADOOP-3509) | FSNamesystem.close() throws NullPointerException |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3489](https://issues.apache.org/jira/browse/HADOOP-3489) | NPE in SafeModeMonitor |  Major | . | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-3511](https://issues.apache.org/jira/browse/HADOOP-3511) | Namenode should not restore the root's quota if the quota was not in the image |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3516](https://issues.apache.org/jira/browse/HADOOP-3516) | TestHarFileSystem.testArchives fails with NullPointerException |  Blocker | test | Nigel Daley | Subru Krishnan |
| [HADOOP-3513](https://issues.apache.org/jira/browse/HADOOP-3513) | Improve NNThroughputBenchmark log messages. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3519](https://issues.apache.org/jira/browse/HADOOP-3519) | NPE in DFS FileSystem rename |  Blocker | . | Tom White | Hairong Kuang |
| [HADOOP-3528](https://issues.apache.org/jira/browse/HADOOP-3528) | Metrics FilesCreated and files\_deleted metrics do not match. |  Blocker | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3418](https://issues.apache.org/jira/browse/HADOOP-3418) | NameNode does not restart if parent directory of a "FileUnderConstruction" is deleted. |  Blocker | . | Raghu Angadi | Tsz Wo Nicholas Sze |
| [HADOOP-3542](https://issues.apache.org/jira/browse/HADOOP-3542) | Hadoop archives should not create \_logs file in the final archive directory. |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-3544](https://issues.apache.org/jira/browse/HADOOP-3544) | The command "archive" is missing in the example in  docs/hadoop\_archives.html (and pdf) |  Blocker | documentation | Jothi Padmanabhan | Jothi Padmanabhan |
| [HADOOP-3523](https://issues.apache.org/jira/browse/HADOOP-3523) | [HOD] If a job does not exist in Torque's list of jobs, HOD allocate on previously allocated directory fails. |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3517](https://issues.apache.org/jira/browse/HADOOP-3517) | The last InMemory merge may be missed |  Blocker | . | Devaraj Das | Arun C Murthy |
| [HADOOP-3548](https://issues.apache.org/jira/browse/HADOOP-3548) | The tools.jar is not included in the distribution |  Blocker | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-3363](https://issues.apache.org/jira/browse/HADOOP-3363) | HDFS throws a InconsistentFSStateException when the name node starts up on a directory that isnt formatted |  Blocker | . | Steve Loughran | Konstantin Shvachko |
| [HADOOP-3560](https://issues.apache.org/jira/browse/HADOOP-3560) | Archvies sometimes create empty part files. |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-3545](https://issues.apache.org/jira/browse/HADOOP-3545) | archive  is failing with "Illegal Capacity" error |  Blocker | . | Jothi Padmanabhan | Mahadev konar |
| [HADOOP-3561](https://issues.apache.org/jira/browse/HADOOP-3561) | With trash enabled, 'hadoop fs -rmr .' still fully deletes the working dir |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-3531](https://issues.apache.org/jira/browse/HADOOP-3531) | Hod does not  report job tracker failure on hod client side when job tracker fails to come up |  Blocker | contrib/hod | Karam Singh | Hemanth Yamijala |
| [HADOOP-3575](https://issues.apache.org/jira/browse/HADOOP-3575) | clover target broken after src restructuring |  Minor | build | Nigel Daley | Nigel Daley |
| [HADOOP-3539](https://issues.apache.org/jira/browse/HADOOP-3539) | Cygwin: cygpath displays an error message in running bin/hadoop script |  Blocker | scripts | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3564](https://issues.apache.org/jira/browse/HADOOP-3564) | Sometime after successful  hod allocation datanode fails to come up with java.net.BindException for dfs.datanode.ipc.address |  Blocker | contrib/hod | Karam Singh | Vinod Kumar Vavilapalli |
| [HADOOP-3520](https://issues.apache.org/jira/browse/HADOOP-3520) | Generation stamp upgrade fails TestDFSUpgradeFromImage |  Blocker | . | Konstantin Shvachko | dhruba borthakur |
| [HADOOP-3586](https://issues.apache.org/jira/browse/HADOOP-3586) | keep combiner backward compatible with earlier versions of hadoop |  Blocker | . | Olga Natkovich | Chris Douglas |
| [HADOOP-3533](https://issues.apache.org/jira/browse/HADOOP-3533) | The api to JobTracker and TaskTracker have changed incompatibly |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-3593](https://issues.apache.org/jira/browse/HADOOP-3593) | Update MapRed tutorial |  Blocker | documentation | Devaraj Das | Devaraj Das |
| [HADOOP-3580](https://issues.apache.org/jira/browse/HADOOP-3580) | Using a har file as input for the Sort example fails |  Blocker | . | Jothi Padmanabhan | Mahadev konar |
| [HADOOP-3333](https://issues.apache.org/jira/browse/HADOOP-3333) | job failing because of reassigning same tasktracker to failing tasks |  Blocker | . | Christian Kunz | Jothi Padmanabhan |
| [HADOOP-3534](https://issues.apache.org/jira/browse/HADOOP-3534) | The namenode ignores ioexceptions in close |  Blocker | . | Owen O'Malley | Tsz Wo Nicholas Sze |
| [HADOOP-3546](https://issues.apache.org/jira/browse/HADOOP-3546) | TaskTracker re-initialization gets stuck in cleaning up |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3320](https://issues.apache.org/jira/browse/HADOOP-3320) | NPE in NetworkTopology.getDistance() |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-3576](https://issues.apache.org/jira/browse/HADOOP-3576) | hadoop dfs -mv throws NullPointerException |  Blocker | . | Lohit Vijayarenu | Tsz Wo Nicholas Sze |
| [HADOOP-3076](https://issues.apache.org/jira/browse/HADOOP-3076) | [HOD] If a cluster directory is specified as a relative path, an existing script.exitcode file will not be deleted. |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3590](https://issues.apache.org/jira/browse/HADOOP-3590) | Null pointer exception in JobTracker when the task tracker is not yet resolved |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-3606](https://issues.apache.org/jira/browse/HADOOP-3606) | Update streaming documentation |  Blocker | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3505](https://issues.apache.org/jira/browse/HADOOP-3505) | omissions in HOD documentation |  Blocker | contrib/hod, documentation | Ari Rabkin | Vinod Kumar Vavilapalli |
| [HADOOP-3603](https://issues.apache.org/jira/browse/HADOOP-3603) | Setting spill threshold to 100% fails to detect spill for records |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-3615](https://issues.apache.org/jira/browse/HADOOP-3615) | DatanodeProtocol.versionID should be 16L |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3537](https://issues.apache.org/jira/browse/HADOOP-3537) | Datanode not starting up with  java.lang.StringIndexOutOfBoundsException in NetworkTopology.remove |  Blocker | . | Koji Noguchi | Hairong Kuang |
| [HADOOP-3487](https://issues.apache.org/jira/browse/HADOOP-3487) | Balancer should not allocate a thread per block move |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3571](https://issues.apache.org/jira/browse/HADOOP-3571) | ArrayIndexOutOfBoundsException in BlocksMap$BlockInfo.setPrevious |  Blocker | . | Tsz Wo Nicholas Sze | Konstantin Shvachko |
| [HADOOP-3559](https://issues.apache.org/jira/browse/HADOOP-3559) | test-libhdfs fails on linux |  Blocker | . | Mukund Madhugiri | Lohit Vijayarenu |
| [HADOOP-3480](https://issues.apache.org/jira/browse/HADOOP-3480) | Need to update Eclipse template to reflect current trunk |  Blocker | build | Tsz Wo Nicholas Sze | Brice Arnould |
| [HADOOP-3645](https://issues.apache.org/jira/browse/HADOOP-3645) | MetricsTimeVaryingRate returns wrong value for metric\_avg\_time |  Blocker | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3552](https://issues.apache.org/jira/browse/HADOOP-3552) | forrest doc for hadoop commands |  Blocker | documentation | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-3588](https://issues.apache.org/jira/browse/HADOOP-3588) | Bug report for archives |  Blocker | . | Hairong Kuang | Mahadev konar |
| [HADOOP-3635](https://issues.apache.org/jira/browse/HADOOP-3635) | Uncaught exception in DataBlockScanner |  Blocker | . | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HADOOP-3639](https://issues.apache.org/jira/browse/HADOOP-3639) | Exception when closing DFSClient while multiple files are open |  Blocker | . | Benjamin Gufler | Benjamin Gufler |
| [HADOOP-3649](https://issues.apache.org/jira/browse/HADOOP-3649) | ArrayIndexOutOfBounds in FSNamesystem.getBlockLocationsInternal |  Blocker | . | Arun C Murthy | Lohit Vijayarenu |
| [HADOOP-3604](https://issues.apache.org/jira/browse/HADOOP-3604) | Reduce stuck at shuffling phase |  Blocker | . | Runping Qi | Arun C Murthy |
| [HADOOP-3668](https://issues.apache.org/jira/browse/HADOOP-3668) | Clean up HOD documentation |  Blocker | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-3597](https://issues.apache.org/jira/browse/HADOOP-3597) | SortValidator always uses the default file system irrespective of the actual input |  Major | test | Jothi Padmanabhan | Jothi Padmanabhan |
| [HADOOP-3688](https://issues.apache.org/jira/browse/HADOOP-3688) | Fix up HDFS docs |  Blocker | . | Robert Chansler | Robert Chansler |
| [HADOOP-3693](https://issues.apache.org/jira/browse/HADOOP-3693) | Fix documentation for Archives, distcp and native libraries |  Blocker | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3653](https://issues.apache.org/jira/browse/HADOOP-3653) | test-patch target not working on hudson.zones.apache.org due to HADOOP-3480 |  Blocker | . | Nigel Daley | Brice Arnould |
| [HADOOP-3692](https://issues.apache.org/jira/browse/HADOOP-3692) | Fix documentation for Cluster setup and Quick start guides |  Blocker | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3691](https://issues.apache.org/jira/browse/HADOOP-3691) | Fix mapred docs |  Blocker | documentation | Amareshwari Sriramadasu | Jothi Padmanabhan |
| [HADOOP-3673](https://issues.apache.org/jira/browse/HADOOP-3673) | Deadlock in Datanode RPC servers |  Blocker | . | dhruba borthakur | Tsz Wo Nicholas Sze |
| [HADOOP-3630](https://issues.apache.org/jira/browse/HADOOP-3630) | CompositeRecordReader: key and values can be in uninitialized state if files being joined have no records |  Major | . | Jingkei Ly | Chris Douglas |
| [HADOOP-3706](https://issues.apache.org/jira/browse/HADOOP-3706) | CompositeInputFormat: Unable to wrap custom InputFormats |  Major | . | Jingkei Ly | Jingkei Ly |
| [HADOOP-3718](https://issues.apache.org/jira/browse/HADOOP-3718) | KFS: write(int v) API writes out an integer rather than a byte |  Minor | . | Sriram Rao | Sriram Rao |
| [HADOOP-3647](https://issues.apache.org/jira/browse/HADOOP-3647) | Corner-case in IFile leads to failed tasks |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3716](https://issues.apache.org/jira/browse/HADOOP-3716) | KFS listStatus() returns NULL on empty directories |  Minor | . | Sriram Rao | Sriram Rao |
| [HADOOP-3752](https://issues.apache.org/jira/browse/HADOOP-3752) | Audit logging fails to record rename |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-3737](https://issues.apache.org/jira/browse/HADOOP-3737) | CompressedWritable throws OutOfMemoryError |  Major | io | Grant Glouser | Grant Glouser |
| [HADOOP-3670](https://issues.apache.org/jira/browse/HADOOP-3670) | JobTracker running out of heap space |  Blocker | . | Christian Kunz | Amareshwari Sriramadasu |
| [HADOOP-3755](https://issues.apache.org/jira/browse/HADOOP-3755) | the gridmix scripts do not work with hod 0.4 |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-3677](https://issues.apache.org/jira/browse/HADOOP-3677) | Problems with generation stamp upgrade |  Blocker | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-3743](https://issues.apache.org/jira/browse/HADOOP-3743) | -libjars, -files and -archives options do not work with 0.18 |  Blocker | . | Mahadev konar | Amareshwari Sriramadasu |
| [HADOOP-3774](https://issues.apache.org/jira/browse/HADOOP-3774) | Typos in shell output |  Blocker | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3798](https://issues.apache.org/jira/browse/HADOOP-3798) | compile-core-test fails to compile |  Blocker | build | Mukund Madhugiri | Mukund Madhugiri |
| [HADOOP-3762](https://issues.apache.org/jira/browse/HADOOP-3762) | Task tracker died due to OOM |  Blocker | . | Runping Qi | Doug Cutting |
| [HADOOP-3794](https://issues.apache.org/jira/browse/HADOOP-3794) | KFS implementation needs to return directory modification time |  Minor | . | Sriram Rao | Sriram Rao |
| [HADOOP-3806](https://issues.apache.org/jira/browse/HADOOP-3806) | Remove debug message from Quicksort |  Trivial | . | Chris Douglas |  |
| [HADOOP-3776](https://issues.apache.org/jira/browse/HADOOP-3776) | NPE in NameNode with unknown blocks |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3521](https://issues.apache.org/jira/browse/HADOOP-3521) | Hadoop mapreduce task metrics, unable to send metrics data. |  Blocker | . | Eric Yang | Arun C Murthy |
| [HADOOP-3724](https://issues.apache.org/jira/browse/HADOOP-3724) | Namenode does not start due to exception throw while saving Image |  Blocker | . | Lohit Vijayarenu | dhruba borthakur |
| [HADOOP-3827](https://issues.apache.org/jira/browse/HADOOP-3827) | Jobs with empty map-outputs and intermediate compression fail |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3855](https://issues.apache.org/jira/browse/HADOOP-3855) | Fix import of MiniDFSCluster in TestCompressedEmptyMapOutputs.java |  Blocker | test | Arun C Murthy | Arun C Murthy |
| [HADOOP-3865](https://issues.apache.org/jira/browse/HADOOP-3865) | SecondaryNameNode runs out of memory |  Blocker | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3884](https://issues.apache.org/jira/browse/HADOOP-3884) | eclipse plugin build is broken with current eclipse versions |  Blocker | contrib/eclipse-plugin | Doug Cutting | Doug Cutting |
| [HADOOP-3897](https://issues.apache.org/jira/browse/HADOOP-3897) | SecondaryNameNode fails with NullPointerException |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3901](https://issues.apache.org/jira/browse/HADOOP-3901) | CLASSPATH in bin/hadoop script is set incorrectly for cygwin |  Blocker | scripts | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3947](https://issues.apache.org/jira/browse/HADOOP-3947) | TaskTrackers fail to connect back upon a re-init action |  Blocker | . | Amar Kamat | Amareshwari Sriramadasu |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3100](https://issues.apache.org/jira/browse/HADOOP-3100) | Develop tests to test the DFS command line interface |  Major | test | Mukund Madhugiri | Mukund Madhugiri |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2984](https://issues.apache.org/jira/browse/HADOOP-2984) | Distcp should have forrest documentation |  Blocker | util | Owen O'Malley | Chris Douglas |
| [HADOOP-3541](https://issues.apache.org/jira/browse/HADOOP-3541) | Namespace recovery from the secondary image should be documented. |  Blocker | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-2632](https://issues.apache.org/jira/browse/HADOOP-2632) | Discussion of fsck operation in the permissions regime |  Major | . | Robert Chansler | Robert Chansler |


