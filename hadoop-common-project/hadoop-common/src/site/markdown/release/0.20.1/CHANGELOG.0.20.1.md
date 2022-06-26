
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

## Release 0.20.1 - 2009-09-01

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5881](https://issues.apache.org/jira/browse/HADOOP-5881) | Simplify configuration related to task-memory-monitoring and memory-based scheduling |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-5726](https://issues.apache.org/jira/browse/HADOOP-5726) | Remove pre-emption from the capacity scheduler code base |  Major | . | Hemanth Yamijala | rahul k singh |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6080](https://issues.apache.org/jira/browse/HADOOP-6080) | Handling of  Trash with quota |  Major | fs | Koji Noguchi | Jakob Homan |
| [HADOOP-5714](https://issues.apache.org/jira/browse/HADOOP-5714) | Metric to show number of fs.exists (or number of getFileInfo) calls |  Minor | metrics | Koji Noguchi | Jakob Homan |
| [HADOOP-3315](https://issues.apache.org/jira/browse/HADOOP-3315) | New binary file format |  Major | io | Owen O'Malley | Hong Tang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-635](https://issues.apache.org/jira/browse/HDFS-635) | HDFS Project page does not show 0.20.1 documentation/release information. |  Major | documentation | Andy Sautins |  |
| [HDFS-527](https://issues.apache.org/jira/browse/HDFS-527) | Refactor DFSClient constructors |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-767](https://issues.apache.org/jira/browse/MAPREDUCE-767) | to remove mapreduce dependency on commons-cli2 |  Major | contrib/streaming | Giridharan Kesavan | Amar Kamat |
| [MAPREDUCE-465](https://issues.apache.org/jira/browse/MAPREDUCE-465) | Deprecate org.apache.hadoop.mapred.lib.MultithreadedMapRunner |  Minor | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6215](https://issues.apache.org/jira/browse/HADOOP-6215) | fix GenericOptionParser to deal with -D with '=' in the value |  Major | . | Owen O'Malley | Amar Kamat |
| [HADOOP-6145](https://issues.apache.org/jira/browse/HADOOP-6145) | No error message for deleting non-existant file or directory. |  Major | fs | Suman Sehgal | Jakob Homan |
| [HADOOP-6141](https://issues.apache.org/jira/browse/HADOOP-6141) | hadoop 0.20 branch "test-patch" is broken |  Major | build | Hong Tang | Hong Tang |
| [HADOOP-6139](https://issues.apache.org/jira/browse/HADOOP-6139) | Incomplete help message is displayed for rm and rmr options. |  Minor | . | Suman Sehgal | Jakob Homan |
| [HADOOP-6017](https://issues.apache.org/jira/browse/HADOOP-6017) | NameNode and SecondaryNameNode fail to restart because of abnormal filenames. |  Blocker | . | Raghu Angadi | Tsz Wo Nicholas Sze |
| [HADOOP-5951](https://issues.apache.org/jira/browse/HADOOP-5951) | StorageInfo needs Apache license header. |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-5937](https://issues.apache.org/jira/browse/HADOOP-5937) | Correct  info message  "Use hadoop dfs -safemode option"  to  " Use hdfs dfsadmin -safemode option"  . |  Minor | . | Ravi Phulari | Ravi Phulari |
| [HADOOP-5932](https://issues.apache.org/jira/browse/HADOOP-5932) | MemoryMatcher logs 0 as freeMemOnTT even though there are free slots available on TaskTraker |  Major | . | Karam Singh | Vinod Kumar Vavilapalli |
| [HADOOP-5924](https://issues.apache.org/jira/browse/HADOOP-5924) | JT fails to recover the jobs after restart after HADOOP:4372 |  Major | . | Ramya Sunil | Amar Kamat |
| [HADOOP-5921](https://issues.apache.org/jira/browse/HADOOP-5921) | JobTracker does not come up because of NotReplicatedYetException |  Major | . | Amareshwari Sriramadasu | Amar Kamat |
| [HADOOP-5920](https://issues.apache.org/jira/browse/HADOOP-5920) | TestJobHistory fails some times. |  Major | . | Amareshwari Sriramadasu | Amar Kamat |
| [HADOOP-5908](https://issues.apache.org/jira/browse/HADOOP-5908) | ArithmeticException in heartbeats with zero map jobs |  Major | . | Vinod Kumar Vavilapalli | Amar Kamat |
| [HADOOP-5884](https://issues.apache.org/jira/browse/HADOOP-5884) | Capacity scheduler should account high memory jobs as using more capacity of the queue |  Major | . | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-5883](https://issues.apache.org/jira/browse/HADOOP-5883) | TaskMemoryMonitorThread might shoot down tasks even if their processes momentarily exceed the requested memory |  Major | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-5882](https://issues.apache.org/jira/browse/HADOOP-5882) | Progress is not updated when the New Reducer is running reduce phase |  Blocker | . | Jothi Padmanabhan | Amareshwari Sriramadasu |
| [HADOOP-5850](https://issues.apache.org/jira/browse/HADOOP-5850) | map/reduce doesn't run jobs with 0 maps |  Critical | . | Owen O'Malley | Vinod Kumar Vavilapalli |
| [HADOOP-5828](https://issues.apache.org/jira/browse/HADOOP-5828) | Use absolute path for JobTracker's mapred.local.dir in MiniMRCluster |  Major | test | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-5746](https://issues.apache.org/jira/browse/HADOOP-5746) | Errors encountered in MROutputThread after the last map/reduce call can go undetected |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-5736](https://issues.apache.org/jira/browse/HADOOP-5736) | Update CapacityScheduler documentation to reflect latest changes |  Major | . | Sreekanth Ramakrishnan | Sreekanth Ramakrishnan |
| [HADOOP-5719](https://issues.apache.org/jira/browse/HADOOP-5719) | Jobs failed during job initalization are never removed from Capacity Schedulers waiting list |  Major | . | Sreekanth Ramakrishnan | Sreekanth Ramakrishnan |
| [HADOOP-5718](https://issues.apache.org/jira/browse/HADOOP-5718) | Capacity Scheduler should not check for presence of default queue while starting up. |  Major | . | Sreekanth Ramakrishnan | Sreekanth Ramakrishnan |
| [HADOOP-5711](https://issues.apache.org/jira/browse/HADOOP-5711) | Change Namenode file close log to info |  Minor | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-5691](https://issues.apache.org/jira/browse/HADOOP-5691) | org.apache.hadoop.mapreduce.Reducer should not be abstract. |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5688](https://issues.apache.org/jira/browse/HADOOP-5688) | HftpFileSystem.getChecksum(..) does not work for the paths with scheme and authority |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-5655](https://issues.apache.org/jira/browse/HADOOP-5655) | TestMRServerPorts fails on java.net.BindException |  Major | . | Hairong Kuang | Devaraj Das |
| [HADOOP-5654](https://issues.apache.org/jira/browse/HADOOP-5654) | TestReplicationPolicy.\<init\> fails on java.net.BindException |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-5648](https://issues.apache.org/jira/browse/HADOOP-5648) | Not able to generate gridmix.jar on already compiled version of hadoop |  Major | benchmarks | Suman Sehgal | Giridharan Kesavan |
| [HADOOP-5646](https://issues.apache.org/jira/browse/HADOOP-5646) | TestQueueCapacities is failing Hudson tests for the last few builds |  Major | . | Jothi Padmanabhan | Vinod Kumar Vavilapalli |
| [HADOOP-5641](https://issues.apache.org/jira/browse/HADOOP-5641) | Possible NPE in CapacityScheduler's MemoryMatcher |  Major | . | Vinod Kumar Vavilapalli | Hemanth Yamijala |
| [HADOOP-5636](https://issues.apache.org/jira/browse/HADOOP-5636) | Job is left in Running state after a killJob |  Critical | . | Amareshwari Sriramadasu | Amar Kamat |
| [HADOOP-5539](https://issues.apache.org/jira/browse/HADOOP-5539) | o.a.h.mapred.Merger not maintaining map out compression on intermediate files |  Blocker | . | Billy Pearson | Jothi Padmanabhan |
| [HADOOP-5533](https://issues.apache.org/jira/browse/HADOOP-5533) | Recovery duration shown on the jobtracker webpage is inaccurate |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-5349](https://issues.apache.org/jira/browse/HADOOP-5349) | When the size required for a path is -1, LocalDirAllocator.getLocalPathForWrite fails with a DiskCheckerException when the disk it selects is bad. |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-5213](https://issues.apache.org/jira/browse/HADOOP-5213) | BZip2CompressionOutputStream NullPointerException |  Blocker | io | Zheng Shao | Zheng Shao |
| [HADOOP-5210](https://issues.apache.org/jira/browse/HADOOP-5210) | Reduce Task Progress shows \> 100% when the total size of map outputs (for a single reducer) is high |  Minor | . | Jothi Padmanabhan | Ravi Gummadi |
| [HADOOP-4674](https://issues.apache.org/jira/browse/HADOOP-4674) | hadoop fs -help should list detailed help info for the following commands: test, text, tail, stat & touchz |  Trivial | fs | David NeSmith | Ravi Phulari |
| [HADOOP-4626](https://issues.apache.org/jira/browse/HADOOP-4626) | API link in forrest doc should point to the same version of hadoop. |  Minor | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1022](https://issues.apache.org/jira/browse/HDFS-1022) | Merge under-10-min tests specs into one file |  Major | test | Erik Steffl | Erik Steffl |
| [HDFS-525](https://issues.apache.org/jira/browse/HDFS-525) | ListPathsServlet.java uses static SimpleDateFormat that has threading issues |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-438](https://issues.apache.org/jira/browse/HDFS-438) | Improve help message for quotas |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HDFS-167](https://issues.apache.org/jira/browse/HDFS-167) | DFSClient continues to retry indefinitely |  Minor | hdfs-client | Derek Wollenstein | Bill Zeller |
| [HDFS-26](https://issues.apache.org/jira/browse/HDFS-26) |  	 HADOOP-5862 for version .20  (Namespace quota exceeded message unclear) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-924](https://issues.apache.org/jira/browse/MAPREDUCE-924) | TestPipes must not directly invoke 'main' of pipes as an exit from main could cause the testcase to crash. |  Major | pipes | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-911](https://issues.apache.org/jira/browse/MAPREDUCE-911) | TestTaskFail fail sometimes |  Major | test | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-838](https://issues.apache.org/jira/browse/MAPREDUCE-838) | Task succeeds even when committer.commitTask fails with IOException |  Blocker | task | Koji Noguchi | Amareshwari Sriramadasu |
| [MAPREDUCE-834](https://issues.apache.org/jira/browse/MAPREDUCE-834) | When TaskTracker config use old memory management values its memory monitoring is diabled. |  Major | . | Karam Singh | Sreekanth Ramakrishnan |
| [MAPREDUCE-832](https://issues.apache.org/jira/browse/MAPREDUCE-832) | Too many WARN messages about deprecated memorty config variables in JobTacker log |  Major | . | Karam Singh | rahul k singh |
| [MAPREDUCE-818](https://issues.apache.org/jira/browse/MAPREDUCE-818) | org.apache.hadoop.mapreduce.Counters.getGroup returns null if the group name doesnt exist. |  Minor | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-807](https://issues.apache.org/jira/browse/MAPREDUCE-807) | Stray user files in mapred.system.dir with permissions other than 777 can prevent the jobtracker from starting up. |  Blocker | jobtracker | Amar Kamat | Amar Kamat |
| [MAPREDUCE-805](https://issues.apache.org/jira/browse/MAPREDUCE-805) | Deadlock in Jobtracker |  Major | . | Michael Tamm | Amar Kamat |
| [MAPREDUCE-796](https://issues.apache.org/jira/browse/MAPREDUCE-796) | Encountered "ClassCastException" on tasktracker while running wordcount with MultithreadedMapRunner |  Major | examples | Suman Sehgal | Amar Kamat |
| [MAPREDUCE-745](https://issues.apache.org/jira/browse/MAPREDUCE-745) | TestRecoveryManager fails sometimes |  Major | jobtracker | Amareshwari Sriramadasu | Amar Kamat |
| [MAPREDUCE-735](https://issues.apache.org/jira/browse/MAPREDUCE-735) | ArrayIndexOutOfBoundsException is thrown by KeyFieldBasedPartitioner |  Major | . | Suman Sehgal | Amar Kamat |
| [MAPREDUCE-687](https://issues.apache.org/jira/browse/MAPREDUCE-687) | TestMiniMRMapRedDebugScript fails sometimes |  Major | test | Amar Kamat | Amareshwari Sriramadasu |
| [MAPREDUCE-657](https://issues.apache.org/jira/browse/MAPREDUCE-657) | CompletedJobStatusStore hardcodes filesystem to hdfs |  Major | jobtracker | Amar Kamat | Amar Kamat |
| [MAPREDUCE-565](https://issues.apache.org/jira/browse/MAPREDUCE-565) | Partitioner does not work with new API |  Blocker | task | Jothi Padmanabhan | Owen O'Malley |
| [MAPREDUCE-430](https://issues.apache.org/jira/browse/MAPREDUCE-430) | Task stuck in cleanup with OutOfMemoryErrors |  Major | . | Amareshwari Sriramadasu | Amar Kamat |
| [MAPREDUCE-421](https://issues.apache.org/jira/browse/MAPREDUCE-421) | mapred pipes might return exit code 0 even when failing |  Major | pipes | Christian Kunz | Christian Kunz |
| [MAPREDUCE-383](https://issues.apache.org/jira/browse/MAPREDUCE-383) | pipes combiner does not reset properly after a spill |  Major | . | Christian Kunz | Christian Kunz |
| [MAPREDUCE-179](https://issues.apache.org/jira/browse/MAPREDUCE-179) | setProgress not called for new RecordReaders |  Blocker | . | Chris Douglas | Chris Douglas |
| [MAPREDUCE-130](https://issues.apache.org/jira/browse/MAPREDUCE-130) | Delete the jobconf copy from the log directory of the JobTracker when the job is retired |  Major | . | Devaraj Das | Amar Kamat |
| [MAPREDUCE-124](https://issues.apache.org/jira/browse/MAPREDUCE-124) | When abortTask of OutputCommitter fails with an Exception for a map-only job, the task is marked as success |  Major | . | Jothi Padmanabhan | Amareshwari Sriramadasu |
| [MAPREDUCE-40](https://issues.apache.org/jira/browse/MAPREDUCE-40) | Memory management variables need a backwards compatibility option after HADOOP-5881 |  Blocker | . | Hemanth Yamijala | rahul k singh |
| [MAPREDUCE-18](https://issues.apache.org/jira/browse/MAPREDUCE-18) | Under load the shuffle sometimes gets incorrect data |  Blocker | . | Owen O'Malley | Ravi Gummadi |
| [MAPREDUCE-2](https://issues.apache.org/jira/browse/MAPREDUCE-2) | ArrayOutOfIndex error in KeyFieldBasedPartitioner on empty key |  Major | . | Amar Kamat | Amar Kamat |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6213](https://issues.apache.org/jira/browse/HADOOP-6213) | Remove commons dependency on commons-cli2 |  Blocker | util | Amar Kamat | Amar Kamat |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


