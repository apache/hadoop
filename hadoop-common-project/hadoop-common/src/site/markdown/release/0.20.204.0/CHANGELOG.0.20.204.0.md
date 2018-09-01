
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

## Release 0.20.204.0 - 2011-09-02

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6255](https://issues.apache.org/jira/browse/HADOOP-6255) | Create an rpm integration project |  Major | . | Owen O'Malley | Eric Yang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7144](https://issues.apache.org/jira/browse/HADOOP-7144) | Expose JMX with something like JMXProxyServlet |  Major | . | Luke Lu | Robert Joseph Evans |
| [MAPREDUCE-2558](https://issues.apache.org/jira/browse/MAPREDUCE-2558) | Add queue-level metrics 0.20-security branch |  Major | jobtracker | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HADOOP-7398](https://issues.apache.org/jira/browse/HADOOP-7398) | create a mechanism to suppress the HADOOP\_HOME deprecated warning |  Major | . | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-1938](https://issues.apache.org/jira/browse/MAPREDUCE-1938) | Ability for having user's classes take precedence over the system classes for tasks' classpath |  Blocker | job submission, task, tasktracker | Devaraj Das | Krishna Ramachandran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1773](https://issues.apache.org/jira/browse/HDFS-1773) | Remove a datanode from cluster if include list is not empty and this datanode is removed from both include and exclude lists |  Minor | namenode | Tanping Wang | Tanping Wang |
| [MAPREDUCE-2456](https://issues.apache.org/jira/browse/MAPREDUCE-2456) | Show the reducer taskid and map/reduce tasktrackers for "Failed fetch notification #\_ for task attempt..." log messages |  Trivial | jobtracker | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HADOOP-7248](https://issues.apache.org/jira/browse/HADOOP-7248) | Have a way to automatically update Eclipse .classpath file when new libs are added to the classpath through Ivy for 0.20-\* based sources |  Minor | build | Konstantin Boudnik | Thomas Graves |
| [HADOOP-7277](https://issues.apache.org/jira/browse/HADOOP-7277) | Add Eclipse launch tasks for the 0.20-security branch |  Minor | build | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2479](https://issues.apache.org/jira/browse/MAPREDUCE-2479) | Backport MAPREDUCE-1568 to hadoop security branch |  Major | tasktracker | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2490](https://issues.apache.org/jira/browse/MAPREDUCE-2490) | Log blacklist debug count |  Trivial | jobtracker | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-2495](https://issues.apache.org/jira/browse/MAPREDUCE-2495) | The distributed cache cleanup thread has no monitoring to check to see if it has died for some reason |  Minor | distributed-cache | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2524](https://issues.apache.org/jira/browse/MAPREDUCE-2524) | Backport trunk heuristics for failing maps when we get fetch failures retrieving map output during shuffle |  Minor | tasktracker | Thomas Graves | Thomas Graves |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-118](https://issues.apache.org/jira/browse/MAPREDUCE-118) | Job.getJobID() will always return null |  Blocker | client | Amar Kamat | Amareshwari Sriramadasu |
| [HDFS-1258](https://issues.apache.org/jira/browse/HDFS-1258) | Clearing namespace quota on "/" corrupts FS image |  Blocker | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1377](https://issues.apache.org/jira/browse/HDFS-1377) | Quota bug for partial blocks allows quotas to be violated |  Blocker | namenode | Eli Collins | Eli Collins |
| [HDFS-1750](https://issues.apache.org/jira/browse/HDFS-1750) | fs -ls hftp://file not working |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1758](https://issues.apache.org/jira/browse/HDFS-1758) | Web UI JSP pages thread safety issue |  Minor | tools | Tanping Wang | Tanping Wang |
| [MAPREDUCE-2411](https://issues.apache.org/jira/browse/MAPREDUCE-2411) | When you submit a job to a queue with no ACLs you get an inscrutible NPE |  Minor | . | Dick King | Dick King |
| [HDFS-1189](https://issues.apache.org/jira/browse/HDFS-1189) | Quota counts missed between clear quota and set quota |  Major | namenode | Kang Xiao | John George |
| [MAPREDUCE-2443](https://issues.apache.org/jira/browse/MAPREDUCE-2443) | Fix FI build - broken after MR-2429 |  Minor | test | Siddharth Seth | Siddharth Seth |
| [HADOOP-7232](https://issues.apache.org/jira/browse/HADOOP-7232) | Fix javadoc warnings |  Blocker | documentation | Owen O'Malley | Owen O'Malley |
| [HDFS-1822](https://issues.apache.org/jira/browse/HDFS-1822) | Editlog opcodes overlap between 20 security and later releases |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2451](https://issues.apache.org/jira/browse/MAPREDUCE-2451) | Log the reason string of healthcheck script |  Trivial | jobtracker | Thomas Graves | Thomas Graves |
| [HADOOP-7274](https://issues.apache.org/jira/browse/HADOOP-7274) | CLONE - IOUtils.readFully and IOUtils.skipFully have typo in exception creation's message |  Minor | util | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-2514](https://issues.apache.org/jira/browse/MAPREDUCE-2514) | ReinitTrackerAction class name misspelled RenitTrackerAction in task tracker log |  Trivial | tasktracker | Jonathan Eagles | Jonathan Eagles |
| [HDFS-1592](https://issues.apache.org/jira/browse/HDFS-1592) | Datanode startup doesn't honor volumes.tolerated |  Major | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7330](https://issues.apache.org/jira/browse/HADOOP-7330) | The metrics source mbean implementation should return the attribute value instead of the object |  Major | metrics | Luke Lu | Luke Lu |
| [HDFS-2023](https://issues.apache.org/jira/browse/HDFS-2023) | Backport of NPE for File.list and File.listFiles |  Major | datanode | Bharath Mundlapudi | Bharath Mundlapudi |
| [MAPREDUCE-2529](https://issues.apache.org/jira/browse/MAPREDUCE-2529) | Recognize Jetty bug 1342 and handle it |  Major | tasktracker | Thomas Graves | Thomas Graves |
| [HADOOP-7364](https://issues.apache.org/jira/browse/HADOOP-7364) | TestMiniMRDFSCaching fails if test.build.dir is set to something other than build/test |  Major | test | Thomas Graves | Thomas Graves |
| [HDFS-1878](https://issues.apache.org/jira/browse/HDFS-1878) | TestHDFSServerPorts unit test failure - race condition in FSNamesystem.close() causes NullPointerException without serious consequence |  Minor | namenode | Matt Foley | Matt Foley |
| [HDFS-2057](https://issues.apache.org/jira/browse/HDFS-2057) | Wait time to terminate the threads causing unit tests to take longer time |  Major | datanode | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7373](https://issues.apache.org/jira/browse/HADOOP-7373) | Tarball deployment doesn't work with {start,stop}-{dfs,mapred} |  Major | . | Owen O'Malley | Owen O'Malley |
| [HDFS-1692](https://issues.apache.org/jira/browse/HDFS-1692) | In secure mode, Datanode process doesn't exit when disks fail. |  Major | datanode | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7324](https://issues.apache.org/jira/browse/HADOOP-7324) | Ganglia plugins for metrics v2 |  Blocker | metrics | Luke Lu | Priyo Mustafi |
| [MAPREDUCE-2409](https://issues.apache.org/jira/browse/MAPREDUCE-2409) | Distributed Cache does not differentiate between file /archive for files with the same path |  Major | distributed-cache | Siddharth Seth | Siddharth Seth |
| [HADOOP-7475](https://issues.apache.org/jira/browse/HADOOP-7475) | hadoop-setup-single-node.sh is broken |  Blocker | . | Eric Yang | Eric Yang |
| [MAPREDUCE-2447](https://issues.apache.org/jira/browse/MAPREDUCE-2447) | Set JvmContext sooner for a task - MR2429 |  Minor | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2621](https://issues.apache.org/jira/browse/MAPREDUCE-2621) | TestCapacityScheduler fails with "Queue "q1" does not exist" |  Minor | . | Sherry Chen | Sherry Chen |
| [MAPREDUCE-2651](https://issues.apache.org/jira/browse/MAPREDUCE-2651) | Race condition in Linux Task Controller for job log directory creation |  Major | task-controller | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7356](https://issues.apache.org/jira/browse/HADOOP-7356) | RPM packages broke bin/hadoop script for hadoop 0.20.205 |  Blocker | . | Eric Yang | Eric Yang |
| [MAPREDUCE-2846](https://issues.apache.org/jira/browse/MAPREDUCE-2846) | a small % of all tasks fail with DefaultTaskController |  Blocker | task, task-controller, tasktracker | Allen Wittenauer | Owen O'Malley |
| [MAPREDUCE-2804](https://issues.apache.org/jira/browse/MAPREDUCE-2804) | "Creation of symlink to attempt log dir failed." message is not useful |  Blocker | . | Allen Wittenauer | Owen O'Malley |
| [MAPREDUCE-2358](https://issues.apache.org/jira/browse/MAPREDUCE-2358) | MapReduce assumes HDFS as the default filesystem |  Major | . | Owen O'Malley | Krishna Ramachandran |
| [MAPREDUCE-517](https://issues.apache.org/jira/browse/MAPREDUCE-517) | The capacity-scheduler should assign multiple tasks per heartbeat |  Critical | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2357](https://issues.apache.org/jira/browse/MAPREDUCE-2357) | When extending inputsplit (non-FileSplit), all exceptions are ignored |  Major | task | Owen O'Malley | Luke Lu |
| [MAPREDUCE-2366](https://issues.apache.org/jira/browse/MAPREDUCE-2366) | TaskTracker can't retrieve stdout and stderr from web UI |  Major | tasktracker | Owen O'Malley | Dick King |
| [MAPREDUCE-2362](https://issues.apache.org/jira/browse/MAPREDUCE-2362) | Unit test failures: TestBadRecords and TestTaskTrackerMemoryManager |  Major | test | Owen O'Malley | Greg Roelofs |
| [MAPREDUCE-2356](https://issues.apache.org/jira/browse/MAPREDUCE-2356) | A task succeeded even though there were errors on all attempts. |  Major | . | Owen O'Malley | Luke Lu |
| [MAPREDUCE-2555](https://issues.apache.org/jira/browse/MAPREDUCE-2555) | JvmInvalidate errors in the gridmix TT logs |  Minor | tasktracker | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2360](https://issues.apache.org/jira/browse/MAPREDUCE-2360) | Pig fails when using non-default FileSystem |  Major | client | Owen O'Malley |  |
| [MAPREDUCE-2359](https://issues.apache.org/jira/browse/MAPREDUCE-2359) | Distributed cache doesn't use non-default FileSystems correctly |  Major | . | Owen O'Malley | Krishna Ramachandran |
| [MAPREDUCE-2418](https://issues.apache.org/jira/browse/MAPREDUCE-2418) | Errors not shown in the JobHistory servlet (specifically Counter Limit Exceeded) |  Minor | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2429](https://issues.apache.org/jira/browse/MAPREDUCE-2429) | Check jvmid during task status report |  Major | tasktracker | Arun C Murthy | Siddharth Seth |
| [MAPREDUCE-2364](https://issues.apache.org/jira/browse/MAPREDUCE-2364) | Shouldn't hold lock on rjob while localizing resources. |  Major | tasktracker | Owen O'Malley | Devaraj Das |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2044](https://issues.apache.org/jira/browse/HDFS-2044) | TestQueueProcessingStatistics failing automatic test due to timing issues |  Major | test | Matt Foley | Matt Foley |
| [HDFS-2218](https://issues.apache.org/jira/browse/HDFS-2218) | Disable TestHdfsProxy.testHdfsProxyInterface in 0.20-security and branch-1 until HDFS-2217 is fixed |  Blocker | contrib/hdfsproxy, test | Matt Foley | Matt Foley |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1445](https://issues.apache.org/jira/browse/HDFS-1445) | Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file |  Major | datanode | Matt Foley | Matt Foley |
| [HDFS-1541](https://issues.apache.org/jira/browse/HDFS-1541) | Not marking datanodes dead When namenode in safemode |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1767](https://issues.apache.org/jira/browse/HDFS-1767) | Namenode should ignore non-initial block reports from datanodes when in safemode during startup |  Major | datanode | Matt Foley | Matt Foley |
| [MAPREDUCE-2413](https://issues.apache.org/jira/browse/MAPREDUCE-2413) | TaskTracker should handle disk failures at both startup and runtime |  Major | task-controller, tasktracker | Bharath Mundlapudi | Ravi Gummadi |
| [MAPREDUCE-2415](https://issues.apache.org/jira/browse/MAPREDUCE-2415) | Distribute TaskTracker userlogs onto multiple disks |  Major | task-controller, tasktracker | Bharath Mundlapudi | Bharath Mundlapudi |


