
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

## Release 0.20.2 - 2010-02-16

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-793](https://issues.apache.org/jira/browse/HDFS-793) | DataNode should first receive the whole packet ack message before it constructs and sends its own ack message for the packet |  Blocker | datanode | Hairong Kuang | Hairong Kuang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-623](https://issues.apache.org/jira/browse/MAPREDUCE-623) | Resolve javac warnings in mapred |  Major | build | Jothi Padmanabhan | Jothi Padmanabhan |
| [HADOOP-1849](https://issues.apache.org/jira/browse/HADOOP-1849) | IPC server max queue size should be configurable |  Major | ipc | Raghu Angadi | Konstantin Shvachko |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5759](https://issues.apache.org/jira/browse/HADOOP-5759) | IllegalArgumentException when CombineFileInputFormat is used as job InputFormat |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-826](https://issues.apache.org/jira/browse/MAPREDUCE-826) | harchive doesn't use ToolRunner / harchive returns 0 even if the job fails with exception |  Trivial | harchive | Koji Noguchi | Koji Noguchi |
| [MAPREDUCE-112](https://issues.apache.org/jira/browse/MAPREDUCE-112) | Reduce Input Records and Reduce Output Records counters are not being set when using the new Mapreduce reducer API |  Blocker | . | Jothi Padmanabhan | Jothi Padmanabhan |
| [HADOOP-6231](https://issues.apache.org/jira/browse/HADOOP-6231) | Allow caching of filesystem instances to be disabled on a per-instance basis |  Major | fs | Tom White | Ben Slusky |
| [MAPREDUCE-979](https://issues.apache.org/jira/browse/MAPREDUCE-979) | JobConf.getMemoryFor{Map\|Reduce}Task doesn't fallback to newer config knobs when mapred.taskmaxvmem is set to DISABLED\_MEMORY\_LIMIT of -1 |  Blocker | jobtracker, tasktracker | Arun C Murthy | Sreekanth Ramakrishnan |
| [HDFS-677](https://issues.apache.org/jira/browse/HDFS-677) | Rename failure due to quota results in deletion of src directory |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-579](https://issues.apache.org/jira/browse/HDFS-579) | HADOOP-3792 update of DfsTask incomplete |  Major | hdfs-client | Christian Kunz | Christian Kunz |
| [MAPREDUCE-1070](https://issues.apache.org/jira/browse/MAPREDUCE-1070) | Deadlock in FairSchedulerServlet |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-6097](https://issues.apache.org/jira/browse/HADOOP-6097) | Multiple bugs w/ Hadoop archives |  Major | fs | Ben Slusky | Ben Slusky |
| [HDFS-723](https://issues.apache.org/jira/browse/HDFS-723) | Deadlock in DFSClient#DFSOutputStream |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HDFS-732](https://issues.apache.org/jira/browse/HDFS-732) | HDFS files are ending up truncated |  Blocker | hdfs-client | Christian Kunz | Tsz Wo Nicholas Sze |
| [MAPREDUCE-1163](https://issues.apache.org/jira/browse/MAPREDUCE-1163) | hdfsJniHelper.h: Yahoo! specific paths are encoded |  Trivial | . | Allen Wittenauer | Allen Wittenauer |
| [HDFS-761](https://issues.apache.org/jira/browse/HDFS-761) | Failure to process rename operation from edits log due to quota verification |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-1068](https://issues.apache.org/jira/browse/MAPREDUCE-1068) | In hadoop-0.20.0 streaming job do not throw proper verbose error message if file is not present |  Major | contrib/streaming | Peeyush Bishnoi | Amareshwari Sriramadasu |
| [HDFS-596](https://issues.apache.org/jira/browse/HDFS-596) | Memory leak in libhdfs: hdfsFreeFileInfo() in libhdfs does not free memory for mOwner and mGroup |  Blocker | fuse-dfs | Zhang Bingjun | Zhang Bingjun |
| [MAPREDUCE-1147](https://issues.apache.org/jira/browse/MAPREDUCE-1147) | Map output records counter missing for map-only jobs in new API |  Blocker | . | Chris Douglas | Amar Kamat |
| [HADOOP-6269](https://issues.apache.org/jira/browse/HADOOP-6269) | Missing synchronization for defaultResources in Configuration.addResource |  Major | conf | Todd Lipcon | Sreekanth Ramakrishnan |
| [MAPREDUCE-1182](https://issues.apache.org/jira/browse/MAPREDUCE-1182) | Reducers fail with OutOfMemoryError while copying Map outputs |  Blocker | . | Chandra Prakash Bhagtani | Chandra Prakash Bhagtani |
| [HDFS-781](https://issues.apache.org/jira/browse/HDFS-781) | Metrics PendingDeletionBlocks is not decremented |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-185](https://issues.apache.org/jira/browse/HDFS-185) | Chown , chgrp , chmod operations allowed when namenode is in safemode . |  Major | . | Ravi Phulari | Ravi Phulari |
| [HADOOP-6428](https://issues.apache.org/jira/browse/HADOOP-6428) | HttpServer sleeps with negative values |  Major | . | Tsz Wo Nicholas Sze | Konstantin Boudnik |
| [HDFS-101](https://issues.apache.org/jira/browse/HDFS-101) | DFS write pipeline : DFSClient sometimes does not detect second datanode failure |  Blocker | datanode | Raghu Angadi | Hairong Kuang |
| [HADOOP-6460](https://issues.apache.org/jira/browse/HADOOP-6460) | Namenode runs of out of memory due to memory leak in ipc Server |  Blocker | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-5623](https://issues.apache.org/jira/browse/HADOOP-5623) | Streaming: process provided status messages are overwritten every 10 seoncds |  Major | . | Rick Cox | Rick Cox |
| [HADOOP-6315](https://issues.apache.org/jira/browse/HADOOP-6315) | GzipCodec should not represent BuiltInZlibInflater as decompressorType |  Major | io | Aaron Kimball | Aaron Kimball |
| [HDFS-187](https://issues.apache.org/jira/browse/HDFS-187) | TestStartup fails if hdfs is running in the same machine |  Major | test | Tsz Wo Nicholas Sze | Todd Lipcon |
| [HDFS-745](https://issues.apache.org/jira/browse/HDFS-745) | TestFsck timeout on 0.20. |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-433](https://issues.apache.org/jira/browse/MAPREDUCE-433) | TestReduceFetch failed. |  Major | . | Tsz Wo Nicholas Sze | Chris Douglas |
| [HADOOP-6498](https://issues.apache.org/jira/browse/HADOOP-6498) | IPC client  bug may cause rpc call hang |  Blocker | ipc | Ruyue Ma | Ruyue Ma |
| [HDFS-872](https://issues.apache.org/jira/browse/HDFS-872) | DFSClient 0.20.1 is incompatible with HDFS 0.20.2 |  Major | datanode, hdfs-client | Bassam Tabbara | Todd Lipcon |
| [MAPREDUCE-1010](https://issues.apache.org/jira/browse/MAPREDUCE-1010) | Adding tests for changes in archives. |  Minor | harchive | Mahadev konar | Mahadev konar |
| [HADOOP-6506](https://issues.apache.org/jira/browse/HADOOP-6506) | Failing tests prevent the rest of test targets from execution. |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-6524](https://issues.apache.org/jira/browse/HADOOP-6524) | Contrib tests are failing Clover'ed build |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-927](https://issues.apache.org/jira/browse/HDFS-927) | DFSInputStream retries too many times for new block locations |  Critical | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HADOOP-5611](https://issues.apache.org/jira/browse/HADOOP-5611) | C++ libraries do not build on Debian Lenny |  Critical | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1251](https://issues.apache.org/jira/browse/MAPREDUCE-1251) | c++ utils doesn't compile |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-5612](https://issues.apache.org/jira/browse/HADOOP-5612) | Some c++ scripts are not chmodded before ant execution |  Major | build | Todd Lipcon | Todd Lipcon |
| [HADOOP-6575](https://issues.apache.org/jira/browse/HADOOP-6575) | Tests do not run on 0.20 branch |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-6576](https://issues.apache.org/jira/browse/HADOOP-6576) | TestStreamingStatus is failing on 0.20 branch |  Major | . | Chris Douglas | Todd Lipcon |
| [MAPREDUCE-617](https://issues.apache.org/jira/browse/MAPREDUCE-617) | Streaming should not throw java.lang.RuntimeException and ERROR while displaying help |  Minor | contrib/streaming | Karam Singh |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-919](https://issues.apache.org/jira/browse/HDFS-919) | Create test to validate the BlocksVerified metric |  Major | test | gary murry |  |
| [HDFS-907](https://issues.apache.org/jira/browse/HDFS-907) | Add  tests for getBlockLocations and totalLoad metrics. |  Minor | namenode | Ravi Phulari | Ravi Phulari |


