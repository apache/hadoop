
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
# Apache Hadoop  0.20.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [MAPREDUCE-826](https://issues.apache.org/jira/browse/MAPREDUCE-826) | *Trivial* | **harchive doesn't use ToolRunner / harchive returns 0 even if the job fails with exception**

Use ToolRunner for archives job and return non zero error code on failure.


---

* [MAPREDUCE-112](https://issues.apache.org/jira/browse/MAPREDUCE-112) | *Blocker* | **Reduce Input Records and Reduce Output Records counters are not being set when using the new Mapreduce reducer API**

Updates of counters for reduce input and output records were added in the new API so they are available for jobs using the new API.


---

* [HADOOP-6231](https://issues.apache.org/jira/browse/HADOOP-6231) | *Major* | **Allow caching of filesystem instances to be disabled on a per-instance basis**

Allow a general mechanism to disable the cache on a per filesystem basis by using property fs.\<schemename\>.impl.disable.cache. eg. fs.har.impl.disable.cache in core-default.xml


---

* [MAPREDUCE-979](https://issues.apache.org/jira/browse/MAPREDUCE-979) | *Blocker* | **JobConf.getMemoryFor{Map\|Reduce}Task doesn't fallback to newer config knobs when mapred.taskmaxvmem is set to DISABLED\_MEMORY\_LIMIT of -1**

Added support to fallback to new task memory configuration when deprecated memory configuration values are set to disabled.


---

* [HDFS-677](https://issues.apache.org/jira/browse/HDFS-677) | *Blocker* | **Rename failure due to quota results in deletion of src directory**

Rename properly considers the case where both source and destination are over quota; operation will fail with error indication.


---

* [HADOOP-6097](https://issues.apache.org/jira/browse/HADOOP-6097) | *Major* | **Multiple bugs w/ Hadoop archives**

Bugs fixed for Hadoop archives: character escaping in paths, LineReader and file system caching.


---

* [HDFS-761](https://issues.apache.org/jira/browse/HDFS-761) | *Major* | **Failure to process rename operation from edits log due to quota verification**

Corrected an error when checking quota policy that resulted in a failure to read the edits log, stopping the primary/secondary name node.


---

* [MAPREDUCE-1068](https://issues.apache.org/jira/browse/MAPREDUCE-1068) | *Major* | **In hadoop-0.20.0 streaming job do not throw proper verbose error message if file is not present**

Fix streaming job to show proper message if file is is not present, for -file option.


---

* [HDFS-596](https://issues.apache.org/jira/browse/HDFS-596) | *Blocker* | **Memory leak in libhdfs: hdfsFreeFileInfo() in libhdfs does not free memory for mOwner and mGroup**

Memory leak in function hdfsFreeFileInfo in libhdfs. This bug affects fuse-dfs severely.


---

* [MAPREDUCE-1147](https://issues.apache.org/jira/browse/MAPREDUCE-1147) | *Blocker* | **Map output records counter missing for map-only jobs in new API**

Adds a counter to track the number of records emitted by map writing directly to HDFS i.e map tasks of job with 0 reducers.


---

* [MAPREDUCE-1182](https://issues.apache.org/jira/browse/MAPREDUCE-1182) | *Blocker* | **Reducers fail with OutOfMemoryError while copying Map outputs**

Modifies shuffle related memory parameters to use 'long' from 'int' so that sizes greater than maximum integer size are handled correctly


---

* [HDFS-781](https://issues.apache.org/jira/browse/HDFS-781) | *Blocker* | **Metrics PendingDeletionBlocks is not decremented**

Correct PendingDeletionBlocks metric to properly decrement counts.


---

* [HDFS-793](https://issues.apache.org/jira/browse/HDFS-793) | *Blocker* | **DataNode should first receive the whole packet ack message before it constructs and sends its own ack message for the packet**

**WARNING: No release note provided for this change.**


---

* [HADOOP-6428](https://issues.apache.org/jira/browse/HADOOP-6428) | *Major* | **HttpServer sleeps with negative values**

Corrected arithmetic error that made sleep times less than zero.


---

* [HADOOP-6460](https://issues.apache.org/jira/browse/HADOOP-6460) | *Blocker* | **Namenode runs of out of memory due to memory leak in ipc Server**

If an IPC server response buffer has grown to than 1MB, it is replaced by a smaller buffer to free up the Java heap that was used. This will improve the longevity of the name service.


---

* [MAPREDUCE-433](https://issues.apache.org/jira/browse/MAPREDUCE-433) | *Major* | **TestReduceFetch failed.**

Resolves the test failure by modifying the test to base it on spill counters rather than on bytes read/written. It also introduces a new configuration parameter "mapred.job.shuffle.input.buffer.percent" to provide finer grained control on the memory limit to be used during shuffle.


---

* [HADOOP-6498](https://issues.apache.org/jira/browse/HADOOP-6498) | *Blocker* | **IPC client  bug may cause rpc call hang**

Correct synchronization error in IPC where handler thread could hang if request reader got an error.


---

* [MAPREDUCE-623](https://issues.apache.org/jira/browse/MAPREDUCE-623) | *Major* | **Resolve javac warnings in mapred**

Removes javac warnings by either resolving them or suppressing them (wherever resolution is not possible)



