
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
# Apache Hadoop  1.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-385](https://issues.apache.org/jira/browse/HDFS-385) | *Major* | **Design a pluggable interface to place replicas of blocks in HDFS**

New experimental API BlockPlacementPolicy allows investigating alternate rules for locating block replicas.


---

* [HADOOP-8164](https://issues.apache.org/jira/browse/HADOOP-8164) | *Major* | **Handle paths using back slash as path separator for windows only**

This jira only allows providing paths using back slash as separator on Windows. The back slash on \*nix system will be used as escape character. The support for paths using back slash as path separator will be removed in HADOOP-8139 in release 23.3.


---

* [MAPREDUCE-4415](https://issues.apache.org/jira/browse/MAPREDUCE-4415) | *Major* | **Backport the Job.getInstance methods from MAPREDUCE-1505 to branch-1**

Backported new APIs to get a Job object to 1.2.0 from 2.0.0. Job API static methods Job.getInstance(), Job.getInstance(Configuration) and Job.getInstance(Configuration, jobName) are now available across both releases to avoid porting pain.


---

* [HDFS-3697](https://issues.apache.org/jira/browse/HDFS-3697) | *Minor* | **Enable fadvise readahead by default**

The datanode now performs 4MB readahead by default when reading data from its disks, if the native libraries are present. This has been shown to improve performance in many workloads. The feature may be disabled by setting dfs.datanode.readahead.bytes to "0".


---

* [MAPREDUCE-4565](https://issues.apache.org/jira/browse/MAPREDUCE-4565) | *Major* | **Backport MR-2855 to branch-1: ResourceBundle lookup during counter name resolution takes a lot of time**

Passing a cached class-loader to ResourceBundle creator to minimize counter names lookup time.


---

* [MAPREDUCE-4629](https://issues.apache.org/jira/browse/MAPREDUCE-4629) | *Major* | **Remove JobHistory.DEBUG\_MODE**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7698](https://issues.apache.org/jira/browse/HADOOP-7698) | *Critical* | **jsvc target fails on x86\_64**

The jsvc build target is now supported for Mac OSX and other platforms as well.


---

* [MAPREDUCE-3678](https://issues.apache.org/jira/browse/MAPREDUCE-3678) | *Major* | **The Map tasks logs should have the value of input split it processed**

A map-task's syslogs now carries basic info on the InputSplit it processed.


---

* [MAPREDUCE-4451](https://issues.apache.org/jira/browse/MAPREDUCE-4451) | *Major* | **fairscheduler fail to init job with kerberos authentication configured**

Using FairScheduler with security configured, job initialization fails. The problem is that threads in JobInitializer runs as RPC user instead of jobtracker, pre-start all the threads fix this bug


---

* [HDFS-4071](https://issues.apache.org/jira/browse/HDFS-4071) | *Minor* | **Add number of stale DataNodes to metrics for Branch-1**

This jira adds a new metric with name "StaleDataNodes" under metrics context "dfs" of type Gauge. This tracks the number of DataNodes marked as stale. A DataNode is marked stale when the heartbeat message from the DataNode is not received within the configured time ""dfs.namenode.stale.datanode.interval".


Please see hdfs-default.xml documentation corresponding to "dfs.namenode.stale.datanode.interval" for more details on how to configure this feature. When this feature is not configured, this metrics would return zero.


---

* [HADOOP-8971](https://issues.apache.org/jira/browse/HADOOP-8971) | *Major* | **Backport: hadoop.util.PureJavaCrc32 cache hit-ratio is low for static data (HADOOP-8926)**

Backport cache-aware improvements for PureJavaCrc32 from trunk (HADOOP-8926)


---

* [HDFS-4122](https://issues.apache.org/jira/browse/HDFS-4122) | *Major* | **Cleanup HDFS logs and reduce the size of logged messages**

The change from this jira changes the content of some of the log messages. No log message are removed. Only the content of the log messages is changed to reduce the size. If you have a tool that depends on the exact content of the log, please look at the patch and make appropriate updates to the tool.


---

* [HDFS-4320](https://issues.apache.org/jira/browse/HDFS-4320) | *Major* | **Add a separate configuration for namenode rpc address instead of only using fs.default.name**

The namenode RPC address is currently identified from configuration "fs.default.name". In some setups where default FS is other than HDFS, the "fs.default.name" cannot be used to get the namenode address. When such a setup co-exists with HDFS, with this change namenode can be identified using a separate configuration parameter "dfs.namenode.rpc-address".

"dfs.namenode.rpc-address", when configured, overrides fs.default.name for identifying namenode RPC address.


---

* [HDFS-4337](https://issues.apache.org/jira/browse/HDFS-4337) | *Major* | **Backport HDFS-4240 to branch-1: Make sure nodes are avoided to place replica if some replica are already under the same nodegroup.**

Backport HDFS-4240 to branch-1


---

* [HDFS-4350](https://issues.apache.org/jira/browse/HDFS-4350) | *Major* | **Make enabling of stale marking on read and write paths independent**

This patch makes an incompatible configuration change, as described below:
In releases 1.1.0 and other point releases 1.1.x, the configuration parameter "dfs.namenode.check.stale.datanode" could be used to turn on checking for the stale nodes. This configuration is no longer supported in release 1.2.0 onwards and is renamed as "dfs.namenode.avoid.read.stale.datanode".

How feature works and configuring this feature:
As described in HDFS-3703 release notes, datanode stale period can be configured using parameter "dfs.namenode.stale.datanode.interval" in seconds (default value is 30 seconds). NameNode can be configured to use this staleness information for reads using configuration "dfs.namenode.avoid.read.stale.datanode". When this parameter is set to true, namenode picks a stale datanode as the last target to read from when returning block locations for reads. Using staleness information for writes is as described in the releases notes of HDFS-3912.


---

* [HDFS-4519](https://issues.apache.org/jira/browse/HDFS-4519) | *Major* | **Support override of jsvc binary and log file locations when launching secure datanode.**

With this improvement the following options are available in release 1.2.0 and later on 1.x release stream:
1. jsvc location can be overridden by setting environment variable JSVC\_HOME. Defaults to jsvc binary packaged within the Hadoop distro.
2. jsvc log output is directed to the file defined by JSVC\_OUTFILE. Defaults to $HADOOP\_LOG\_DIR/jsvc.out.
3. jsvc error output is directed to the file defined by JSVC\_ERRFILE file.  Defaults to $HADOOP\_LOG\_DIR/jsvc.err.

With this improvement the following options are available in release 2.0.4 and later on 2.x release stream:
1. jsvc log output is directed to the file defined by JSVC\_OUTFILE. Defaults to $HADOOP\_LOG\_DIR/jsvc.out.
2. jsvc error output is directed to the file defined by JSVC\_ERRFILE file.  Defaults to $HADOOP\_LOG\_DIR/jsvc.err.

For overriding jsvc location on 2.x releases, here is the release notes from HDFS-2303:
To run secure Datanodes users must install jsvc for their platform and set JSVC\_HOME to point to the location of jsvc in their environment.


---

* [HADOOP-8817](https://issues.apache.org/jira/browse/HADOOP-8817) | *Major* | **Backport Network Topology Extension for Virtualization (HADOOP-8468) to branch-1**

A new 4-layer network topology NetworkToplogyWithNodeGroup is available to make Hadoop more robust and efficient in virtualized environment.


---

* [MAPREDUCE-4737](https://issues.apache.org/jira/browse/MAPREDUCE-4737) | *Major* | ** Hadoop does not close output file / does not call Mapper.cleanup if exception in map**

Ensure that mapreduce APIs are semantically consistent with mapred API w.r.t Mapper.cleanup and Reducer.cleanup; in the sense that cleanup is now called even if there is an error. The old mapred API already ensures that Mapper.close and Reducer.close are invoked during error handling. Note that it is an incompatible change, however end-users can override Mapper.run and Reducer.run to get the old (inconsistent) behaviour.


---

* [HADOOP-8470](https://issues.apache.org/jira/browse/HADOOP-8470) | *Major* | **Implementation of 4-layer subclass of NetworkTopology (NetworkTopologyWithNodeGroup)**

This patch should be checked in together (or after) with JIRA Hadoop-8469: https://issues.apache.org/jira/browse/HADOOP-8469



