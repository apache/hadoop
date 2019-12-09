
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
# Apache Hadoop  0.23.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-2887](https://issues.apache.org/jira/browse/HDFS-2887) | *Major* | **Define a FSVolume interface**

FSVolume, is a part of FSDatasetInterface implementation, should not be referred outside FSDataset.  A new FSVolumeInterface is defined.  The BlockVolumeChoosingPolicy.chooseVolume(..) method signature is also updated.


---

* [HDFS-2950](https://issues.apache.org/jira/browse/HDFS-2950) | *Minor* | **Secondary NN HTTPS address should be listed as a NAMESERVICE\_SPECIFIC\_KEY**

The configuration dfs.secondary.https.port has been renamed to dfs.namenode.secondary.https-port for consistency. The old configuration is still supported via a deprecation path.


---

* [MAPREDUCE-3634](https://issues.apache.org/jira/browse/MAPREDUCE-3634) | *Major* | **All daemons should crash instead of hanging around when their EventHandlers get exceptions**

Fixed all daemons to crash instead of hanging around when their EventHandlers get exceptions.


---

* [MAPREDUCE-3798](https://issues.apache.org/jira/browse/MAPREDUCE-3798) | *Major* | **TestJobCleanup testCustomCleanup is failing**

Fixed failing TestJobCleanup.testCusomCleanup() and moved it to the maven build.


---

* [HDFS-2907](https://issues.apache.org/jira/browse/HDFS-2907) | *Minor* | **Make FSDataset in Datanode Pluggable**

Add a private conf property dfs.datanode.fsdataset.factory to make FSDataset in Datanode pluggable.


---

* [MAPREDUCE-3738](https://issues.apache.org/jira/browse/MAPREDUCE-3738) | *Critical* | **NM can hang during shutdown if AppLogAggregatorImpl thread dies unexpectedly**

Committed to trunk and branch-0.23. Thanks Jason.


---

* [MAPREDUCE-3866](https://issues.apache.org/jira/browse/MAPREDUCE-3866) | *Minor* | **bin/yarn prints the command line unnecessarily**

Fixed the bin/yarn script to not print the command line unnecessarily.


---

* [MAPREDUCE-3730](https://issues.apache.org/jira/browse/MAPREDUCE-3730) | *Minor* | **Allow restarted NM to rejoin cluster before RM expires it**

Modified RM to allow restarted NMs to be able to join the cluster without waiting for expiry.


---

* [MAPREDUCE-2793](https://issues.apache.org/jira/browse/MAPREDUCE-2793) | *Critical* | **[MR-279] Maintain consistency in naming appIDs, jobIDs and attemptIDs**

Corrected AppIDs, JobIDs, TaskAttemptIDs to be of correct format on the web pages.


---

* [MAPREDUCE-3910](https://issues.apache.org/jira/browse/MAPREDUCE-3910) | *Blocker* | **user not allowed to submit jobs even though queue -showacls shows it allows**

Fixed a bug in CapacityScheduler LeafQueue which was causing app-submission to fail.


---

* [MAPREDUCE-3686](https://issues.apache.org/jira/browse/MAPREDUCE-3686) | *Critical* | **history server web ui - job counter values for map/reduce not shown properly**

Fixed two bugs in Counters because of which web app displays zero counter values for framework counters.


---

* [MAPREDUCE-3901](https://issues.apache.org/jira/browse/MAPREDUCE-3901) | *Major* | **lazy load JobHistory Task and TaskAttempt details**

Modified JobHistory records in YARN to lazily load job and task reports so as to improve UI response times.


---

* [MAPREDUCE-2855](https://issues.apache.org/jira/browse/MAPREDUCE-2855) | *Major* | **ResourceBundle lookup during counter name resolution takes a lot of time**

Passing a cached class-loader to ResourceBundle creator to minimize counter names lookup time.


---

* [MAPREDUCE-3922](https://issues.apache.org/jira/browse/MAPREDUCE-3922) | *Minor* | **Fix the potential problem compiling 32 bit binaries on a x86\_64 host.**

Fixed build to not compile 32bit container-executor binary by default on all platforms.


---

* [MAPREDUCE-3931](https://issues.apache.org/jira/browse/MAPREDUCE-3931) | *Major* | **MR tasks failing due to changing timestamps on Resources to download**

Changed PB implementation of LocalResource to take locks so that race conditions don't fail tasks by inadvertantly changing the timestamps.


---

* [MAPREDUCE-3920](https://issues.apache.org/jira/browse/MAPREDUCE-3920) | *Major* | **Revise yarn default port number selection**

port number changes for resourcemanager and nodemanager


---

* [HADOOP-8131](https://issues.apache.org/jira/browse/HADOOP-8131) | *Critical* | **FsShell put doesn't correctly handle a non-existent dir**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-3792](https://issues.apache.org/jira/browse/MAPREDUCE-3792) | *Critical* | **job -list displays only the jobs submitted by a particular user**

Fix "bin/mapred job -list" to display all jobs instead of only the jobs owned by the user.


---

* [MAPREDUCE-3614](https://issues.apache.org/jira/browse/MAPREDUCE-3614) | *Major* | ** finalState UNDEFINED if AM is killed by hand**

Fixed MR AM to close history file quickly and send a correct final state to the RM when it is killed.


---

* [MAPREDUCE-3009](https://issues.apache.org/jira/browse/MAPREDUCE-3009) | *Major* | **RM UI -\> Applications -\> Application(Job History) -\> Map Tasks -\> Task ID -\> Node link is not working**

Fixed node link on JobHistory webapp.


---

* [MAPREDUCE-3954](https://issues.apache.org/jira/browse/MAPREDUCE-3954) | *Blocker* | **Clean up passing HEAPSIZE to yarn and mapred commands.**

Added new envs to separate heap size for different daemons started via bin scripts.


---

* [MAPREDUCE-3975](https://issues.apache.org/jira/browse/MAPREDUCE-3975) | *Blocker* | **Default value not set for Configuration parameter mapreduce.job.local.dir**

Exporting mapreduce.job.local.dir for mapreduce tasks to use as job-level shared scratch space.


---

* [MAPREDUCE-3982](https://issues.apache.org/jira/browse/MAPREDUCE-3982) | *Critical* | **TestEmptyJob fails with FileNotFound**

Fixed FileOutputCommitter to not err out for an 'empty-job' whose tasks don't write any outputs.


---

* [HADOOP-8164](https://issues.apache.org/jira/browse/HADOOP-8164) | *Major* | **Handle paths using back slash as path separator for windows only**

This jira only allows providing paths using back slash as separator on Windows. The back slash on \*nix system will be used as escape character. The support for paths using back slash as path separator will be removed in HADOOP-8139 in release 23.3.


---

* [HADOOP-8175](https://issues.apache.org/jira/browse/HADOOP-8175) | *Major* | **Add mkdir -p flag**

FsShell mkdir now accepts a -p flag.  Like unix, mkdir -p will not fail if the directory already exists.  Unlike unix, intermediate directories are always created, regardless of the flag, to avoid incompatibilities at this time.



