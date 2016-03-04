
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
# Apache Hadoop  0.20.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5565](https://issues.apache.org/jira/browse/HADOOP-5565) | *Major* | **The job instrumentation API needs to have a method for finalizeJob,**

Add finalizeJob & terminateJob methods to JobTrackerInstrumentation class


---

* [HADOOP-5548](https://issues.apache.org/jira/browse/HADOOP-5548) | *Blocker* | **Observed negative running maps on the job tracker**

Adds synchronization for JobTracker methods in RecoveryManager.


---

* [HADOOP-5531](https://issues.apache.org/jira/browse/HADOOP-5531) | *Blocker* | **Remove Chukwa on branch-0.20**

Disabled Chukwa unit tests for 0.20 branch only.


---

* [HADOOP-5521](https://issues.apache.org/jira/browse/HADOOP-5521) | *Major* | **Remove dependency of testcases on RESTART\_COUNT**

This patch makes TestJobHistory and its dependent testcases independent of RESTART\_COUNT.


---

* [HADOOP-5468](https://issues.apache.org/jira/browse/HADOOP-5468) | *Major* | **Change Hadoop doc menu to sub-menus**

Reformatted HTML documentation for Hadoop to use submenus at the left column.


---

* [HADOOP-5030](https://issues.apache.org/jira/browse/HADOOP-5030) | *Major* | **Chukwa RPM build improvements**

Changed RPM install location to the value specified by build.properties file.


---

* [HADOOP-4970](https://issues.apache.org/jira/browse/HADOOP-4970) | *Major* | **Use the full path when move files to .Trash/Current**

Changed trash facility to use absolute path of the deleted file.


---

* [HADOOP-4873](https://issues.apache.org/jira/browse/HADOOP-4873) | *Major* | **display minMaps/Reduces on advanced scheduler page**

Changed fair scheduler UI to display minMaps and minReduces variables.


---

* [HADOOP-4843](https://issues.apache.org/jira/browse/HADOOP-4843) | *Major* | **Collect Job History log file and Job Conf file into Chukwa**

Introduced Chuckwa collection of job history.


---

* [HADOOP-4827](https://issues.apache.org/jira/browse/HADOOP-4827) | *Major* | **Improve data aggregation in database**

Improved framework for data aggregation in Chuckwa.


---

* [HADOOP-4826](https://issues.apache.org/jira/browse/HADOOP-4826) | *Major* | **Admin command saveNamespace.**

Introduced new dfsadmin command saveNamespace to command the name service to do an immediate save of the file system image.


---

* [HADOOP-4789](https://issues.apache.org/jira/browse/HADOOP-4789) | *Minor* | **Change fair scheduler to share between pools by default, not between invidual jobs**

Changed fair scheduler to divide resources equally between pools, not jobs.


---

* [HADOOP-4783](https://issues.apache.org/jira/browse/HADOOP-4783) | *Blocker* | **History files are given world readable permissions.**

Changed history directory permissions to 750 and history file permissions to 740.


---

* [HADOOP-4749](https://issues.apache.org/jira/browse/HADOOP-4749) | *Major* | **reducer should output input data size when shuffling is done**

Added a new counter REDUCE\_INPUT\_BYTES.


---

* [HADOOP-4661](https://issues.apache.org/jira/browse/HADOOP-4661) | *Major* | **distch: a tool for distributed ch{mod,own}**

Introduced distch tool for parallel ch{mod, own, grp}.


---

* [HADOOP-4631](https://issues.apache.org/jira/browse/HADOOP-4631) | *Major* | **Split the default configurations into 3 parts**

Split hadoop-default.xml into core-default.xml, hdfs-default.xml and mapreduce-default.xml.


---

* [HADOOP-4618](https://issues.apache.org/jira/browse/HADOOP-4618) | *Major* | **Move http server from FSNamesystem into NameNode.**

Moved HTTP server from FSNameSystem to NameNode. Removed FSNamesystem.getNameNodeInfoPort(). Replaced FSNamesystem.getDFSNameNodeMachine() and FSNamesystem.getDFSNameNodePort() with new method  FSNamesystem.getDFSNameNodeAddress(). Removed constructor NameNode(bindAddress, conf).


---

* [HADOOP-4576](https://issues.apache.org/jira/browse/HADOOP-4576) | *Major* | **Modify pending tasks count in the UI to pending jobs count in the UI**

Changed capacity scheduler UI to better present number of running and pending tasks.


---

* [HADOOP-4575](https://issues.apache.org/jira/browse/HADOOP-4575) | *Major* | **An independent HTTPS proxy for HDFS**

Introduced independent HSFTP proxy server for authenticated access to clusters.


---

* [HADOOP-4572](https://issues.apache.org/jira/browse/HADOOP-4572) | *Major* | **INode and its sub-classes should be package private**

Moved org.apache.hadoop.hdfs.{CreateEditsLog, NNThroughputBenchmark} to org.apache.hadoop.hdfs.server.namenode.


---

* [HADOOP-4567](https://issues.apache.org/jira/browse/HADOOP-4567) | *Major* | **GetFileBlockLocations should return the NetworkTopology information of the machines that hosts those blocks**

Changed GetFileBlockLocations to return topology information for nodes that host the block replicas.


---

* [HADOOP-4565](https://issues.apache.org/jira/browse/HADOOP-4565) | *Major* | **MultiFileInputSplit can use data locality information to create splits**

Improved MultiFileInputFormat so that multiple blocks from the same node or same rack can be combined into a single split.


---

* [HADOOP-4454](https://issues.apache.org/jira/browse/HADOOP-4454) | *Minor* | **Support comments in 'slaves'  file**

Changed processing of conf/slaves file to allow # to begin a comment.


---

* [HADOOP-4445](https://issues.apache.org/jira/browse/HADOOP-4445) | *Major* | **Wrong number of running map/reduce tasks are displayed in queue information.**

Changed JobTracker UI to better present the number of active tasks.


---

* [HADOOP-4435](https://issues.apache.org/jira/browse/HADOOP-4435) | *Minor* | **The JobTracker should display the amount of heap memory used**

Changed JobTracker web status page to display the amount of heap memory in use. This changes the JobSubmissionProtocol.


---

* [HADOOP-4422](https://issues.apache.org/jira/browse/HADOOP-4422) | *Major* | **S3 file systems should not create bucket**

Modified Hadoop file system to no longer create S3 buckets. Applications can create buckets for their S3 file systems by other means, for example, using the JetS3t API.


---

* [HADOOP-4374](https://issues.apache.org/jira/browse/HADOOP-4374) | *Major* | **JVM should not be killed but given an opportunity to exit gracefully**

This patch (1) Adds a shutdownHook that does syncLogs sothat logs of the current task are flushed and log.index is up to date in cases like System.exit(), or killed using signals(other than SIGKILL).
(2) Changes writeToIndexFile() to write to a temporary index file first and then rename to log.index sothat updates to log.index file are atomic.


---

* [HADOOP-4305](https://issues.apache.org/jira/browse/HADOOP-4305) | *Major* | **repeatedly blacklisted tasktrackers should get declared dead**

Improved TaskTracker blacklisting strategy to better exclude faulty tracker from executing tasks.


---

* [HADOOP-4284](https://issues.apache.org/jira/browse/HADOOP-4284) | *Major* | **Support for user configurable global filters on HttpServer**

Introduced HttpServer method to support global filters.


---

* [HADOOP-4253](https://issues.apache.org/jira/browse/HADOOP-4253) | *Major* | **Fix warnings generated by FindBugs**

Removed  from class org.apache.hadoop.fs.RawLocalFileSystem deprecated methods public String getName(), public void lock(Path p, boolean shared) and public void release(Path p).


---

* [HADOOP-4234](https://issues.apache.org/jira/browse/HADOOP-4234) | *Minor* | **KFS: Allow KFS layer to interface with multiple KFS namenodes**

Changed KFS glue layer to allow applications to interface with multiple KFS metaservers.


---

* [HADOOP-4210](https://issues.apache.org/jira/browse/HADOOP-4210) | *Major* | **Findbugs warnings are printed related to equals implementation of several classes**

Changed public class org.apache.hadoop.mapreduce.ID to be an abstract class. Removed from class org.apache.hadoop.mapreduce.ID the methods  public static ID read(DataInput in) and public static ID forName(String str).


---

* [HADOOP-4188](https://issues.apache.org/jira/browse/HADOOP-4188) | *Major* | **Remove Task's dependency on concrete file systems**

Removed Task's dependency on concrete file systems by taking list from FileSystem class. Added statistics table to FileSystem class. Deprecated FileSystem method getStatistics(Class\<? extends FileSystem\> cls).


---

* [HADOOP-4179](https://issues.apache.org/jira/browse/HADOOP-4179) | *Major* | **Hadoop-Vaidya : Rule based performance diagnostic tool for Map/Reduce jobs**

Introduced Vaidya rule based performance diagnostic tool for Map/Reduce jobs.


---

* [HADOOP-4103](https://issues.apache.org/jira/browse/HADOOP-4103) | *Major* | **Alert for missing blocks**

Modified dfsadmin -report to report under replicated blocks. blocks with corrupt replicas, and missing blocks".


---

* [HADOOP-4035](https://issues.apache.org/jira/browse/HADOOP-4035) | *Blocker* | **Modify the capacity scheduler (HADOOP-3445) to schedule tasks based on memory requirements and task trackers free memory**

Changed capacity scheduler policy to take note of task memory requirements and task tracker memory availability.


---

* [HADOOP-4029](https://issues.apache.org/jira/browse/HADOOP-4029) | *Major* | **NameNode should report status and performance for each replica of image and log**

Added name node storage information to the dfshealth page, and moved data node information to a separated page.


---

* [HADOOP-3986](https://issues.apache.org/jira/browse/HADOOP-3986) | *Major* | **JobClient should not have a static configuration**

Removed classes org.apache.hadoop.mapred.JobShell and org.apache.hadoop.mapred.TestJobShell. Removed from JobClient methods static void  setCommandLineConfig(Configuration conf) and public static Configuration getCommandLineConfig().


---

* [HADOOP-3923](https://issues.apache.org/jira/browse/HADOOP-3923) | *Minor* | **Deprecate org.apache.hadoop.mapred.StatusHttpServer**

Moved class org.apache.hadoop.mapred.StatusHttpServer to org.apache.hadoop.http.HttpServer.


---

* [HADOOP-3750](https://issues.apache.org/jira/browse/HADOOP-3750) | *Major* | **Fix and enforce module dependencies**

Removed deprecated method parseArgs from org.apache.hadoop.fs.FileSystem.


---

* [HADOOP-3497](https://issues.apache.org/jira/browse/HADOOP-3497) | *Major* | **File globbing with a PathFilter is too restrictive**

Changed the semantics of file globbing with a PathFilter (using the globStatus method of FileSystem). Previously, the filtering was too restrictive, so that a glob of /\*/\* and a filter that only accepts /a/b would not have matched /a/b. With this change /a/b does match.


---

* [HADOOP-3422](https://issues.apache.org/jira/browse/HADOOP-3422) | *Major* | **Ganglia counter metrics are all reported with the metric name "value", so the counter values can not be seen**

Changed names of ganglia metrics to avoid conflicts and to better identify source function.


---

* [HADOOP-3344](https://issues.apache.org/jira/browse/HADOOP-3344) | *Major* | **libhdfs: always builds 32bit, even when x86\_64 Java used**

Changed build procedure for libhdfs to build correctly for different platforms. Build instructions are in the Jira item.


---

* [HADOOP-3063](https://issues.apache.org/jira/browse/HADOOP-3063) | *Major* | **BloomMapFile - fail-fast version of MapFile for sparsely populated key space**

Introduced BloomMapFile subclass of MapFile that creates a Bloom filter from all keys.


---

* [HADOOP-1650](https://issues.apache.org/jira/browse/HADOOP-1650) | *Major* | **Upgrade Jetty to 6.x**

Upgraded all core servers to use Jetty 6


---

* [HADOOP-1230](https://issues.apache.org/jira/browse/HADOOP-1230) | *Major* | **Replace parameters with context objects in Mapper, Reducer, Partitioner, InputFormat, and OutputFormat classes**

Replaced parameters with context obejcts in Mapper, Reducer, Partitioner, InputFormat, and OutputFormat classes.



