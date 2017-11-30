
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
# Apache Hadoop  0.19.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3595](https://issues.apache.org/jira/browse/HADOOP-3595) | *Major* | **Remove deprecated mapred.combine.once functionality**

 Removed deprecated methods for mapred.combine.once functionality.


---

* [HADOOP-2664](https://issues.apache.org/jira/browse/HADOOP-2664) | *Major* | **lzop-compatible CompresionCodec**

Introduced LZOP codec.


---

* [HADOOP-3667](https://issues.apache.org/jira/browse/HADOOP-3667) | *Major* | **Remove deprecated methods in JobConf**

Removed the following deprecated methods from JobConf:
      addInputPath(Path)
      getInputPaths()
      getMapOutputCompressionType()
      getOutputPath()
      getSystemDir()
      setInputPath(Path)
      setMapOutputCompressionType(CompressionType style)
      setOutputPath(Path)


---

* [HADOOP-3652](https://issues.apache.org/jira/browse/HADOOP-3652) | *Major* | **Remove deprecated class OutputFormatBase**

Removed deprecated org.apache.hadoop.mapred.OutputFormatBase.


---

* [HADOOP-2325](https://issues.apache.org/jira/browse/HADOOP-2325) | *Major* | **Require Java 6**

Hadoop now requires Java 6.


---

* [HADOOP-3695](https://issues.apache.org/jira/browse/HADOOP-3695) | *Major* | **[HOD] Have an ability to run multiple slaves per node**

Added an ability in HOD to start multiple workers (TaskTrackers and/or DataNodes) per node to assist testing and simulation of scale. A configuration variable ringmaster.workers\_per\_ring was added to specify the number of workers to start.


---

* [HADOOP-3149](https://issues.apache.org/jira/browse/HADOOP-3149) | *Major* | **supporting multiple outputs for M/R jobs**

Introduced MultipleOutputs class so Map/Reduce jobs can write data to different output files. Each output can use a different OutputFormat. Outpufiles are created within the job output directory. FileOutputFormat.getPathForCustomFile() creates a filename under the outputdir that is named with the task ID and task type (i.e. myfile-r-00001).


---

* [HADOOP-3684](https://issues.apache.org/jira/browse/HADOOP-3684) | *Major* | **The data\_join should allow the user to implement a customer cloning function**

Allowed user to overwrite clone function in a subclass of TaggedMapOutput class.


---

* [HADOOP-3478](https://issues.apache.org/jira/browse/HADOOP-3478) | *Major* | **The algorithm to decide map re-execution on fetch failures can be improved**

Changed reducers to fetch maps in the same order for a given host to speed up identification of the faulty maps; reducers still randomize the host selection to distribute load.


---

* [HADOOP-3714](https://issues.apache.org/jira/browse/HADOOP-3714) | *Trivial* | **Bash tab completion support**

Adds a new contrib, bash-tab-completion, which enables bash tab completion for the bin/hadoop script. See the README file in the contrib directory for the installation.


---

* [HADOOP-3730](https://issues.apache.org/jira/browse/HADOOP-3730) | *Major* | **add new JobConf constructor that disables loading default configurations**

 Added a JobConf constructor that disables loading  default configurations so as to take all default values from the JobTracker's configuration.


---

* [HADOOP-3485](https://issues.apache.org/jira/browse/HADOOP-3485) | *Minor* | **fix writes**

Introduce write support for Fuse; requires Linux kernel 2.6.15 or better.


---

* [HADOOP-3412](https://issues.apache.org/jira/browse/HADOOP-3412) | *Minor* | **Refactor the scheduler out of the JobTracker**

Added the ability to chose between many schedulers, and to limit the number of running tasks per job.


---

* [HADOOP-1700](https://issues.apache.org/jira/browse/HADOOP-1700) | *Major* | **Append to files in HDFS**

Introduced append operation for HDFS files.


---

* [HADOOP-3646](https://issues.apache.org/jira/browse/HADOOP-3646) | *Major* | **Providing bzip2 as codec**

Introduced support for bzip2 compressed files.


---

* [HADOOP-3796](https://issues.apache.org/jira/browse/HADOOP-3796) | *Major* | **fuse-dfs should take rw,ro,trashon,trashoff,protected=blah mount arguments rather than them being compiled in**

Changed Fuse configuration to use mount options.


---

* [HADOOP-3837](https://issues.apache.org/jira/browse/HADOOP-3837) | *Major* | **hadop streaming does not use progress reporting to detect hung tasks**

Changed streaming tasks to adhere to task timeout value specified in the job configuration.


---

* [HADOOP-3792](https://issues.apache.org/jira/browse/HADOOP-3792) | *Minor* | **exit code from "hadoop dfs -test ..." is wrong for Unix shell**

Changed exit code from hadoop.fs.FsShell -test to match the usual Unix convention.


---

* [HADOOP-2302](https://issues.apache.org/jira/browse/HADOOP-2302) | *Major* | ** Streaming should provide an option for numerical sort of keys**

Introduced numerical key comparison for streaming.


---

* [HADOOP-153](https://issues.apache.org/jira/browse/HADOOP-153) | *Major* | **skip records that fail Task**

Introduced record skipping where tasks fail on certain records. (org.apache.hadoop.mapred.SkipBadRecords)


---

* [HADOOP-3719](https://issues.apache.org/jira/browse/HADOOP-3719) | *Major* | **Chukwa**

Introduced Chukwa data collection and analysis framework.


---

* [HADOOP-3873](https://issues.apache.org/jira/browse/HADOOP-3873) | *Major* | **DistCp should have an option for limiting the number of files/bytes being copied**

Added two new options -filelimit \<n\> and -sizelimit \<n\> to DistCp for limiting the total number of files and the total size in bytes, respectively.


---

* [HADOOP-3889](https://issues.apache.org/jira/browse/HADOOP-3889) | *Minor* | **distcp: Better Error Message should be thrown when accessing source files/directory with no read permission**

Changed DistCp error messages when there is a RemoteException.  Changed the corresponding return value from -999 to -3.


---

* [HADOOP-3585](https://issues.apache.org/jira/browse/HADOOP-3585) | *Minor* | **Hardware Failure Monitoring in large clusters running Hadoop/HDFS**

Added FailMon as a contrib project for hardware failure monitoring and analysis, under /src/contrib/failmon. Created User Manual and Quick Start Guide.


---

* [HADOOP-3549](https://issues.apache.org/jira/browse/HADOOP-3549) | *Major* | **meaningful errno values in libhdfs**

Improved error reporting for libhdfs so permission problems now return EACCES.


---

* [HADOOP-3062](https://issues.apache.org/jira/browse/HADOOP-3062) | *Major* | **Need to capture the metrics for the network ios generate by dfs reads/writes and map/reduce shuffling  and break them down by racks**

Introduced additional log records for data transfers.


---

* [HADOOP-3854](https://issues.apache.org/jira/browse/HADOOP-3854) | *Major* | **org.apache.hadoop.http.HttpServer should support user configurable filter**

Added a configuration property hadoop.http.filter.initializers and a class org.apache.hadoop.http.FilterInitializer for supporting servlet filter.  Cluster administrator could possibly configure customized filters for their web site.


---

* [HADOOP-3908](https://issues.apache.org/jira/browse/HADOOP-3908) | *Minor* | **Better error message if llibhdfs.so doesn't exist**

Improved Fuse-dfs better error message if llibhdfs.so doesn't exist.


---

* [HADOOP-3746](https://issues.apache.org/jira/browse/HADOOP-3746) | *Minor* | **A fair sharing job scheduler**

Introduced Fair Scheduler.


---

* [HADOOP-3828](https://issues.apache.org/jira/browse/HADOOP-3828) | *Major* | **Write skipped records' bytes to DFS**

Skipped records can optionally be written to the HDFS. Refer org.apache.hadoop.mapred.SkipBadRecords.setSkipOutputPath for setting the output path.


---

* [HADOOP-3939](https://issues.apache.org/jira/browse/HADOOP-3939) | *Major* | **DistCp should support an option for deleting non-existing files.**

Added a new option -delete to DistCp so that if the files/directories exist in dst but not in src will be deleted.  It uses FsShell to do delete, so that it will use trash if  the trash is enable.


---

* [HADOOP-3601](https://issues.apache.org/jira/browse/HADOOP-3601) | *Minor* | **Hive as a contrib project**

Introduced Hive Data Warehouse built on top of Hadoop that enables structuring Hadoop files as tables and partitions and allows users to query this data through a SQL like language using a command line interface.


---

* [HADOOP-3498](https://issues.apache.org/jira/browse/HADOOP-3498) | *Major* | **File globbing alternation should be able to span path components**

Extended file globbing alternation to cross path components. For example, {/a/b,/c/d} expands to a path that matches the files /a/b and /c/d.


---

* [HADOOP-3150](https://issues.apache.org/jira/browse/HADOOP-3150) | *Major* | **Move task file promotion into the task**

Moved task file promotion to the Task. When the task has finished, it will do a commit and is declared SUCCEDED. Job cleanup is done by a separate task. Job is declared SUCCEDED/FAILED after the cleanup task has finished. Added public classes org.apache.hadoop.mapred.JobContext, TaskAttemptContext, OutputCommitter and FileOutputCommiitter. Added public APIs:   public OutputCommitter getOutputCommitter() and
public void setOutputCommitter(Class\<? extends OutputCommitter\> theClass) in org.apache.hadoop.mapred.JobConf


---

* [HADOOP-3941](https://issues.apache.org/jira/browse/HADOOP-3941) | *Major* | **Extend FileSystem API to return file-checksums/file-digests**

Added new FileSystem APIs: FileChecksum and FileSystem.getFileChecksum(Path).


---

* [HADOOP-3963](https://issues.apache.org/jira/browse/HADOOP-3963) | *Minor* | **libhdfs should never exit on its own but rather return errors to the calling application**

Modified libhdfs to return NULL or error code when unrecoverable error occurs rather than exiting itself.


---

* [HADOOP-1869](https://issues.apache.org/jira/browse/HADOOP-1869) | *Major* | **access times of HDFS files**

Added HDFS file access times. By default, access times will be precise to the most recent hour boundary. A configuration parameter dfs.access.time.precision (milliseconds) is used to control this precision. Setting a value of 0 will disable persisting access times for HDFS files.


---

* [HADOOP-3581](https://issues.apache.org/jira/browse/HADOOP-3581) | *Major* | **Prevent memory intensive user tasks from taking down nodes**

Added the ability to kill process trees transgressing memory limits. TaskTracker uses the configuration parameters introduced in HADOOP-3759. In addition, mapred.tasktracker.taskmemorymanager.monitoring-interval specifies the interval for which TT waits between cycles of monitoring tasks' memory usage, and mapred.tasktracker.procfsbasedprocesstree.sleeptime-before-sigkill specifies the time TT waits for sending a SIGKILL to a process-tree that has overrun memory limits, after it has been sent a SIGTERM.


---

* [HADOOP-3970](https://issues.apache.org/jira/browse/HADOOP-3970) | *Major* | **Counters written to the job history cannot be recovered back**

Added getEscapedCompactString() and fromEscapedCompactString() to Counters.java to represent counters as Strings and to reconstruct the counters from the Strings.


---

* [HADOOP-3702](https://issues.apache.org/jira/browse/HADOOP-3702) | *Major* | **add support for chaining Maps in a single Map and after a Reduce [M\*/RM\*]**

Introduced ChainMapper and the ChainReducer classes to allow composing chains of Maps and Reduces in a single Map/Reduce job, something like MAP+ REDUCE MAP\*.


---

* [HADOOP-3445](https://issues.apache.org/jira/browse/HADOOP-3445) | *Major* | **Implementing core scheduler functionality in Resource Manager (V1) for Hadoop**

Introduced Capacity Task Scheduler.


---

* [HADOOP-3992](https://issues.apache.org/jira/browse/HADOOP-3992) | *Major* | **Synthetic Load Generator for NameNode testing**

Added a synthetic load generation facility to the test directory.


---

* [HADOOP-3981](https://issues.apache.org/jira/browse/HADOOP-3981) | *Major* | **Need a distributed file checksum algorithm for HDFS**

Implemented MD5-of-xxxMD5-of-yyyCRC32 which is a distributed file checksum algorithm for HDFS, where xxx is the number of CRCs per block and yyy is the number of bytes per CRC.

Changed DistCp to use file checksum for comparing files if both source and destination FileSystem(s) support getFileChecksum(...).


---

* [HADOOP-3245](https://issues.apache.org/jira/browse/HADOOP-3245) | *Major* | **Provide ability to persist running jobs (extend HADOOP-1876)**

Introduced recovery of jobs when JobTracker restarts. This facility is off by default. Introduced config parameters mapred.jobtracker.restart.recover, mapred.jobtracker.job.history.block.size, and mapred.jobtracker.job.history.buffer.size.


---

* [HADOOP-3911](https://issues.apache.org/jira/browse/HADOOP-3911) | *Minor* | **' -blocks ' option not being recognized**

Added a check to fsck options to make sure -files is not the first option so as to resolve conflicts with GenericOptionsParser.


---

* [HADOOP-4138](https://issues.apache.org/jira/browse/HADOOP-4138) | *Major* | **[Hive] refactor the SerDe library**

Introduced new SerDe library for src/contrib/hive.


---

* [HADOOP-3722](https://issues.apache.org/jira/browse/HADOOP-3722) | *Minor* | **Provide a unified way to pass jobconf options from bin/hadoop**

Changed streaming StreamJob and Submitter to implement Tool and Configurable, and to use GenericOptionsParser arguments  -fs, -jt, -conf, -D, -libjars, -files, and -archives. Deprecated -jobconf, -cacheArchive, -dfs, -cacheArchive, -additionalconfspec,  from streaming and pipes in favor of the generic options. Removed from streaming  -config, -mapred.job.tracker, and -cluster.


---

* [HADOOP-4117](https://issues.apache.org/jira/browse/HADOOP-4117) | *Major* | **Improve configurability of Hadoop EC2 instances**

Changed scripts to pass initialization script for EC2 instances at boot time (as EC2 user data) rather than embedding initialization information in the EC2 image. This change makes it easy to customize the hadoop-site.xml file for your cluster before launch, by editing the hadoop-ec2-init-remote.sh script, or by setting the environment variable USER\_DATA\_FILE in hadoop-ec2-env.sh to run a script of your choice.


---

* [HADOOP-2411](https://issues.apache.org/jira/browse/HADOOP-2411) | *Major* | **Add support for larger EC2 instance types**

Added support for c1.\* instance types and associated kernels for EC2.


---

* [HADOOP-3829](https://issues.apache.org/jira/browse/HADOOP-3829) | *Major* | **Narrown down skipped records based on user acceptable value**

Introduced new config parameter org.apache.hadoop.mapred.SkipBadRecords.setMapperMaxSkipRecords to set range of records to be skipped in the neighborhood of a failed record.


---

* [HADOOP-4084](https://issues.apache.org/jira/browse/HADOOP-4084) | *Major* | **Add explain plan capabilities to Hive QL**

Introduced "EXPLAIN" plan for Hive.


---

* [HADOOP-3930](https://issues.apache.org/jira/browse/HADOOP-3930) | *Major* | **Decide how to integrate scheduler info into CLI and job tracker web page**

Changed TaskScheduler to expose API for Web UI and Command Line Tool.


---

* [HADOOP-4106](https://issues.apache.org/jira/browse/HADOOP-4106) | *Major* | **add time, permission and user attribute support to fuse-dfs**

Added time, permission and user attribute support to libhdfs.


---

* [HADOOP-4176](https://issues.apache.org/jira/browse/HADOOP-4176) | *Major* | **Implement getFileChecksum(Path) in HftpFileSystem**

Implemented getFileChecksum(Path) in HftpFileSystemfor distcp support.


---

* [HADOOP-249](https://issues.apache.org/jira/browse/HADOOP-249) | *Major* | **Improving Map -\> Reduce performance and Task JVM reuse**

Enabled task JVMs to be reused via the job config mapred.job.reuse.jvm.num.tasks.


---

* [HADOOP-2816](https://issues.apache.org/jira/browse/HADOOP-2816) | *Major* | **Cluster summary at name node web has confusing report for space utilization**

Improved space reporting for NameNode Web UI. Applications that parse the Web UI output should be reviewed.


---

* [HADOOP-4227](https://issues.apache.org/jira/browse/HADOOP-4227) | *Minor* | **Remove the deprecated, unused class ShellCommand.**

Removed the deprecated class org.apache.hadoop.fs.ShellCommand.


---

* [HADOOP-3019](https://issues.apache.org/jira/browse/HADOOP-3019) | *Major* | **want input sampler & sorted partitioner**

Added a partitioner that effects a total order of output data, and an input sampler for generating the partition keyset for TotalOrderPartitioner for when the map's input keytype and distribution approximates its output.


---

* [HADOOP-3938](https://issues.apache.org/jira/browse/HADOOP-3938) | *Major* | **Quotas for disk space management**

Introducted byte space quotas for directories. The count shell command modified to report both name and byte quotas.


---

* [HADOOP-4205](https://issues.apache.org/jira/browse/HADOOP-4205) | *Major* | **[Hive] metastore and ql to use the refactored SerDe library**

Improved Hive metastore and ql to use the refactored SerDe library.


---

* [HADOOP-4116](https://issues.apache.org/jira/browse/HADOOP-4116) | *Blocker* | **Balancer should provide better resource management**

Changed DataNode protocol version without impact to clients other than to compel use of current version of client application.


---

* [HADOOP-4190](https://issues.apache.org/jira/browse/HADOOP-4190) | *Blocker* | **Changes to JobHistory makes it backward incompatible**

Changed job history format to add a dot at end of each line.


---

* [HADOOP-4293](https://issues.apache.org/jira/browse/HADOOP-4293) | *Major* | **Remove WritableJobConf**

Made Configuration Writable and rename the old write method to writeXml.


---

* [HADOOP-4281](https://issues.apache.org/jira/browse/HADOOP-4281) | *Blocker* | **Capacity reported in some of the commands is not consistent with the Web UI reported data**

Changed command "hadoop dfsadmin -report" to be consistent with Web UI for both Namenode and Datanode reports. "Total raw bytes" is changed to "Configured Capacity". "Present Capacity" is newly added to indicate the present capacity of the DFS. "Remaining raw bytes" is changed to "DFS Remaining". "Used raw bytes" is changed to "DFS Used". "% used" is changed to "DFS Used%". Applications that parse command output should be reviewed.


---

* [HADOOP-4018](https://issues.apache.org/jira/browse/HADOOP-4018) | *Major* | **limit memory usage in jobtracker**

Introduced new configuration parameter mapred.max.tasks.per.job to specifie the maximum number of tasks per job.


---

* [HADOOP-4430](https://issues.apache.org/jira/browse/HADOOP-4430) | *Blocker* | **Namenode Web UI capacity report is inconsistent with Balancer**

Changed reporting in the NameNode Web UI to more closely reflect the behavior of the re-balancer. Removed no longer used config parameter dfs.datanode.du.pct from hadoop-default.xml.


---

* [HADOOP-4086](https://issues.apache.org/jira/browse/HADOOP-4086) | *Major* | **Add limit to Hive QL**

Added LIMIT to Hive query language.


---

* [HADOOP-4466](https://issues.apache.org/jira/browse/HADOOP-4466) | *Blocker* | **SequenceFileOutputFormat is coupled to WritableComparable and Writable**

Ensure that SequenceFileOutputFormat isn't tied to Writables and can be used with other Serialization frameworks.


---

* [HADOOP-4433](https://issues.apache.org/jira/browse/HADOOP-4433) | *Major* | **Improve data loader for collecting metrics and log files from hadoop and system**

- Added startup and shutdown script
- Added torque metrics data loader
- Improve handling of Exec Plugin
- Added Test cases for File Tailing Adaptors
- Added Test cases for Start streaming at specific offset


---

* [HADOOP-1823](https://issues.apache.org/jira/browse/HADOOP-1823) | *Major* | **want InputFormat for bzip2 files**

bzip2 provided as codec in 0.19.0 https://issues.apache.org/jira/browse/HADOOP-3646



