
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

## Release 0.19.0 - 2008-11-20

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4430](https://issues.apache.org/jira/browse/HADOOP-4430) | Namenode Web UI capacity report is inconsistent with Balancer |  Blocker | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-4293](https://issues.apache.org/jira/browse/HADOOP-4293) | Remove WritableJobConf |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-4281](https://issues.apache.org/jira/browse/HADOOP-4281) | Capacity reported in some of the commands is not consistent with the Web UI reported data |  Blocker | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-4227](https://issues.apache.org/jira/browse/HADOOP-4227) | Remove the deprecated, unused class ShellCommand. |  Minor | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4190](https://issues.apache.org/jira/browse/HADOOP-4190) | Changes to JobHistory makes it backward incompatible |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-4116](https://issues.apache.org/jira/browse/HADOOP-4116) | Balancer should provide better resource management |  Blocker | . | Raghu Angadi | Hairong Kuang |
| [HADOOP-3981](https://issues.apache.org/jira/browse/HADOOP-3981) | Need a distributed file checksum algorithm for HDFS |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3963](https://issues.apache.org/jira/browse/HADOOP-3963) | libhdfs should never exit on its own but rather return errors to the calling application |  Minor | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3938](https://issues.apache.org/jira/browse/HADOOP-3938) | Quotas for disk space management |  Major | . | Robert Chansler | Raghu Angadi |
| [HADOOP-3911](https://issues.apache.org/jira/browse/HADOOP-3911) | ' -blocks ' option not being recognized |  Minor | fs, util | Koji Noguchi | Lohit Vijayarenu |
| [HADOOP-3889](https://issues.apache.org/jira/browse/HADOOP-3889) | distcp: Better Error Message should be thrown when accessing source files/directory with no read permission |  Minor | . | Peeyush Bishnoi | Tsz Wo Nicholas Sze |
| [HADOOP-3837](https://issues.apache.org/jira/browse/HADOOP-3837) | hadop streaming does not use progress reporting to detect hung tasks |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3796](https://issues.apache.org/jira/browse/HADOOP-3796) | fuse-dfs should take rw,ro,trashon,trashoff,protected=blah mount arguments rather than them being compiled in |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3792](https://issues.apache.org/jira/browse/HADOOP-3792) | exit code from "hadoop dfs -test ..." is wrong for Unix shell |  Minor | fs | Ben Slusky | Ben Slusky |
| [HADOOP-3722](https://issues.apache.org/jira/browse/HADOOP-3722) | Provide a unified way to pass jobconf options from bin/hadoop |  Minor | conf | Matei Zaharia | Enis Soztutar |
| [HADOOP-3667](https://issues.apache.org/jira/browse/HADOOP-3667) | Remove deprecated methods in JobConf |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3652](https://issues.apache.org/jira/browse/HADOOP-3652) | Remove deprecated class OutputFormatBase |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3595](https://issues.apache.org/jira/browse/HADOOP-3595) | Remove deprecated mapred.combine.once functionality |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3245](https://issues.apache.org/jira/browse/HADOOP-3245) | Provide ability to persist running jobs (extend HADOOP-1876) |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-3150](https://issues.apache.org/jira/browse/HADOOP-3150) | Move task file promotion into the task |  Major | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-3062](https://issues.apache.org/jira/browse/HADOOP-3062) | Need to capture the metrics for the network ios generate by dfs reads/writes and map/reduce shuffling  and break them down by racks |  Major | metrics | Runping Qi | Chris Douglas |
| [HADOOP-2816](https://issues.apache.org/jira/browse/HADOOP-2816) | Cluster summary at name node web has confusing report for space utilization |  Major | . | Robert Chansler | Suresh Srinivas |
| [HADOOP-2325](https://issues.apache.org/jira/browse/HADOOP-2325) | Require Java 6 |  Major | build | Doug Cutting | Doug Cutting |
| [HADOOP-1869](https://issues.apache.org/jira/browse/HADOOP-1869) | access times of HDFS files |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1700](https://issues.apache.org/jira/browse/HADOOP-1700) | Append to files in HDFS |  Major | . | stack | dhruba borthakur |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4406](https://issues.apache.org/jira/browse/HADOOP-4406) | Make TCTLSeparatedProtocol configurable and have DynamicSerDe initialize, initialize the SerDe |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4301](https://issues.apache.org/jira/browse/HADOOP-4301) | Forrest doc for skip bad records feature |  Blocker | documentation | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-4260](https://issues.apache.org/jira/browse/HADOOP-4260) | support show partitions in hive |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4176](https://issues.apache.org/jira/browse/HADOOP-4176) | Implement getFileChecksum(Path) in HftpFileSystem |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4120](https://issues.apache.org/jira/browse/HADOOP-4120) | [Hive] print time taken by query in interactive shell |  Minor | . | Raghotham Murthy | Raghotham Murthy |
| [HADOOP-4106](https://issues.apache.org/jira/browse/HADOOP-4106) | add time, permission and user attribute support to fuse-dfs |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4104](https://issues.apache.org/jira/browse/HADOOP-4104) | add time, permission and user attribute support to libhdfs |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4095](https://issues.apache.org/jira/browse/HADOOP-4095) | [Hive] enhance describe table & partition |  Major | . | Prasad Chakka | Namit Jain |
| [HADOOP-4086](https://issues.apache.org/jira/browse/HADOOP-4086) | Add limit to Hive QL |  Major | . | Ashish Thusoo | Namit Jain |
| [HADOOP-4084](https://issues.apache.org/jira/browse/HADOOP-4084) | Add explain plan capabilities to Hive QL |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4070](https://issues.apache.org/jira/browse/HADOOP-4070) | [Hive] Provide a mechanism for registering UDFs from the query language |  Major | . | Tom White | Tom White |
| [HADOOP-3992](https://issues.apache.org/jira/browse/HADOOP-3992) | Synthetic Load Generator for NameNode testing |  Major | . | Robert Chansler | Hairong Kuang |
| [HADOOP-3941](https://issues.apache.org/jira/browse/HADOOP-3941) | Extend FileSystem API to return file-checksums/file-digests |  Major | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3939](https://issues.apache.org/jira/browse/HADOOP-3939) | DistCp should support an option for deleting non-existing files. |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3924](https://issues.apache.org/jira/browse/HADOOP-3924) | Add a 'Killed' job status |  Critical | . | Alejandro Abdelnur | Subru Krishnan |
| [HADOOP-3873](https://issues.apache.org/jira/browse/HADOOP-3873) | DistCp should have an option for limiting the number of files/bytes being copied |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3854](https://issues.apache.org/jira/browse/HADOOP-3854) | org.apache.hadoop.http.HttpServer should support user configurable filter |  Major | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3829](https://issues.apache.org/jira/browse/HADOOP-3829) | Narrown down skipped records based on user acceptable value |  Major | . | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-3828](https://issues.apache.org/jira/browse/HADOOP-3828) | Write skipped records' bytes to DFS |  Major | . | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-3754](https://issues.apache.org/jira/browse/HADOOP-3754) | Support a Thrift Interface to access files/directories in HDFS |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3746](https://issues.apache.org/jira/browse/HADOOP-3746) | A fair sharing job scheduler |  Minor | . | Matei Zaharia | Matei Zaharia |
| [HADOOP-3730](https://issues.apache.org/jira/browse/HADOOP-3730) | add new JobConf constructor that disables loading default configurations |  Major | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3714](https://issues.apache.org/jira/browse/HADOOP-3714) | Bash tab completion support |  Trivial | scripts | Chris Smith | Chris Smith |
| [HADOOP-3702](https://issues.apache.org/jira/browse/HADOOP-3702) | add support for chaining Maps in a single Map and after a Reduce [M\*/RM\*] |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3698](https://issues.apache.org/jira/browse/HADOOP-3698) | Implement access control for submitting jobs to queues in the JobTracker |  Major | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3695](https://issues.apache.org/jira/browse/HADOOP-3695) | [HOD] Have an ability to run multiple slaves per node |  Major | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3585](https://issues.apache.org/jira/browse/HADOOP-3585) | Hardware Failure Monitoring in large clusters running Hadoop/HDFS |  Minor | metrics | Ioannis Koltsidas | Ioannis Koltsidas |
| [HADOOP-3485](https://issues.apache.org/jira/browse/HADOOP-3485) | fix writes |  Minor | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3479](https://issues.apache.org/jira/browse/HADOOP-3479) | Implement configuration items useful for Hadoop resource manager (v1) |  Major | conf | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3445](https://issues.apache.org/jira/browse/HADOOP-3445) | Implementing core scheduler functionality in Resource Manager (V1) for Hadoop |  Major | . | Vivek Ratan | Vivek Ratan |
| [HADOOP-3402](https://issues.apache.org/jira/browse/HADOOP-3402) | Add example code to support run terasort on hadoop |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-3361](https://issues.apache.org/jira/browse/HADOOP-3361) | Implement renames for NativeS3FileSystem |  Major | fs/s3 | Tom White | Tom White |
| [HADOOP-3149](https://issues.apache.org/jira/browse/HADOOP-3149) | supporting multiple outputs for M/R jobs |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3019](https://issues.apache.org/jira/browse/HADOOP-3019) | want input sampler & sorted partitioner |  Major | . | Doug Cutting | Chris Douglas |
| [HADOOP-2664](https://issues.apache.org/jira/browse/HADOOP-2664) | lzop-compatible CompresionCodec |  Major | io | Chris Douglas | Chris Douglas |
| [HADOOP-2658](https://issues.apache.org/jira/browse/HADOOP-2658) | Design and Implement a Test Plan to support appends to HDFS files |  Blocker | test | dhruba borthakur | dhruba borthakur |
| [HADOOP-2536](https://issues.apache.org/jira/browse/HADOOP-2536) | MapReduce for MySQL |  Minor | . | Fredrik Hedberg | Fredrik Hedberg |
| [HADOOP-1823](https://issues.apache.org/jira/browse/HADOOP-1823) | want InputFormat for bzip2 files |  Major | . | Doug Cutting |  |
| [HADOOP-1480](https://issues.apache.org/jira/browse/HADOOP-1480) | pipes should be able to set user counters |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-372](https://issues.apache.org/jira/browse/HADOOP-372) | should allow to specify different inputformat classes for different input dirs for Map/Reduce jobs |  Major | . | Runping Qi | Chris Smith |
| [HADOOP-153](https://issues.apache.org/jira/browse/HADOOP-153) | skip records that fail Task |  Major | . | Doug Cutting | Sharad Agarwal |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4433](https://issues.apache.org/jira/browse/HADOOP-4433) | Improve data loader for collecting metrics and log files from hadoop and system |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-4431](https://issues.apache.org/jira/browse/HADOOP-4431) | Add versionning/tags to Chukwa Chunk |  Major | . | Jerome Boulon | Jerome Boulon |
| [HADOOP-4424](https://issues.apache.org/jira/browse/HADOOP-4424) | menu layout change for Hadoop documentation |  Blocker | documentation | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-4353](https://issues.apache.org/jira/browse/HADOOP-4353) | enable multi-line query from Hive CLI |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4345](https://issues.apache.org/jira/browse/HADOOP-4345) | Hive: Check that partitioning predicate is present when hive.partition.pruning = strict |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4307](https://issues.apache.org/jira/browse/HADOOP-4307) | add an option to  describe table to show extended properties of the table such as serialization/deserialization properties |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4279](https://issues.apache.org/jira/browse/HADOOP-4279) | write the random number generator seed to log in the append-related tests |  Blocker | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4252](https://issues.apache.org/jira/browse/HADOOP-4252) | Catch Ctrl-C in Hive CLI so that corresponding hadoop jobs can be killed |  Minor | . | Prasad Chakka | Pete Wyckoff |
| [HADOOP-4231](https://issues.apache.org/jira/browse/HADOOP-4231) | Hive: converting complex objects to JSON failed. |  Minor | . | Zheng Shao | Zheng Shao |
| [HADOOP-4230](https://issues.apache.org/jira/browse/HADOOP-4230) | Hive: GroupBy should not pass the whole row from mapper to reducer |  Blocker | . | Zheng Shao | Ashish Thusoo |
| [HADOOP-4205](https://issues.apache.org/jira/browse/HADOOP-4205) | [Hive] metastore and ql to use the refactored SerDe library |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-4194](https://issues.apache.org/jira/browse/HADOOP-4194) | Add JobConf and JobID to job related methods in JobTrackerInstrumentation |  Major | . | Mac Yang | Mac Yang |
| [HADOOP-4181](https://issues.apache.org/jira/browse/HADOOP-4181) | some minor things to make Hadoop friendlier to git |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-4174](https://issues.apache.org/jira/browse/HADOOP-4174) | Move non-client methods ou of ClientProtocol |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4151](https://issues.apache.org/jira/browse/HADOOP-4151) | Add a memcmp-compatible interface for key types |  Minor | . | Chris Douglas | Chris Douglas |
| [HADOOP-4138](https://issues.apache.org/jira/browse/HADOOP-4138) | [Hive] refactor the SerDe library |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-4124](https://issues.apache.org/jira/browse/HADOOP-4124) | Changing priority of a job should be available in CLI and available on the web UI only along with the Kill Job actions |  Major | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4117](https://issues.apache.org/jira/browse/HADOOP-4117) | Improve configurability of Hadoop EC2 instances |  Major | contrib/cloud | Tom White | Tom White |
| [HADOOP-4113](https://issues.apache.org/jira/browse/HADOOP-4113) |  libhdfs should never exit on its own but rather return errors to the calling application - missing diff files |  Minor | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4094](https://issues.apache.org/jira/browse/HADOOP-4094) | [Hive]implement hive-site.xml similar to hadoop-site.xml |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4090](https://issues.apache.org/jira/browse/HADOOP-4090) | The configuration file lists two paths to hadoop directories (bin and conf).  Startup should check that these are valid directories and give appropriate messages. |  Minor | . | Ashish Thusoo | Raghotham Murthy |
| [HADOOP-4083](https://issues.apache.org/jira/browse/HADOOP-4083) | change new config attribute queue.name to mapred.job.queue.name |  Major | . | Owen O'Malley | Hemanth Yamijala |
| [HADOOP-4075](https://issues.apache.org/jira/browse/HADOOP-4075) | test-patch.sh should output the ant commands that it runs |  Major | build | Nigel Daley | Ramya Sunil |
| [HADOOP-4062](https://issues.apache.org/jira/browse/HADOOP-4062) | IPC client does not need to be synchronized on the output stream when a connection is closed |  Major | ipc | Hairong Kuang | Hairong Kuang |
| [HADOOP-4053](https://issues.apache.org/jira/browse/HADOOP-4053) | Schedulers need to know when a job has completed |  Blocker | . | Vivek Ratan | Amar Kamat |
| [HADOOP-3975](https://issues.apache.org/jira/browse/HADOOP-3975) | test-patch can report the modifications found in the workspace along with the error message |  Minor | test | Hemanth Yamijala | Ramya Sunil |
| [HADOOP-3965](https://issues.apache.org/jira/browse/HADOOP-3965) | Make DataBlockScanner package private |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3948](https://issues.apache.org/jira/browse/HADOOP-3948) | Separate Namenodes edits and fsimage |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3944](https://issues.apache.org/jira/browse/HADOOP-3944) | TupleWritable listed as public class but cannot be used without methods private to the package |  Trivial | documentation | Michael Andrews | Chris Douglas |
| [HADOOP-3943](https://issues.apache.org/jira/browse/HADOOP-3943) | NetworkTopology.pseudoSortByDistance does not need to be a synchronized method |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3935](https://issues.apache.org/jira/browse/HADOOP-3935) | Extract classes from DataNode.java |  Trivial | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-3930](https://issues.apache.org/jira/browse/HADOOP-3930) | Decide how to integrate scheduler info into CLI and job tracker web page |  Major | . | Matei Zaharia | Sreekanth Ramakrishnan |
| [HADOOP-3908](https://issues.apache.org/jira/browse/HADOOP-3908) | Better error message if llibhdfs.so doesn't exist |  Minor | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3905](https://issues.apache.org/jira/browse/HADOOP-3905) | Create a generic interface for edits log. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3892](https://issues.apache.org/jira/browse/HADOOP-3892) | Include Unix group name in JobConf |  Trivial | conf | Matei Zaharia | Matei Zaharia |
| [HADOOP-3866](https://issues.apache.org/jira/browse/HADOOP-3866) | Improve Hadoop Jobtracker Admin |  Major | scripts | craig weisenfluh | craig weisenfluh |
| [HADOOP-3861](https://issues.apache.org/jira/browse/HADOOP-3861) | Make MapFile.Reader and Writer implement java.io.Closeable |  Major | io | Tom White | Tom White |
| [HADOOP-3860](https://issues.apache.org/jira/browse/HADOOP-3860) | Compare name-node performance when journaling is performed into local hard-drives or nfs. |  Major | benchmarks | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3853](https://issues.apache.org/jira/browse/HADOOP-3853) | Move multiple input format extension to library package |  Major | . | Tom White | Tom White |
| [HADOOP-3852](https://issues.apache.org/jira/browse/HADOOP-3852) | If ShellCommandExecutor had a toString() operator that listed the command run, its error messages may be more meaningful |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-3844](https://issues.apache.org/jira/browse/HADOOP-3844) | include message of local exception in Client call failures |  Minor | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-3805](https://issues.apache.org/jira/browse/HADOOP-3805) | improve fuse-dfs write performance which is 33% slower than hadoop dfs -copyFromLocal |  Minor | . | Pete Wyckoff |  |
| [HADOOP-3780](https://issues.apache.org/jira/browse/HADOOP-3780) | JobTracker should synchronously resolve the tasktracker's network location when the tracker registers |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-3769](https://issues.apache.org/jira/browse/HADOOP-3769) | expose static SampleMapper and SampleReducer classes of GenericMRLoadGenerator class for gridmix reuse |  Major | test | Lingyun Yang | Lingyun Yang |
| [HADOOP-3759](https://issues.apache.org/jira/browse/HADOOP-3759) | Provide ability to run memory intensive jobs without affecting other running tasks on the nodes |  Major | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3747](https://issues.apache.org/jira/browse/HADOOP-3747) | Add counter support to MultipleOutputs |  Minor | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-3721](https://issues.apache.org/jira/browse/HADOOP-3721) | CompositeRecordReader::next is unnecessarily complex |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3719](https://issues.apache.org/jira/browse/HADOOP-3719) | Chukwa |  Major | . | Ari Rabkin | Ari Rabkin |
| [HADOOP-3694](https://issues.apache.org/jira/browse/HADOOP-3694) | if MiniDFS startup time could be improved, testing time would be reduced |  Major | test | Steve Loughran | Doug Cutting |
| [HADOOP-3684](https://issues.apache.org/jira/browse/HADOOP-3684) | The data\_join should allow the user to implement a customer cloning function |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-3664](https://issues.apache.org/jira/browse/HADOOP-3664) | Remove deprecated methods introduced in changes to validating input paths (HADOOP-3095) |  Major | . | Tom White | Tom White |
| [HADOOP-3661](https://issues.apache.org/jira/browse/HADOOP-3661) | Normalize fuse-dfs handling of moving things to trash wrt the way hadoop dfs does it (only when non posix trash flag is enabled in compile) |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3660](https://issues.apache.org/jira/browse/HADOOP-3660) | Add replication factor for injecting blocks in the data node cluster |  Major | benchmarks | Sanjay Radia | Sanjay Radia |
| [HADOOP-3655](https://issues.apache.org/jira/browse/HADOOP-3655) | provide more control options for the junit run |  Minor | build | Steve Loughran | Steve Loughran |
| [HADOOP-3646](https://issues.apache.org/jira/browse/HADOOP-3646) | Providing bzip2 as codec |  Major | conf, io | Abdul Qadeer | Abdul Qadeer |
| [HADOOP-3638](https://issues.apache.org/jira/browse/HADOOP-3638) | Cache the iFile index files in memory to reduce seeks during map output serving |  Major | . | Devaraj Das | Jothi Padmanabhan |
| [HADOOP-3624](https://issues.apache.org/jira/browse/HADOOP-3624) | CreateEditsLog could be improved to create tree directory structure |  Minor | test | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3620](https://issues.apache.org/jira/browse/HADOOP-3620) | Namenode should synchronously resolve a datanode's network location when the datanode registers |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-3617](https://issues.apache.org/jira/browse/HADOOP-3617) | Writes from map serialization include redundant checks for accounting space |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3605](https://issues.apache.org/jira/browse/HADOOP-3605) | Added an abort on unset AWS\_ACCOUNT\_ID to luanch-hadoop-master |  Minor | contrib/cloud | Al Hoang | Al Hoang |
| [HADOOP-3581](https://issues.apache.org/jira/browse/HADOOP-3581) | Prevent memory intensive user tasks from taking down nodes |  Major | . | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3577](https://issues.apache.org/jira/browse/HADOOP-3577) | Tools to inject blocks into name node and simulated data nodes for testing |  Minor | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-3563](https://issues.apache.org/jira/browse/HADOOP-3563) | Seperate out datanode and namenode functionality of generation stamp upgrade process |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3556](https://issues.apache.org/jira/browse/HADOOP-3556) | Substitute the synchronized code in MD5Hash to avoid lock contention. Use ThreadLocal instead. |  Major | io | Iván de Prado | Iván de Prado |
| [HADOOP-3549](https://issues.apache.org/jira/browse/HADOOP-3549) | meaningful errno values in libhdfs |  Major | . | Ben Slusky | Ben Slusky |
| [HADOOP-3514](https://issues.apache.org/jira/browse/HADOOP-3514) | Reduce seeks during shuffle, by inline crcs |  Major | . | Devaraj Das | Jothi Padmanabhan |
| [HADOOP-3498](https://issues.apache.org/jira/browse/HADOOP-3498) | File globbing alternation should be able to span path components |  Major | fs | Tom White | Tom White |
| [HADOOP-3478](https://issues.apache.org/jira/browse/HADOOP-3478) | The algorithm to decide map re-execution on fetch failures can be improved |  Major | . | Jothi Padmanabhan | Jothi Padmanabhan |
| [HADOOP-3446](https://issues.apache.org/jira/browse/HADOOP-3446) | The reduce task should not flush the in memory file system before starting the reducer |  Critical | . | Owen O'Malley | Chris Douglas |
| [HADOOP-3412](https://issues.apache.org/jira/browse/HADOOP-3412) | Refactor the scheduler out of the JobTracker |  Minor | . | Brice Arnould | Brice Arnould |
| [HADOOP-3368](https://issues.apache.org/jira/browse/HADOOP-3368) | Can commons-logging.properties be pulled from hadoop-core? |  Major | build | Steve Loughran | Steve Loughran |
| [HADOOP-3342](https://issues.apache.org/jira/browse/HADOOP-3342) | Better safety of killing jobs via web interface |  Minor | . | Daniel Naber | Enis Soztutar |
| [HADOOP-3341](https://issues.apache.org/jira/browse/HADOOP-3341) | make key-value separators in hadoop streaming fully configurable |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-3328](https://issues.apache.org/jira/browse/HADOOP-3328) | DFS write pipeline : only the last datanode needs to verify checksum |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3202](https://issues.apache.org/jira/browse/HADOOP-3202) | Deprecate org.apache.hadoop.fs.FileUtil.fullyDelete(FileSystem fs, Path dir) |  Major | fs | Tsz Wo Nicholas Sze | Amareshwari Sriramadasu |
| [HADOOP-3169](https://issues.apache.org/jira/browse/HADOOP-3169) | LeaseChecker daemon should not be started in DFSClient constructor |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-2411](https://issues.apache.org/jira/browse/HADOOP-2411) | Add support for larger EC2 instance types |  Major | contrib/cloud | Tom White | Chris K Wensel |
| [HADOOP-2330](https://issues.apache.org/jira/browse/HADOOP-2330) | Preallocate transaction log to improve namenode transaction logging performance |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2302](https://issues.apache.org/jira/browse/HADOOP-2302) |  Streaming should provide an option for numerical sort of keys |  Major | . | Lohit Vijayarenu | Devaraj Das |
| [HADOOP-2165](https://issues.apache.org/jira/browse/HADOOP-2165) | Augment JobHistory to store tasks' userlogs |  Major | . | Arun C Murthy | Vinod Kumar Vavilapalli |
| [HADOOP-2130](https://issues.apache.org/jira/browse/HADOOP-2130) | Pipes submit job should be Non-blocking |  Critical | . | Srikanth Kakani | Arun C Murthy |
| [HADOOP-1627](https://issues.apache.org/jira/browse/HADOOP-1627) | DFSAdmin incorrectly reports cluster data. |  Minor | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-657](https://issues.apache.org/jira/browse/HADOOP-657) | Free temporary space should be modelled better |  Major | . | Owen O'Malley | Ari Rabkin |
| [HADOOP-249](https://issues.apache.org/jira/browse/HADOOP-249) | Improving Map -\> Reduce performance and Task JVM reuse |  Major | . | Benjamin Reed | Devaraj Das |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4595](https://issues.apache.org/jira/browse/HADOOP-4595) | JVM Reuse triggers RuntimeException("Invalid state") |  Major | . | Aaron Kimball | Devaraj Das |
| [HADOOP-4552](https://issues.apache.org/jira/browse/HADOOP-4552) | Deadlock in RPC Server |  Major | ipc | Raghu Angadi | Raghu Angadi |
| [HADOOP-4525](https://issues.apache.org/jira/browse/HADOOP-4525) | config ipc.server.tcpnodelay is no loger being respected |  Major | ipc | Clint Morgan | Clint Morgan |
| [HADOOP-4510](https://issues.apache.org/jira/browse/HADOOP-4510) | FileOutputFormat protects getTaskOutputPath |  Blocker | . | Chris K Wensel | Chris K Wensel |
| [HADOOP-4500](https://issues.apache.org/jira/browse/HADOOP-4500) | multifilesplit is using job default filesystem incorrectly |  Major | . | Joydeep Sen Sarma | Joydeep Sen Sarma |
| [HADOOP-4498](https://issues.apache.org/jira/browse/HADOOP-4498) | JobHistory does not escape literal jobName when used in a regex pattern |  Blocker | . | Chris K Wensel | Chris K Wensel |
| [HADOOP-4471](https://issues.apache.org/jira/browse/HADOOP-4471) | Capacity Scheduler should maintain the right ordering of jobs in its running queue |  Blocker | . | Vivek Ratan | Amar Kamat |
| [HADOOP-4466](https://issues.apache.org/jira/browse/HADOOP-4466) | SequenceFileOutputFormat is coupled to WritableComparable and Writable |  Blocker | io | Chris K Wensel | Chris K Wensel |
| [HADOOP-4457](https://issues.apache.org/jira/browse/HADOOP-4457) | Input split logging in history is broken in 0.19 |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4455](https://issues.apache.org/jira/browse/HADOOP-4455) | Upload the derby.jar and TestSeDe.jar needed for fixes to 0.19 bugs |  Blocker | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4449](https://issues.apache.org/jira/browse/HADOOP-4449) | Minor formatting changes to quota related commands |  Trivial | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-4446](https://issues.apache.org/jira/browse/HADOOP-4446) | Update  Scheduling Information display in Web UI |  Major | . | Karam Singh | Sreekanth Ramakrishnan |
| [HADOOP-4439](https://issues.apache.org/jira/browse/HADOOP-4439) | Cleanup memory related resource management |  Blocker | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4438](https://issues.apache.org/jira/browse/HADOOP-4438) | Add new/missing dfs commands in forrest |  Blocker | documentation | Hemanth Yamijala | Suresh Srinivas |
| [HADOOP-4427](https://issues.apache.org/jira/browse/HADOOP-4427) | Add new/missing commands in forrest |  Blocker | documentation | Sharad Agarwal | Sreekanth Ramakrishnan |
| [HADOOP-4425](https://issues.apache.org/jira/browse/HADOOP-4425) | Edits log takes much longer to load |  Blocker | . | Chris Douglas | Chris Douglas |
| [HADOOP-4423](https://issues.apache.org/jira/browse/HADOOP-4423) | FSDataset.getStoredBlock(id) should not return corrupted information |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4418](https://issues.apache.org/jira/browse/HADOOP-4418) | Update documentation in forrest for Mapred, streaming and pipes |  Blocker | documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4410](https://issues.apache.org/jira/browse/HADOOP-4410) | TestMiniMRDebugScript fails on trunk |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4405](https://issues.apache.org/jira/browse/HADOOP-4405) | all creation of hadoop dfs queries from with in hive shell |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4404](https://issues.apache.org/jira/browse/HADOOP-4404) | saveFSImage() should remove files from a storage directory that do not correspond to its type. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4400](https://issues.apache.org/jira/browse/HADOOP-4400) | Add "hdfs://" to fs.default.name on quickstart.html |  Trivial | documentation | Jeff Hammerbacher | Jeff Hammerbacher |
| [HADOOP-4393](https://issues.apache.org/jira/browse/HADOOP-4393) | Merge AccessControlException and AccessControlIOException into one exception class |  Blocker | fs | Owen O'Malley | Owen O'Malley |
| [HADOOP-4387](https://issues.apache.org/jira/browse/HADOOP-4387) | TestHDFSFileSystemContract fails on windows |  Blocker | test | Raghu Angadi | Raghu Angadi |
| [HADOOP-4380](https://issues.apache.org/jira/browse/HADOOP-4380) | Make new classes in mapred package private instead of public |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-4378](https://issues.apache.org/jira/browse/HADOOP-4378) | TestJobQueueInformation fails regularly |  Blocker | test | Tsz Wo Nicholas Sze | Sreekanth Ramakrishnan |
| [HADOOP-4376](https://issues.apache.org/jira/browse/HADOOP-4376) | Fix line formatting in hadoop-default.xml for hadoop.http.filter.initializers |  Blocker | conf | Enis Soztutar | Enis Soztutar |
| [HADOOP-4373](https://issues.apache.org/jira/browse/HADOOP-4373) | Guaranteed Capacity calculation is not calculated correctly |  Blocker | . | Karam Singh | Hemanth Yamijala |
| [HADOOP-4367](https://issues.apache.org/jira/browse/HADOOP-4367) | Hive: UDAF functions cannot handle NULL values |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-4366](https://issues.apache.org/jira/browse/HADOOP-4366) | Provide way to replace existing column names for columnSet tables |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4361](https://issues.apache.org/jira/browse/HADOOP-4361) | Corner cases in killJob from command line |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4358](https://issues.apache.org/jira/browse/HADOOP-4358) | NPE from CreateEditsLog |  Blocker | test | Chris Douglas | Raghu Angadi |
| [HADOOP-4356](https://issues.apache.org/jira/browse/HADOOP-4356) | [Hive] for a 2-stage map-reduce job, number of reducers not set correctly |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4355](https://issues.apache.org/jira/browse/HADOOP-4355) | hive 2 case sensitivity issues |  Major | . | Zheng Shao |  |
| [HADOOP-4344](https://issues.apache.org/jira/browse/HADOOP-4344) | Hive: Partition pruning causes semantic exception with joins |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4342](https://issues.apache.org/jira/browse/HADOOP-4342) | [hive] bug in partition pruning |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4336](https://issues.apache.org/jira/browse/HADOOP-4336) | fix sampling bug in fractional bucket case |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4335](https://issues.apache.org/jira/browse/HADOOP-4335) | FsShell -ls fails for file systems without owners or groups |  Major | scripts | David Phillips | David Phillips |
| [HADOOP-4333](https://issues.apache.org/jira/browse/HADOOP-4333) | add ability to drop partitions through DDL |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4330](https://issues.apache.org/jira/browse/HADOOP-4330) | Hive: AS clause with subqueries having group bys is not propogated to the outer query block |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4329](https://issues.apache.org/jira/browse/HADOOP-4329) | Hive: [] operator with maps does not work |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4327](https://issues.apache.org/jira/browse/HADOOP-4327) | Create table hive does not set delimeters |  Major | . | Edward Capriolo | Namit Jain |
| [HADOOP-4321](https://issues.apache.org/jira/browse/HADOOP-4321) | Document the capacity scheduler in Forrest |  Blocker | documentation | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4320](https://issues.apache.org/jira/browse/HADOOP-4320) | [Hive] TCTLSeparatedProtocol implement maps/lists/sets read/writes |  Major | . | Pete Wyckoff |  |
| [HADOOP-4319](https://issues.apache.org/jira/browse/HADOOP-4319) | fuse-dfs dfs\_read function may return less than the requested #of bytes even if EOF not reached |  Blocker | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4316](https://issues.apache.org/jira/browse/HADOOP-4316) | [Hive] extra new lines at output |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4315](https://issues.apache.org/jira/browse/HADOOP-4315) | Hive: Cleanup temporary files once the job is done |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4309](https://issues.apache.org/jira/browse/HADOOP-4309) | eclipse-plugin no longer compiles on trunk |  Blocker | contrib/eclipse-plugin | Chris Douglas | Chris Douglas |
| [HADOOP-4303](https://issues.apache.org/jira/browse/HADOOP-4303) | Hive: trim and rtrim UDFs behaviors are reversed |  Major | . | Ashish Thusoo | Ashish Thusoo |
| [HADOOP-4302](https://issues.apache.org/jira/browse/HADOOP-4302) | TestReduceFetch fails intermittently |  Blocker | . | Devaraj Das | Chris Douglas |
| [HADOOP-4299](https://issues.apache.org/jira/browse/HADOOP-4299) | Unable to access a file by a different user in the same group when permissions is set to 770 or when permissions is turned OFF |  Blocker | . | Ramya Sunil | Hairong Kuang |
| [HADOOP-4296](https://issues.apache.org/jira/browse/HADOOP-4296) | Spasm of JobClient failures on successful jobs every once in a while |  Blocker | . | Joydeep Sen Sarma | dhruba borthakur |
| [HADOOP-4294](https://issues.apache.org/jira/browse/HADOOP-4294) | Hive: Parser should pass field schema to SerDe |  Major | . | Zheng Shao |  |
| [HADOOP-4288](https://issues.apache.org/jira/browse/HADOOP-4288) | java.lang.NullPointerException is observed in Jobtracker log while   call heartbeat |  Blocker | . | Karam Singh | Amar Kamat |
| [HADOOP-4287](https://issues.apache.org/jira/browse/HADOOP-4287) | [mapred] jobqueue\_details.jsp shows negative count of running and waiting reduces with CapacityTaskScheduler. |  Blocker | . | Vinod Kumar Vavilapalli | Sreekanth Ramakrishnan |
| [HADOOP-4282](https://issues.apache.org/jira/browse/HADOOP-4282) | User configurable filter fails to filter accesses to certain directories |  Blocker | . | Kan Zhang | Tsz Wo Nicholas Sze |
| [HADOOP-4280](https://issues.apache.org/jira/browse/HADOOP-4280) | test-libhdfs consistently fails on trunk |  Blocker | . | Raghu Angadi | Pete Wyckoff |
| [HADOOP-4278](https://issues.apache.org/jira/browse/HADOOP-4278) | TestDatanodeDeath failed occasionally |  Blocker | . | Tsz Wo Nicholas Sze | dhruba borthakur |
| [HADOOP-4275](https://issues.apache.org/jira/browse/HADOOP-4275) | New public methods added to the \*ID classes |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-4274](https://issues.apache.org/jira/browse/HADOOP-4274) | Capacity scheduler's implementation of getJobs modifies the list of running jobs inadvertently |  Blocker | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4273](https://issues.apache.org/jira/browse/HADOOP-4273) | [Hive] job submission exception if input is null |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4272](https://issues.apache.org/jira/browse/HADOOP-4272) | Hive: metadataTypedColumnsetSerDe should check if SERIALIZATION.LIB is old columnsetSerDe |  Major | . | Zheng Shao | Prasad Chakka |
| [HADOOP-4269](https://issues.apache.org/jira/browse/HADOOP-4269) | LineRecordReader.LineReader should use util.LineReader |  Major | util | Chris Douglas | Chris Douglas |
| [HADOOP-4267](https://issues.apache.org/jira/browse/HADOOP-4267) | TestDBJob failed on Linux |  Blocker | . | Raghu Angadi | Enis Soztutar |
| [HADOOP-4266](https://issues.apache.org/jira/browse/HADOOP-4266) | Hive: Support "IS NULL", "IS NOT NULL", and size(x) for map and list |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-4265](https://issues.apache.org/jira/browse/HADOOP-4265) | [Hive] error when user specifies the delimiter |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4261](https://issues.apache.org/jira/browse/HADOOP-4261) | Jobs failing in the init stage will never cleanup |  Blocker | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-4256](https://issues.apache.org/jira/browse/HADOOP-4256) | Remove Completed and Failed Job tables from jobqueue\_details.jsp |  Blocker | . | Sreekanth Ramakrishnan | Sreekanth Ramakrishnan |
| [HADOOP-4254](https://issues.apache.org/jira/browse/HADOOP-4254) | Cannot setSpaceQuota to 1TB |  Blocker | . | Tsz Wo Nicholas Sze | Raghu Angadi |
| [HADOOP-4250](https://issues.apache.org/jira/browse/HADOOP-4250) | Remove short names of serdes from Deserializer, Serializer & SerDe interface and relevant code. |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4249](https://issues.apache.org/jira/browse/HADOOP-4249) | Declare hsqldb.jar in eclipse plugin |  Blocker | contrib/eclipse-plugin | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4248](https://issues.apache.org/jira/browse/HADOOP-4248) | Remove HADOOP-1230 API from 0.19 |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-4247](https://issues.apache.org/jira/browse/HADOOP-4247) | hadoop jar throwing exception when running examples |  Blocker | . | Hemanth Yamijala | Owen O'Malley |
| [HADOOP-4246](https://issues.apache.org/jira/browse/HADOOP-4246) | Reduce task copy errors may not kill it eventually |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4242](https://issues.apache.org/jira/browse/HADOOP-4242) | Remove an extra ";" in FSDirectory |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4241](https://issues.apache.org/jira/browse/HADOOP-4241) | -hiveconf config parameters in hive cli should override all config variables |  Major | . | Joydeep Sen Sarma | Joydeep Sen Sarma |
| [HADOOP-4236](https://issues.apache.org/jira/browse/HADOOP-4236) | JobTracker.killJob() fails to kill a job if the job is not yet initialized |  Blocker | . | Amar Kamat | Sharad Agarwal |
| [HADOOP-4232](https://issues.apache.org/jira/browse/HADOOP-4232) | Race condition in JVM reuse when more than one slot becomes free |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-4225](https://issues.apache.org/jira/browse/HADOOP-4225) | FSEditLog logs modification time instead of access time. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4213](https://issues.apache.org/jira/browse/HADOOP-4213) | NPE in TestLimitTasksPerJobTaskScheduler |  Major | test | Tsz Wo Nicholas Sze | Sreekanth Ramakrishnan |
| [HADOOP-4209](https://issues.apache.org/jira/browse/HADOOP-4209) | The TaskAttemptID should not have the JobTracker start time |  Blocker | . | Owen O'Malley | Amar Kamat |
| [HADOOP-4200](https://issues.apache.org/jira/browse/HADOOP-4200) | Hadoop-Patch build is failing |  Major | build | Ramya Sunil | Ramya Sunil |
| [HADOOP-4197](https://issues.apache.org/jira/browse/HADOOP-4197) | Need to update DATA\_TRANSFER\_VERSION |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4195](https://issues.apache.org/jira/browse/HADOOP-4195) | SequenceFile.Writer close() uses compressor after returning it to CodecPool. |  Major | io | Hong Tang | Arun C Murthy |
| [HADOOP-4189](https://issues.apache.org/jira/browse/HADOOP-4189) | HADOOP-3245 is incomplete |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-4183](https://issues.apache.org/jira/browse/HADOOP-4183) | select \* to console issues in Hive |  Major | . | Joydeep Sen Sarma |  |
| [HADOOP-4175](https://issues.apache.org/jira/browse/HADOOP-4175) | Incorporate metastore server review comments |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4173](https://issues.apache.org/jira/browse/HADOOP-4173) | TestProcfsBasedProcessTree failing on Windows machine |  Major | test, util | Ramya Sunil | Vinod Kumar Vavilapalli |
| [HADOOP-4169](https://issues.apache.org/jira/browse/HADOOP-4169) | 'compressed' keyword in DDL syntax misleading and does not compress |  Major | . | Joydeep Sen Sarma | Joydeep Sen Sarma |
| [HADOOP-4163](https://issues.apache.org/jira/browse/HADOOP-4163) | If a reducer failed at shuffling stage, the task should fail, not just logging an exception |  Blocker | . | Runping Qi | Sharad Agarwal |
| [HADOOP-4155](https://issues.apache.org/jira/browse/HADOOP-4155) | JobHisotry::JOBTRACKER\_START\_TIME is not initialized properly |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-4154](https://issues.apache.org/jira/browse/HADOOP-4154) | Fix javac warning in WritableUtils |  Minor | io | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4149](https://issues.apache.org/jira/browse/HADOOP-4149) | JobQueueJobInProgressListener.jobUpdated() might not work as expected |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-4147](https://issues.apache.org/jira/browse/HADOOP-4147) | Remove JobWithTaskContext from JobInProgress |  Trivial | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-4146](https://issues.apache.org/jira/browse/HADOOP-4146) | [Hive] null pointer exception on a join |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4139](https://issues.apache.org/jira/browse/HADOOP-4139) | [Hive] multi group by statement is not optimized |  Major | . | Namit Jain | Namit Jain |
| [HADOOP-4135](https://issues.apache.org/jira/browse/HADOOP-4135) | change max length of database columns for metastore to 767 |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4133](https://issues.apache.org/jira/browse/HADOOP-4133) | remove derby.log files form repository and also change the location where these files get created |  Minor | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4129](https://issues.apache.org/jira/browse/HADOOP-4129) | Memory limits of TaskTracker and Tasks should be in kiloBytes. |  Blocker | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-4125](https://issues.apache.org/jira/browse/HADOOP-4125) | Reduce cleanup tip web ui is does not show attempts |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4121](https://issues.apache.org/jira/browse/HADOOP-4121) | HistoryViewer initialization failure should log exception trace |  Trivial | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4112](https://issues.apache.org/jira/browse/HADOOP-4112) | Got ArrayOutOfBound exception while analyzing the job history |  Major | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-4100](https://issues.apache.org/jira/browse/HADOOP-4100) | Scheduler.assignTasks should not be dealing with cleanupTask |  Major | . | Devaraj Das | Amareshwari Sriramadasu |
| [HADOOP-4099](https://issues.apache.org/jira/browse/HADOOP-4099) | HFTP interface compatibility with older releases broken |  Blocker | fs | Kan Zhang | dhruba borthakur |
| [HADOOP-4097](https://issues.apache.org/jira/browse/HADOOP-4097) | Hive interaction with speculative execution is broken |  Critical | . | Joydeep Sen Sarma | Joydeep Sen Sarma |
| [HADOOP-4093](https://issues.apache.org/jira/browse/HADOOP-4093) | [Hive]unify Table.getCols() & get\_fields() |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4089](https://issues.apache.org/jira/browse/HADOOP-4089) | Check if the tmp file used in the CLI exists before using it. |  Major | . | Ashish Thusoo |  |
| [HADOOP-4087](https://issues.apache.org/jira/browse/HADOOP-4087) | Make Hive metastore server to work for PHP & Python clients |  Major | . | Prasad Chakka | Prasad Chakka |
| [HADOOP-4078](https://issues.apache.org/jira/browse/HADOOP-4078) | TestKosmosFileSystem fails on trunk |  Blocker | fs | Amareshwari Sriramadasu | Lohit Vijayarenu |
| [HADOOP-4077](https://issues.apache.org/jira/browse/HADOOP-4077) | Access permissions for setting access times and modification times for files |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-4071](https://issues.apache.org/jira/browse/HADOOP-4071) | FSNameSystem.isReplicationInProgress should add an underReplicated block to the neededReplication queue using method "add" not "update" |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4054](https://issues.apache.org/jira/browse/HADOOP-4054) | During edit log loading, an underconstruction file's lease gets removed twice |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4050](https://issues.apache.org/jira/browse/HADOOP-4050) | TestFairScheduler failed on Linux |  Major | . | Tsz Wo Nicholas Sze | Matei Zaharia |
| [HADOOP-4036](https://issues.apache.org/jira/browse/HADOOP-4036) | Increment InterTrackerProtocol version number due to changes in HADOOP-3759 |  Major | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4030](https://issues.apache.org/jira/browse/HADOOP-4030) | LzopCodec shouldn't be in the default list of codecs i.e. io.compression.codecs |  Major | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-4027](https://issues.apache.org/jira/browse/HADOOP-4027) | When streaming utility is run without specifying mapper/reducer/input/output options, it returns 0. |  Major | . | Ramya Sunil |  |
| [HADOOP-4023](https://issues.apache.org/jira/browse/HADOOP-4023) | javadoc warnings: incorrect references |  Major | documentation | Tsz Wo Nicholas Sze | Owen O'Malley |
| [HADOOP-4018](https://issues.apache.org/jira/browse/HADOOP-4018) | limit memory usage in jobtracker |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-4014](https://issues.apache.org/jira/browse/HADOOP-4014) | DFS upgrade fails on Windows |  Blocker | fs | NOMURA Yoshihide | Konstantin Shvachko |
| [HADOOP-3991](https://issues.apache.org/jira/browse/HADOOP-3991) | updates to hadoop-ec2-env.sh for 0.18.0 |  Minor | contrib/cloud | Karl Anderson | Tom White |
| [HADOOP-3985](https://issues.apache.org/jira/browse/HADOOP-3985) | TestHDFSServerPorts fails on trunk |  Major | . | Amar Kamat | Hairong Kuang |
| [HADOOP-3970](https://issues.apache.org/jira/browse/HADOOP-3970) | Counters written to the job history cannot be recovered back |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-3968](https://issues.apache.org/jira/browse/HADOOP-3968) | test-libhdfs fails on trunk |  Major | . | Lohit Vijayarenu | Pete Wyckoff |
| [HADOOP-3964](https://issues.apache.org/jira/browse/HADOOP-3964) | javadoc warnings by failmon |  Major | build | Tsz Wo Nicholas Sze | dhruba borthakur |
| [HADOOP-3962](https://issues.apache.org/jira/browse/HADOOP-3962) | Shell command "fs -count" should support paths with different file systsms |  Major | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3961](https://issues.apache.org/jira/browse/HADOOP-3961) | resource estimation works badly in some cases |  Blocker | . | Ari Rabkin | Ari Rabkin |
| [HADOOP-3959](https://issues.apache.org/jira/browse/HADOOP-3959) | [HOD] --resource\_manager.options is not passed to qsub |  Major | contrib/hod | Craig Macdonald | Vinod Kumar Vavilapalli |
| [HADOOP-3958](https://issues.apache.org/jira/browse/HADOOP-3958) | TestMapRed ignores failures of the test case |  Major | test | Owen O'Malley | Owen O'Malley |
| [HADOOP-3957](https://issues.apache.org/jira/browse/HADOOP-3957) | Fix javac warnings in DistCp and the corresponding tests |  Minor | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3954](https://issues.apache.org/jira/browse/HADOOP-3954) | Skip records enabled as default. |  Critical | . | Koji Noguchi | Sharad Agarwal |
| [HADOOP-3952](https://issues.apache.org/jira/browse/HADOOP-3952) | TestDataJoin references dfs.MiniDFSCluster instead of hdfs.MiniDFSCluster |  Major | test | Owen O'Malley | Owen O'Malley |
| [HADOOP-3951](https://issues.apache.org/jira/browse/HADOOP-3951) | The package name used in FSNamesystem is incorrect |  Trivial | . | Tsz Wo Nicholas Sze | Chris Douglas |
| [HADOOP-3950](https://issues.apache.org/jira/browse/HADOOP-3950) | TestMapRed and TestMiniMRDFSSort failed on trunk |  Major | test | Tsz Wo Nicholas Sze | Enis Soztutar |
| [HADOOP-3949](https://issues.apache.org/jira/browse/HADOOP-3949) | javadoc warnings: Multiple sources of package comments found for package |  Major | build, documentation | Tsz Wo Nicholas Sze | Jerome Boulon |
| [HADOOP-3946](https://issues.apache.org/jira/browse/HADOOP-3946) | TestMapRed fails on trunk |  Blocker | test | Amareshwari Sriramadasu | Tom White |
| [HADOOP-3937](https://issues.apache.org/jira/browse/HADOOP-3937) | Job history may get disabled due to overly long job names |  Major | . | Matei Zaharia | Matei Zaharia |
| [HADOOP-3933](https://issues.apache.org/jira/browse/HADOOP-3933) | DataNode's BlockSender sends more data than necessary |  Minor | . | Ning Li | Ning Li |
| [HADOOP-3919](https://issues.apache.org/jira/browse/HADOOP-3919) | hadoop conf got slightly mangled by 3772 |  Minor | . | Ari Rabkin | Ari Rabkin |
| [HADOOP-3910](https://issues.apache.org/jira/browse/HADOOP-3910) | Are ClusterTestDFSNamespaceLogging and ClusterTestDFS still valid tests? |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3907](https://issues.apache.org/jira/browse/HADOOP-3907) | INodeDirectoryWithQuota should be in its own .java file |  Minor | . | Steve Loughran | Tsz Wo Nicholas Sze |
| [HADOOP-3904](https://issues.apache.org/jira/browse/HADOOP-3904) | A few tests still using old hdfs package name |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3875](https://issues.apache.org/jira/browse/HADOOP-3875) | Fix TaskTracker's heartbeat timer to note the time the hearbeat RPC returned to decide next heartbeat time |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3864](https://issues.apache.org/jira/browse/HADOOP-3864) | JobTracker lockup due to JobInProgress.initTasks taking significant time for large jobs on large clusters |  Critical | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3863](https://issues.apache.org/jira/browse/HADOOP-3863) | Use a thread-local rather than static ENCODER/DECODER variables in Text for synchronization |  Critical | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3851](https://issues.apache.org/jira/browse/HADOOP-3851) | spelling error in FSNamesystemMetrics log message |  Trivial | . | Steve Loughran | Steve Loughran |
| [HADOOP-3848](https://issues.apache.org/jira/browse/HADOOP-3848) | TaskTracker.localizeJob calls getSystemDir for each task rather than caching it |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-3846](https://issues.apache.org/jira/browse/HADOOP-3846) | CreateEditsLog used for benchmark misses creating parent directories |  Minor | benchmarks | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3836](https://issues.apache.org/jira/browse/HADOOP-3836) | TestMultipleOutputs will fail if it is ran more than one times |  Major | test | Tsz Wo Nicholas Sze | Alejandro Abdelnur |
| [HADOOP-3831](https://issues.apache.org/jira/browse/HADOOP-3831) | slow-reading dfs clients do not recover from datanode-write-timeouts |  Major | . | Christian Kunz | Raghu Angadi |
| [HADOOP-3820](https://issues.apache.org/jira/browse/HADOOP-3820) | gridmix-env has a syntax error, and wrongly defines USE\_REAL\_DATASET by default |  Major | benchmarks | Arun C Murthy | Arun C Murthy |
| [HADOOP-3819](https://issues.apache.org/jira/browse/HADOOP-3819) | can not get svn revision # at build time if locale is not english |  Minor | build | Rong-En Fan | Rong-En Fan |
| [HADOOP-3816](https://issues.apache.org/jira/browse/HADOOP-3816) | KFS changes for faster directory listing |  Minor | fs | Sriram Rao | Sriram Rao |
| [HADOOP-3814](https://issues.apache.org/jira/browse/HADOOP-3814) | [HOD] Remove dfs.client.buffer.dir generation, as this is removed in Hadoop 0.19. |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-3795](https://issues.apache.org/jira/browse/HADOOP-3795) | NameNode does not save image if different dfs.name.dir have different checkpoint stamps |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3785](https://issues.apache.org/jira/browse/HADOOP-3785) | FileSystem cache should be case-insensitive |  Major | fs | Doug Cutting | Bill de hOra |
| [HADOOP-3783](https://issues.apache.org/jira/browse/HADOOP-3783) | "deprecated filesystem name" warning on EC2 |  Minor | contrib/cloud | Stuart Sierra | Tom White |
| [HADOOP-3778](https://issues.apache.org/jira/browse/HADOOP-3778) | seek(long) in DFSInputStream should catch socket exception for retry later |  Minor | . | Luo Ning | Luo Ning |
| [HADOOP-3777](https://issues.apache.org/jira/browse/HADOOP-3777) | Failure to load native lzo libraries causes job failure |  Major | . | Chris Douglas | Chris Douglas |
| [HADOOP-3776](https://issues.apache.org/jira/browse/HADOOP-3776) | NPE in NameNode with unknown blocks |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-3773](https://issues.apache.org/jira/browse/HADOOP-3773) | Setting the conf twice in Pipes Submitter |  Trivial | . | Koji Noguchi | Koji Noguchi |
| [HADOOP-3756](https://issues.apache.org/jira/browse/HADOOP-3756) | dfs.client.buffer.dir isn't used in hdfs, but it's still in conf/hadoop-default.xml |  Trivial | . | Michael Bieniosek | Raghu Angadi |
| [HADOOP-3732](https://issues.apache.org/jira/browse/HADOOP-3732) | Block scanner should read block information during initialization. |  Blocker | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-3728](https://issues.apache.org/jira/browse/HADOOP-3728) | Cannot run more than one instance of examples.SleepJob at the same time. |  Minor | . | Brice Arnould | Brice Arnould |
| [HADOOP-3726](https://issues.apache.org/jira/browse/HADOOP-3726) | TestCLI loses exception details on setup/teardown |  Minor | test | Steve Loughran | Steve Loughran |
| [HADOOP-3725](https://issues.apache.org/jira/browse/HADOOP-3725) | TestMiniMRMapRedDebugScript loses exception details |  Minor | test | Steve Loughran | Steve Loughran |
| [HADOOP-3723](https://issues.apache.org/jira/browse/HADOOP-3723) | libhdfs only accepts O\_WRONLY and O\_RDONLY so does not accept things like O\_WRONLY \| O\_CREAT |  Minor | . | Pete Wyckoff | Pi Song |
| [HADOOP-3720](https://issues.apache.org/jira/browse/HADOOP-3720) | dfsadmin -refreshNodes should re-read the config file. |  Major | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3711](https://issues.apache.org/jira/browse/HADOOP-3711) | Streaming input is not parsed properly to find the separator |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-3705](https://issues.apache.org/jira/browse/HADOOP-3705) | CompositeInputFormat is unable to parse InputFormat classes with names containing '\_' or '$' |  Major | . | Jingkei Ly | Chris Douglas |
| [HADOOP-3658](https://issues.apache.org/jira/browse/HADOOP-3658) | Incorrect destination IP logged for receiving blocks |  Minor | . | Koji Noguchi | Chris Douglas |
| [HADOOP-3643](https://issues.apache.org/jira/browse/HADOOP-3643) | jobtasks.jsp when called for running tasks should ignore completed TIPs |  Major | . | Amar Kamat | Amar Kamat |
| [HADOOP-3640](https://issues.apache.org/jira/browse/HADOOP-3640) | NativeS3FsInputStream read() method for reading a single byte is incorrect |  Major | fs/s3 | Tom White | Tom White |
| [HADOOP-3623](https://issues.apache.org/jira/browse/HADOOP-3623) | LeaseManager needs refactoring. |  Major | . | Konstantin Shvachko | Tsz Wo Nicholas Sze |
| [HADOOP-3592](https://issues.apache.org/jira/browse/HADOOP-3592) | org.apache.hadoop.fs.FileUtil.copy() will leak input streams if the destination can't be opened |  Minor | fs | Steve Loughran | Bill de hOra |
| [HADOOP-3570](https://issues.apache.org/jira/browse/HADOOP-3570) | Including user specified jar files in the client side classpath path in Hadoop 0.17 streaming |  Major | . | Suhas Gogate | Sharad Agarwal |
| [HADOOP-3560](https://issues.apache.org/jira/browse/HADOOP-3560) | Archvies sometimes create empty part files. |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-3543](https://issues.apache.org/jira/browse/HADOOP-3543) | Need to increment the year field for the copyright notice |  Trivial | documentation | Chris Douglas | Chris Douglas |
| [HADOOP-3542](https://issues.apache.org/jira/browse/HADOOP-3542) | Hadoop archives should not create \_logs file in the final archive directory. |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-3528](https://issues.apache.org/jira/browse/HADOOP-3528) | Metrics FilesCreated and files\_deleted metrics do not match. |  Blocker | metrics | Lohit Vijayarenu | Lohit Vijayarenu |
| [HADOOP-3506](https://issues.apache.org/jira/browse/HADOOP-3506) | Occasional NPE in Jets3tFileSystemStore |  Major | fs/s3 | Robert | Tom White |
| [HADOOP-3488](https://issues.apache.org/jira/browse/HADOOP-3488) | the rsync command in hadoop-daemon.sh also rsync the logs folder from the master, what deletes the datanode / tasktracker log files. |  Critical | scripts | Stefan Groschupf | Craig Macdonald |
| [HADOOP-3319](https://issues.apache.org/jira/browse/HADOOP-3319) | [HOD]checknodes prints errors messages on stdout |  Major | contrib/hod | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-3155](https://issues.apache.org/jira/browse/HADOOP-3155) | reducers stuck at shuffling |  Blocker | . | Runping Qi | dhruba borthakur |
| [HADOOP-3131](https://issues.apache.org/jira/browse/HADOOP-3131) | enabling BLOCK compression for map outputs breaks the reduce progress counters |  Major | . | Colin Evans | Matei Zaharia |
| [HADOOP-3076](https://issues.apache.org/jira/browse/HADOOP-3076) | [HOD] If a cluster directory is specified as a relative path, an existing script.exitcode file will not be deleted. |  Blocker | contrib/hod | Hemanth Yamijala | Vinod Kumar Vavilapalli |
| [HADOOP-2403](https://issues.apache.org/jira/browse/HADOOP-2403) | JobHistory log files contain data that cannot be parsed by org.apache.hadoop.mapred.JobHistory |  Critical | . | Runping Qi | Amareshwari Sriramadasu |
| [HADOOP-2168](https://issues.apache.org/jira/browse/HADOOP-2168) | Pipes with a C++ record reader does not update progress in the map until it is 100% |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1945](https://issues.apache.org/jira/browse/HADOOP-1945) | pipes examples aren't in the release |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-9](https://issues.apache.org/jira/browse/HADOOP-9) | mapred.local.dir  temp dir. space allocation limited by smallest area |  Minor | . | Paul Baclace | Ari Rabkin |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4464](https://issues.apache.org/jira/browse/HADOOP-4464) | Separate testClientTriggeredLeaseRecovery() out from TestFileCreation |  Blocker | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4426](https://issues.apache.org/jira/browse/HADOOP-4426) | TestCapacityScheduler is broken |  Blocker | . | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-4390](https://issues.apache.org/jira/browse/HADOOP-4390) | Hive: test for case sensitivity in serde2 thrift serde |  Minor | . | Zheng Shao |  |
| [HADOOP-4259](https://issues.apache.org/jira/browse/HADOOP-4259) | findbugs should run over the tools.jar also |  Minor | test | Owen O'Malley | Chris Douglas |
| [HADOOP-4237](https://issues.apache.org/jira/browse/HADOOP-4237) | TestStreamingBadRecords.testNarrowDown fails intermittently |  Minor | test | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-4069](https://issues.apache.org/jira/browse/HADOOP-4069) | TestKosmosFileSystem can fail when run through ant test on systems shared by users |  Minor | fs | Hemanth Yamijala | Lohit Vijayarenu |
| [HADOOP-4056](https://issues.apache.org/jira/browse/HADOOP-4056) | Unit test for DynamicSerDe |  Minor | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3966](https://issues.apache.org/jira/browse/HADOOP-3966) | Place the new findbugs warnings introduced by the patch in the /tmp directory when "ant test-patch" is run. |  Minor | test | Ramya Sunil | Ramya Sunil |
| [HADOOP-3790](https://issues.apache.org/jira/browse/HADOOP-3790) | Add more unit tests to test appending to files in HDFS |  Blocker | test | dhruba borthakur | Tsz Wo Nicholas Sze |
| [HADOOP-3587](https://issues.apache.org/jira/browse/HADOOP-3587) | contrib/data\_join needs unit tests |  Major | test | Chris Douglas | Chris Douglas |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4354](https://issues.apache.org/jira/browse/HADOOP-4354) | Separate TestDatanodeDeath.testDatanodeDeath() into 4 tests |  Blocker | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4186](https://issues.apache.org/jira/browse/HADOOP-4186) | Move LineRecordReader.LineReader class to util package |  Major | . | Tom White | Tom White |
| [HADOOP-4184](https://issues.apache.org/jira/browse/HADOOP-4184) | Fix simple module dependencies between core, hdfs and mapred |  Major | . | Tom White | Tom White |
| [HADOOP-3824](https://issues.apache.org/jira/browse/HADOOP-3824) | Refactor org.apache.hadoop.mapred.StatusHttpServer |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3601](https://issues.apache.org/jira/browse/HADOOP-3601) | Hive as a contrib project |  Minor | . | Joydeep Sen Sarma | Ashish Thusoo |
| [HADOOP-4105](https://issues.apache.org/jira/browse/HADOOP-4105) | libhdfs wiki is very out-of-date and contains mostly broken links |  Minor | documentation | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4076](https://issues.apache.org/jira/browse/HADOOP-4076) | fuse-dfs REAME lists wrong ant flags and is not specific in some place |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-3942](https://issues.apache.org/jira/browse/HADOOP-3942) | Update DistCp documentation |  Blocker | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3791](https://issues.apache.org/jira/browse/HADOOP-3791) | Use generics in ReflectionUtils |  Trivial | . | Chris Smith | Chris Smith |


