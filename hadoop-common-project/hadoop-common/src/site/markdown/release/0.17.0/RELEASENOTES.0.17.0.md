
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
# Apache Hadoop  0.17.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3382](https://issues.apache.org/jira/browse/HADOOP-3382) | *Blocker* | **Memory leak when files are not cleanly closed**

Fixed a memory leak associated with 'abandoned' files (i.e. not cleanly closed). This held up significant amounts of memory depending on activity and how long NameNode has been running.


---

* [HADOOP-3280](https://issues.apache.org/jira/browse/HADOOP-3280) | *Blocker* | **virtual address space limits break streaming apps**

This patch adds the mapred.child.ulimit to limit the virtual memory for children processes to the given value.


---

* [HADOOP-3266](https://issues.apache.org/jira/browse/HADOOP-3266) | *Major* | **Remove HOD changes from CHANGES.txt, as they are now inside src/contrib/hod**

Moved HOD change items from CHANGES.txt to a new file src/contrib/hod/CHANGES.txt.


---

* [HADOOP-3239](https://issues.apache.org/jira/browse/HADOOP-3239) | *Major* | **exists() calls logs FileNotFoundException in namenode log**

getFileInfo returns null for File not found instead of throwing FileNotFoundException


---

* [HADOOP-3223](https://issues.apache.org/jira/browse/HADOOP-3223) | *Blocker* | **Hadoop dfs -help for permissions contains a typo**

Minor typo fix in help message for chmod. impact : none.


---

* [HADOOP-3204](https://issues.apache.org/jira/browse/HADOOP-3204) | *Blocker* | **LocalFSMerger needs to catch throwable**

Fixes LocalFSMerger in ReduceTask.java to handle errors/exceptions better. Prior to this all exceptions except IOException would be silently ignored.


---

* [HADOOP-3168](https://issues.apache.org/jira/browse/HADOOP-3168) | *Major* | **reduce amount of logging in hadoop streaming**

Decreases the frequency of logging from streaming from every 100 records to every 10,000 records.


---

* [HADOOP-3162](https://issues.apache.org/jira/browse/HADOOP-3162) | *Blocker* | **Map/reduce stops working with comma separated input paths**

The public methods org.apache.hadoop.mapred.JobConf.setInputPath(Path) and org.apache.hadoop.mapred.JobConf.addInputPath(Path) are deprecated. And the methods have the semantics of branch 0.16.
The following public APIs  are added in org.apache.hadoop.mapred.FileInputFormat :
public static void setInputPaths(JobConf job, Path... paths);
public static void setInputPaths(JobConf job, String commaSeparatedPaths);
public static void addInputPath(JobConf job, Path path);
public static void addInputPaths(JobConf job, String commaSeparatedPaths);
Earlier code calling JobConf.setInputPath(Path), JobConf.addInputPath(Path) should now call FileInputFormat.setInputPaths(JobConf, Path...) and FileInputFormat.addInputPath(Path) respectively


---

* [HADOOP-3152](https://issues.apache.org/jira/browse/HADOOP-3152) | *Minor* | **Make index interval configuable when using MapFileOutputFormat for map-reduce job**

Add a static method MapFile#setIndexInterval(Configuration, int interval) so that MapReduce jobs that use MapFileOutputFormat can set the index interval.


---

* [HADOOP-3140](https://issues.apache.org/jira/browse/HADOOP-3140) | *Major* | **JobTracker should not try to promote a (map) task if it does not write to DFS at all**

Tasks that don't generate any output are not inserted in the commit queue of the JobTracker. They are marked as SUCCESSFUL by the TaskTracker and the JobTracker updates their state short-circuiting the commit queue.


---

* [HADOOP-3137](https://issues.apache.org/jira/browse/HADOOP-3137) | *Major* | **[HOD] Update hod version number**

Build script was changed to make HOD versions follow Hadoop version numbers. As a result of this change, the next version of HOD would not be 0.5, but would be synchronized to the Hadoop version number. Users who rely on the version number of HOD should note the unexpected jump in version numbers.


---

* [HADOOP-3124](https://issues.apache.org/jira/browse/HADOOP-3124) | *Major* | **DFS data node should not use hard coded 10 minutes as write timeout.**

Makes DataNode socket write timeout configurable. User impact : none.


---

* [HADOOP-3099](https://issues.apache.org/jira/browse/HADOOP-3099) | *Blocker* | **Need new options in distcp for preserving ower, group and permission**

Added a new option -p to distcp for preserving file/directory status.
-p[rbugp]              Preserve status
                       r: replication number
                       b: block size
                       u: user
                       g: group
                       p: permission
                       -p alone is equivalent to -prbugp


---

* [HADOOP-3093](https://issues.apache.org/jira/browse/HADOOP-3093) | *Major* | **ma/reduce throws the following exception if "io.serializations" is not set:**

The following public APIs  are added in org.apache.hadoop.conf.Configuration
 String[] Configuration.getStrings(String name, String... defaultValue)  and
 void Configuration.setStrings(String name, String... values)


---

* [HADOOP-3091](https://issues.apache.org/jira/browse/HADOOP-3091) | *Major* | **hadoop dfs -put should support multiple src**

hadoop dfs -put accepts multiple sources when destination is a directory.


---

* [HADOOP-3073](https://issues.apache.org/jira/browse/HADOOP-3073) | *Blocker* | **SocketOutputStream.close() should close the channel.**

SocketOutputStream.close() closes the underlying channel. Increase compatibility with java.net.Socket.getOutputStream. User Impact : none.


---

* [HADOOP-3060](https://issues.apache.org/jira/browse/HADOOP-3060) | *Major* | **MiniMRCluster is ignoring parameter taskTrackerFirst**

The parameter boolean taskTrackerFirst is removed from org.apache.hadoop.mapred.MiniMRCluster constructors.
Thus signature of following APIs
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, boolean taskTrackerFirst, int numDir)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, boolean taskTrackerFirst, int numDir, String[] racks)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, boolean taskTrackerFirst, int numDir, String[] racks, String[] hosts)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, boolean taskTrackerFirst, int numDir, String[] racks, String[] hosts, UnixUserGroupInformation ugi )
is changed to
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, int numDir)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, int numDir, String[] racks)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, int numDir, String[] racks, String[] hosts)
  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, int numDir, String[] racks, String[] hosts, UnixUserGroupInformation ugi )
respectively.
Since the old signatures were not deprecated, any code using the old constructors must be changed to use the new constructors.


---

* [HADOOP-3048](https://issues.apache.org/jira/browse/HADOOP-3048) | *Blocker* | **Stringifier**

 A new Interface and a default implementation to convert and restore serializations of objects to strings.


---

* [HADOOP-3041](https://issues.apache.org/jira/browse/HADOOP-3041) | *Blocker* | **Within a task, the value ofJobConf.getOutputPath() method is modified**

1. Deprecates JobConf.setOutputPath and JobConf.getOutputPath
JobConf.getOutputPath() still returns the same value that it used to return. 
2. Deprecates OutputFormatBase. Adds FileOutputFormat. Existing output formats extending OutputFormatBase, now extend FileOutputFormat.
3. Adds the following APIs in FileOutputFormat :
public static void setOutputPath(JobConf conf, Path outputDir); // sets mapred.output.dir
public static Path getOutputPath(JobConf conf) ; // gets mapred.output.dir
public static Path getWorkOutputPath(JobConf conf); // gets mapred.work.output.dir
4. static void setWorkOutputPath(JobConf conf, Path outputDir) is also added to FileOutputFormat. This is used by the framework to set mapred.work.output.dir as task's temporary output dir .


---

* [HADOOP-3040](https://issues.apache.org/jira/browse/HADOOP-3040) | *Major* | **Streaming should assume an empty key if the first character on a line is the seperator (stream.map.output.field.separator, by default, tab)**

If the first character on a line is the separator, empty key is assumed, and the whole line is the value (due to a bug this was not the case).


---

* [HADOOP-3001](https://issues.apache.org/jira/browse/HADOOP-3001) | *Blocker* | **FileSystems should track how many bytes are read and written**

Adds new framework map/reduce counters that track the number of bytes read and written to HDFS, local, KFS, and S3 file systems.


---

* [HADOOP-2982](https://issues.apache.org/jira/browse/HADOOP-2982) | *Blocker* | **[HOD] checknodes should look for free nodes without the jobs attribute**

The number of free nodes in the cluster is computed using a better algorithm that filters out inconsistencies in node status as reported by Torque.


---

* [HADOOP-2947](https://issues.apache.org/jira/browse/HADOOP-2947) | *Blocker* | **[HOD] Hod should redirect stderr and stdout of Hadoop daemons to assist debugging**

The stdout and stderr streams of daemons are redirected to files that are created under the hadoop log directory. Users can now send kill 3 signals to the daemons to get stack traces and thread dumps for debugging.


---

* [HADOOP-2899](https://issues.apache.org/jira/browse/HADOOP-2899) | *Major* | **[HOD] hdfs:///mapredsystem directory not cleaned up after deallocation**

The mapred system directory generated by HOD is cleaned up at cluster deallocation time.


---

* [HADOOP-2873](https://issues.apache.org/jira/browse/HADOOP-2873) | *Major* | **Namenode fails to re-start after cluster shutdown - DFSClient: Could not obtain blocks even all datanodes were up & live**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-2855](https://issues.apache.org/jira/browse/HADOOP-2855) | *Blocker* | **[HOD] HOD fails to allocate a cluster if the tarball specified is a relative path**

Changes were made to handle relative paths correctly for important HOD options such as the cluster directory, tarball option, and script file.


---

* [HADOOP-2854](https://issues.apache.org/jira/browse/HADOOP-2854) | *Blocker* | **Remove the deprecated ipc.Server.getUserInfo()**

Removes deprecated method Server.getUserInfo()


---

* [HADOOP-2839](https://issues.apache.org/jira/browse/HADOOP-2839) | *Blocker* | **Remove deprecated methods in FileSystem**

Removes deprecated API FileSystem#globPaths()


---

* [HADOOP-2831](https://issues.apache.org/jira/browse/HADOOP-2831) | *Blocker* | **Remove the deprecated INode.getAbsoluteName()**

Removes deprecated method INode#getAbsoluteName()


---

* [HADOOP-2828](https://issues.apache.org/jira/browse/HADOOP-2828) | *Major* | **Remove deprecated methods in Configuration.java**

The following deprecated methods in org.apache.hadoop.conf.Configuration are removed.
public Object getObject(String name)
public void setObject(String name, Object value)
public Object get(String name, Object defaultValue)
public void set(String name, Object value)
and public Iterator entries()


---

* [HADOOP-2826](https://issues.apache.org/jira/browse/HADOOP-2826) | *Major* | **FileSplit.getFile(), LineRecordReader. readLine() need to be removed**

The deprecated methods, public File org.apache.hadoop.mapred.FileSplit.getFile() and 
  public static  long org.apache.hadoop.mapred.LineRecordReader.readLine(InputStream in,  OutputStream out)
are removed.
The constructor org.apache.hadoop.mapred.LineRecordReader.LineReader(InputStream in, Configuration conf) 's visibility is made public.
The signature of the public org.apache.hadoop.streaming.UTF8ByteArrayUtils.readLIne(InputStream) method is changed to UTF8ByteArrayUtils.readLIne(LineReader, Text).  Since the old signature is not deprecated, any code using the old method must be changed to use the new method.


---

* [HADOOP-2825](https://issues.apache.org/jira/browse/HADOOP-2825) | *Major* | **MapOutputLocation.getFile() needs to be removed**

The deprecated method, public long org.apache.hadoop.mapred.MapOutputLocation.getFile(FileSystem fileSys, Path localFilename, int reduce, Progressable pingee, int timeout) is removed.


---

* [HADOOP-2824](https://issues.apache.org/jira/browse/HADOOP-2824) | *Major* | **One of MiniMRCluster constructors needs tobe removed**

The deprecated constructor org.apache.hadoop.mapred.MiniMRCluster.MiniMRCluster(int jobTrackerPort, int taskTrackerPort, int numTaskTrackers, String namenode, boolean taskTrackerFirst) is removed.


---

* [HADOOP-2823](https://issues.apache.org/jira/browse/HADOOP-2823) | *Major* | **SimpleCharStream.getColumn(),  getLine() methods to be removed.**

The deprecated methods in org.apache.hadoop.record.compiler.generated.SimpleCharStream :
public int getColumn()
and public int getLine() are removed


---

* [HADOOP-2822](https://issues.apache.org/jira/browse/HADOOP-2822) | *Major* | **Remove deprecated classes in mapred**

The deprecated classes org.apache.hadoop.mapred.InputFormatBase and org.apache.hadoop.mapred.PhasedFileSystem are removed.


---

* [HADOOP-2821](https://issues.apache.org/jira/browse/HADOOP-2821) | *Major* | **Remove deprecated classes in util**

The deprecated classes org.apache.hadoop.util.ShellUtil and org.apache.hadoop.util.ToolBase are removed.


---

* [HADOOP-2820](https://issues.apache.org/jira/browse/HADOOP-2820) | *Major* | **Remove deprecated classes in streaming**

The deprecated classes org.apache.hadoop.streaming.StreamLineRecordReader,  org.apache.hadoop.streaming.StreamOutputFormat and org.apache.hadoop.streaming.StreamSequenceRecordReader are removed


---

* [HADOOP-2819](https://issues.apache.org/jira/browse/HADOOP-2819) | *Major* | **Remove deprecated methods in JobConf()**

The following deprecated methods are removed from org.apache.hadoop.JobConf :
public Class getInputKeyClass()
public void setInputKeyClass(Class theClass)
public Class getInputValueClass()
public void setInputValueClass(Class theClass)

The methods, public boolean org.apache.hadoop.JobConf.getSpeculativeExecution() and 
public void org.apache.hadoop.JobConf.setSpeculativeExecution(boolean speculativeExecution) are undeprecated.


---

* [HADOOP-2818](https://issues.apache.org/jira/browse/HADOOP-2818) | *Major* | **Remove deprecated Counters.getDisplayName(),  getCounterNames(),   getCounter(String counterName)**

The deprecated methods public String org.apache.hadoop.mapred.Counters.getDisplayName(String counter) and 
public synchronized Collection\<String\> org.apache.hadoop.mapred.Counters.getCounterNames() are removed.
The deprecated method public synchronized long org.apache.hadoop.mapred.Counters.getCounter(String counterName) is undeprecated.


---

* [HADOOP-2817](https://issues.apache.org/jira/browse/HADOOP-2817) | *Major* | **Remove deprecated mapred.tasktracker.tasks.maximum and clusterStatus.getMaxTasks()**

The deprecated method public int org.apache.hadoop.mapred.ClusterStatus.getMaxTasks() is removed.
The deprecated configuration property "mapred.tasktracker.tasks.maximum" is removed.


---

* [HADOOP-2796](https://issues.apache.org/jira/browse/HADOOP-2796) | *Major* | **For script option hod should exit with distinguishable exit codes for script code and hod exit code.**

A provision to reliably detect a failing script's exit code was added. In case the hod script option returned a non-zero exit code, users can now look for a 'script.exitcode' file written to the HOD cluster directory. If this file is present, it means the script failed with the returned exit code.


---

* [HADOOP-2775](https://issues.apache.org/jira/browse/HADOOP-2775) | *Major* | **[HOD] Put in place unit test framework for HOD**

A unit testing framework based on pyunit is added to HOD. Developers contributing patches to HOD should now contribute unit tests along with the patches where possible.


---

* [HADOOP-2765](https://issues.apache.org/jira/browse/HADOOP-2765) | *Major* | **setting memory limits for tasks**

This feature enables specifying ulimits for streaming/pipes tasks. Now pipes and streaming tasks have same virtual memory available as the java process which invokes them. Ulimit value will be the same as -Xmx value for java processes provided using mapred.child.java.opts.


---

* [HADOOP-2758](https://issues.apache.org/jira/browse/HADOOP-2758) | *Major* | **Reduce memory copies when data is read from DFS**

DataNode takes 50% less CPU while serving data to clients.


---

* [HADOOP-2657](https://issues.apache.org/jira/browse/HADOOP-2657) | *Major* | **Enhancements to DFSClient to support flushing data at any point in time**

A new API DFSOututStream.flush() flushes all outstanding data to the pipeline of datanodes.


---

* [HADOOP-2634](https://issues.apache.org/jira/browse/HADOOP-2634) | *Blocker* | **Deprecate exists() and isDir() to simplify ClientProtocol.**

Deprecates exists() from ClientProtocol


---

* [HADOOP-2563](https://issues.apache.org/jira/browse/HADOOP-2563) | *Blocker* | **Remove deprecated FileSystem#listPaths()**

Removes deprecated method FileSystem#listPaths()


---

* [HADOOP-2559](https://issues.apache.org/jira/browse/HADOOP-2559) | *Major* | **DFS should place one replica per rack**

Change DFS block placement to allocate the first replica locally, the second off-rack, and the third intra-rack from the second.


---

* [HADOOP-2551](https://issues.apache.org/jira/browse/HADOOP-2551) | *Blocker* | **hadoop-env.sh needs finer granularity**

New environment variables were introduced to allow finer grained control of Java options passed to server and client JVMs.  See the new \*\_OPTS variables in conf/hadoop-env.sh.


---

* [HADOOP-2470](https://issues.apache.org/jira/browse/HADOOP-2470) | *Major* | **Open and isDir should be removed from ClientProtocol**

Open and isDir were removed from ClientProtocol.


---

* [HADOOP-2423](https://issues.apache.org/jira/browse/HADOOP-2423) | *Major* | **The codes in FSDirectory.mkdirs(...) is inefficient.**

Improved FSDirectory.mkdirs(...) performance.  In NNThroughputBenchmark-create, the ops per sec in  was improved ~54%.


---

* [HADOOP-2410](https://issues.apache.org/jira/browse/HADOOP-2410) | *Major* | **Make EC2 cluster nodes more independent of each other**

The command "hadoop-ec2 run" has been replaced by "hadoop-ec2 launch-cluster \<group\> \<number of instances\>", and "hadoop-ec2 start-hadoop" has been removed since Hadoop is started on instance start up. See http://wiki.apache.org/hadoop/AmazonEC2 for details.


---

* [HADOOP-2399](https://issues.apache.org/jira/browse/HADOOP-2399) | *Major* | **Input key and value to combiner and reducer should be reused**

The key and value objects that are given to the Combiner and Reducer are now reused between calls. This is much more efficient, but the user can not assume the objects are constant.


---

* [HADOOP-2345](https://issues.apache.org/jira/browse/HADOOP-2345) | *Major* | **new transactions to support HDFS Appends**

Introduce new namenode transactions to support appending to HDFS files.


---

* [HADOOP-2239](https://issues.apache.org/jira/browse/HADOOP-2239) | *Major* | **Security:  Need to be able to encrypt Hadoop socket connections**

This patch adds a new FileSystem, HftpsFileSystem, that allows access to HDFS data over HTTPS.


---

* [HADOOP-2219](https://issues.apache.org/jira/browse/HADOOP-2219) | *Major* | **du like command to count number of files under a given directory**

Added a new fs command fs -count for counting the number of bytes, files and directories under a given path.

Added a new RPC getContentSummary(String path) to ClientProtocol.


---

* [HADOOP-2192](https://issues.apache.org/jira/browse/HADOOP-2192) | *Major* | **dfs mv command differs from POSIX standards**

this patch makes dfs -mv more like linux mv command getting rid of unnecessary output in dfs -mv and returns an error message when moving non existent files/directories --- mv: cannot stat "filename": No such file or directory.


---

* [HADOOP-2178](https://issues.apache.org/jira/browse/HADOOP-2178) | *Major* | **Job history on HDFS**

This feature provides facility to store job history on DFS. Now cluster admin can provide either localFS location or DFS location using configuration property "mapred.job.history.location"  to store job histroy. History will be logged in user specified location also. User can specify history location using configuration property "mapred.job.history.user.location" .
The classes org.apache.hadoop.mapred.DefaultJobHistoryParser.MasterIndex and org.apache.hadoop.mapred.DefaultJobHistoryParser.MasterIndexParseListener, and public method org.apache.hadoop.mapred.DefaultJobHistoryParser.parseMasterIndex are not available.
The signature of public method org.apache.hadoop.mapred.DefaultJobHistoryParser.parseJobTasks(File jobHistoryFile, JobHistory.JobInfo job) is changed to DefaultJobHistoryParser.parseJobTasks(String jobHistoryFile, JobHistory.JobInfo job, FileSystem fs).
The signature of public method org.apache.hadoop.mapred.JobHistory.parseHistory(File path, Listener l) is changed to JobHistory.parseHistoryFromFS(String path, Listener l, FileSystem fs)


---

* [HADOOP-2119](https://issues.apache.org/jira/browse/HADOOP-2119) | *Critical* | **JobTracker becomes non-responsive if the task trackers finish task too fast**

This removes many inefficiencies in task placement and scheduling logic. The JobTracker would perform linear scans of the list of submitted tasks in cases where it did not find an obvious candidate task for a node. With better data structures for managing job state, all task placement operations now run in constant time (in most cases). Also, the task output promotions are batched.


---

* [HADOOP-2116](https://issues.apache.org/jira/browse/HADOOP-2116) | *Major* | **Job.local.dir to be exposed to tasks**

This issue restructures local job directory on the tasktracker.
Users are provided with a job-specific shared directory  (mapred-local/taskTracker/jobcache/$jobid/ work) for using it as scratch space, through configuration property and system property "job.local.dir". Now, the directory "../work" is not available from the task's cwd.


---

* [HADOOP-2063](https://issues.apache.org/jira/browse/HADOOP-2063) | *Blocker* | **Command to pull corrupted files**

Added a new option -ignoreCrc to fs -get, or equivalently, fs -copyToLocal, such that crc checksum will be ignored for the command.  The use of this option is to download the corrupted files.


---

* [HADOOP-2055](https://issues.apache.org/jira/browse/HADOOP-2055) | *Minor* | **JobConf should have a setInputPathFilter method**

This issue provides users the ability to specify what paths to ignore for processing in the job input directory (apart from the filenames that start with "\_" and "."). Defines two new APIs - FileInputFormat.setInputPathFilter(JobConf, PathFilter), and, FileInputFormat.getInputPathFilter(JobConf).


---

* [HADOOP-2027](https://issues.apache.org/jira/browse/HADOOP-2027) | *Major* | **FileSystem should provide byte ranges for file locations**

New FileSystem API getFileBlockLocations to return the number of bytes in each block in a file via a single rpc to the namenode to speed up job planning. Deprecates getFileCacheHints.


---

* [HADOOP-1986](https://issues.apache.org/jira/browse/HADOOP-1986) | *Major* | **Add support for a general serialization mechanism for Map Reduce**

Programs that implement the raw Mapper or Reducer interfaces will need modification to compile with this release. For example, 

class MyMapper implements Mapper {
  public void map(WritableComparable key, Writable val,
    OutputCollector out, Reporter reporter) throws IOException {
    // ...
  }
  // ...
}

will need to be changed to refer to the parameterized type. For example:

class MyMapper implements Mapper\<WritableComparable, Writable, WritableComparable, Writable\> {
  public void map(WritableComparable key, Writable val,
    OutputCollector\<WritableComparable, Writable\> out, Reporter reporter) throws IOException {
    // ...
  }
  // ...
}

Similarly implementations of the following raw interfaces will need modification: InputFormat, OutputCollector, OutputFormat, Partitioner, RecordReader, RecordWriter


---

* [HADOOP-1985](https://issues.apache.org/jira/browse/HADOOP-1985) | *Major* | **Abstract node to switch mapping into a topology service class used by namenode and jobtracker**

This issue introduces rack awareness for map tasks. It also moves the rack resolution logic to the central servers - NameNode & JobTracker. The administrator can specify a loadable class given by topology.node.switch.mapping.impl to specify the class implementing the logic for rack resolution. The class must implement a method - resolve(List\<String\> names), where names is the list of DNS-names/IP-addresses that we want resolved. The return value is a list of resolved network paths of the form /foo/rack, where rack is the rackID where the node belongs to and foo is the switch where multiple racks are connected, and so on. The default implementation of this class is packaged along with hadoop and points to org.apache.hadoop.net.ScriptBasedMapping and this class loads a script that can be used for rack resolution. The script location is configurable. It is specified by topology.script.file.name and defaults to an empty script. In the case where the script name is empty, /default-rack is returned for all dns-names/IP-addresses. The loadable topology.node.switch.mapping.impl provides administrators fleixibilty to define how their site's node resolution should happen.
For mapred, one can also specify the level of the cache w.r.t the number of levels in the resolved network path - defaults to two. This means that the JobTracker will cache tasks at the host level and at the rack level. 
Known issue: the task caching will not work with levels greater than 2 (beyond racks). This bug is tracked in HADOOP-3296.


---

* [HADOOP-1622](https://issues.apache.org/jira/browse/HADOOP-1622) | *Major* | **Hadoop should provide a way to allow the user to specify jar file(s) the user job depends on**

This patch allows new command line options for 

hadoop jar 
which are 

hadoop jar -files \<comma seperated list of files\> -libjars \<comma seperated list of jars\> -archives \<comma seperated list of archives\>

-files options allows you to speficy comma seperated list of path which would be present in your current working directory of your task
-libjars option allows you to add jars to the classpaths of the maps and reduces. 
-archives allows you to pass archives as arguments that are unzipped/unjarred and a link with name of the jar/zip are created in the current working directory if tasks.


---

* [HADOOP-1593](https://issues.apache.org/jira/browse/HADOOP-1593) | *Major* | **FsShell should work with paths in non-default FileSystem**

This bug allows non default path to specifeid in fsshell commands.

So, you can now specify hadoop dfs -ls hdfs://remotehost1:port/path 
  and  hadoop dfs -ls hdfs://remotehost2:port/path without changing the config.


---

* [HADOOP-910](https://issues.apache.org/jira/browse/HADOOP-910) | *Major* | **Reduces can do merges for the on-disk map output files in parallel with their copying**

Reducers now perform merges of shuffle data (both in-memory and on disk) while fetching map outputs. Earlier, during shuffle they used to merge only the in-memory outputs.


---

* [HADOOP-771](https://issues.apache.org/jira/browse/HADOOP-771) | *Major* | **Namenode should return error when trying to delete non-empty directory**

This patch adds a new api to file system i.e delete(path, boolean), deprecating the previous delete(path). 
the new api recursively deletes files only if boolean is set to true. 
If path is a file, the boolean value does not matter, if path is a directory and the directory is non empty delete(path, false) will throw an exception and delete(path, true) will delete all files recursively.



