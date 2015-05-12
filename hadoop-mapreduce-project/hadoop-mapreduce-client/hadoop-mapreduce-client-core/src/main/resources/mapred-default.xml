<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<!-- Do not modify this file directly.  Instead, copy entries that you -->
<!-- wish to modify from this file into mapred-site.xml and change them -->
<!-- there.  If mapred-site.xml does not already exist, create it.      -->

<configuration>

<property>
  <name>mapreduce.jobtracker.jobhistory.location</name>
  <value></value>
  <description> If job tracker is static the history files are stored 
  in this single well known place. If No value is set here, by default,
  it is in the local file system at ${hadoop.log.dir}/history.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.jobhistory.task.numberprogresssplits</name>
  <value>12</value>
  <description> Every task attempt progresses from 0.0 to 1.0 [unless
  it fails or is killed].  We record, for each task attempt, certain 
  statistics over each twelfth of the progress range.  You can change
  the number of intervals we divide the entire range of progress into
  by setting this property.  Higher values give more precision to the
  recorded data, but costs more memory in the job tracker at runtime.
  Each increment in this attribute costs 16 bytes per running task.
  </description>
</property>

<property>
  <name>mapreduce.job.userhistorylocation</name>
  <value></value>
  <description> User can specify a location to store the history files of 
  a particular job. If nothing is specified, the logs are stored in 
  output directory. The files are stored in "_logs/history/" in the directory.
  User can stop logging by giving the value "none". 
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.jobhistory.completed.location</name>
  <value></value>
  <description> The completed job history files are stored at this single well 
  known location. If nothing is specified, the files are stored at 
  ${mapreduce.jobtracker.jobhistory.location}/done.
  </description>
</property>

<property>
  <name>mapreduce.job.committer.setup.cleanup.needed</name>
  <value>true</value>
  <description> true, if job needs job-setup and job-cleanup.
                false, otherwise  
  </description>
</property>
<!-- i/o properties -->

<property>
  <name>mapreduce.task.io.sort.factor</name>
  <value>10</value>
  <description>The number of streams to merge at once while sorting
  files.  This determines the number of open file handles.</description>
</property>

<property>
  <name>mapreduce.task.io.sort.mb</name>
  <value>100</value>
  <description>The total amount of buffer memory to use while sorting 
  files, in megabytes.  By default, gives each merge stream 1MB, which
  should minimize seeks.</description>
</property>

<property>
  <name>mapreduce.map.sort.spill.percent</name>
  <value>0.80</value>
  <description>The soft limit in the serialization buffer. Once reached, a
  thread will begin to spill the contents to disk in the background. Note that
  collection will not block if this threshold is exceeded while a spill is
  already in progress, so spills may be larger than this threshold when it is
  set to less than .5</description>
</property>

<property>
  <name>mapreduce.jobtracker.address</name>
  <value>local</value>
  <description>The host and port that the MapReduce job tracker runs
  at.  If "local", then jobs are run in-process as a single map
  and reduce task.
  </description>
</property>

<property>
  <name>mapreduce.local.clientfactory.class.name</name>
  <value>org.apache.hadoop.mapred.LocalClientFactory</value>
  <description>This the client factory that is responsible for 
  creating local job runner client</description>
</property>

<property>
  <name>mapreduce.jobtracker.http.address</name>
  <value>0.0.0.0:50030</value>
  <description>
    The job tracker http server address and port the server will listen on.
    If the port is 0 then the server will start on a free port.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.handler.count</name>
  <value>10</value>
  <description>
    The number of server threads for the JobTracker. This should be roughly
    4% of the number of tasktracker nodes.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.report.address</name>
  <value>127.0.0.1:0</value>
  <description>The interface and port that task tracker server listens on. 
  Since it is only connected to by the tasks, it uses the local interface.
  EXPERT ONLY. Should only be changed if your host does not have the loopback 
  interface.</description>
</property>

<property>
  <name>mapreduce.cluster.local.dir</name>
  <value>${hadoop.tmp.dir}/mapred/local</value>
  <description>The local directory where MapReduce stores intermediate
  data files.  May be a comma-separated list of
  directories on different devices in order to spread disk i/o.
  Directories that do not exist are ignored.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.system.dir</name>
  <value>${hadoop.tmp.dir}/mapred/system</value>
  <description>The directory where MapReduce stores control files.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.staging.root.dir</name>
  <value>${hadoop.tmp.dir}/mapred/staging</value>
  <description>The root of the staging area for users' job files
  In practice, this should be the directory where users' home 
  directories are located (usually /user)
  </description>
</property>

<property>
  <name>mapreduce.cluster.temp.dir</name>
  <value>${hadoop.tmp.dir}/mapred/temp</value>
  <description>A shared directory for temporary files.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.local.dir.minspacestart</name>
  <value>0</value>
  <description>If the space in mapreduce.cluster.local.dir drops under this, 
  do not ask for more tasks.
  Value in bytes.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.local.dir.minspacekill</name>
  <value>0</value>
  <description>If the space in mapreduce.cluster.local.dir drops under this, 
    do not ask more tasks until all the current ones have finished and 
    cleaned up. Also, to save the rest of the tasks we have running, 
    kill one of them, to clean up some space. Start with the reduce tasks,
    then go with the ones that have finished the least.
    Value in bytes.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.expire.trackers.interval</name>
  <value>600000</value>
  <description>Expert: The time-interval, in miliseconds, after which
  a tasktracker is declared 'lost' if it doesn't send heartbeats.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.instrumentation</name>
  <value>org.apache.hadoop.mapred.TaskTrackerMetricsInst</value>
  <description>Expert: The instrumentation class to associate with each TaskTracker.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.resourcecalculatorplugin</name>
  <value></value>
  <description>
   Name of the class whose instance will be used to query resource information
   on the tasktracker.
   
   The class must be an instance of 
   org.apache.hadoop.util.ResourceCalculatorPlugin. If the value is null, the
   tasktracker attempts to use a class appropriate to the platform. 
   Currently, the only platform supported is Linux.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.taskmemorymanager.monitoringinterval</name>
  <value>5000</value>
  <description>The interval, in milliseconds, for which the tasktracker waits
   between two cycles of monitoring its tasks' memory usage. Used only if
   tasks' memory management is enabled via mapred.tasktracker.tasks.maxmemory.
   </description>
</property>

<property>
  <name>mapreduce.tasktracker.tasks.sleeptimebeforesigkill</name>
  <value>5000</value>
  <description>The time, in milliseconds, the tasktracker waits for sending a
  SIGKILL to a task, after it has been sent a SIGTERM. This is currently
  not used on WINDOWS where tasks are just sent a SIGTERM.
  </description>
</property>

<property>
  <name>mapreduce.job.maps</name>
  <value>2</value>
  <description>The default number of map tasks per job.
  Ignored when mapreduce.jobtracker.address is "local".  
  </description>
</property>

<property>
  <name>mapreduce.job.reduces</name>
  <value>1</value>
  <description>The default number of reduce tasks per job. Typically set to 99%
  of the cluster's reduce capacity, so that if a node fails the reduces can 
  still be executed in a single wave.
  Ignored when mapreduce.jobtracker.address is "local".
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.restart.recover</name>
  <value>false</value>
  <description>"true" to enable (job) recovery upon restart,
               "false" to start afresh
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.jobhistory.block.size</name>
  <value>3145728</value>
  <description>The block size of the job history file. Since the job recovery
               uses job history, its important to dump job history to disk as 
               soon as possible. Note that this is an expert level parameter.
               The default value is set to 3 MB.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.taskscheduler</name>
  <value>org.apache.hadoop.mapred.JobQueueTaskScheduler</value>
  <description>The class responsible for scheduling the tasks.</description>
</property>

<property>
  <name>mapreduce.job.running.map.limit</name>
  <value>0</value>
  <description>The maximum number of simultaneous map tasks per job.
  There is no limit if this value is 0 or negative.
  </description>
</property>

<property>
  <name>mapreduce.job.running.reduce.limit</name>
  <value>0</value>
  <description>The maximum number of simultaneous reduce tasks per job.
  There is no limit if this value is 0 or negative.
  </description>
</property>

<property>
  <name>mapreduce.job.reducer.preempt.delay.sec</name>
  <value>0</value>
  <description>The threshold in terms of seconds after which an unsatisfied mapper 
  request triggers reducer preemption to free space. Default 0 implies that the 
  reduces should be preempted immediately after allocation if there is currently no
  room for newly allocated mappers.
  </description>
</property>

<property>
    <name>mapreduce.job.max.split.locations</name>
    <value>10</value>
    <description>The max number of block locations to store for each split for 
    locality calculation.
    </description>
</property>

<property>
  <name>mapreduce.job.split.metainfo.maxsize</name>
  <value>10000000</value>
  <description>The maximum permissible size of the split metainfo file. 
  The JobTracker won't attempt to read split metainfo files bigger than
  the configured value.
  No limits if set to -1.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob</name>
  <value></value>
  <description>The maximum number of running tasks for a job before
  it gets preempted. No limits if undefined.
  </description>
</property>

<property>
  <name>mapreduce.map.maxattempts</name>
  <value>4</value>
  <description>Expert: The maximum number of attempts per map task.
  In other words, framework will try to execute a map task these many number
  of times before giving up on it.
  </description>
</property>

<property>
  <name>mapreduce.reduce.maxattempts</name>
  <value>4</value>
  <description>Expert: The maximum number of attempts per reduce task.
  In other words, framework will try to execute a reduce task these many number
  of times before giving up on it.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.fetch.retry.enabled</name>
  <value>${yarn.nodemanager.recovery.enabled}</value>
  <description>Set to enable fetch retry during host restart.</description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.fetch.retry.interval-ms</name>
  <value>1000</value>
  <description>Time of interval that fetcher retry to fetch again when some 
  non-fatal failure happens because of some events like NM restart.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.fetch.retry.timeout-ms</name>
  <value>30000</value>
  <description>Timeout value for fetcher to retry to fetch again when some 
  non-fatal failure happens because of some events like NM restart.</description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.retry-delay.max.ms</name>
  <value>60000</value>
  <description>The maximum number of ms the reducer will delay before retrying
  to download map data.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.parallelcopies</name>
  <value>5</value>
  <description>The default number of parallel transfers run by reduce
  during the copy(shuffle) phase.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.connect.timeout</name>
  <value>180000</value>
  <description>Expert: The maximum amount of time (in milli seconds) reduce
  task spends in trying to connect to a tasktracker for getting map output.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.read.timeout</name>
  <value>180000</value>
  <description>Expert: The maximum amount of time (in milli seconds) reduce
  task waits for map output data to be available for reading after obtaining
  connection.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.connection-keep-alive.enable</name>
  <value>false</value>
  <description>set to true to support keep-alive connections.</description>
</property>

<property>
  <name>mapreduce.shuffle.connection-keep-alive.timeout</name>
  <value>5</value>
  <description>The number of seconds a shuffle client attempts to retain
   http connection. Refer "Keep-Alive: timeout=" header in
   Http specification
  </description>
</property>

<property>
  <name>mapreduce.task.timeout</name>
  <value>600000</value>
  <description>The number of milliseconds before a task will be
  terminated if it neither reads an input, writes an output, nor
  updates its status string.  A value of 0 disables the timeout.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.map.tasks.maximum</name>
  <value>2</value>
  <description>The maximum number of map tasks that will be run
  simultaneously by a task tracker.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.reduce.tasks.maximum</name>
  <value>2</value>
  <description>The maximum number of reduce tasks that will be run
  simultaneously by a task tracker.
  </description>
</property>

<property>
  <name>mapreduce.map.memory.mb</name>
  <value>1024</value>
  <description>The amount of memory to request from the scheduler for each
  map task.
  </description>
</property>

<property>
  <name>mapreduce.map.cpu.vcores</name>
  <value>1</value>
  <description>The number of virtual cores to request from the scheduler for
  each map task.
  </description>
</property>

<property>
  <name>mapreduce.reduce.memory.mb</name>
  <value>1024</value>
  <description>The amount of memory to request from the scheduler for each
  reduce task.
  </description>
</property>

<property>
  <name>mapreduce.reduce.cpu.vcores</name>
  <value>1</value>
  <description>The number of virtual cores to request from the scheduler for
  each reduce task.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.retiredjobs.cache.size</name>
  <value>1000</value>
  <description>The number of retired job status to keep in the cache.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.outofband.heartbeat</name>
  <value>false</value>
  <description>Expert: Set this to true to let the tasktracker send an 
  out-of-band heartbeat on task-completion for better latency.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.jobhistory.lru.cache.size</name>
  <value>5</value>
  <description>The number of job history files loaded in memory. The jobs are 
  loaded when they are first accessed. The cache is cleared based on LRU.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.instrumentation</name>
  <value>org.apache.hadoop.mapred.JobTrackerMetricsInst</value>
  <description>Expert: The instrumentation class to associate with each JobTracker.
  </description>
</property>

<property>
  <name>mapred.child.java.opts</name>
  <value>-Xmx200m</value>
  <description>Java opts for the task processes.
  The following symbol, if present, will be interpolated: @taskid@ is replaced 
  by current TaskID. Any other occurrences of '@' will go unchanged.
  For example, to enable verbose gc logging to a file named for the taskid in
  /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of:
        -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
  
  Usage of -Djava.library.path can cause programs to no longer function if
  hadoop native libraries are used. These values should instead be set as part 
  of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and 
  mapreduce.reduce.env config settings. 
  </description>
</property>

<!-- This is commented out so that it won't override mapred.child.java.opts.
<property>
  <name>mapreduce.map.java.opts</name>
  <value></value>
  <description>Java opts only for the child processes that are maps. If set,
  this will be used instead of mapred.child.java.opts.
  </description>
</property>
-->

<!-- This is commented out so that it won't override mapred.child.java.opts.
<property>
  <name>mapreduce.reduce.java.opts</name>
  <value></value>
  <description>Java opts only for the child processes that are reduces. If set,
  this will be used instead of mapred.child.java.opts.
  </description>
</property>
-->

<property>
  <name>mapred.child.env</name>
  <value></value>
  <description>User added environment variables for the task processes.
  Example :
  1) A=foo  This will set the env variable A to foo
  2) B=$B:c This is inherit nodemanager's B env variable on Unix.
  3) B=%B%;c This is inherit nodemanager's B env variable on Windows.
  </description>
</property>

<!-- This is commented out so that it won't override mapred.child.env.
<property>
  <name>mapreduce.map.env</name>
  <value></value>
  <description>User added environment variables for the map task processes.
  </description>
</property>
-->

<!-- This is commented out so that it won't override mapred.child.env.
<property>
  <name>mapreduce.reduce.env</name>
  <value></value>
  <description>User added environment variables for the reduce task processes.
  </description>
</property>
-->

<property>
  <name>mapreduce.admin.user.env</name>
  <value></value>
  <description>
  Expert: Additional execution environment entries for 
  map and reduce task processes. This is not an additive property.
  You must preserve the original value if you want your map and
  reduce tasks to have access to native libraries (compression, etc). 
  When this value is empty, the command to set execution 
  envrionment will be OS dependent: 
  For linux, use LD_LIBRARY_PATH=$HADOOP_COMMON_HOME/lib/native.
  For windows, use PATH = %PATH%;%HADOOP_COMMON_HOME%\\bin.
  </description>
</property>

<property>
  <name>mapreduce.map.log.level</name>
  <value>INFO</value>
  <description>The logging level for the map task. The allowed levels are:
  OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
  The setting here could be overridden if "mapreduce.job.log4j-properties-file"
  is set.
  </description>
</property>

<property>
  <name>mapreduce.reduce.log.level</name>
  <value>INFO</value>
  <description>The logging level for the reduce task. The allowed levels are:
  OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
  The setting here could be overridden if "mapreduce.job.log4j-properties-file"
  is set.
  </description>
</property>

<property>
  <name>mapreduce.map.cpu.vcores</name>
  <value>1</value>
  <description>
      The number of virtual cores required for each map task.
  </description>
</property>

<property>
  <name>mapreduce.reduce.cpu.vcores</name>
  <value>1</value>
  <description>
      The number of virtual cores required for each reduce task.
  </description>
</property>

<property>
  <name>mapreduce.reduce.merge.inmem.threshold</name>
  <value>1000</value>
  <description>The threshold, in terms of the number of files 
  for the in-memory merge process. When we accumulate threshold number of files
  we initiate the in-memory merge and spill to disk. A value of 0 or less than
  0 indicates we want to DON'T have any threshold and instead depend only on
  the ramfs's memory consumption to trigger the merge.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.merge.percent</name>
  <value>0.66</value>
  <description>The usage threshold at which an in-memory merge will be
  initiated, expressed as a percentage of the total memory allocated to
  storing in-memory map outputs, as defined by
  mapreduce.reduce.shuffle.input.buffer.percent.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
  <value>0.70</value>
  <description>The percentage of memory to be allocated from the maximum heap
  size to storing map outputs during the shuffle.
  </description>
</property>

<property>
  <name>mapreduce.reduce.input.buffer.percent</name>
  <value>0.0</value>
  <description>The percentage of memory- relative to the maximum heap size- to
  retain map outputs during the reduce. When the shuffle is concluded, any
  remaining map outputs in memory must consume less than this threshold before
  the reduce can begin.
  </description>
</property>

<property>
  <name>mapreduce.reduce.shuffle.memory.limit.percent</name>
  <value>0.25</value>
  <description>Expert: Maximum percentage of the in-memory limit that a
  single shuffle can consume</description>
</property>

<property>
  <name>mapreduce.shuffle.ssl.enabled</name>
  <value>false</value>
  <description>
    Whether to use SSL for for the Shuffle HTTP endpoints.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.ssl.file.buffer.size</name>
  <value>65536</value>
  <description>Buffer size for reading spills from file when using SSL.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.max.connections</name>
  <value>0</value>
  <description>Max allowed connections for the shuffle.  Set to 0 (zero)
               to indicate no limit on the number of connections.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.max.threads</name>
  <value>0</value>
  <description>Max allowed threads for serving shuffle connections. Set to zero
  to indicate the default of 2 times the number of available
  processors (as reported by Runtime.availableProcessors()). Netty is used to
  serve requests, so a thread is not needed for each connection.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.transferTo.allowed</name>
  <value></value>
  <description>This option can enable/disable using nio transferTo method in 
  the shuffle phase. NIO transferTo does not perform well on windows in the 
  shuffle phase. Thus, with this configuration property it is possible to 
  disable it, in which case custom transfer method will be used. Recommended 
  value is false when running Hadoop on Windows. For Linux, it is recommended 
  to set it to true. If nothing is set then the default value is false for 
  Windows, and true for Linux.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.transfer.buffer.size</name>
  <value>131072</value>
  <description>This property is used only if 
  mapreduce.shuffle.transferTo.allowed is set to false. In that case, 
  this property defines the size of the buffer used in the buffer copy code
  for the shuffle phase. The size of this buffer determines the size of the IO
  requests.
  </description>
</property>

<property>
  <name>mapreduce.reduce.markreset.buffer.percent</name>
  <value>0.0</value>
  <description>The percentage of memory -relative to the maximum heap size- to
  be used for caching values when using the mark-reset functionality.
  </description>
</property>

<property>
  <name>mapreduce.map.speculative</name>
  <value>true</value>
  <description>If true, then multiple instances of some map tasks 
               may be executed in parallel.</description>
</property>

<property>
  <name>mapreduce.reduce.speculative</name>
  <value>true</value>
  <description>If true, then multiple instances of some reduce tasks 
               may be executed in parallel.</description>
</property>

<property>
  <name>mapreduce.job.speculative.speculative-cap-running-tasks</name>
  <value>0.1</value>
  <description>The max percent (0-1) of running tasks that
  can be speculatively re-executed at any time.</description>
</property>

<property>
  <name>mapreduce.job.speculative.speculative-cap-total-tasks</name>
  <value>0.01</value>
  <description>The max percent (0-1) of all tasks that
  can be speculatively re-executed at any time.</description>
</property>

<property>
  <name>mapreduce.job.speculative.minimum-allowed-tasks</name>
  <value>10</value>
  <description>The minimum allowed tasks that
  can be speculatively re-executed at any time.</description>
</property>

<property>
  <name>mapreduce.job.speculative.retry-after-no-speculate</name>
  <value>1000</value>
  <description>The waiting time(ms) to do next round of speculation
  if there is no task speculated in this round.</description>
</property>

<property>
  <name>mapreduce.job.speculative.retry-after-speculate</name>
  <value>15000</value>
  <description>The waiting time(ms) to do next round of speculation
  if there are tasks speculated in this round.</description>
</property>

<property>
  <name>mapreduce.job.map.output.collector.class</name>
  <value>org.apache.hadoop.mapred.MapTask$MapOutputBuffer</value>
  <description>
    The MapOutputCollector implementation(s) to use. This may be a comma-separated
    list of class names, in which case the map task will try to initialize each
    of the collectors in turn. The first to successfully initialize will be used.
  </description>
</property>
 
<property>
  <name>mapreduce.job.speculative.slowtaskthreshold</name>
  <value>1.0</value>
  <description>The number of standard deviations by which a task's
  ave progress-rates must be lower than the average of all running tasks'
  for the task to be considered too slow.
  </description>
</property>

<property>
  <name>mapreduce.job.jvm.numtasks</name>
  <value>1</value>
  <description>How many tasks to run per jvm. If set to -1, there is
  no limit. 
  </description>
</property>

<property>
  <name>mapreduce.job.ubertask.enable</name>
  <value>false</value>
  <description>Whether to enable the small-jobs "ubertask" optimization,
  which runs "sufficiently small" jobs sequentially within a single JVM.
  "Small" is defined by the following maxmaps, maxreduces, and maxbytes
  settings. Note that configurations for application masters also affect
  the "Small" definition - yarn.app.mapreduce.am.resource.mb must be
  larger than both mapreduce.map.memory.mb and mapreduce.reduce.memory.mb,
  and yarn.app.mapreduce.am.resource.cpu-vcores must be larger than
  both mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores to enable
  ubertask. Users may override this value.
  </description>
</property>

<property>
  <name>mapreduce.job.ubertask.maxmaps</name>
  <value>9</value>
  <description>Threshold for number of maps, beyond which job is considered
  too big for the ubertasking optimization.  Users may override this value,
  but only downward.
  </description>
</property>

<property>
  <name>mapreduce.job.ubertask.maxreduces</name>
  <value>1</value>
  <description>Threshold for number of reduces, beyond which job is considered
  too big for the ubertasking optimization.  CURRENTLY THE CODE CANNOT SUPPORT
  MORE THAN ONE REDUCE and will ignore larger values.  (Zero is a valid max,
  however.)  Users may override this value, but only downward.
  </description>
</property>

<property>
  <name>mapreduce.job.ubertask.maxbytes</name>
  <value></value>
  <description>Threshold for number of input bytes, beyond which job is
  considered too big for the ubertasking optimization.  If no value is
  specified, dfs.block.size is used as a default.  Be sure to specify a
  default value in mapred-site.xml if the underlying filesystem is not HDFS.
  Users may override this value, but only downward.
  </description>
</property>

<property>
    <name>mapreduce.job.emit-timeline-data</name>
    <value>false</value>
    <description>Specifies if the Application Master should emit timeline data
    to the timeline server. Individual jobs can override this value.
    </description>
</property>

<property>
  <name>mapreduce.input.fileinputformat.split.minsize</name>
  <value>0</value>
  <description>The minimum size chunk that map input should be split
  into.  Note that some file formats may have minimum split sizes that
  take priority over this setting.</description>
</property>

<property>
  <name>mapreduce.input.fileinputformat.list-status.num-threads</name>
  <value>1</value>
  <description>The number of threads to use to list and fetch block locations
  for the specified input paths. Note: multiple threads should not be used
  if a custom non thread-safe path filter is used.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.maxtasks.perjob</name>
  <value>-1</value>
  <description>The maximum number of tasks for a single job.
  A value of -1 indicates that there is no maximum.  </description>
</property>

<property>
  <name>mapreduce.input.lineinputformat.linespermap</name>
  <value>1</value>
  <description>When using NLineInputFormat, the number of lines of input data
  to include in each split.</description>
</property>

<property>
  <name>mapreduce.client.submit.file.replication</name>
  <value>10</value>
  <description>The replication level for submitted job files.  This
  should be around the square root of the number of nodes.
  </description>
</property>


<property>
  <name>mapreduce.tasktracker.dns.interface</name>
  <value>default</value>
  <description>The name of the Network Interface from which a task
  tracker should report its IP address.
  </description>
 </property>
 
<property>
  <name>mapreduce.tasktracker.dns.nameserver</name>
  <value>default</value>
  <description>The host name or IP address of the name server (DNS)
  which a TaskTracker should use to determine the host name used by
  the JobTracker for communication and display purposes.
  </description>
 </property>
 
<property>
  <name>mapreduce.tasktracker.http.threads</name>
  <value>40</value>
  <description>The number of worker threads that for the http server. This is
               used for map output fetching
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.http.address</name>
  <value>0.0.0.0:50060</value>
  <description>
    The task tracker http server address and port.
    If the port is 0 then the server will start on a free port.
  </description>
</property>

<property>
  <name>mapreduce.task.files.preserve.failedtasks</name>
  <value>false</value>
  <description>Should the files for failed tasks be kept. This should only be 
               used on jobs that are failing, because the storage is never
               reclaimed. It also prevents the map outputs from being erased
               from the reduce directory as they are consumed.</description>
</property>


<!-- 
  <property>
  <name>mapreduce.task.files.preserve.filepattern</name>
  <value>.*_m_123456_0</value>
  <description>Keep all files from tasks whose task names match the given
               regular expression. Defaults to none.</description>
  </property>
-->

<property>
  <name>mapreduce.output.fileoutputformat.compress</name>
  <value>false</value>
  <description>Should the job outputs be compressed?
  </description>
</property>

<property>
  <name>mapreduce.output.fileoutputformat.compress.type</name>
  <value>RECORD</value>
  <description>If the job outputs are to compressed as SequenceFiles, how should
               they be compressed? Should be one of NONE, RECORD or BLOCK.
  </description>
</property>

<property>
  <name>mapreduce.output.fileoutputformat.compress.codec</name>
  <value>org.apache.hadoop.io.compress.DefaultCodec</value>
  <description>If the job outputs are compressed, how should they be compressed?
  </description>
</property>

<property>
  <name>mapreduce.map.output.compress</name>
  <value>false</value>
  <description>Should the outputs of the maps be compressed before being
               sent across the network. Uses SequenceFile compression.
  </description>
</property>

<property>
  <name>mapreduce.map.output.compress.codec</name>
  <value>org.apache.hadoop.io.compress.DefaultCodec</value>
  <description>If the map outputs are compressed, how should they be 
               compressed?
  </description>
</property>

<property>
  <name>map.sort.class</name>
  <value>org.apache.hadoop.util.QuickSort</value>
  <description>The default sort class for sorting keys.
  </description>
</property>

<property>
  <name>mapreduce.task.userlog.limit.kb</name>
  <value>0</value>
  <description>The maximum size of user-logs of each task in KB. 0 disables the cap.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.container.log.limit.kb</name>
  <value>0</value>
  <description>The maximum size of the MRAppMaster attempt container logs in KB.
    0 disables the cap.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.task.container.log.backups</name>
  <value>0</value>
  <description>Number of backup files for task logs when using
    ContainerRollingLogAppender (CRLA). See
    org.apache.log4j.RollingFileAppender.maxBackupIndex. By default,
    ContainerLogAppender (CLA) is used, and container logs are not rolled. CRLA
    is enabled for tasks when both mapreduce.task.userlog.limit.kb and
    yarn.app.mapreduce.task.container.log.backups are greater than zero.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.container.log.backups</name>
  <value>0</value>
  <description>Number of backup files for the ApplicationMaster logs when using
    ContainerRollingLogAppender (CRLA). See
    org.apache.log4j.RollingFileAppender.maxBackupIndex. By default,
    ContainerLogAppender (CLA) is used, and container logs are not rolled. CRLA
    is enabled for the ApplicationMaster when both
    mapreduce.task.userlog.limit.kb and
    yarn.app.mapreduce.am.container.log.backups are greater than zero.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.shuffle.log.separate</name>
  <value>true</value>
  <description>If enabled ('true') logging generated by the client-side shuffle
    classes in a reducer will be written in a dedicated log file
    'syslog.shuffle' instead of 'syslog'.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.shuffle.log.limit.kb</name>
  <value>0</value>
  <description>Maximum size of the syslog.shuffle file in kilobytes
    (0 for no limit).
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.shuffle.log.backups</name>
  <value>0</value>
  <description>If yarn.app.mapreduce.shuffle.log.limit.kb and
    yarn.app.mapreduce.shuffle.log.backups are greater than zero
    then a ContainerRollngLogAppender is used instead of ContainerLogAppender
    for syslog.shuffle. See
    org.apache.log4j.RollingFileAppender.maxBackupIndex
  </description>
</property>

<property>
  <name>mapreduce.job.userlog.retain.hours</name>
  <value>24</value>
  <description>The maximum time, in hours, for which the user-logs are to be 
               retained after the job completion.
  </description>
</property>

<property>
  <name>mapreduce.jobtracker.hosts.filename</name>
  <value></value>
  <description>Names a file that contains the list of nodes that may
  connect to the jobtracker.  If the value is empty, all hosts are
  permitted.</description>
</property>

<property>
  <name>mapreduce.jobtracker.hosts.exclude.filename</name>
  <value></value>
  <description>Names a file that contains the list of hosts that
  should be excluded by the jobtracker.  If the value is empty, no
  hosts are excluded.</description>
</property>

<property>
  <name>mapreduce.jobtracker.heartbeats.in.second</name>
  <value>100</value>
  <description>Expert: Approximate number of heart-beats that could arrive 
               at JobTracker in a second. Assuming each RPC can be processed 
               in 10msec, the default value is made 100 RPCs in a second.
  </description>
</property> 

<property>
  <name>mapreduce.jobtracker.tasktracker.maxblacklists</name>
  <value>4</value>
  <description>The number of blacklists for a taskTracker by various jobs
               after which the task tracker could be blacklisted across
               all jobs. The tracker will be given a tasks later
               (after a day). The tracker will become a healthy
               tracker after a restart.
  </description>
</property> 

<property>
  <name>mapreduce.job.maxtaskfailures.per.tracker</name>
  <value>3</value>
  <description>The number of task-failures on a tasktracker of a given job 
               after which new tasks of that job aren't assigned to it. It
               MUST be less than mapreduce.map.maxattempts and
               mapreduce.reduce.maxattempts otherwise the failed task will
               never be tried on a different node.
  </description>
</property>

<property>
  <name>mapreduce.client.output.filter</name>
  <value>FAILED</value>
  <description>The filter for controlling the output of the task's userlogs sent
               to the console of the JobClient. 
               The permissible options are: NONE, KILLED, FAILED, SUCCEEDED and 
               ALL.
  </description>
</property>

  <property>
    <name>mapreduce.client.completion.pollinterval</name>
    <value>5000</value>
    <description>The interval (in milliseconds) between which the JobClient
    polls the JobTracker for updates about job status. You may want to set this
    to a lower value to make tests run faster on a single node system. Adjusting
    this value in production may lead to unwanted client-server traffic.
    </description>
  </property>

  <property>
    <name>mapreduce.client.progressmonitor.pollinterval</name>
    <value>1000</value>
    <description>The interval (in milliseconds) between which the JobClient
    reports status to the console and checks for job completion. You may want to set this
    to a lower value to make tests run faster on a single node system. Adjusting
    this value in production may lead to unwanted client-server traffic.
    </description>
  </property>

  <property>
    <name>mapreduce.jobtracker.persist.jobstatus.active</name>
    <value>true</value>
    <description>Indicates if persistency of job status information is
      active or not.
    </description>
  </property>

  <property>
  <name>mapreduce.jobtracker.persist.jobstatus.hours</name>
  <value>1</value>
  <description>The number of hours job status information is persisted in DFS.
    The job status information will be available after it drops of the memory
    queue and between jobtracker restarts. With a zero value the job status
    information is not persisted at all in DFS.
  </description>
</property>

  <property>
    <name>mapreduce.jobtracker.persist.jobstatus.dir</name>
    <value>/jobtracker/jobsInfo</value>
    <description>The directory where the job status information is persisted
      in a file system to be available after it drops of the memory queue and
      between jobtracker restarts.
    </description>
  </property>

  <property>
    <name>mapreduce.task.profile</name>
    <value>false</value>
    <description>To set whether the system should collect profiler
     information for some of the tasks in this job? The information is stored
     in the user log directory. The value is "true" if task profiling
     is enabled.</description>
  </property>

  <property>
    <name>mapreduce.task.profile.maps</name>
    <value>0-2</value>
    <description> To set the ranges of map tasks to profile.
    mapreduce.task.profile has to be set to true for the value to be accounted.
    </description>
  </property>

  <property>
    <name>mapreduce.task.profile.reduces</name>
    <value>0-2</value>
    <description> To set the ranges of reduce tasks to profile.
    mapreduce.task.profile has to be set to true for the value to be accounted.
    </description>
  </property>

  <property>
    <name>mapreduce.task.profile.params</name>
    <value>-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s</value>
    <description>JVM profiler parameters used to profile map and reduce task
      attempts. This string may contain a single format specifier %s that will
      be replaced by the path to profile.out in the task attempt log directory.
      To specify different profiling options for map tasks and reduce tasks,
      more specific parameters mapreduce.task.profile.map.params and
      mapreduce.task.profile.reduce.params should be used.</description>
  </property>

  <property>
    <name>mapreduce.task.profile.map.params</name>
    <value>${mapreduce.task.profile.params}</value>
    <description>Map-task-specific JVM profiler parameters. See
      mapreduce.task.profile.params</description>
  </property>

  <property>
    <name>mapreduce.task.profile.reduce.params</name>
    <value>${mapreduce.task.profile.params}</value>
    <description>Reduce-task-specific JVM profiler parameters. See
      mapreduce.task.profile.params</description>
  </property>

  <property>
    <name>mapreduce.task.skip.start.attempts</name>
    <value>2</value>
    <description> The number of Task attempts AFTER which skip mode 
    will be kicked off. When skip mode is kicked off, the 
    tasks reports the range of records which it will process 
    next, to the TaskTracker. So that on failures, TT knows which 
    ones are possibly the bad records. On further executions, 
    those are skipped.
    </description>
  </property>
  
  <property>
    <name>mapreduce.map.skip.proc.count.autoincr</name>
    <value>true</value>
    <description> The flag which if set to true, 
    SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS is incremented 
    by MapRunner after invoking the map function. This value must be set to 
    false for applications which process the records asynchronously 
    or buffer the input records. For example streaming. 
    In such cases applications should increment this counter on their own.
    </description>
  </property>
  
  <property>
    <name>mapreduce.reduce.skip.proc.count.autoincr</name>
    <value>true</value>
    <description> The flag which if set to true, 
    SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS is incremented 
    by framework after invoking the reduce function. This value must be set to 
    false for applications which process the records asynchronously 
    or buffer the input records. For example streaming. 
    In such cases applications should increment this counter on their own.
    </description>
  </property>
  
  <property>
    <name>mapreduce.job.skip.outdir</name>
    <value></value>
    <description> If no value is specified here, the skipped records are 
    written to the output directory at _logs/skip.
    User can stop writing skipped records by giving the value "none". 
    </description>
  </property>

  <property>
    <name>mapreduce.map.skip.maxrecords</name>
    <value>0</value>
    <description> The number of acceptable skip records surrounding the bad 
    record PER bad record in mapper. The number includes the bad record as well.
    To turn the feature of detection/skipping of bad records off, set the 
    value to 0.
    The framework tries to narrow down the skipped range by retrying  
    until this threshold is met OR all attempts get exhausted for this task. 
    Set the value to Long.MAX_VALUE to indicate that framework need not try to 
    narrow down. Whatever records(depends on application) get skipped are 
    acceptable.
    </description>
  </property>
  
  <property>
    <name>mapreduce.reduce.skip.maxgroups</name>
    <value>0</value>
    <description> The number of acceptable skip groups surrounding the bad 
    group PER bad group in reducer. The number includes the bad group as well.
    To turn the feature of detection/skipping of bad groups off, set the 
    value to 0.
    The framework tries to narrow down the skipped range by retrying  
    until this threshold is met OR all attempts get exhausted for this task. 
    Set the value to Long.MAX_VALUE to indicate that framework need not try to 
    narrow down. Whatever groups(depends on application) get skipped are 
    acceptable.
    </description>
  </property>

  <property>
    <name>mapreduce.ifile.readahead</name>
    <value>true</value>
    <description>Configuration key to enable/disable IFile readahead.
    </description>
  </property>

  <property>
    <name>mapreduce.ifile.readahead.bytes</name>
    <value>4194304</value>
    <description>Configuration key to set the IFile readahead length in bytes.
    </description>
  </property>
  
<!-- Proxy Configuration -->
<property>
  <name>mapreduce.jobtracker.taskcache.levels</name>
  <value>2</value>
  <description> This is the max level of the task cache. For example, if
    the level is 2, the tasks cached are at the host level and at the rack
    level.
  </description>
</property>

<property>
  <name>mapreduce.job.queuename</name>
  <value>default</value>
  <description> Queue to which a job is submitted. This must match one of the
    queues defined in mapred-queues.xml for the system. Also, the ACL setup
    for the queue must allow the current user to submit a job to the queue.
    Before specifying a queue, ensure that the system is configured with 
    the queue, and access is allowed for submitting jobs to the queue.
  </description>
</property>

  <property>
    <name>mapreduce.job.tags</name>
    <value></value>
    <description> Tags for the job that will be passed to YARN at submission 
      time. Queries to YARN for applications can filter on these tags.
    </description>
  </property>

<property>
  <name>mapreduce.cluster.acls.enabled</name>
  <value>false</value>
  <description> Specifies whether ACLs should be checked
    for authorization of users for doing various queue and job level operations.
    ACLs are disabled by default. If enabled, access control checks are made by
    JobTracker and TaskTracker when requests are made by users for queue
    operations like submit job to a queue and kill a job in the queue and job
    operations like viewing the job-details (See mapreduce.job.acl-view-job)
    or for modifying the job (See mapreduce.job.acl-modify-job) using
    Map/Reduce APIs, RPCs or via the console and web user interfaces.
    For enabling this flag(mapreduce.cluster.acls.enabled), this is to be set
    to true in mapred-site.xml on JobTracker node and on all TaskTracker nodes.
  </description>
</property>

<property>
  <name>mapreduce.job.acl-modify-job</name>
  <value> </value>
  <description> Job specific access-control list for 'modifying' the job. It
    is only used if authorization is enabled in Map/Reduce by setting the
    configuration property mapreduce.cluster.acls.enabled to true.
    This specifies the list of users and/or groups who can do modification
    operations on the job. For specifying a list of users and groups the
    format to use is "user1,user2 group1,group". If set to '*', it allows all
    users/groups to modify this job. If set to ' '(i.e. space), it allows
    none. This configuration is used to guard all the modifications with respect
    to this job and takes care of all the following operations:
      o killing this job
      o killing a task of this job, failing a task of this job
      o setting the priority of this job
    Each of these operations are also protected by the per-queue level ACL
    "acl-administer-jobs" configured via mapred-queues.xml. So a caller should
    have the authorization to satisfy either the queue-level ACL or the
    job-level ACL.

    Irrespective of this ACL configuration, (a) job-owner, (b) the user who
    started the cluster, (c) members of an admin configured supergroup
    configured via mapreduce.cluster.permissions.supergroup and (d) queue
    administrators of the queue to which this job was submitted to configured
    via acl-administer-jobs for the specific queue in mapred-queues.xml can
    do all the modification operations on a job.

    By default, nobody else besides job-owner, the user who started the cluster,
    members of supergroup and queue administrators can perform modification
    operations on a job.
  </description>
</property>

<property>
  <name>mapreduce.job.acl-view-job</name>
  <value> </value>
  <description> Job specific access-control list for 'viewing' the job. It is
    only used if authorization is enabled in Map/Reduce by setting the
    configuration property mapreduce.cluster.acls.enabled to true.
    This specifies the list of users and/or groups who can view private details
    about the job. For specifying a list of users and groups the
    format to use is "user1,user2 group1,group". If set to '*', it allows all
    users/groups to modify this job. If set to ' '(i.e. space), it allows
    none. This configuration is used to guard some of the job-views and at
    present only protects APIs that can return possibly sensitive information
    of the job-owner like
      o job-level counters
      o task-level counters
      o tasks' diagnostic information
      o task-logs displayed on the TaskTracker web-UI and
      o job.xml showed by the JobTracker's web-UI
    Every other piece of information of jobs is still accessible by any other
    user, for e.g., JobStatus, JobProfile, list of jobs in the queue, etc.

    Irrespective of this ACL configuration, (a) job-owner, (b) the user who
    started the cluster, (c) members of an admin configured supergroup
    configured via mapreduce.cluster.permissions.supergroup and (d) queue
    administrators of the queue to which this job was submitted to configured
    via acl-administer-jobs for the specific queue in mapred-queues.xml can
    do all the view operations on a job.

    By default, nobody else besides job-owner, the user who started the
    cluster, memebers of supergroup and queue administrators can perform
    view operations on a job.
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.indexcache.mb</name>
  <value>10</value>
  <description> The maximum memory that a task tracker allows for the 
    index cache that is used when serving map outputs to reducers.
  </description>
</property>

<property>
  <name>mapreduce.job.token.tracking.ids.enabled</name>
  <value>false</value>
  <description>Whether to write tracking ids of tokens to
    job-conf. When true, the configuration property
    "mapreduce.job.token.tracking.ids" is set to the token-tracking-ids of
    the job</description>
</property>

<property>
  <name>mapreduce.job.token.tracking.ids</name>
  <value></value>
  <description>When mapreduce.job.token.tracking.ids.enabled is
    set to true, this is set by the framework to the
    token-tracking-ids used by the job.</description>
</property>

<property>
  <name>mapreduce.task.merge.progress.records</name>
  <value>10000</value>
  <description> The number of records to process during merge before
   sending a progress notification to the TaskTracker.
  </description>
</property>

<property>
  <name>mapreduce.task.combine.progress.records</name>
  <value>10000</value>
  <description> The number of records to process during combine output collection
   before sending a progress notification.
  </description>
</property>

<property>
  <name>mapreduce.job.reduce.slowstart.completedmaps</name>
  <value>0.05</value>
  <description>Fraction of the number of maps in the job which should be 
  complete before reduces are scheduled for the job. 
  </description>
</property>

<property>
<name>mapreduce.job.complete.cancel.delegation.tokens</name>
  <value>true</value>
  <description> if false - do not unregister/cancel delegation tokens from 
    renewal, because same tokens may be used by spawned jobs
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.taskcontroller</name>
  <value>org.apache.hadoop.mapred.DefaultTaskController</value>
  <description>TaskController which is used to launch and manage task execution 
  </description>
</property>

<property>
  <name>mapreduce.tasktracker.group</name>
  <value></value>
  <description>Expert: Group to which TaskTracker belongs. If 
   LinuxTaskController is configured via mapreduce.tasktracker.taskcontroller,
   the group owner of the task-controller binary should be same as this group.
  </description>
</property>

<property>
  <name>mapreduce.shuffle.port</name>
  <value>13562</value>
  <description>Default port that the ShuffleHandler will run on. ShuffleHandler 
   is a service run at the NodeManager to facilitate transfers of intermediate 
   Map outputs to requesting Reducers.
  </description>
</property>

<property>
  <name>mapreduce.job.reduce.shuffle.consumer.plugin.class</name>
  <value>org.apache.hadoop.mapreduce.task.reduce.Shuffle</value>
  <description>
  Name of the class whose instance will be used
  to send shuffle requests by reducetasks of this job.
  The class must be an instance of org.apache.hadoop.mapred.ShuffleConsumerPlugin.
  </description>
</property>

<!--  Node health script variables -->

<property>
  <name>mapreduce.tasktracker.healthchecker.script.path</name>
  <value></value>
  <description>Absolute path to the script which is
  periodicallyrun by the node health monitoring service to determine if
  the node is healthy or not. If the value of this key is empty or the
  file does not exist in the location configured here, the node health
  monitoring service is not started.</description>
</property>

<property>
  <name>mapreduce.tasktracker.healthchecker.interval</name>
  <value>60000</value>
  <description>Frequency of the node health script to be run,
  in milliseconds</description>
</property>

<property>
  <name>mapreduce.tasktracker.healthchecker.script.timeout</name>
  <value>600000</value>
  <description>Time after node health script should be killed if 
  unresponsive and considered that the script has failed.</description>
</property>

<property>
  <name>mapreduce.tasktracker.healthchecker.script.args</name>
  <value></value>
  <description>List of arguments which are to be passed to 
  node health script when it is being launched comma seperated.
  </description>
</property>

<!--  end of node health script variables -->

<!-- MR YARN Application properties -->

<property>
 <name>mapreduce.job.counters.limit</name>
  <value>120</value>
  <description>Limit on the number of user counters allowed per job.
  </description>
</property>

<property>
  <name>mapreduce.framework.name</name>
  <value>local</value>
  <description>The runtime framework for executing MapReduce jobs.
  Can be one of local, classic or yarn.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.staging-dir</name>
  <value>/tmp/hadoop-yarn/staging</value>
  <description>The staging dir used while submitting jobs.
  </description>
</property>

<property>
  <name>mapreduce.am.max-attempts</name>
  <value>2</value>
  <description>The maximum number of application attempts. It is a
  application-specific setting. It should not be larger than the global number
  set by resourcemanager. Otherwise, it will be override. The default number is
  set to 2, to allow at least one retry for AM.</description>
</property>

<!-- Job Notification Configuration -->
<property>
 <name>mapreduce.job.end-notification.url</name>
 <!--<value>http://localhost:8080/jobstatus.php?jobId=$jobId&amp;jobStatus=$jobStatus</value>-->
 <description>Indicates url which will be called on completion of job to inform
              end status of job.
              User can give at most 2 variables with URI : $jobId and $jobStatus.
              If they are present in URI, then they will be replaced by their
              respective values.
</description>
</property>

<property>
  <name>mapreduce.job.end-notification.retry.attempts</name>
  <value>0</value>
  <description>The number of times the submitter of the job wants to retry job
    end notification if it fails. This is capped by
    mapreduce.job.end-notification.max.attempts</description>
</property>

<property>
  <name>mapreduce.job.end-notification.retry.interval</name>
  <value>1000</value>
  <description>The number of milliseconds the submitter of the job wants to
    wait before job end notification is retried if it fails. This is capped by
    mapreduce.job.end-notification.max.retry.interval</description>
</property>

<property>
  <name>mapreduce.job.end-notification.max.attempts</name>
  <value>5</value>
  <final>true</final>
  <description>The maximum number of times a URL will be read for providing job
    end notification. Cluster administrators can set this to limit how long
    after end of a job, the Application Master waits before exiting. Must be
    marked as final to prevent users from overriding this.
  </description>
</property>

  <property>
    <name>mapreduce.job.log4j-properties-file</name>
    <value></value>
    <description>Used to override the default settings of log4j in container-log4j.properties
    for NodeManager. Like container-log4j.properties, it requires certain
    framework appenders properly defined in this overriden file. The file on the
    path will be added to distributed cache and classpath. If no-scheme is given
    in the path, it defaults to point to a log4j file on the local FS.
    </description>
  </property>

<property>
  <name>mapreduce.job.end-notification.max.retry.interval</name>
  <value>5000</value>
  <final>true</final>
  <description>The maximum amount of time (in milliseconds) to wait before
     retrying job end notification. Cluster administrators can set this to
     limit how long the Application Master waits before exiting. Must be marked
     as final to prevent users from overriding this.</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.env</name>
  <value></value>
  <description>User added environment variables for the MR App Master 
  processes. Example :
  1) A=foo  This will set the env variable A to foo
  2) B=$B:c This is inherit tasktracker's B env variable.  
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.admin.user.env</name>
  <value></value>
  <description> Environment variables for the MR App Master 
  processes for admin purposes. These values are set first and can be 
  overridden by the user env (yarn.app.mapreduce.am.env) Example :
  1) A=foo  This will set the env variable A to foo
  2) B=$B:c This is inherit app master's B env variable.  
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.command-opts</name>
  <value>-Xmx1024m</value>
  <description>Java opts for the MR App Master processes.  
  The following symbol, if present, will be interpolated: @taskid@ is replaced 
  by current TaskID. Any other occurrences of '@' will go unchanged.
  For example, to enable verbose gc logging to a file named for the taskid in
  /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of:
        -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
  
  Usage of -Djava.library.path can cause programs to no longer function if
  hadoop native libraries are used. These values should instead be set as part 
  of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and 
  mapreduce.reduce.env config settings. 
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.admin-command-opts</name>
  <value></value>
  <description>Java opts for the MR App Master processes for admin purposes.
  It will appears before the opts set by yarn.app.mapreduce.am.command-opts and
  thus its options can be overridden user. 
  
  Usage of -Djava.library.path can cause programs to no longer function if
  hadoop native libraries are used. These values should instead be set as part 
  of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and 
  mapreduce.reduce.env config settings. 
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.job.task.listener.thread-count</name>
  <value>30</value>
  <description>The number of threads used to handle RPC calls in the 
    MR AppMaster from remote tasks</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.job.client.port-range</name>
  <value></value>
  <description>Range of ports that the MapReduce AM can use when binding.
    Leave blank if you want all possible ports.  
    For example 50000-50050,50100-50200</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.job.committer.cancel-timeout</name>
  <value>60000</value>
  <description>The amount of time in milliseconds to wait for the output
    committer to cancel an operation if the job is killed</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.job.committer.commit-window</name>
  <value>10000</value>
  <description>Defines a time window in milliseconds for output commit
  operations.  If contact with the RM has occurred within this window then
  commits are allowed, otherwise the AM will not allow output commits until
  contact with the RM has been re-established.</description>
</property>

<property>
  <name>mapreduce.fileoutputcommitter.algorithm.version</name>
  <value>1</value>
  <description>The file output committer algorithm version
  valid algorithm version number: 1 or 2
  default to 1, which is the original algorithm

  In algorithm version 1,

  1. commitTask will rename directory
  $joboutput/_temporary/$appAttemptID/_temporary/$taskAttemptID/
  to
  $joboutput/_temporary/$appAttemptID/$taskID/

  2. recoverTask will also do a rename
  $joboutput/_temporary/$appAttemptID/$taskID/
  to
  $joboutput/_temporary/($appAttemptID + 1)/$taskID/

  3. commitJob will merge every task output file in
  $joboutput/_temporary/$appAttemptID/$taskID/
  to
  $joboutput/, then it will delete $joboutput/_temporary/
  and write $joboutput/_SUCCESS

  It has a performance regression, which is discussed in MAPREDUCE-4815.
  If a job generates many files to commit then the commitJob
  method call at the end of the job can take minutes.
  the commit is single-threaded and waits until all
  tasks have completed before commencing.

  algorithm version 2 will change the behavior of commitTask,
  recoverTask, and commitJob.

  1. commitTask will rename all files in
  $joboutput/_temporary/$appAttemptID/_temporary/$taskAttemptID/
  to $joboutput/

  2. recoverTask actually doesn't require to do anything, but for
  upgrade from version 1 to version 2 case, it will check if there
  are any files in
  $joboutput/_temporary/($appAttemptID - 1)/$taskID/
  and rename them to $joboutput/

  3. commitJob can simply delete $joboutput/_temporary and write
  $joboutput/_SUCCESS

  This algorithm will reduce the output commit time for
  large jobs by having the tasks commit directly to the final
  output directory as they were completing and commitJob had
  very little to do.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms</name>
  <value>1000</value>
  <description>The interval in ms at which the MR AppMaster should send
    heartbeats to the ResourceManager</description>
</property>

<property>
  <name>yarn.app.mapreduce.client-am.ipc.max-retries</name>
  <value>3</value>
  <description>The number of client retries to the AM - before reconnecting
    to the RM to fetch Application Status.</description>
</property>

<property>
  <name>yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts</name>
  <value>3</value>
  <description>The number of client retries on socket timeouts to the AM - before
    reconnecting to the RM to fetch Application Status.</description>
</property>

<property>
  <name>yarn.app.mapreduce.client.max-retries</name>
  <value>3</value>
  <description>The number of client retries to the RM/HS before
    throwing exception. This is a layer above the ipc.</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.resource.mb</name>
  <value>1536</value>
  <description>The amount of memory the MR AppMaster needs.</description>
</property>

<property>
  <name>yarn.app.mapreduce.am.resource.cpu-vcores</name>
  <value>1</value>
  <description>
      The number of virtual CPU cores the MR AppMaster needs.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.hard-kill-timeout-ms</name>
  <value>10000</value>
  <description>
     Number of milliseconds to wait before the job client kills the application.
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.client.job.max-retries</name>
  <value>0</value>
  <description>The number of retries the client will make for getJob and
  dependent calls.  The default is 0 as this is generally only needed for
  non-HDFS DFS where additional, high level retries are required to avoid
  spurious failures during the getJob call.  30 is a good value for
  WASB</description>
</property>

<property>
  <name>yarn.app.mapreduce.client.job.retry-interval</name>
  <value>2000</value>
  <description>The delay between getJob retries in ms for retries configured
  with yarn.app.mapreduce.client.job.max-retries.</description>
</property>

<property>
  <description>CLASSPATH for MR applications. A comma-separated list
  of CLASSPATH entries. If mapreduce.application.framework is set then this
  must specify the appropriate classpath for that archive, and the name of
  the archive must be present in the classpath.
  If mapreduce.app-submission.cross-platform is false, platform-specific
  environment vairable expansion syntax would be used to construct the default
  CLASSPATH entries.
  For Linux:
  $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,
  $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*.
  For Windows:
  %HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/*,
  %HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/lib/*.

  If mapreduce.app-submission.cross-platform is true, platform-agnostic default
  CLASSPATH for MR applications would be used:
  {{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*,
  {{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/*
  Parameter expansion marker will be replaced by NodeManager on container
  launch based on the underlying OS accordingly.
  </description>
   <name>mapreduce.application.classpath</name>
   <value></value>
</property>

<property>
  <description>If enabled, user can submit an application cross-platform
  i.e. submit an application from a Windows client to a Linux/Unix server or
  vice versa.
  </description>
  <name>mapreduce.app-submission.cross-platform</name>
  <value>false</value>
</property>

<property>
  <description>Path to the MapReduce framework archive. If set, the framework
    archive will automatically be distributed along with the job, and this
    path would normally reside in a public location in an HDFS filesystem. As
    with distributed cache files, this can be a URL with a fragment specifying
    the alias to use for the archive name. For example,
    hdfs:/mapred/framework/hadoop-mapreduce-2.1.1.tar.gz#mrframework would
    alias the localized archive as "mrframework".

    Note that mapreduce.application.classpath must include the appropriate
    classpath for the specified framework. The base name of the archive, or
    alias of the archive if an alias is used, must appear in the specified
    classpath.
  </description>
   <name>mapreduce.application.framework.path</name>
   <value></value>
</property>

<property>
   <name>mapreduce.job.classloader</name>
   <value>false</value>
  <description>Whether to use a separate (isolated) classloader for
    user classes in the task JVM.</description>
</property>

<property>
   <name>mapreduce.job.classloader.system.classes</name>
   <value></value>
  <description>Used to override the default definition of the system classes for
    the job classloader. The system classes are a comma-separated list of
    patterns that indicate whether to load a class from the system classpath,
    instead from the user-supplied JARs, when mapreduce.job.classloader is
    enabled.

    A positive pattern is defined as:
        1. A single class name 'C' that matches 'C' and transitively all nested
            classes 'C$*' defined in C;
        2. A package name ending with a '.' (e.g., "com.example.") that matches
            all classes from that package.
    A negative pattern is defined by a '-' in front of a positive pattern
    (e.g., "-com.example.").

    A class is considered a system class if and only if it matches one of the
    positive patterns and none of the negative ones. More formally:
    A class is a member of the inclusion set I if it matches one of the positive
    patterns. A class is a member of the exclusion set E if it matches one of
    the negative patterns. The set of system classes S = I \ E.
  </description>
</property>

<!-- jobhistory properties -->

<property>
  <name>mapreduce.jobhistory.address</name>
  <value>0.0.0.0:10020</value>
  <description>MapReduce JobHistory Server IPC host:port</description>
</property>

<property>
  <name>mapreduce.jobhistory.webapp.address</name>
  <value>0.0.0.0:19888</value>
  <description>MapReduce JobHistory Server Web UI host:port</description>
</property>

<property>
  <name>mapreduce.jobhistory.keytab</name>
  <description>
    Location of the kerberos keytab file for the MapReduce
    JobHistory Server.
  </description>
  <value>/etc/security/keytab/jhs.service.keytab</value>
</property>

<property>
  <name>mapreduce.jobhistory.principal</name>
  <description>
    Kerberos principal name for the MapReduce JobHistory Server.
  </description>
  <value>jhs/_HOST@REALM.TLD</value>
</property>

<property>
  <name>mapreduce.jobhistory.intermediate-done-dir</name>
  <value>${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate</value>
  <description></description>
</property>

<property>
  <name>mapreduce.jobhistory.done-dir</name>
  <value>${yarn.app.mapreduce.am.staging-dir}/history/done</value>
  <description></description>
</property>

<property>
  <name>mapreduce.jobhistory.cleaner.enable</name>
  <value>true</value>
  <description></description>
</property>

<property>
  <name>mapreduce.jobhistory.cleaner.interval-ms</name>
  <value>86400000</value>
  <description> How often the job history cleaner checks for files to delete, 
  in milliseconds. Defaults to 86400000 (one day). Files are only deleted if
  they are older than mapreduce.jobhistory.max-age-ms.
  </description>
</property>

<property>
  <name>mapreduce.jobhistory.max-age-ms</name>
  <value>604800000</value>
  <description> Job history files older than this many milliseconds will
  be deleted when the history cleaner runs. Defaults to 604800000 (1 week).
  </description>
</property>

<property>
  <name>mapreduce.jobhistory.client.thread-count</name>
  <value>10</value>
  <description>The number of threads to handle client API requests</description>
</property>

<property>
  <name>mapreduce.jobhistory.datestring.cache.size</name>
  <value>200000</value>
  <description>Size of the date string cache. Effects the number of directories
  which will be scanned to find a job.</description>
</property>

<property>
  <name>mapreduce.jobhistory.joblist.cache.size</name>
  <value>20000</value>
  <description>Size of the job list cache</description>
</property>

<property>
  <name>mapreduce.jobhistory.loadedjobs.cache.size</name>
  <value>5</value>
  <description>Size of the loaded job cache</description>
</property>

<property>
  <name>mapreduce.jobhistory.move.interval-ms</name>
  <value>180000</value>
  <description>Scan for history files to more from intermediate done dir to done
  dir at this frequency.
  </description>
</property>

<property>
  <name>mapreduce.jobhistory.move.thread-count</name>
  <value>3</value>
  <description>The number of threads used to move files.</description>
</property>

<property>
  <name>mapreduce.jobhistory.store.class</name>
  <value></value>
  <description>The HistoryStorage class to use to cache history data.</description>
</property>

<property>
  <name>mapreduce.jobhistory.minicluster.fixed.ports</name>
  <value>false</value>
  <description>Whether to use fixed ports with the minicluster</description>
</property>

<property>
  <name>mapreduce.jobhistory.admin.address</name>
  <value>0.0.0.0:10033</value>
  <description>The address of the History server admin interface.</description>
</property>

<property>
  <name>mapreduce.jobhistory.admin.acl</name>
  <value>*</value>
  <description>ACL of who can be admin of the History server.</description>
</property>

<property>
  <name>mapreduce.jobhistory.recovery.enable</name>
  <value>false</value>
  <description>Enable the history server to store server state and recover
  server state upon startup.  If enabled then
  mapreduce.jobhistory.recovery.store.class must be specified.</description>
</property>

<property>
  <name>mapreduce.jobhistory.recovery.store.class</name>
  <value>org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService</value>
  <description>The HistoryServerStateStoreService class to store history server
  state for recovery.</description>
</property>

<property>
  <name>mapreduce.jobhistory.recovery.store.fs.uri</name>
  <value>${hadoop.tmp.dir}/mapred/history/recoverystore</value>
  <!--value>hdfs://localhost:9000/mapred/history/recoverystore</value-->
  <description>The URI where history server state will be stored if
  HistoryServerFileSystemStateStoreService is configured as the recovery
  storage class.</description>
</property>

<property>
  <name>mapreduce.jobhistory.recovery.store.leveldb.path</name>
  <value>${hadoop.tmp.dir}/mapred/history/recoverystore</value>
  <description>The URI where history server state will be stored if
  HistoryServerLeveldbSystemStateStoreService is configured as the recovery
  storage class.</description>
</property>

<property>
  <name>mapreduce.jobhistory.http.policy</name>
  <value>HTTP_ONLY</value>
  <description>
    This configures the HTTP endpoint for JobHistoryServer web UI.
    The following values are supported:
    - HTTP_ONLY : Service is provided only on http
    - HTTPS_ONLY : Service is provided only on https
  </description>
</property>

<property>
  <name>yarn.app.mapreduce.am.containerlauncher.threadpool-initial-size</name>
  <value>10</value>
  <description>The initial size of thread pool to launch containers in the
    app master.
  </description>
</property>
</configuration>
