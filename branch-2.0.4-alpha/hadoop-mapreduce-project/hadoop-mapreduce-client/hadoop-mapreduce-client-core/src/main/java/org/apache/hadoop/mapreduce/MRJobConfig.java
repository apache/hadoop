/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MRJobConfig {

  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.job.inputformat.class";

  public static final String MAP_CLASS_ATTR = "mapreduce.job.map.class";

  public static final String MAP_OUTPUT_COLLECTOR_CLASS_ATTR
                                  = "mapreduce.job.map.output.collector.class";

  public static final String COMBINE_CLASS_ATTR = "mapreduce.job.combine.class";

  public static final String REDUCE_CLASS_ATTR = "mapreduce.job.reduce.class";

  public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";

  public static final String PARTITIONER_CLASS_ATTR = "mapreduce.job.partitioner.class";

  public static final String SETUP_CLEANUP_NEEDED = "mapreduce.job.committer.setup.cleanup.needed";

  public static final String TASK_CLEANUP_NEEDED = "mapreduce.job.committer.task.cleanup.needed";

  public static final String JAR = "mapreduce.job.jar";

  public static final String ID = "mapreduce.job.id";

  public static final String JOB_NAME = "mapreduce.job.name";

  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  public static final String USER_NAME = "mapreduce.job.user.name";

  public static final String PRIORITY = "mapreduce.job.priority";

  public static final String QUEUE_NAME = "mapreduce.job.queuename";

  public static final String JVM_NUMTASKS_TORUN = "mapreduce.job.jvm.numtasks";

  public static final String SPLIT_FILE = "mapreduce.job.splitfile";

  public static final String SPLIT_METAINFO_MAXSIZE = "mapreduce.job.split.metainfo.maxsize";
  public static final long DEFAULT_SPLIT_METAINFO_MAXSIZE = 10000000L;

  public static final String NUM_MAPS = "mapreduce.job.maps";

  public static final String MAX_TASK_FAILURES_PER_TRACKER = "mapreduce.job.maxtaskfailures.per.tracker";

  public static final String COMPLETED_MAPS_FOR_REDUCE_SLOWSTART = "mapreduce.job.reduce.slowstart.completedmaps";

  public static final String NUM_REDUCES = "mapreduce.job.reduces";

  public static final String SKIP_RECORDS = "mapreduce.job.skiprecords";

  public static final String SKIP_OUTDIR = "mapreduce.job.skip.outdir";

  public static final String SPECULATIVE_SLOWNODE_THRESHOLD = "mapreduce.job.speculative.slownodethreshold";

  public static final String SPECULATIVE_SLOWTASK_THRESHOLD = "mapreduce.job.speculative.slowtaskthreshold";

  public static final String SPECULATIVECAP = "mapreduce.job.speculative.speculativecap";

  public static final String JOB_LOCAL_DIR = "mapreduce.job.local.dir";

  public static final String OUTPUT_KEY_CLASS = "mapreduce.job.output.key.class";

  public static final String OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";

  public static final String KEY_COMPARATOR = "mapreduce.job.output.key.comparator.class";

  public static final String GROUP_COMPARATOR_CLASS = "mapreduce.job.output.group.comparator.class";

  public static final String WORKING_DIR = "mapreduce.job.working.dir";

  public static final String CLASSPATH_ARCHIVES = "mapreduce.job.classpath.archives";

  public static final String CLASSPATH_FILES = "mapreduce.job.classpath.files";

  public static final String CACHE_FILES = "mapreduce.job.cache.files";

  public static final String CACHE_ARCHIVES = "mapreduce.job.cache.archives";

  public static final String CACHE_FILES_SIZES = "mapreduce.job.cache.files.filesizes"; // internal use only

  public static final String CACHE_ARCHIVES_SIZES = "mapreduce.job.cache.archives.filesizes"; // ditto

  public static final String CACHE_LOCALFILES = "mapreduce.job.cache.local.files";

  public static final String CACHE_LOCALARCHIVES = "mapreduce.job.cache.local.archives";

  public static final String CACHE_FILE_TIMESTAMPS = "mapreduce.job.cache.files.timestamps";

  public static final String CACHE_ARCHIVES_TIMESTAMPS = "mapreduce.job.cache.archives.timestamps";

  public static final String CACHE_FILE_VISIBILITIES = "mapreduce.job.cache.files.visibilities";

  public static final String CACHE_ARCHIVES_VISIBILITIES = "mapreduce.job.cache.archives.visibilities";

  /**
   * @deprecated Symlinks are always on and cannot be disabled.
   */
  @Deprecated
  public static final String CACHE_SYMLINK = "mapreduce.job.cache.symlink.create";

  public static final String USER_LOG_RETAIN_HOURS = "mapreduce.job.userlog.retain.hours";

  public static final String MAPREDUCE_JOB_USER_CLASSPATH_FIRST = "mapreduce.job.user.classpath.first";

  public static final String MAPREDUCE_JOB_CLASSLOADER = "mapreduce.job.classloader";

  public static final String MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES = "mapreduce.job.classloader.system.classes";

  public static final String IO_SORT_FACTOR = "mapreduce.task.io.sort.factor";

  public static final String IO_SORT_MB = "mapreduce.task.io.sort.mb";

  public static final String INDEX_CACHE_MEMORY_LIMIT = "mapreduce.task.index.cache.limit.bytes";

  public static final String PRESERVE_FAILED_TASK_FILES = "mapreduce.task.files.preserve.failedtasks";

  public static final String PRESERVE_FILES_PATTERN = "mapreduce.task.files.preserve.filepattern";

  public static final String TASK_TEMP_DIR = "mapreduce.task.tmp.dir";

  public static final String TASK_DEBUGOUT_LINES = "mapreduce.task.debugout.lines";

  public static final String RECORDS_BEFORE_PROGRESS = "mapreduce.task.merge.progress.records";

  public static final String SKIP_START_ATTEMPTS = "mapreduce.task.skip.start.attempts";

  public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";

  public static final String TASK_ISMAP = "mapreduce.task.ismap";

  public static final String TASK_PARTITION = "mapreduce.task.partition";

  public static final String TASK_PROFILE = "mapreduce.task.profile";

  public static final String TASK_PROFILE_PARAMS = "mapreduce.task.profile.params";

  public static final String NUM_MAP_PROFILES = "mapreduce.task.profile.maps";

  public static final String NUM_REDUCE_PROFILES = "mapreduce.task.profile.reduces";

  public static final String TASK_MAP_PROFILE_PARAMS = "mapreduce.task.profile.map.params";
  
  public static final String TASK_REDUCE_PROFILE_PARAMS = "mapreduce.task.profile.reduce.params";
  
  public static final String TASK_TIMEOUT = "mapreduce.task.timeout";

  public static final String TASK_TIMEOUT_CHECK_INTERVAL_MS = "mapreduce.task.timeout.check-interval-ms";
  
  public static final String TASK_ID = "mapreduce.task.id";

  public static final String TASK_OUTPUT_DIR = "mapreduce.task.output.dir";

  public static final String TASK_USERLOG_LIMIT = "mapreduce.task.userlog.limit.kb";

  public static final String MAP_SORT_SPILL_PERCENT = "mapreduce.map.sort.spill.percent";

  public static final String MAP_INPUT_FILE = "mapreduce.map.input.file";

  public static final String MAP_INPUT_PATH = "mapreduce.map.input.length";

  public static final String MAP_INPUT_START = "mapreduce.map.input.start";

  public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  public static final int DEFAULT_MAP_MEMORY_MB = 1024;

  public static final String MAP_CPU_VCORES = "mapreduce.map.cpu.vcores";
  public static final int DEFAULT_MAP_CPU_VCORES = 1;

  public static final String MAP_ENV = "mapreduce.map.env";

  public static final String MAP_JAVA_OPTS = "mapreduce.map.java.opts";

  public static final String MAP_MAX_ATTEMPTS = "mapreduce.map.maxattempts";

  public static final String MAP_DEBUG_SCRIPT = "mapreduce.map.debug.script";

  public static final String MAP_SPECULATIVE = "mapreduce.map.speculative";

  public static final String MAP_FAILURES_MAX_PERCENT = "mapreduce.map.failures.maxpercent";

  public static final String MAP_SKIP_INCR_PROC_COUNT = "mapreduce.map.skip.proc-count.auto-incr";

  public static final String MAP_SKIP_MAX_RECORDS = "mapreduce.map.skip.maxrecords";

  public static final String MAP_COMBINE_MIN_SPILLS = "mapreduce.map.combine.minspills";

  public static final String MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";

  public static final String MAP_OUTPUT_COMPRESS_CODEC = "mapreduce.map.output.compress.codec";

  public static final String MAP_OUTPUT_KEY_CLASS = "mapreduce.map.output.key.class";

  public static final String MAP_OUTPUT_VALUE_CLASS = "mapreduce.map.output.value.class";

  public static final String MAP_OUTPUT_KEY_FIELD_SEPERATOR = "mapreduce.map.output.key.field.separator";

  public static final String MAP_LOG_LEVEL = "mapreduce.map.log.level";

  public static final String REDUCE_LOG_LEVEL = "mapreduce.reduce.log.level";

  public static final String DEFAULT_LOG_LEVEL = "INFO";

  public static final String REDUCE_MERGE_INMEM_THRESHOLD = "mapreduce.reduce.merge.inmem.threshold";

  public static final String REDUCE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.input.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_PERCENT = "mapreduce.reduce.markreset.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_SIZE = "mapreduce.reduce.markreset.buffer.size";

  public static final String REDUCE_MEMORY_MB = "mapreduce.reduce.memory.mb";
  public static final int DEFAULT_REDUCE_MEMORY_MB = 1024;

  public static final String REDUCE_CPU_VCORES = "mapreduce.reduce.cpu.vcores";
  public static final int DEFAULT_REDUCE_CPU_VCORES = 1;

  public static final String REDUCE_MEMORY_TOTAL_BYTES = "mapreduce.reduce.memory.totalbytes";

  public static final String SHUFFLE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.shuffle.input.buffer.percent";

  public static final String SHUFFLE_MEMORY_LIMIT_PERCENT
    = "mapreduce.reduce.shuffle.memory.limit.percent";

  public static final String SHUFFLE_MERGE_PERCENT = "mapreduce.reduce.shuffle.merge.percent";

  public static final String REDUCE_FAILURES_MAXPERCENT = "mapreduce.reduce.failures.maxpercent";

  public static final String REDUCE_ENV = "mapreduce.reduce.env";

  public static final String REDUCE_JAVA_OPTS = "mapreduce.reduce.java.opts";

  public static final String MAPREDUCE_JOB_DIR = "mapreduce.job.dir";

  public static final String REDUCE_MAX_ATTEMPTS = "mapreduce.reduce.maxattempts";

  public static final String SHUFFLE_PARALLEL_COPIES = "mapreduce.reduce.shuffle.parallelcopies";

  public static final String REDUCE_DEBUG_SCRIPT = "mapreduce.reduce.debug.script";

  public static final String REDUCE_SPECULATIVE = "mapreduce.reduce.speculative";

  public static final String SHUFFLE_CONNECT_TIMEOUT = "mapreduce.reduce.shuffle.connect.timeout";

  public static final String SHUFFLE_READ_TIMEOUT = "mapreduce.reduce.shuffle.read.timeout";

  public static final String SHUFFLE_FETCH_FAILURES = "mapreduce.reduce.shuffle.maxfetchfailures";

  public static final String SHUFFLE_NOTIFY_READERROR = "mapreduce.reduce.shuffle.notify.readerror";
  
  public static final String MAX_SHUFFLE_FETCH_RETRY_DELAY = "mapreduce.reduce.shuffle.retry-delay.max.ms";
  public static final long DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY = 60000;

  public static final String REDUCE_SKIP_INCR_PROC_COUNT = "mapreduce.reduce.skip.proc-count.auto-incr";

  public static final String REDUCE_SKIP_MAXGROUPS = "mapreduce.reduce.skip.maxgroups";

  public static final String REDUCE_MEMTOMEM_THRESHOLD = "mapreduce.reduce.merge.memtomem.threshold";

  public static final String REDUCE_MEMTOMEM_ENABLED = "mapreduce.reduce.merge.memtomem.enabled";

  public static final String COMBINE_RECORDS_BEFORE_PROGRESS = "mapreduce.task.combine.progress.records";

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_JOBTRACKER_ID = "mapreduce.job.kerberos.jtprinicipal";

  public static final String JOB_CANCEL_DELEGATION_TOKEN = "mapreduce.job.complete.cancel.delegation.tokens";

  public static final String JOB_ACL_VIEW_JOB = "mapreduce.job.acl-view-job";

  public static final String DEFAULT_JOB_ACL_VIEW_JOB = " ";

  public static final String JOB_ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";

  public static final String DEFAULT_JOB_ACL_MODIFY_JOB = " ";
  
  /* config for tracking the local file where all the credentials for the job
   * credentials.
   */
  public static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = 
      "mapreduce.job.credentials.binary";

  public static final String JOB_SUBMITHOST =
    "mapreduce.job.submithostname";
  public static final String JOB_SUBMITHOSTADDR =
    "mapreduce.job.submithostaddress";

  public static final String COUNTERS_MAX_KEY = "mapreduce.job.counters.max";
  public static final int COUNTERS_MAX_DEFAULT = 120;

  public static final String COUNTER_GROUP_NAME_MAX_KEY = "mapreduce.job.counters.group.name.max";
  public static final int COUNTER_GROUP_NAME_MAX_DEFAULT = 128;

  public static final String COUNTER_NAME_MAX_KEY = "mapreduce.job.counters.counter.name.max";
  public static final int COUNTER_NAME_MAX_DEFAULT = 64;

  public static final String COUNTER_GROUPS_MAX_KEY = "mapreduce.job.counters.groups.max";
  public static final int COUNTER_GROUPS_MAX_DEFAULT = 50;
  public static final String JOB_UBERTASK_ENABLE =
    "mapreduce.job.ubertask.enable";
  public static final String JOB_UBERTASK_MAXMAPS =
    "mapreduce.job.ubertask.maxmaps";
  public static final String JOB_UBERTASK_MAXREDUCES =
    "mapreduce.job.ubertask.maxreduces";
  public static final String JOB_UBERTASK_MAXBYTES =
    "mapreduce.job.ubertask.maxbytes";

  public static final String MR_PREFIX = "yarn.app.mapreduce.";

  public static final String MR_AM_PREFIX = MR_PREFIX + "am.";

  /** The number of client retires to the AM - before reconnecting to the RM
   * to fetch Application State. 
   */
  public static final String MR_CLIENT_TO_AM_IPC_MAX_RETRIES = 
    MR_PREFIX + "client-am.ipc.max-retries";
  public static final int DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES = 3;
  
  /**
   * The number of client retries to the RM/HS/AM before throwing exception.
   */
  public static final String MR_CLIENT_MAX_RETRIES = 
    MR_PREFIX + "client.max-retries";
  public static final int DEFAULT_MR_CLIENT_MAX_RETRIES = 3;
  
  /** The staging directory for map reduce.*/
  public static final String MR_AM_STAGING_DIR = 
    MR_AM_PREFIX+"staging-dir";
  public static final String DEFAULT_MR_AM_STAGING_DIR = 
    "/tmp/hadoop-yarn/staging";

  /** The amount of memory the MR app master needs.*/
  public static final String MR_AM_VMEM_MB =
    MR_AM_PREFIX+"resource.mb";
  public static final int DEFAULT_MR_AM_VMEM_MB = 1536;

  /** The number of virtual cores the MR app master needs.*/
  public static final String MR_AM_CPU_VCORES =
    MR_AM_PREFIX+"resource.cpu-vcores";
  public static final int DEFAULT_MR_AM_CPU_VCORES = 1;

  /** Command line arguments passed to the MR app master.*/
  public static final String MR_AM_COMMAND_OPTS =
    MR_AM_PREFIX+"command-opts";
  public static final String DEFAULT_MR_AM_COMMAND_OPTS = "-Xmx1024m";

  /** Admin command opts passed to the MR app master.*/
  public static final String MR_AM_ADMIN_COMMAND_OPTS =
      MR_AM_PREFIX+"admin-command-opts";
  public static final String DEFAULT_MR_AM_ADMIN_COMMAND_OPTS = "";
  
  /** Root Logging level passed to the MR app master.*/
  public static final String MR_AM_LOG_LEVEL = 
    MR_AM_PREFIX+"log.level";
  public static final String DEFAULT_MR_AM_LOG_LEVEL = "INFO";

  /**The number of splits when reporting progress in MR*/
  public static final String MR_AM_NUM_PROGRESS_SPLITS = 
    MR_AM_PREFIX+"num-progress-splits";
  public static final int DEFAULT_MR_AM_NUM_PROGRESS_SPLITS = 12;

  /**
   * Upper limit on the number of threads user to launch containers in the app
   * master. Expect level config, you shouldn't be needing it in most cases.
   */
  public static final String MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT =
    MR_AM_PREFIX+"containerlauncher.thread-count-limit";

  public static final int DEFAULT_MR_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT = 
      500;

  /** Number of threads to handle job client RPC requests.*/
  public static final String MR_AM_JOB_CLIENT_THREAD_COUNT =
    MR_AM_PREFIX + "job.client.thread-count";
  public static final int DEFAULT_MR_AM_JOB_CLIENT_THREAD_COUNT = 1;

  /** 
   * Range of ports that the MapReduce AM can use when binding. Leave blank
   * if you want all possible ports.
   */
  public static final String MR_AM_JOB_CLIENT_PORT_RANGE = 
    MR_AM_PREFIX + "job.client.port-range";
  
  /** Enable blacklisting of nodes in the job.*/
  public static final String MR_AM_JOB_NODE_BLACKLISTING_ENABLE = 
    MR_AM_PREFIX  + "job.node-blacklisting.enable";

  /** Ignore blacklisting if a certain percentage of nodes have been blacklisted */
  public static final String MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT =
      MR_AM_PREFIX + "job.node-blacklisting.ignore-threshold-node-percent";
  public static final int DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT =
      33;
  
  /** Enable job recovery.*/
  public static final String MR_AM_JOB_RECOVERY_ENABLE = 
    MR_AM_PREFIX + "job.recovery.enable";

  /** 
   * Limit on the number of reducers that can be preempted to ensure that at
   *  least one map task can run if it needs to. Percentage between 0.0 and 1.0
   */
  public static final String MR_AM_JOB_REDUCE_PREEMPTION_LIMIT = 
    MR_AM_PREFIX  + "job.reduce.preemption.limit";
  public static final float DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT = 0.5f;
  
  /** AM ACL disabled. **/
  public static final String JOB_AM_ACCESS_DISABLED = 
    "mapreduce.job.am-access-disabled";
  public static final boolean DEFAULT_JOB_AM_ACCESS_DISABLED = false;

  /**
   * Limit reduces starting until a certain percentage of maps have finished.
   *  Percentage between 0.0 and 1.0
   */
  public static final String MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT = 
    MR_AM_PREFIX  + "job.reduce.rampup.limit";
  public static final float DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT = 0.5f;

  /** The class that should be used for speculative execution calculations.*/
  public static final String MR_AM_JOB_SPECULATOR =
    MR_AM_PREFIX + "job.speculator.class";

  /** Class used to estimate task resource needs.*/
  public static final String MR_AM_TASK_ESTIMATOR =
    MR_AM_PREFIX + "job.task.estimator.class";

  /** The lambda value in the smoothing function of the task estimator.*/
  public static final String MR_AM_TASK_ESTIMATOR_SMOOTH_LAMBDA_MS =
    MR_AM_PREFIX
    + "job.task.estimator.exponential.smooth.lambda-ms";

  public static final long DEFAULT_MR_AM_TASK_ESTIMATOR_SMOOTH_LAMBDA_MS = 
  1000L * 60;

  /** true if the smoothing rate should be exponential.*/
  public static final String MR_AM_TASK_ESTIMATOR_EXPONENTIAL_RATE_ENABLE =
    MR_AM_PREFIX + "job.task.estimator.exponential.smooth.rate";

  /** The number of threads used to handle task RPC calls.*/
  public static final String MR_AM_TASK_LISTENER_THREAD_COUNT =
    MR_AM_PREFIX + "job.task.listener.thread-count";
  public static final int DEFAULT_MR_AM_TASK_LISTENER_THREAD_COUNT = 30;

  /** How often the AM should send heartbeats to the RM.*/
  public static final String MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS =
    MR_AM_PREFIX + "scheduler.heartbeat.interval-ms";
  public static final int DEFAULT_MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS = 1000;

  /**
   * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
   * milliseconds before aborting. During this interval, AM will still try
   * to contact the RM.
   */
  public static final String MR_AM_TO_RM_WAIT_INTERVAL_MS =
    MR_AM_PREFIX + "scheduler.connection.wait.interval-ms";
  public static final int DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS = 360000;

  /**
   * How long to wait in milliseconds for the output committer to cancel
   * an operation when the job is being killed
   */
  public static final String MR_AM_COMMITTER_CANCEL_TIMEOUT_MS =
      MR_AM_PREFIX + "job.committer.cancel-timeout";
  public static final int DEFAULT_MR_AM_COMMITTER_CANCEL_TIMEOUT_MS =
      60 * 1000;

  /**
   * Defines a time window in milliseconds for output committer operations.
   * If contact with the RM has occurred within this window then commit
   * operations are allowed, otherwise the AM will not allow output committer
   * operations until contact with the RM has been re-established.
   */
  public static final String MR_AM_COMMIT_WINDOW_MS =
      MR_AM_PREFIX + "job.committer.commit-window";
  public static final int DEFAULT_MR_AM_COMMIT_WINDOW_MS = 10 * 1000;

  /**
   * Boolean. Create the base dirs in the JobHistoryEventHandler
   * Set to false for multi-user clusters.  This is an internal config that
   * is set by the MR framework and read by it too.
   */
  public static final String MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR = 
    MR_AM_PREFIX + "create-intermediate-jh-base-dir";
  
  public static final String MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS =
      MR_AM_PREFIX + "history.max-unflushed-events";
  public static final int DEFAULT_MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS =
      200;

  public static final String MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER =
      MR_AM_PREFIX + "history.job-complete-unflushed-multiplier";
  public static final int DEFAULT_MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER =
      30;

  public static final String MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS =
      MR_AM_PREFIX + "history.complete-event-flush-timeout";
  public static final long DEFAULT_MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS =
      30 * 1000l;

  public static final String MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD =
      MR_AM_PREFIX + "history.use-batched-flush.queue-size.threshold";
  public static final int DEFAULT_MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD =
      50;
  
  public static final String MR_AM_ENV =
      MR_AM_PREFIX + "env";
  
  public static final String MR_AM_ADMIN_USER_ENV =
      MR_AM_PREFIX + "admin.user.env";
  
  public static final String MAPRED_MAP_ADMIN_JAVA_OPTS =
      "mapreduce.admin.map.child.java.opts";

  public static final String MAPRED_REDUCE_ADMIN_JAVA_OPTS =
      "mapreduce.admin.reduce.child.java.opts";

  public static final String DEFAULT_MAPRED_ADMIN_JAVA_OPTS =
      "-Djava.net.preferIPv4Stack=true " +
          "-Dhadoop.metrics.log.level=WARN ";

  public static final String MAPRED_ADMIN_USER_SHELL =
      "mapreduce.admin.user.shell";

  public static final String DEFAULT_SHELL = "/bin/bash";

  public static final String MAPRED_ADMIN_USER_ENV =
      "mapreduce.admin.user.env";

  public static final String DEFAULT_MAPRED_ADMIN_USER_ENV =
      "LD_LIBRARY_PATH=$HADOOP_COMMON_HOME/lib/native";

  public static final String WORKDIR = "work";

  public static final String OUTPUT = "output";

  public static final String HADOOP_WORK_DIR = "HADOOP_WORK_DIR";

  // Environment variables used by Pipes. (TODO: these
  // do not appear to be used by current pipes source code!)
  public static final String STDOUT_LOGFILE_ENV = "STDOUT_LOGFILE_ENV";
  public static final String STDERR_LOGFILE_ENV = "STDERR_LOGFILE_ENV";

  public static final String APPLICATION_ATTEMPT_ID_ENV = "APPLICATION_ATTEMPT_ID_ENV";

  // This should be the directory where splits file gets localized on the node
  // running ApplicationMaster.
  public static final String JOB_SUBMIT_DIR = "jobSubmitDir";

  // This should be the name of the localized job-configuration file on the node
  // running ApplicationMaster and Task
  public static final String JOB_CONF_FILE = "job.xml";

  // This should be the name of the localized job-jar file on the node running
  // individual containers/tasks.
  public static final String JOB_JAR = "job.jar";

  public static final String JOB_SPLIT = "job.split";

  public static final String JOB_SPLIT_METAINFO = "job.splitmetainfo";

  public static final String APPLICATION_MASTER_CLASS =
      "org.apache.hadoop.mapreduce.v2.app.MRAppMaster";

  // The token file for the application. Should contain tokens for access to
  // remote file system and may optionally contain application specific tokens.
  // For now, generated by the AppManagers and used by NodeManagers and the
  // Containers.
  public static final String APPLICATION_TOKENS_FILE = "appTokens";
  
  /** The log directory for the containers */
  public static final String TASK_LOG_DIR = MR_PREFIX + "container.log.dir";
  
  public static final String TASK_LOG_SIZE = MR_PREFIX + "container.log.filesize";
  
  public static final String MAPREDUCE_V2_CHILD_CLASS = 
      "org.apache.hadoop.mapred.YarnChild";

  public static final String APPLICATION_ATTEMPT_ID =
      "mapreduce.job.application.attempt.id";

  /**
   * Job end notification.
   */
  public static final String MR_JOB_END_NOTIFICATION_URL =
    "mapreduce.job.end-notification.url";

  public static final String MR_JOB_END_NOTIFICATION_PROXY =
    "mapreduce.job.end-notification.proxy";

  public static final String MR_JOB_END_RETRY_ATTEMPTS =
    "mapreduce.job.end-notification.retry.attempts";

  public static final String MR_JOB_END_RETRY_INTERVAL =
    "mapreduce.job.end-notification.retry.interval";

  public static final String MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS =
    "mapreduce.job.end-notification.max.attempts";

  public static final String MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL =
    "mapreduce.job.end-notification.max.retry.interval";

  /*
   * MR AM Service Authorization
   */
  public static final String   
  MR_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL =
      "security.job.task.protocol.acl";
  public static final String   
  MR_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT =
      "security.job.client.protocol.acl";

  /**
   * CLASSPATH for all YARN MapReduce applications.
   */
  public static final String MAPREDUCE_APPLICATION_CLASSPATH = 
      "mapreduce.application.classpath";

  /**
   * Default CLASSPATH for all YARN MapReduce applications.
   */
  public static final String[] DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH = {
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*",
      "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*",
  };

  public static final String WORKFLOW_ID = "mapreduce.workflow.id";
  
  public static final String WORKFLOW_NAME = "mapreduce.workflow.name";
  
  public static final String WORKFLOW_NODE_NAME =
      "mapreduce.workflow.node.name";
  
  public static final String WORKFLOW_ADJACENCY_PREFIX_STRING =
      "mapreduce.workflow.adjacency.";
  
  public static final String WORKFLOW_ADJACENCY_PREFIX_PATTERN =
      "^mapreduce\\.workflow\\.adjacency\\..+";
  
}
