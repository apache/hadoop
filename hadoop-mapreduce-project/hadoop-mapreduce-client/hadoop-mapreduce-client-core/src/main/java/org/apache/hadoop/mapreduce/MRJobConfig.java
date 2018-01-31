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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.util.Apps;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MRJobConfig {

  // Used by MapTask
  public static final String MAP_SORT_CLASS = "map.sort.class";

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

  public static final String JOB_SINGLE_DISK_LIMIT_BYTES =
          "mapreduce.job.local-fs.single-disk-limit.bytes";
  // negative values disable the limit
  public static final long DEFAULT_JOB_SINGLE_DISK_LIMIT_BYTES = -1;

  public static final String JOB_SINGLE_DISK_LIMIT_KILL_LIMIT_EXCEED =
      "mapreduce.job.local-fs.single-disk-limit.check.kill-limit-exceed";
  // setting to false only logs the kill
  public static final boolean DEFAULT_JOB_SINGLE_DISK_LIMIT_KILL_LIMIT_EXCEED = true;

  public static final String JOB_SINGLE_DISK_LIMIT_CHECK_INTERVAL_MS =
      "mapreduce.job.local-fs.single-disk-limit.check.interval-ms";
  public static final long DEFAULT_JOB_SINGLE_DISK_LIMIT_CHECK_INTERVAL_MS = 5000;

  public static final String TASK_LOCAL_WRITE_LIMIT_BYTES =
          "mapreduce.task.local-fs.write-limit.bytes";
  // negative values disable the limit
  public static final long DEFAULT_TASK_LOCAL_WRITE_LIMIT_BYTES = -1;

  public static final String JAR = "mapreduce.job.jar";

  public static final String ID = "mapreduce.job.id";

  public static final String JOB_NAME = "mapreduce.job.name";

  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  public static final String USER_NAME = "mapreduce.job.user.name";

  public static final String PRIORITY = "mapreduce.job.priority";

  public static final String QUEUE_NAME = "mapreduce.job.queuename";

  /**
   *  Node Label expression applicable for all Job containers.
   */
  public static final String JOB_NODE_LABEL_EXP = "mapreduce.job.node-label-expression";

  /**
   * Node Label expression applicable for AM containers.
   */
  public static final String AM_NODE_LABEL_EXP = "mapreduce.job.am.node-label-expression";

  /**
   *  Node Label expression applicable for map containers.
   */
  public static final String MAP_NODE_LABEL_EXP = "mapreduce.map.node-label-expression";

  /**
   * Node Label expression applicable for reduce containers.
   */
  public static final String REDUCE_NODE_LABEL_EXP = "mapreduce.reduce.node-label-expression";

  /**
   * Specify strict locality on a comma-separated list of racks and/or nodes.
   * Syntax: /rack or /rack/node or node (assumes /default-rack)
   */
  public static final String AM_STRICT_LOCALITY = "mapreduce.job.am.strict-locality";

  public static final String RESERVATION_ID = "mapreduce.job.reservation.id";

  public static final String JOB_TAGS = "mapreduce.job.tags";

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

  // SPECULATIVE_SLOWNODE_THRESHOLD is obsolete and will be deleted in the future
  @Deprecated
  public static final String SPECULATIVE_SLOWNODE_THRESHOLD = "mapreduce.job.speculative.slownodethreshold";

  public static final String SPECULATIVE_SLOWTASK_THRESHOLD = "mapreduce.job.speculative.slowtaskthreshold";

  // SPECULATIVECAP is obsolete and will be deleted in the future
  @Deprecated
  public static final String SPECULATIVECAP = "mapreduce.job.speculative.speculativecap";

  public static final String SPECULATIVECAP_RUNNING_TASKS =
      "mapreduce.job.speculative.speculative-cap-running-tasks";
  public static final double DEFAULT_SPECULATIVECAP_RUNNING_TASKS =
      0.1;

  public static final String SPECULATIVECAP_TOTAL_TASKS =
      "mapreduce.job.speculative.speculative-cap-total-tasks";
  public static final double DEFAULT_SPECULATIVECAP_TOTAL_TASKS =
      0.01;

  public static final String SPECULATIVE_MINIMUM_ALLOWED_TASKS =
      "mapreduce.job.speculative.minimum-allowed-tasks";
  public static final int DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS =
      10;

  public static final String SPECULATIVE_RETRY_AFTER_NO_SPECULATE =
      "mapreduce.job.speculative.retry-after-no-speculate";
  public static final long DEFAULT_SPECULATIVE_RETRY_AFTER_NO_SPECULATE =
      1000L;

  public static final String SPECULATIVE_RETRY_AFTER_SPECULATE =
      "mapreduce.job.speculative.retry-after-speculate";
  public static final long DEFAULT_SPECULATIVE_RETRY_AFTER_SPECULATE =
      15000L;

  public static final String JOB_LOCAL_DIR = "mapreduce.job.local.dir";

  public static final String OUTPUT_KEY_CLASS = "mapreduce.job.output.key.class";

  public static final String OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";

  public static final String KEY_COMPARATOR = "mapreduce.job.output.key.comparator.class";

  public static final String COMBINER_GROUP_COMPARATOR_CLASS = "mapreduce.job.combiner.group.comparator.class";

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
   * This parameter controls the visibility of the localized job jar on the node
   * manager. If set to true, the visibility will be set to
   * LocalResourceVisibility.PUBLIC. If set to false, the visibility will be set
   * to LocalResourceVisibility.APPLICATION. This is a generated parameter and
   * should not be set manually via config files.
   */
  String JOBJAR_VISIBILITY = "mapreduce.job.jobjar.visibility";
  boolean JOBJAR_VISIBILITY_DEFAULT = false;

  /**
   * This is a generated parameter and should not be set manually via config
   * files.
   */
  String JOBJAR_SHARED_CACHE_UPLOAD_POLICY =
      "mapreduce.job.jobjar.sharedcache.uploadpolicy";
  boolean JOBJAR_SHARED_CACHE_UPLOAD_POLICY_DEFAULT = false;

  /**
   * This is a generated parameter and should not be set manually via config
   * files.
   */
  String CACHE_FILES_SHARED_CACHE_UPLOAD_POLICIES =
      "mapreduce.job.cache.files.sharedcache.uploadpolicies";

  /**
   * This is a generated parameter and should not be set manually via config
   * files.
   */
  String CACHE_ARCHIVES_SHARED_CACHE_UPLOAD_POLICIES =
      "mapreduce.job.cache.archives.sharedcache.uploadpolicies";

  /**
   * A comma delimited list of file resources that are needed for this MapReduce
   * job. These resources, if the files resource type is enabled, should either
   * use the shared cache or be added to the shared cache. This parameter can be
   * modified programmatically using the MapReduce Job api.
   */
  String FILES_FOR_SHARED_CACHE = "mapreduce.job.cache.sharedcache.files";

  /**
   * A comma delimited list of libjar resources that are needed for this
   * MapReduce job. These resources, if the libjars resource type is enabled,
   * should either use the shared cache or be added to the shared cache. These
   * resources will also be added to the classpath of all tasks for this
   * MapReduce job. This parameter can be modified programmatically using the
   * MapReduce Job api.
   */
  String FILES_FOR_CLASSPATH_AND_SHARED_CACHE =
      "mapreduce.job.cache.sharedcache.files.addtoclasspath";

  /**
   * A comma delimited list of archive resources that are needed for this
   * MapReduce job. These resources, if the archives resource type is enabled,
   * should either use the shared cache or be added to the shared cache. This
   * parameter can be modified programmatically using the MapReduce Job api.
   */
  String ARCHIVES_FOR_SHARED_CACHE =
      "mapreduce.job.cache.sharedcache.archives";

  /**
   * A comma delimited list of resource categories that are enabled for the
   * shared cache. If a category is enabled, resources in that category will be
   * uploaded to the shared cache. The valid categories are: jobjar, libjars,
   * files, archives. If "disabled" is specified then all categories are
   * disabled. If "enabled" is specified then all categories are enabled.
   */
  String SHARED_CACHE_MODE = "mapreduce.job.sharedcache.mode";

  String SHARED_CACHE_MODE_DEFAULT = "disabled";

  /**
   * @deprecated Symlinks are always on and cannot be disabled.
   */
  @Deprecated
  public static final String CACHE_SYMLINK = "mapreduce.job.cache.symlink.create";

  public static final String USER_LOG_RETAIN_HOURS = "mapreduce.job.userlog.retain.hours";

  public static final String MAPREDUCE_JOB_USER_CLASSPATH_FIRST = "mapreduce.job.user.classpath.first";

  public static final String MAPREDUCE_JOB_CLASSLOADER = "mapreduce.job.classloader";

  /**
   * A comma-separated list of services that function as ShuffleProvider aux-services
   * (in addition to the built-in ShuffleHandler).
   * These services can serve shuffle requests from reducetasks.
   */
  public static final String MAPREDUCE_JOB_SHUFFLE_PROVIDER_SERVICES = "mapreduce.job.shuffle.provider.services";

  public static final String MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES = "mapreduce.job.classloader.system.classes";

  public static final String MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG = "mapreduce.jvm.system-properties-to-log";
  public static final String DEFAULT_MAPREDUCE_JVM_SYSTEM_PROPERTIES_TO_LOG =
    "os.name,os.version,java.home,java.runtime.version,java.vendor," +
    "java.version,java.vm.name,java.class.path,java.io.tmpdir,user.dir,user.name";

  public static final String IO_SORT_FACTOR = "mapreduce.task.io.sort.factor";

  public static final int DEFAULT_IO_SORT_FACTOR = 10;

  public static final String IO_SORT_MB = "mapreduce.task.io.sort.mb";

  public static final int DEFAULT_IO_SORT_MB = 100;

  public static final String INDEX_CACHE_MEMORY_LIMIT = "mapreduce.task.index.cache.limit.bytes";

  public static final String PRESERVE_FAILED_TASK_FILES = "mapreduce.task.files.preserve.failedtasks";

  public static final String PRESERVE_FILES_PATTERN = "mapreduce.task.files.preserve.filepattern";

  public static final String TASK_DEBUGOUT_LINES = "mapreduce.task.debugout.lines";

  public static final String RECORDS_BEFORE_PROGRESS = "mapreduce.task.merge.progress.records";

  public static final String SKIP_START_ATTEMPTS = "mapreduce.task.skip.start.attempts";

  public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";

  public static final String TASK_ISMAP = "mapreduce.task.ismap";
  public static final boolean DEFAULT_TASK_ISMAP = true;

  public static final String TASK_PARTITION = "mapreduce.task.partition";

  public static final String TASK_PROFILE = "mapreduce.task.profile";

  public static final String TASK_PROFILE_PARAMS = "mapreduce.task.profile.params";

  public static final String DEFAULT_TASK_PROFILE_PARAMS =
      "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,"
          + "verbose=n,file=%s";

  public static final String NUM_MAP_PROFILES = "mapreduce.task.profile.maps";

  public static final String NUM_REDUCE_PROFILES = "mapreduce.task.profile.reduces";

  public static final String TASK_MAP_PROFILE_PARAMS = "mapreduce.task.profile.map.params";
  
  public static final String TASK_REDUCE_PROFILE_PARAMS = "mapreduce.task.profile.reduce.params";
  
  public static final String TASK_TIMEOUT = "mapreduce.task.timeout";
  long DEFAULT_TASK_TIMEOUT_MILLIS = 5 * 60 * 1000L;

  String TASK_PROGRESS_REPORT_INTERVAL =
      "mapreduce.task.progress-report.interval";

  public static final String TASK_TIMEOUT_CHECK_INTERVAL_MS = "mapreduce.task.timeout.check-interval-ms";

  public static final String TASK_EXIT_TIMEOUT = "mapreduce.task.exit.timeout";

  public static final int TASK_EXIT_TIMEOUT_DEFAULT = 60 * 1000;

  public static final String TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS = "mapreduce.task.exit.timeout.check-interval-ms";

  public static final int TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS_DEFAULT = 20 * 1000;

  public static final String TASK_ID = "mapreduce.task.id";

  public static final String TASK_OUTPUT_DIR = "mapreduce.task.output.dir";

  public static final String TASK_USERLOG_LIMIT = "mapreduce.task.userlog.limit.kb";

  public static final String MAP_SORT_SPILL_PERCENT = "mapreduce.map.sort.spill.percent";

  public static final String MAP_INPUT_FILE = "mapreduce.map.input.file";

  public static final String MAP_INPUT_PATH = "mapreduce.map.input.length";

  public static final String MAP_INPUT_START = "mapreduce.map.input.start";

  /**
   * Configuration key for specifying memory requirement for the mapper.
   * Kept for backward-compatibility, mapreduce.map.resource.memory
   * is the new preferred way to specify this.
   */
  public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  public static final int DEFAULT_MAP_MEMORY_MB = 1024;

  /**
   * Configuration key for specifying CPU requirement for the mapper.
   * Kept for backward-compatibility, mapreduce.map.resource.vcores
   * is the new preferred way to specify this.
   */
  public static final String MAP_CPU_VCORES = "mapreduce.map.cpu.vcores";
  public static final int DEFAULT_MAP_CPU_VCORES = 1;

  /**
   * Custom resource names required by the mapper should be
   * appended to this prefix, the value's format is {amount}[ ][{unit}].
   * If no unit is defined, the default unit will be used.
   * Standard resource names: memory (default unit: Mi), vcores
   */
  public static final String MAP_RESOURCE_TYPE_PREFIX =
      "mapreduce.map.resource.";

  /**
   * Resource type name for CPU vcores.
   */
  public static final String RESOURCE_TYPE_NAME_VCORE = "vcores";

  /**
   * Resource type name for memory.
   */
  public static final String RESOURCE_TYPE_NAME_MEMORY = "memory";

  /**
   * Alternative resource type name for memory.
   */
  public static final String RESOURCE_TYPE_ALTERNATIVE_NAME_MEMORY =
      "memory-mb";

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

  public static final String MAP_OUTPUT_KEY_FIELD_SEPARATOR = "mapreduce.map.output.key.field.separator";

  /**
   * @deprecated Use {@link #MAP_OUTPUT_KEY_FIELD_SEPARATOR}
   */
  @Deprecated
  public static final String MAP_OUTPUT_KEY_FIELD_SEPERATOR = MAP_OUTPUT_KEY_FIELD_SEPARATOR;

  public static final String MAP_LOG_LEVEL = "mapreduce.map.log.level";

  public static final String REDUCE_LOG_LEVEL = "mapreduce.reduce.log.level";

  public static final String DEFAULT_LOG_LEVEL = "INFO";

  public static final String REDUCE_MERGE_INMEM_THRESHOLD = "mapreduce.reduce.merge.inmem.threshold";

  public static final String REDUCE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.input.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_PERCENT = "mapreduce.reduce.markreset.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_SIZE = "mapreduce.reduce.markreset.buffer.size";

  /**
   * Configuration key for specifying memory requirement for the reducer.
   * Kept for backward-compatibility, mapreduce.reduce.resource.memory
   * is the new preferred way to specify this.
   */
  public static final String REDUCE_MEMORY_MB = "mapreduce.reduce.memory.mb";
  public static final int DEFAULT_REDUCE_MEMORY_MB = 1024;

  /**
   * Configuration key for specifying CPU requirement for the reducer.
   * Kept for backward-compatibility, mapreduce.reduce.resource.vcores
   * is the new preferred way to specify this.
   */
  public static final String REDUCE_CPU_VCORES = "mapreduce.reduce.cpu.vcores";
  public static final int DEFAULT_REDUCE_CPU_VCORES = 1;

  /**
   * Resource names required by the reducer should be
   * appended to this prefix, the value's format is {amount}[ ][{unit}].
   * If no unit is defined, the default unit will be used.
   * Standard resource names: memory (default unit: Mi), vcores
   */
  public static final String REDUCE_RESOURCE_TYPE_PREFIX =
      "mapreduce.reduce.resource.";

  public static final String REDUCE_MEMORY_TOTAL_BYTES = "mapreduce.reduce.memory.totalbytes";

  public static final String SHUFFLE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.shuffle.input.buffer.percent";
  public static final float DEFAULT_SHUFFLE_INPUT_BUFFER_PERCENT = 0.70f;

  public static final String SHUFFLE_MEMORY_LIMIT_PERCENT
    = "mapreduce.reduce.shuffle.memory.limit.percent";

  public static final String SHUFFLE_MERGE_PERCENT = "mapreduce.reduce.shuffle.merge.percent";
  public static final float DEFAULT_SHUFFLE_MERGE_PERCENT = 0.66f;

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
  public static final String MAX_ALLOWED_FETCH_FAILURES_FRACTION = "mapreduce.reduce.shuffle.max-fetch-failures-fraction";
  public static final float DEFAULT_MAX_ALLOWED_FETCH_FAILURES_FRACTION = 0.5f;
  
  public static final String MAX_FETCH_FAILURES_NOTIFICATIONS = "mapreduce.reduce.shuffle.max-fetch-failures-notifications";
  public static final int DEFAULT_MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  public static final String SHUFFLE_FETCH_RETRY_INTERVAL_MS = "mapreduce.reduce.shuffle.fetch.retry.interval-ms";
  /** Default interval that fetcher retry to fetch during NM restart.*/
  public final static int DEFAULT_SHUFFLE_FETCH_RETRY_INTERVAL_MS = 1000;
  
  public static final String SHUFFLE_FETCH_RETRY_TIMEOUT_MS = "mapreduce.reduce.shuffle.fetch.retry.timeout-ms";
  
  public static final String SHUFFLE_FETCH_RETRY_ENABLED = "mapreduce.reduce.shuffle.fetch.retry.enabled";

  public static final String SHUFFLE_NOTIFY_READERROR = "mapreduce.reduce.shuffle.notify.readerror";
  
  public static final String MAX_SHUFFLE_FETCH_RETRY_DELAY = "mapreduce.reduce.shuffle.retry-delay.max.ms";
  public static final long DEFAULT_MAX_SHUFFLE_FETCH_RETRY_DELAY = 60000;
  
  public static final String MAX_SHUFFLE_FETCH_HOST_FAILURES = "mapreduce.reduce.shuffle.max-host-failures";
  public static final int DEFAULT_MAX_SHUFFLE_FETCH_HOST_FAILURES = 5;

  public static final String REDUCE_SKIP_INCR_PROC_COUNT = "mapreduce.reduce.skip.proc-count.auto-incr";

  public static final String REDUCE_SKIP_MAXGROUPS = "mapreduce.reduce.skip.maxgroups";

  public static final String REDUCE_MEMTOMEM_THRESHOLD = "mapreduce.reduce.merge.memtomem.threshold";

  public static final String REDUCE_MEMTOMEM_ENABLED = "mapreduce.reduce.merge.memtomem.enabled";

  public static final String COMBINE_RECORDS_BEFORE_PROGRESS = "mapreduce.task.combine.progress.records";

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE = "mapreduce.job.hdfs-servers.token-renewal.exclude";

  public static final String JOB_JOBTRACKER_ID = "mapreduce.job.kerberos.jtprinicipal";

  public static final String JOB_CANCEL_DELEGATION_TOKEN = "mapreduce.job.complete.cancel.delegation.tokens";

  public static final String JOB_ACL_VIEW_JOB = "mapreduce.job.acl-view-job";

  public static final String DEFAULT_JOB_ACL_VIEW_JOB = " ";

  public static final String JOB_ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";

  public static final String DEFAULT_JOB_ACL_MODIFY_JOB = " ";

  public static final String JOB_RUNNING_MAP_LIMIT =
      "mapreduce.job.running.map.limit";
  public static final int DEFAULT_JOB_RUNNING_MAP_LIMIT = 0;

  public static final String JOB_RUNNING_REDUCE_LIMIT =
      "mapreduce.job.running.reduce.limit";
  public static final int DEFAULT_JOB_RUNNING_REDUCE_LIMIT = 0;

  /* Config for Limit on the number of map tasks allowed per job
   * There is no limit if this value is negative.
   */
  public static final String JOB_MAX_MAP =
      "mapreduce.job.max.map";
  public static final int DEFAULT_JOB_MAX_MAP = -1;

  /* config for tracking the local file where all the credentials for the job
   * credentials.
   */
  public static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = 
      "mapreduce.job.credentials.binary";

  /* Configs for tracking ids of tokens used by a job */
  public static final String JOB_TOKEN_TRACKING_IDS_ENABLED =
      "mapreduce.job.token.tracking.ids.enabled";
  public static final boolean DEFAULT_JOB_TOKEN_TRACKING_IDS_ENABLED = false;
  public static final String JOB_TOKEN_TRACKING_IDS =
      "mapreduce.job.token.tracking.ids";

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

  public static final String MAPREDUCE_JOB_EMIT_TIMELINE_DATA =
    "mapreduce.job.emit-timeline-data";
  public static final boolean DEFAULT_MAPREDUCE_JOB_EMIT_TIMELINE_DATA =
      false;

  public static final String MR_PREFIX = "yarn.app.mapreduce.";

  public static final String MR_AM_PREFIX = MR_PREFIX + "am.";

  /** The number of client retries to the AM - before reconnecting to the RM
   * to fetch Application State. 
   */
  public static final String MR_CLIENT_TO_AM_IPC_MAX_RETRIES = 
    MR_PREFIX + "client-am.ipc.max-retries";
  public static final int DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES = 3;
  
  /** The number of client retries on socket timeouts to the AM - before
   * reconnecting to the RM to fetch Application Status.
   */
  public static final String MR_CLIENT_TO_AM_IPC_MAX_RETRIES_ON_TIMEOUTS =
    MR_PREFIX + "client-am.ipc.max-retries-on-timeouts";
  public static final int
    DEFAULT_MR_CLIENT_TO_AM_IPC_MAX_RETRIES_ON_TIMEOUTS = 3;

  /**
   * The number of client retries to the RM/HS before throwing exception.
   */
  public static final String MR_CLIENT_MAX_RETRIES = 
    MR_PREFIX + "client.max-retries";
  public static final int DEFAULT_MR_CLIENT_MAX_RETRIES = 3;
  
  /**
   * How many times to retry jobclient calls (via getjob)
   */
  public static final String MR_CLIENT_JOB_MAX_RETRIES =
      MR_PREFIX + "client.job.max-retries";
  public static final int DEFAULT_MR_CLIENT_JOB_MAX_RETRIES = 3;

  /**
   * How long to wait between jobclient retries on failure
   */
  public static final String MR_CLIENT_JOB_RETRY_INTERVAL =
      MR_PREFIX + "client.job.retry-interval";
  public static final long DEFAULT_MR_CLIENT_JOB_RETRY_INTERVAL =
      2000;

  /** The staging directory for map reduce.*/
  public static final String MR_AM_STAGING_DIR = 
    MR_AM_PREFIX+"staging-dir";
  public static final String DEFAULT_MR_AM_STAGING_DIR = 
    "/tmp/hadoop-yarn/staging";

  /** The amount of memory the MR app master needs.
   * Kept for backward-compatibility, yarn.app.mapreduce.am.resource.memory is
   * the new preferred way to specify this
   */
  public static final String MR_AM_VMEM_MB =
    MR_AM_PREFIX+"resource.mb";
  public static final int DEFAULT_MR_AM_VMEM_MB = 1536;

  /** The number of virtual cores the MR app master needs.*/
  public static final String MR_AM_CPU_VCORES =
    MR_AM_PREFIX+"resource.cpu-vcores";
  public static final int DEFAULT_MR_AM_CPU_VCORES = 1;

  /**
   * Resource names required by the MR AM should be
   * appended to this prefix, the value's format is {amount}[ ][{unit}].
   * If no unit is defined, the default unit will be used
   * Standard resource names: memory (default unit: Mi), vcores
   */
  public static final String MR_AM_RESOURCE_PREFIX =
      MR_AM_PREFIX + "resource.";

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

  public static final String MR_AM_LOG_KB =
      MR_AM_PREFIX + "container.log.limit.kb";
  public static final int DEFAULT_MR_AM_LOG_KB = 0; // don't roll

  public static final String MR_AM_LOG_BACKUPS =
      MR_AM_PREFIX + "container.log.backups";
  public static final int DEFAULT_MR_AM_LOG_BACKUPS = 0;

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

  /**
   * The initial size of thread pool to launch containers in the app master
   */
  public static final String MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE =
      MR_AM_PREFIX+"containerlauncher.threadpool-initial-size";
  public static final int DEFAULT_MR_AM_CONTAINERLAUNCHER_THREADPOOL_INITIAL_SIZE =
      10;

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

  /**
   * Range of ports that the MapReduce AM can use when binding for its webapp.
   * Leave blank if you want all possible ports.
   */
  String MR_AM_WEBAPP_PORT_RANGE = MR_AM_PREFIX + "webapp.port-range";

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
  public static final boolean MR_AM_JOB_RECOVERY_ENABLE_DEFAULT = true;

  /** 
   * Limit on the number of reducers that can be preempted to ensure that at
   *  least one map task can run if it needs to. Percentage between 0.0 and 1.0
   */
  public static final String MR_AM_JOB_REDUCE_PREEMPTION_LIMIT = 
    MR_AM_PREFIX  + "job.reduce.preemption.limit";
  public static final float DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT = 0.5f;

  /**
   * Policy class encoding responses to preemption requests.
   */
  public static final String MR_AM_PREEMPTION_POLICY =
    MR_AM_PREFIX + "preemption.policy";

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

  public static final String MR_AM_HARD_KILL_TIMEOUT_MS =
      MR_AM_PREFIX + "hard-kill-timeout-ms";
  public static final long DEFAULT_MR_AM_HARD_KILL_TIMEOUT_MS =
      10 * 1000l;

  /**
   * Duration to wait before forcibly preempting a reducer to allow
   * allocating new mappers, even when YARN reports positive headroom.
   */
  public static final String MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC =
      "mapreduce.job.reducer.unconditional-preempt.delay.sec";

  public static final int
      DEFAULT_MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC = 5 * 60;

  /**
   * Duration to wait before preempting a reducer, when there is no headroom
   * to allocate new mappers.
   */
  public static final String MR_JOB_REDUCER_PREEMPT_DELAY_SEC =
      "mapreduce.job.reducer.preempt.delay.sec";
  public static final int DEFAULT_MR_JOB_REDUCER_PREEMPT_DELAY_SEC = 0;

  public static final String MR_AM_ENV =
      MR_AM_PREFIX + "env";
  
  public static final String MR_AM_ADMIN_USER_ENV =
      MR_AM_PREFIX + "admin.user.env";

  // although the AM admin user env default should be the same as the task user
  // env default, there are problems in making it work on Windows currently
  // MAPREDUCE-6588 should address the issue and set it to a proper non-empty
  // value
  public static final String DEFAULT_MR_AM_ADMIN_USER_ENV =
      Shell.WINDOWS ?
          "" :
          "LD_LIBRARY_PATH=" + Apps.crossPlatformify("HADOOP_COMMON_HOME") +
              "/lib/native";

  public static final String MR_AM_PROFILE = MR_AM_PREFIX + "profile";
  public static final boolean DEFAULT_MR_AM_PROFILE = false;
  public static final String MR_AM_PROFILE_PARAMS = MR_AM_PREFIX
      + "profile.params";

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

  // the "%...%" macros can be expanded prematurely and are probably not OK
  // this should be addressed by MAPREDUCE-6588
  public static final String DEFAULT_MAPRED_ADMIN_USER_ENV =
      Shell.WINDOWS ?
          "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin" :
          "LD_LIBRARY_PATH=" + Apps.crossPlatformify("HADOOP_COMMON_HOME") +
              "/lib/native";

  public static final String WORKDIR = "work";

  public static final String OUTPUT = "output";

  public static final String HADOOP_WORK_DIR = "HADOOP_WORK_DIR";

  // Environment variables used by Pipes. (TODO: these
  // do not appear to be used by current pipes source code!)
  public static final String STDOUT_LOGFILE_ENV = "STDOUT_LOGFILE_ENV";
  public static final String STDERR_LOGFILE_ENV = "STDERR_LOGFILE_ENV";

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

  public static final String MR_JOB_END_NOTIFICATION_TIMEOUT =
      "mapreduce.job.end-notification.timeout";

  public static final String MR_JOB_END_RETRY_ATTEMPTS =
    "mapreduce.job.end-notification.retry.attempts";

  public static final String MR_JOB_END_RETRY_INTERVAL =
    "mapreduce.job.end-notification.retry.interval";

  public static final String MR_JOB_END_NOTIFICATION_MAX_ATTEMPTS =
    "mapreduce.job.end-notification.max.attempts";

  public static final String MR_JOB_END_NOTIFICATION_MAX_RETRY_INTERVAL =
    "mapreduce.job.end-notification.max.retry.interval";

  public static final int DEFAULT_MR_JOB_END_NOTIFICATION_TIMEOUT =
      5000;

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
  
  public static final String MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE = 
      "mapreduce.job.log4j-properties-file";

  /**
   * Path to MapReduce framework archive
   */
  public static final String MAPREDUCE_APPLICATION_FRAMEWORK_PATH =
      "mapreduce.application.framework.path";

  /**
   * Default CLASSPATH for all YARN MapReduce applications constructed with
   * platform-agnostic syntax.
   */
  @Public
  @Unstable
  public final String DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH = Apps
    .crossPlatformify("HADOOP_MAPRED_HOME")
      + "/share/hadoop/mapreduce/*,"
      + Apps.crossPlatformify("HADOOP_MAPRED_HOME")
      + "/share/hadoop/mapreduce/lib/*";

  /**
   * Default platform-specific CLASSPATH for all YARN MapReduce applications
   * constructed based on client OS syntax.
   * <p>
   * Note: Use {@link DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH}
   * for cross-platform practice i.e. submit an application from a Windows
   * client to a Linux/Unix server or vice versa.
   * </p>
   */
  public final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
      Shell.WINDOWS ? "%HADOOP_MAPRED_HOME%\\share\\hadoop\\mapreduce\\*,"
          + "%HADOOP_MAPRED_HOME%\\share\\hadoop\\mapreduce\\lib\\*"
          : "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,"
              + "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";

  public static final String WORKFLOW_ID = "mapreduce.workflow.id";

  public static final String TASK_LOG_BACKUPS =
      MR_PREFIX + "task.container.log.backups";
  public static final int DEFAULT_TASK_LOG_BACKUPS = 0; // don't roll

  public static final String REDUCE_SEPARATE_SHUFFLE_LOG =
      MR_PREFIX + "shuffle.log.separate";
  public static final boolean DEFAULT_REDUCE_SEPARATE_SHUFFLE_LOG = true;

  public static final String SHUFFLE_LOG_BACKUPS =
      MR_PREFIX + "shuffle.log.backups";
  public static final int DEFAULT_SHUFFLE_LOG_BACKUPS = 0; // don't roll

  public static final String SHUFFLE_LOG_KB =
      MR_PREFIX + "shuffle.log.limit.kb";
  public static final long DEFAULT_SHUFFLE_LOG_KB = 0L;

  public static final String WORKFLOW_NAME = "mapreduce.workflow.name";
  
  public static final String WORKFLOW_NODE_NAME =
      "mapreduce.workflow.node.name";
  
  public static final String WORKFLOW_ADJACENCY_PREFIX_STRING =
      "mapreduce.workflow.adjacency.";
  
  public static final String WORKFLOW_ADJACENCY_PREFIX_PATTERN =
      "^mapreduce\\.workflow\\.adjacency\\..+";

  public static final String WORKFLOW_TAGS = "mapreduce.workflow.tags";

  /**
   * The maximum number of application attempts.
   * It is a application-specific setting.
   */
  public static final String MR_AM_MAX_ATTEMPTS = "mapreduce.am.max-attempts";

  public static final int DEFAULT_MR_AM_MAX_ATTEMPTS = 2;
  
  public static final String MR_APPLICATION_TYPE = "MAPREDUCE";
  
  public static final String TASK_PREEMPTION =
      "mapreduce.job.preemption";

  public static final String HEAP_MEMORY_MB_RATIO =
      "mapreduce.job.heap.memory-mb.ratio";

  public static final float DEFAULT_HEAP_MEMORY_MB_RATIO = 0.8f;

  public static final String MR_ENCRYPTED_INTERMEDIATE_DATA =
      "mapreduce.job.encrypted-intermediate-data";
  public static final boolean DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA = false;

  public static final String MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS =
      "mapreduce.job.encrypted-intermediate-data-key-size-bits";
  public static final int DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEY_SIZE_BITS =
      128;

  public static final String MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB =
      "mapreduce.job.encrypted-intermediate-data.buffer.kb";
  public static final int DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB =
          128;

  /**
   * The maximum number of resources a map reduce job is allowed to submit for
   * localization via files, libjars, archives, and jobjar command line
   * arguments and through the distributed cache. If set to 0 the limit is
   * ignored.
   */
  String MAX_RESOURCES = "mapreduce.job.cache.limit.max-resources";
  int MAX_RESOURCES_DEFAULT = 0;

  /**
   * The maximum size (in MB) a map reduce job is allowed to submit for
   * localization via files, libjars, archives, and jobjar command line
   * arguments and through the distributed cache. If set to 0 the limit is
   * ignored.
   */
  String MAX_RESOURCES_MB = "mapreduce.job.cache.limit.max-resources-mb";
  long MAX_RESOURCES_MB_DEFAULT = 0;

  /**
   * The maximum size (in MB) of a single resource a map reduce job is allow to
   * submit for localization via files, libjars, archives, and jobjar command
   * line arguments and through the distributed cache. If set to 0 the limit is
   * ignored.
   */
  String MAX_SINGLE_RESOURCE_MB =
      "mapreduce.job.cache.limit.max-single-resource-mb";
  long MAX_SINGLE_RESOURCE_MB_DEFAULT = 0;

  /**
   * Number of OPPORTUNISTIC Containers per 100 containers that will be
   * requested by the MRAppMaster. The Default value is 0, which implies all
   * maps will be guaranteed. A value of 100 means all maps will be requested
   * as opportunistic. For any other value say 'x', the FIRST 'x' maps
   * requested by the AM will be opportunistic. If the total number of maps
   * for the job is less than 'x', then ALL maps will be OPPORTUNISTIC
   */
  public static final String MR_NUM_OPPORTUNISTIC_MAPS_PERCENT =
      "mapreduce.job.num-opportunistic-maps-percent";
  public static final int DEFAULT_MR_NUM_OPPORTUNISTIC_MAPS_PERCENT = 0;

  /**
   * A comma-separated list of properties whose value will be redacted.
   */
  String MR_JOB_REDACTED_PROPERTIES = "mapreduce.job.redacted-properties";

  String MR_JOB_SEND_TOKEN_CONF = "mapreduce.job.send-token-conf";

  String FINISH_JOB_WHEN_REDUCERS_DONE =
      "mapreduce.job.finish-when-all-reducers-done";
  boolean DEFAULT_FINISH_JOB_WHEN_REDUCERS_DONE = true;

  String MR_AM_STAGING_DIR_ERASURECODING_ENABLED =
      MR_AM_STAGING_DIR + ".erasurecoding.enabled";

  boolean DEFAULT_MR_AM_STAGING_ERASURECODING_ENABLED = false;
}
