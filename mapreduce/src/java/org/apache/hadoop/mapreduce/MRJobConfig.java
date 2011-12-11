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

  public static final String COMBINE_CLASS_ATTR = "mapreduce.job.combine.class";

  public static final String REDUCE_CLASS_ATTR = "mapreduce.job.reduce.class";

  public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";

  public static final String PARTITIONER_CLASS_ATTR = "mapreduce.job.partitioner.class";

  public static final String SETUP_CLEANUP_NEEDED = "mapreduce.job.committer.setup.cleanup.needed";

  public static final String JAR = "mapreduce.job.jar";

  public static final String ID = "mapreduce.job.id";

  public static final String JOB_NAME = "mapreduce.job.name";

  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  public static final String USER_NAME = "mapreduce.job.user.name";

  public static final String PRIORITY = "mapreduce.job.priority";

  public static final String QUEUE_NAME = "mapreduce.job.queuename";

  public static final String JVM_NUMTASKS_TORUN = "mapreduce.job.jvm.numtasks";

  public static final String SPLIT_FILE = "mapreduce.job.splitfile";

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

  public static final String END_NOTIFICATION_URL = "mapreduce.job.end-notification.url";

  public static final String END_NOTIFICATION_RETRIES = "mapreduce.job.end-notification.retry.attempts";

  public static final String END_NOTIFICATION_RETRIE_INTERVAL = "mapreduce.job.end-notification.retry.interval";

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

  public static final String CACHE_SYMLINK = "mapreduce.job.cache.symlink.create";

  public static final String USER_LOG_RETAIN_HOURS = "mapreduce.job.userlog.retain.hours";

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

  public static final String TASK_TIMEOUT = "mapreduce.task.timeout";

  public static final String TASK_ID = "mapreduce.task.id";

  public static final String TASK_OUTPUT_DIR = "mapreduce.task.output.dir";

  public static final String TASK_USERLOG_LIMIT = "mapreduce.task.userlog.limit.kb";

  public static final String MAP_SORT_SPILL_PERCENT = "mapreduce.map.sort.spill.percent";

  public static final String MAP_INPUT_FILE = "mapreduce.map.input.file";

  public static final String MAP_INPUT_PATH = "mapreduce.map.input.length";

  public static final String MAP_INPUT_START = "mapreduce.map.input.start";

  public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";

  public static final String MAP_MEMORY_PHYSICAL_MB = "mapreduce.map.memory.physical.mb";

  public static final String MAP_ENV = "mapreduce.map.env";

  public static final String MAP_JAVA_OPTS = "mapreduce.map.java.opts";

  public static final String MAP_ULIMIT = "mapreduce.map.ulimit";

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

  public static final String REDUCE_MERGE_INMEM_THRESHOLD = "mapreduce.reduce.merge.inmem.threshold";

  public static final String REDUCE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.input.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_PERCENT = "mapreduce.reduce.markreset.buffer.percent";

  public static final String REDUCE_MARKRESET_BUFFER_SIZE = "mapreduce.reduce.markreset.buffer.size";

  public static final String REDUCE_MEMORY_PHYSICAL_MB = "mapreduce.reduce.memory.physical.mb";

  public static final String REDUCE_MEMORY_MB = "mapreduce.reduce.memory.mb";

  public static final String REDUCE_MEMORY_TOTAL_BYTES = "mapreduce.reduce.memory.totalbytes";

  public static final String SHUFFLE_INPUT_BUFFER_PERCENT = "mapreduce.reduce.shuffle.input.buffer.percent";

  public static final String SHUFFLE_MERGE_EPRCENT = "mapreduce.reduce.shuffle.merge.percent";

  public static final String REDUCE_FAILURES_MAXPERCENT = "mapreduce.reduce.failures.maxpercent";

  public static final String REDUCE_ENV = "mapreduce.reduce.env";

  public static final String REDUCE_JAVA_OPTS = "mapreduce.reduce.java.opts";

  public static final String REDUCE_ULIMIT = "mapreduce.reduce.ulimit";

  public static final String REDUCE_MAX_ATTEMPTS = "mapreduce.reduce.maxattempts";

  public static final String SHUFFLE_PARALLEL_COPIES = "mapreduce.reduce.shuffle.parallelcopies";

  public static final String REDUCE_DEBUG_SCRIPT = "mapreduce.reduce.debug.script";

  public static final String REDUCE_SPECULATIVE = "mapreduce.reduce.speculative";

  public static final String SHUFFLE_CONNECT_TIMEOUT = "mapreduce.reduce.shuffle.connect.timeout";

  public static final String SHUFFLE_READ_TIMEOUT = "mapreduce.reduce.shuffle.read.timeout";

  public static final String SHUFFLE_FETCH_FAILURES = "mapreduce.reduce.shuffle.maxfetchfailures";

  public static final String SHUFFLE_NOTIFY_READERROR = "mapreduce.reduce.shuffle.notify.readerror";

  public static final String REDUCE_SKIP_INCR_PROC_COUNT = "mapreduce.reduce.skip.proc-count.auto-incr";

  public static final String REDUCE_SKIP_MAXGROUPS = "mapreduce.reduce.skip.maxgroups";

  public static final String REDUCE_MEMTOMEM_THRESHOLD = "mapreduce.reduce.merge.memtomem.threshold";

  public static final String REDUCE_MEMTOMEM_ENABLED = "mapreduce.reduce.merge.memtomem.enabled";

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_JOBTRACKER_ID = "mapreduce.job.kerberos.jtprinicipal";

  public static final String JOB_CANCEL_DELEGATION_TOKEN = "mapreduce.job.complete.cancel.delegation.tokens";

  public static final String JOB_ACL_VIEW_JOB = "mapreduce.job.acl-view-job";

  public static final String JOB_ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";
  public static final String JOB_SUBMITHOST =
    "mapreduce.job.submithostname";
  public static final String JOB_SUBMITHOSTADDR =
    "mapreduce.job.submithostaddress";
}
