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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface JobContext {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  public static final String INPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.job.inputformat.class";
  public static final String MAP_CLASS_ATTR = "mapreduce.job.map.class";
  public static final String COMBINE_CLASS_ATTR = 
    "mapreduce.job.combine.class";
  public static final String REDUCE_CLASS_ATTR = 
    "mapreduce.job.reduce.class";
  public static final String OUTPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.job.outputformat.class";
  public static final String PARTITIONER_CLASS_ATTR = 
    "mapreduce.job.partitioner.class";

  public static final String SETUP_CLEANUP_NEEDED = 
    "mapreduce.job.committer.setup.cleanup.needed";
  public static final String JAR = "mapreduce.job.jar";
  public static final String ID = "mapreduce.job.id";
  public static final String JOB_NAME = "mapreduce.job.name";
  public static final String USER_NAME = "mapreduce.job.user.name";
  public static final String PRIORITY = "mapreduce.job.priority";
  public static final String QUEUE_NAME = "mapreduce.job.queuename";
  public static final String JVM_NUMTASKS_TORUN = 
    "mapreduce.job.jvm.numtasks";
  public static final String SPLIT_FILE = "mapreduce.job.splitfile";
  public static final String NUM_MAPS = "mapreduce.job.maps";
  public static final String MAX_TASK_FAILURES_PER_TRACKER = 
    "mapreduce.job.maxtaskfailures.per.tracker";
  public static final String COMPLETED_MAPS_FOR_REDUCE_SLOWSTART =
    "mapreduce.job.reduce.slowstart.completedmaps";
  public static final String NUM_REDUCES = "mapreduce.job.reduces";
  public static final String SKIP_RECORDS = "mapreduce.job.skiprecords";
  public static final String SKIP_OUTDIR = "mapreduce.job.skip.outdir";
  public static final String SPECULATIVE_SLOWNODE_THRESHOLD =
    "mapreduce.job.speculative.slownodethreshold";
  public static final String SPECULATIVE_SLOWTASK_THRESHOLD = 
    "mapreduce.job.speculative.slowtaskthreshold";
  public static final String SPECULATIVECAP = 
    "mapreduce.job.speculative.speculativecap";
  public static final String JOB_LOCAL_DIR = "mapreduce.job.local.dir";
  public static final String OUTPUT_KEY_CLASS = 
    "mapreduce.job.output.key.class";
  public static final String OUTPUT_VALUE_CLASS = 
    "mapreduce.job.output.value.class";
  public static final String KEY_COMPARATOR = 
    "mapreduce.job.output.key.comparator.class";
  public static final String GROUP_COMPARATOR_CLASS = 
    "mapreduce.job.output.group.comparator.class";
  public static final String WORKING_DIR = "mapreduce.job.working.dir";
  public static final String HISTORY_LOCATION = 
    "mapreduce.job.userhistorylocation"; 
  public static final String END_NOTIFICATION_URL = 
    "mapreduce.job.end-notification.url";
  public static final String END_NOTIFICATION_RETRIES = 
    "mapreduce.job.end-notification.retry.attempts";
  public static final String END_NOTIFICATION_RETRIE_INTERVAL = 
    "mapreduce.job.end-notification.retry.interval";
  public static final String CLASSPATH_ARCHIVES = 
    "mapreduce.job.classpath.archives";
  public static final String CLASSPATH_FILES = "mapreduce.job.classpath.files";
  public static final String CACHE_FILES = "mapreduce.job.cache.files";
  public static final String CACHE_ARCHIVES = "mapreduce.job.cache.archives";
  public static final String CACHE_LOCALFILES = 
    "mapreduce.job.cache.local.files";
  public static final String CACHE_LOCALARCHIVES = 
    "mapreduce.job.cache.local.archives";
  public static final String CACHE_FILE_TIMESTAMPS = 
    "mapreduce.job.cache.files.timestamps";
  public static final String CACHE_ARCHIVES_TIMESTAMPS = 
    "mapreduce.job.cache.archives.timestamps";
  public static final String CACHE_SYMLINK = 
    "mapreduce.job.cache.symlink.create";
  
  public static final String IO_SORT_FACTOR = 
    "mapreduce.task.io.sort.factor"; 
  public static final String IO_SORT_MB = "mapreduce.task.io.sort.mb";
  public static final String PRESERVE_FAILED_TASK_FILES = 
    "mapreduce.task.files.preserve.failedtasks";
  public static final String PRESERVE_FILES_PATTERN = 
    "mapreduce.task.files.preserve.filepattern";
  public static final String TASK_TEMP_DIR = "mapreduce.task.tmp.dir";
  public static final String TASK_DEBUGOUT_LINES = 
    "mapreduce.task.debugout.lines";
  public static final String RECORDS_BEFORE_PROGRESS = 
    "mapreduce.task.merge.progress.records";
  public static final String SKIP_START_ATTEMPTS = 
    "mapreduce.task.skip.start.attempts";
  public static final String TASK_ATTEMPT_ID = "mapreduce.task.attempt.id";
  public static final String TASK_ISMAP = "mapreduce.task.ismap";
  public static final String TASK_PARTITION = "mapreduce.task.partition";
  public static final String TASK_PROFILE = "mapreduce.task.profile";
  public static final String TASK_PROFILE_PARAMS = 
    "mapreduce.task.profile.params";
  public static final String NUM_MAP_PROFILES = 
    "mapreduce.task.profile.maps";
  public static final String NUM_REDUCE_PROFILES = 
    "mapreduce.task.profile.reduces";
  public static final String TASK_TIMEOUT = "mapreduce.task.timeout";
  public static final String TASK_ID = "mapreduce.task.id";
  public static final String TASK_OUTPUT_DIR = "mapreduce.task.output.dir";
  public static final String TASK_USERLOG_LIMIT = 
    "mapreduce.task.userlog.limit.kb";
  public static final String TASK_LOG_RETAIN_HOURS = 
    "mapred.task.userlog.retain.hours";
  
  public static final String MAP_SORT_RECORD_PERCENT = 
    "mapreduce.map.sort.record.percent";
  public static final String MAP_SORT_SPILL_PERCENT =
    "mapreduce.map.sort.spill.percent";
  public static final String MAP_INPUT_FILE = "mapreduce.map.input.file";
  public static final String MAP_INPUT_PATH = "mapreduce.map.input.length";
  public static final String MAP_INPUT_START = "mapreduce.map.input.start";
  public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  public static final String MAP_ENV = "mapreduce.map.env";
  public static final String MAP_JAVA_OPTS = "mapreduce.map.java.opts";
  public static final String MAP_ULIMIT = "mapreduce.map.ulimit"; 
  public static final String MAP_MAX_ATTEMPTS = "mapreduce.map.maxattempts";
  public static final String MAP_DEBUG_SCRIPT = 
    "mapreduce.map.debug.script";
  public static final String MAP_SPECULATIVE = "mapreduce.map.speculative";
  public static final String MAP_FAILURES_MAX_PERCENT = 
    "mapreduce.map.failures.maxpercent";
  public static final String MAP_SKIP_INCR_PROC_COUNT = 
    "mapreduce.map.skip.proc-count.auto-incr";
  public static final String MAP_SKIP_MAX_RECORDS = 
    "mapreduce.map.skip.maxrecords";
  public static final String MAP_COMBINE_MIN_SPILLS = 
    "mapreduce.map.combine.minspills";
  public static final String MAP_OUTPUT_COMPRESS = 
    "mapreduce.map.output.compress";
  public static final String MAP_OUTPUT_COMPRESS_CODEC = 
    "mapreduce.map.output.compress.codec";
  public static final String MAP_OUTPUT_KEY_CLASS = 
    "mapreduce.map.output.key.class";
  public static final String MAP_OUTPUT_VALUE_CLASS = 
    "mapreduce.map.output.value.class";
  public static final String MAP_OUTPUT_KEY_FIELD_SEPERATOR = 
    "mapreduce.map.output.key.field.separator";
  public static final String MAP_LOG_LEVEL = "mapreduce.map.log.level";
 
  public static final String REDUCE_LOG_LEVEL = 
    "mapreduce.reduce.log.level";
  public static final String REDUCE_MERGE_INMEM_THRESHOLD = 
    "mapreduce.reduce.merge.inmem.threshold";
  public static final String REDUCE_INPUT_BUFFER_PERCENT = 
    "mapreduce.reduce.input.buffer.percent";
  public static final String REDUCE_MARKRESET_BUFFER_PERCENT = 
    "mapreduce.reduce.markreset.buffer.percent";
  public static final String REDUCE_MARKRESET_BUFFER_SIZE = 
    "mapreduce.reduce.markreset.buffer.size";
  public static final String REDUCE_MEMORY_MB = 
    "mapreduce.reduce.memory.mb";
  public static final String REDUCE_MEMORY_TOTAL_BYTES = 
    "mapreduce.reduce.memory.totalbytes";
  public static final String SHUFFLE_INPUT_BUFFER_PERCENT = 
    "mapreduce.reduce.shuffle.input.buffer.percent";
  public static final String SHUFFLE_MERGE_EPRCENT = 
    "mapreduce.reduce.shuffle.merge.percent";
  public static final String REDUCE_FAILURES_MAXPERCENT = 
   "mapreduce.reduce.failures.maxpercent";
  public static final String REDUCE_ENV = "mapreduce.reduce.env";
  public static final String REDUCE_JAVA_OPTS = 
    "mapreduce.reduce.java.opts";
  public static final String REDUCE_ULIMIT = "mapreduce.reduce.ulimit"; 
  public static final String REDUCE_MAX_ATTEMPTS = 
    "mapreduce.reduce.maxattempts";
  public static final String SHUFFLE_PARALLEL_COPIES = 
    "mapreduce.reduce.shuffle.parallelcopies";
  public static final String REDUCE_DEBUG_SCRIPT = 
    "mapreduce.reduce.debug.script";
  public static final String REDUCE_SPECULATIVE = 
    "mapreduce.reduce.speculative";
  public static final String SHUFFLE_CONNECT_TIMEOUT = 
    "mapreduce.reduce.shuffle.connect.timeout";
  public static final String SHUFFLE_READ_TIMEOUT = 
    "mapreduce.reduce.shuffle.read.timeout";
  public static final String SHUFFLE_FETCH_FAILURES = 
    "mapreduce.reduce.shuffle.maxfetchfailures";
  public static final String SHUFFLE_NOTIFY_READERROR = 
    "mapreduce.reduce.shuffle.notify.readerror";
  public static final String REDUCE_SKIP_INCR_PROC_COUNT = 
    "mapreduce.reduce.skip.proc-count.auto-incr";
  public static final String REDUCE_SKIP_MAXGROUPS = 
    "mapreduce.reduce.skip.maxgroups";
  public static final String REDUCE_MEMTOMEM_THRESHOLD = 
    "mapreduce.reduce.merge.memtomem.threshold";
  public static final String REDUCE_MEMTOMEM_ENABLED = 
    "mapreduce.reduce.merge.memtomem.enabled";

  /**
   * Return the configuration for the job.
   * @return the shared configuration object
   */
  public Configuration getConfiguration();

  /**
   * Get the unique ID for the job.
   * @return the object with the job id
   */
  public JobID getJobID();
  
  /**
   * Get configured the number of reduce tasks for this job. Defaults to 
   * <code>1</code>.
   * @return the number of reduce tasks for this job.
   */
  public int getNumReduceTasks();
  
  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException;

  /**
   * Get the key class for the job output data.
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass();
  
  /**
   * Get the value class for job outputs.
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass();

  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output key class. This allows the map output key class to be
   * different than the final output key class.
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass();

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass();

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName();

  /**
   * Get the {@link InputFormat} class for the job.
   * 
   * @return the {@link InputFormat} class for the job.
   */
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Mapper} class for the job.
   * 
   * @return the {@link Mapper} class for the job.
   */
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException;

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Reducer} class for the job.
   * 
   * @return the {@link Reducer} class for the job.
   */
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException;

  /**
   * Get the {@link RawComparator} comparator used to compare keys.
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator();

  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar();

  /** 
   * Get the user defined {@link RawComparator} comparator for 
   * grouping keys of inputs to the reduce.
   * 
   * @return comparator set by the user for grouping values.
   * @see Job#setGroupingComparatorClass(Class) for details.  
   */
  public RawComparator<?> getGroupingComparator();
  
  /**
   * Get whether job-setup and job-cleanup is needed for the job 
   * 
   * @return boolean 
   */
  public boolean getJobSetupCleanupNeeded();

  /**
   * Get whether the task profiling is enabled.
   * @return true if some tasks will be profiled
   */
  public boolean getProfileEnabled();

  /**
   * Get the profiler configuration arguments.
   *
   * The default value for this property is
   * "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
   * 
   * @return the parameters to pass to the task child to configure profiling
   */
  public String getProfileParams();

  /**
   * Get the range of maps or reduces to profile.
   * @param isMap is the task a map?
   * @return the task ranges
   */
  public IntegerRanges getProfileTaskRange(boolean isMap);

  /**
   * Get the reported username for this job.
   * 
   * @return the username
   */
  public String getUser();
  
  /**
   * This method checks to see if symlinks are to be create for the 
   * localized cache files in the current working directory 
   * @return true if symlinks are to be created- else return false
   */
  public boolean getSymlink();
  
  /**
   * Get the archive entries in classpath as an array of Path
   */
  public Path[] getArchiveClassPaths();

  /**
   * Get cache archives set in the Configuration
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */
  public URI[] getCacheArchives() throws IOException;

  /**
   * Get cache files set in the Configuration
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */

  public URI[] getCacheFiles() throws IOException;

  /**
   * Return the path array of the localized caches
   * @return A path array of localized caches
   * @throws IOException
   */
  public Path[] getLocalCacheArchives() throws IOException;

  /**
   * Return the path array of the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public Path[] getLocalCacheFiles() throws IOException;

  /**
   * Get the file entries in classpath as an array of Path
   */
  public Path[] getFileClassPaths();
  
  /**
   * Get the timestamps of the archives.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   * @throws IOException
   */
  public String[] getArchiveTimestamps();

  /**
   * Get the timestamps of the files.  Used by internal
   * DistributedCache and MapReduce code.
   * @return a string array of timestamps 
   * @throws IOException
   */
  public String[] getFileTimestamps();

  /** 
   * Get the configured number of maximum attempts that will be made to run a
   * map task, as specified by the <code>mapred.map.max.attempts</code>
   * property. If this property is not already set, the default is 4 attempts.
   *  
   * @return the max number of attempts per map task.
   */
  public int getMaxMapAttempts();

  /** 
   * Get the configured number of maximum attempts  that will be made to run a
   * reduce task, as specified by the <code>mapred.reduce.max.attempts</code>
   * property. If this property is not already set, the default is 4 attempts.
   * 
   * @return the max number of attempts per reduce task.
   */
  public int getMaxReduceAttempts();

}
