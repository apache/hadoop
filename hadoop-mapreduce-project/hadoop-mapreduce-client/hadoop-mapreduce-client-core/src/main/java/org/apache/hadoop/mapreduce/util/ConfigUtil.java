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
package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.DeprecationDelta;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

/**
 * Place holder for deprecated keys in the framework 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfigUtil {

  /**
   * Adds all the deprecated keys. Loads mapred-default.xml and mapred-site.xml
   */
  public static void loadResources() {
    addDeprecatedKeys();
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
  }
  
  /**
   * Adds deprecated keys and the corresponding new keys to the Configuration
   */
  @SuppressWarnings("deprecation")
  private static void addDeprecatedKeys()  {
    Configuration.addDeprecations(new DeprecationDelta[] {
      new DeprecationDelta("mapred.temp.dir",
        MRConfig.TEMP_DIR),
      new DeprecationDelta("mapred.local.dir",
        MRConfig.LOCAL_DIR),
      new DeprecationDelta("mapred.cluster.map.memory.mb",
        MRConfig.MAPMEMORY_MB),
      new DeprecationDelta("mapred.cluster.reduce.memory.mb",
        MRConfig.REDUCEMEMORY_MB),
      new DeprecationDelta("mapred.acls.enabled",
          MRConfig.MR_ACLS_ENABLED),

      new DeprecationDelta("mapred.cluster.max.map.memory.mb",
        JTConfig.JT_MAX_MAPMEMORY_MB),
      new DeprecationDelta("mapred.cluster.max.reduce.memory.mb",
        JTConfig.JT_MAX_REDUCEMEMORY_MB),

      new DeprecationDelta("mapred.system.dir",
        JTConfig.JT_SYSTEM_DIR),
      new DeprecationDelta("mapred.job.tracker",
        JTConfig.JT_IPC_ADDRESS),
      new DeprecationDelta("mapred.job.tracker.persist.jobstatus.active",
        JTConfig.JT_PERSIST_JOBSTATUS),
      new DeprecationDelta("mapred.permissions.supergroup",
        MRConfig.MR_SUPERGROUP),
      new DeprecationDelta("mapred.task.cache.levels",
        JTConfig.JT_TASKCACHE_LEVELS),
      new DeprecationDelta("mapred.job.tracker.retire.jobs",
        JTConfig.JT_RETIREJOBS),
      new DeprecationDelta("mapred.tasktracker.indexcache.mb",
        TTConfig.TT_INDEX_CACHE),
      new DeprecationDelta("mapred.tasktracker.map.tasks.maximum",
        TTConfig.TT_MAP_SLOTS),
      new DeprecationDelta("mapred.tasktracker.memory_calculator_plugin",
        TTConfig.TT_RESOURCE_CALCULATOR_PLUGIN),
      new DeprecationDelta("mapred.tasktracker.memorycalculatorplugin",
        TTConfig.TT_RESOURCE_CALCULATOR_PLUGIN),
      new DeprecationDelta("yarn.app.mapreduce.yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts",
        MRJobConfig.MR_CLIENT_TO_AM_IPC_MAX_RETRIES_ON_TIMEOUTS),
      new DeprecationDelta("job.end.notification.url",
        MRJobConfig.MR_JOB_END_NOTIFICATION_URL),
      new DeprecationDelta("job.end.retry.attempts",
        MRJobConfig.MR_JOB_END_RETRY_ATTEMPTS),
      new DeprecationDelta("job.end.retry.interval",
        MRJobConfig.MR_JOB_END_RETRY_INTERVAL),
      new DeprecationDelta("mapred.committer.job.setup.cleanup.needed",
        MRJobConfig.SETUP_CLEANUP_NEEDED),
      new DeprecationDelta("mapred.jar",
        MRJobConfig.JAR),
      new DeprecationDelta("mapred.job.id",
        MRJobConfig.ID),
      new DeprecationDelta("mapred.job.name",
        MRJobConfig.JOB_NAME),
      new DeprecationDelta("mapred.job.priority",
        MRJobConfig.PRIORITY),
      new DeprecationDelta("mapred.job.queue.name",
        MRJobConfig.QUEUE_NAME),
      new DeprecationDelta("mapred.job.reuse.jvm.num.tasks",
        MRJobConfig.JVM_NUMTASKS_TORUN),
      new DeprecationDelta("mapred.map.tasks",
        MRJobConfig.NUM_MAPS),
      new DeprecationDelta("mapred.max.tracker.failures",
        MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER),
      new DeprecationDelta("mapred.reduce.slowstart.completed.maps",
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART),
      new DeprecationDelta("mapred.reduce.tasks",
        MRJobConfig.NUM_REDUCES),
      new DeprecationDelta("mapred.skip.on",
        MRJobConfig.SKIP_RECORDS),
      new DeprecationDelta("mapred.skip.out.dir",
        MRJobConfig.SKIP_OUTDIR),
      new DeprecationDelta("mapred.speculative.execution.slowTaskThreshold",
        MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD),
      new DeprecationDelta("mapred.speculative.execution.speculativeCap",
        MRJobConfig.SPECULATIVECAP_RUNNING_TASKS),
      new DeprecationDelta("job.local.dir",
        MRJobConfig.JOB_LOCAL_DIR),
      new DeprecationDelta("mapreduce.inputformat.class",
        MRJobConfig.INPUT_FORMAT_CLASS_ATTR),
      new DeprecationDelta("mapreduce.map.class",
        MRJobConfig.MAP_CLASS_ATTR),
      new DeprecationDelta("mapreduce.combine.class",
        MRJobConfig.COMBINE_CLASS_ATTR),
      new DeprecationDelta("mapreduce.reduce.class",
        MRJobConfig.REDUCE_CLASS_ATTR),
      new DeprecationDelta("mapreduce.outputformat.class",
        MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR),
      new DeprecationDelta("mapreduce.partitioner.class",
        MRJobConfig.PARTITIONER_CLASS_ATTR),
      new DeprecationDelta("mapred.job.classpath.archives",
        MRJobConfig.CLASSPATH_ARCHIVES),
      new DeprecationDelta("mapred.job.classpath.files",
        MRJobConfig.CLASSPATH_FILES),
      new DeprecationDelta("mapred.cache.files",
        MRJobConfig.CACHE_FILES),
      new DeprecationDelta("mapred.cache.archives",
        MRJobConfig.CACHE_ARCHIVES),
      new DeprecationDelta("mapred.cache.localFiles",
        MRJobConfig.CACHE_LOCALFILES),
      new DeprecationDelta("mapred.cache.localArchives",
        MRJobConfig.CACHE_LOCALARCHIVES),
      new DeprecationDelta("mapred.cache.files.filesizes",
        MRJobConfig.CACHE_FILES_SIZES),
      new DeprecationDelta("mapred.cache.archives.filesizes",
        MRJobConfig.CACHE_ARCHIVES_SIZES),
      new DeprecationDelta("mapred.cache.files.timestamps",
        MRJobConfig.CACHE_FILE_TIMESTAMPS),
      new DeprecationDelta("mapred.cache.archives.timestamps",
        MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS),
      new DeprecationDelta("mapred.working.dir",
        MRJobConfig.WORKING_DIR),
      new DeprecationDelta("user.name",
        MRJobConfig.USER_NAME),
      new DeprecationDelta("mapred.output.key.class",
        MRJobConfig.OUTPUT_KEY_CLASS),
      new DeprecationDelta("mapred.output.value.class",
        MRJobConfig.OUTPUT_VALUE_CLASS),
      new DeprecationDelta("mapred.output.value.groupfn.class",
        MRJobConfig.GROUP_COMPARATOR_CLASS),
      new DeprecationDelta("mapred.output.key.comparator.class",
        MRJobConfig.KEY_COMPARATOR),
      new DeprecationDelta("io.sort.factor",
        MRJobConfig.IO_SORT_FACTOR),
      new DeprecationDelta("io.sort.mb",
        MRJobConfig.IO_SORT_MB),
      new DeprecationDelta("keep.failed.task.files",
        MRJobConfig.PRESERVE_FAILED_TASK_FILES),
      new DeprecationDelta("keep.task.files.pattern",
        MRJobConfig.PRESERVE_FILES_PATTERN),
      new DeprecationDelta("mapred.debug.out.lines",
        MRJobConfig.TASK_DEBUGOUT_LINES),
      new DeprecationDelta("mapred.merge.recordsBeforeProgress",
        MRJobConfig.RECORDS_BEFORE_PROGRESS),
      new DeprecationDelta("mapred.merge.recordsBeforeProgress",
        MRJobConfig.COMBINE_RECORDS_BEFORE_PROGRESS),
      new DeprecationDelta("mapred.skip.attempts.to.start.skipping",
        MRJobConfig.SKIP_START_ATTEMPTS),
      new DeprecationDelta("mapred.task.id",
        MRJobConfig.TASK_ATTEMPT_ID),
      new DeprecationDelta("mapred.task.is.map",
        MRJobConfig.TASK_ISMAP),
      new DeprecationDelta("mapred.task.partition",
        MRJobConfig.TASK_PARTITION),
      new DeprecationDelta("mapred.task.profile",
        MRJobConfig.TASK_PROFILE),
      new DeprecationDelta("mapred.task.profile.maps",
        MRJobConfig.NUM_MAP_PROFILES),
      new DeprecationDelta("mapred.task.profile.reduces",
        MRJobConfig.NUM_REDUCE_PROFILES),
      new DeprecationDelta("mapred.task.timeout",
        MRJobConfig.TASK_TIMEOUT),
      new DeprecationDelta("mapred.tip.id",
        MRJobConfig.TASK_ID),
      new DeprecationDelta("mapred.work.output.dir",
        MRJobConfig.TASK_OUTPUT_DIR),
      new DeprecationDelta("mapred.userlog.limit.kb",
        MRJobConfig.TASK_USERLOG_LIMIT),
      new DeprecationDelta("mapred.task.profile.params",
        MRJobConfig.TASK_PROFILE_PARAMS),
      new DeprecationDelta("io.sort.spill.percent",
        MRJobConfig.MAP_SORT_SPILL_PERCENT),
      new DeprecationDelta("map.input.file",
        MRJobConfig.MAP_INPUT_FILE),
      new DeprecationDelta("map.input.length",
        MRJobConfig.MAP_INPUT_PATH),
      new DeprecationDelta("map.input.start",
        MRJobConfig.MAP_INPUT_START),
      new DeprecationDelta("mapred.job.map.memory.mb",
        MRJobConfig.MAP_MEMORY_MB),
      new DeprecationDelta("mapred.map.child.env",
        MRJobConfig.MAP_ENV),
      new DeprecationDelta("mapred.map.child.java.opts",
        MRJobConfig.MAP_JAVA_OPTS),
      new DeprecationDelta("mapred.map.max.attempts",
        MRJobConfig.MAP_MAX_ATTEMPTS),
      new DeprecationDelta("mapred.map.task.debug.script",
        MRJobConfig.MAP_DEBUG_SCRIPT),
      new DeprecationDelta("mapred.map.tasks.speculative.execution",
        MRJobConfig.MAP_SPECULATIVE),
      new DeprecationDelta("mapred.max.map.failures.percent",
        MRJobConfig.MAP_FAILURES_MAX_PERCENT),
      new DeprecationDelta("mapred.skip.map.auto.incr.proc.count",
        MRJobConfig.MAP_SKIP_INCR_PROC_COUNT),
      new DeprecationDelta("mapred.skip.map.max.skip.records",
        MRJobConfig.MAP_SKIP_MAX_RECORDS),
      new DeprecationDelta("min.num.spills.for.combine",
        MRJobConfig.MAP_COMBINE_MIN_SPILLS),
      new DeprecationDelta("mapred.compress.map.output",
        MRJobConfig.MAP_OUTPUT_COMPRESS),
      new DeprecationDelta("mapred.map.output.compression.codec",
        MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC),
      new DeprecationDelta("mapred.mapoutput.key.class",
        MRJobConfig.MAP_OUTPUT_KEY_CLASS),
      new DeprecationDelta("mapred.mapoutput.value.class",
        MRJobConfig.MAP_OUTPUT_VALUE_CLASS),
      new DeprecationDelta("map.output.key.field.separator",
        MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPARATOR),
      new DeprecationDelta("mapred.map.child.log.level",
        MRJobConfig.MAP_LOG_LEVEL),
      new DeprecationDelta("mapred.inmem.merge.threshold",
        MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD),
      new DeprecationDelta("mapred.job.reduce.input.buffer.percent",
        MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT),
      new DeprecationDelta("mapred.job.reduce.markreset.buffer.percent",
        MRJobConfig.REDUCE_MARKRESET_BUFFER_PERCENT),
      new DeprecationDelta("mapred.job.reduce.memory.mb",
        MRJobConfig.REDUCE_MEMORY_MB),
      new DeprecationDelta("mapred.job.reduce.total.mem.bytes",
        MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES),
      new DeprecationDelta("mapred.job.shuffle.input.buffer.percent",
        MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT),
      new DeprecationDelta("mapred.job.shuffle.merge.percent",
        MRJobConfig.SHUFFLE_MERGE_PERCENT),
      new DeprecationDelta("mapred.max.reduce.failures.percent",
        MRJobConfig.REDUCE_FAILURES_MAXPERCENT),
      new DeprecationDelta("mapred.reduce.child.env",
        MRJobConfig.REDUCE_ENV),
      new DeprecationDelta("mapred.reduce.child.java.opts",
        MRJobConfig.REDUCE_JAVA_OPTS),
      new DeprecationDelta("mapred.reduce.max.attempts",
        MRJobConfig.REDUCE_MAX_ATTEMPTS),
      new DeprecationDelta("mapred.reduce.parallel.copies",
        MRJobConfig.SHUFFLE_PARALLEL_COPIES),
      new DeprecationDelta("mapred.reduce.task.debug.script",
        MRJobConfig.REDUCE_DEBUG_SCRIPT),
      new DeprecationDelta("mapred.reduce.tasks.speculative.execution",
        MRJobConfig.REDUCE_SPECULATIVE),
      new DeprecationDelta("mapred.shuffle.connect.timeout",
        MRJobConfig.SHUFFLE_CONNECT_TIMEOUT),
      new DeprecationDelta("mapred.shuffle.read.timeout",
        MRJobConfig.SHUFFLE_READ_TIMEOUT),
      new DeprecationDelta("mapred.skip.reduce.auto.incr.proc.count",
        MRJobConfig.REDUCE_SKIP_INCR_PROC_COUNT),
      new DeprecationDelta("mapred.skip.reduce.max.skip.groups",
        MRJobConfig.REDUCE_SKIP_MAXGROUPS),
      new DeprecationDelta("mapred.reduce.child.log.level",
        MRJobConfig.REDUCE_LOG_LEVEL),
      new DeprecationDelta("mapreduce.job.counters.limit",
        MRJobConfig.COUNTERS_MAX_KEY),
      new DeprecationDelta("jobclient.completion.poll.interval",
        Job.COMPLETION_POLL_INTERVAL_KEY),
      new DeprecationDelta("jobclient.progress.monitor.poll.interval",
        Job.PROGRESS_MONITOR_POLL_INTERVAL_KEY),
      new DeprecationDelta("jobclient.output.filter",
        Job.OUTPUT_FILTER),
      new DeprecationDelta("mapred.submit.replication",
        Job.SUBMIT_REPLICATION),
      new DeprecationDelta("mapred.used.genericoptionsparser",
        Job.USED_GENERIC_PARSER),
      new DeprecationDelta("mapred.input.dir",
         
          org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR),
      new DeprecationDelta("mapred.input.pathFilter.class",
        org.apache.hadoop.mapreduce.lib.input.
          FileInputFormat.PATHFILTER_CLASS),
      new DeprecationDelta("mapred.max.split.size",
        org.apache.hadoop.mapreduce.lib.input.
          FileInputFormat.SPLIT_MAXSIZE),
      new DeprecationDelta("mapred.min.split.size",
        org.apache.hadoop.mapreduce.lib.input.
          FileInputFormat.SPLIT_MINSIZE),
      new DeprecationDelta("mapred.output.compress",
        org.apache.hadoop.mapreduce.lib.output.
          FileOutputFormat.COMPRESS),
      new DeprecationDelta("mapred.output.compression.codec",
        org.apache.hadoop.mapreduce.lib.output.
          FileOutputFormat.COMPRESS_CODEC),
      new DeprecationDelta("mapred.output.compression.type",
        org.apache.hadoop.mapreduce.lib.output.
          FileOutputFormat.COMPRESS_TYPE),
      new DeprecationDelta("mapred.output.dir",
        org.apache.hadoop.mapreduce.lib.output.
          FileOutputFormat.OUTDIR),
      new DeprecationDelta("mapred.seqbinary.output.key.class",
        org.apache.hadoop.mapreduce.lib.output.
          SequenceFileAsBinaryOutputFormat.KEY_CLASS),
      new DeprecationDelta("mapred.seqbinary.output.value.class",
        org.apache.hadoop.mapreduce.lib.output.
          SequenceFileAsBinaryOutputFormat.VALUE_CLASS),
      new DeprecationDelta("sequencefile.filter.class",
        org.apache.hadoop.mapreduce.lib.input.
          SequenceFileInputFilter.FILTER_CLASS),
      new DeprecationDelta("sequencefile.filter.regex",
        org.apache.hadoop.mapreduce.lib.input.
          SequenceFileInputFilter.FILTER_REGEX),
      new DeprecationDelta("sequencefile.filter.frequency",
        org.apache.hadoop.mapreduce.lib.input.
          SequenceFileInputFilter.FILTER_FREQUENCY),
      new DeprecationDelta("mapred.input.dir.mappers",
        org.apache.hadoop.mapreduce.lib.input.
          MultipleInputs.DIR_MAPPERS),
      new DeprecationDelta("mapred.input.dir.formats",
        org.apache.hadoop.mapreduce.lib.input.
          MultipleInputs.DIR_FORMATS),
      new DeprecationDelta("mapred.line.input.format.linespermap",
        org.apache.hadoop.mapreduce.lib.input.
          NLineInputFormat.LINES_PER_MAP),
      new DeprecationDelta("mapred.binary.partitioner.left.offset",
        org.apache.hadoop.mapreduce.lib.partition.
          BinaryPartitioner.LEFT_OFFSET_PROPERTY_NAME),
      new DeprecationDelta("mapred.binary.partitioner.right.offset",
        org.apache.hadoop.mapreduce.lib.partition.
          BinaryPartitioner.RIGHT_OFFSET_PROPERTY_NAME),
      new DeprecationDelta("mapred.text.key.comparator.options",
        org.apache.hadoop.mapreduce.lib.partition.
          KeyFieldBasedComparator.COMPARATOR_OPTIONS),
      new DeprecationDelta("mapred.text.key.partitioner.options",
        org.apache.hadoop.mapreduce.lib.partition.
          KeyFieldBasedPartitioner.PARTITIONER_OPTIONS),
      new DeprecationDelta("mapred.mapper.regex.group",
        org.apache.hadoop.mapreduce.lib.map.RegexMapper.GROUP),
      new DeprecationDelta("mapred.mapper.regex",
        org.apache.hadoop.mapreduce.lib.map.RegexMapper.PATTERN),
      new DeprecationDelta("create.empty.dir.if.nonexist",
        org.apache.hadoop.mapreduce.lib.jobcontrol.
                      ControlledJob.CREATE_DIR),
      new DeprecationDelta("mapred.data.field.separator",
        org.apache.hadoop.mapreduce.lib.fieldsel.
                      FieldSelectionHelper.DATA_FIELD_SEPARATOR),
      new DeprecationDelta("map.output.key.value.fields.spec",
        org.apache.hadoop.mapreduce.lib.fieldsel.
                      FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC),
      new DeprecationDelta("reduce.output.key.value.fields.spec",
        org.apache.hadoop.mapreduce.lib.fieldsel.
                      FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC),
      new DeprecationDelta("mapred.min.split.size.per.node",
        org.apache.hadoop.mapreduce.lib.input.
                      CombineFileInputFormat.SPLIT_MINSIZE_PERNODE),
      new DeprecationDelta("mapred.min.split.size.per.rack",
        org.apache.hadoop.mapreduce.lib.input.
                      CombineFileInputFormat.SPLIT_MINSIZE_PERRACK),
      new DeprecationDelta("key.value.separator.in.input.line",
        org.apache.hadoop.mapreduce.lib.input.
                      KeyValueLineRecordReader.KEY_VALUE_SEPARATOR),
      new DeprecationDelta("mapred.linerecordreader.maxlength",
        org.apache.hadoop.mapreduce.lib.input.
                      LineRecordReader.MAX_LINE_LENGTH),
      new DeprecationDelta("mapred.lazy.output.format",
        org.apache.hadoop.mapreduce.lib.output.
                      LazyOutputFormat.OUTPUT_FORMAT),
      new DeprecationDelta("mapred.textoutputformat.separator",
        org.apache.hadoop.mapreduce.lib.output.
                      TextOutputFormat.SEPARATOR),
      new DeprecationDelta("mapred.join.expr",
        org.apache.hadoop.mapreduce.lib.join.
                      CompositeInputFormat.JOIN_EXPR),
      new DeprecationDelta("mapred.join.keycomparator",
        org.apache.hadoop.mapreduce.lib.join.
                      CompositeInputFormat.JOIN_COMPARATOR),
      new DeprecationDelta("hadoop.pipes.command-file.keep",
        org.apache.hadoop.mapred.pipes.
                      Submitter.PRESERVE_COMMANDFILE),
      new DeprecationDelta("hadoop.pipes.executable",
        org.apache.hadoop.mapred.pipes.Submitter.EXECUTABLE),
      new DeprecationDelta("hadoop.pipes.executable.interpretor",
        org.apache.hadoop.mapred.pipes.Submitter.INTERPRETOR),
      new DeprecationDelta("hadoop.pipes.java.mapper",
        org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_MAP),
      new DeprecationDelta("hadoop.pipes.java.recordreader",
        org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_RR),
      new DeprecationDelta("hadoop.pipes.java.recordwriter",
        org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_RW),
      new DeprecationDelta("hadoop.pipes.java.reducer",
        org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_REDUCE),
      new DeprecationDelta("hadoop.pipes.partitioner",
        org.apache.hadoop.mapred.pipes.Submitter.PARTITIONER),
      new DeprecationDelta("mapred.pipes.user.inputformat",
        org.apache.hadoop.mapred.pipes.Submitter.INPUT_FORMAT),

      new DeprecationDelta("security.task.umbilical.protocol.acl",
        MRJobConfig.MR_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL),
      new DeprecationDelta("security.job.submission.protocol.acl",
        MRJobConfig.MR_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT),
      new DeprecationDelta("mapreduce.user.classpath.first",
        MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST ),
      new DeprecationDelta("mapred.input.dir.recursive",
        FileInputFormat.INPUT_DIR_RECURSIVE)
    });
  }

  public static void main(String[] args) {
    loadResources();
    Configuration.dumpDeprecatedKeys();
  }
}

