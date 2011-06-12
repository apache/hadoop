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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;

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
  }
  
  /**
   * Adds deprecated keys and the corresponding new keys to the Configuration
   */
  private static void addDeprecatedKeys()  {
    Configuration.addDeprecation("mapred.temp.dir", 
      new String[] {MRConfig.TEMP_DIR});
    Configuration.addDeprecation("mapred.local.dir", 
      new String[] {MRConfig.LOCAL_DIR});
    Configuration.addDeprecation("mapred.cluster.map.memory.mb", 
      new String[] {MRConfig.MAPMEMORY_MB});
    Configuration.addDeprecation("mapred.cluster.reduce.memory.mb", 
      new String[] {MRConfig.REDUCEMEMORY_MB});
    Configuration.addDeprecation("mapred.acls.enabled", 
        new String[] {MRConfig.MR_ACLS_ENABLED});

    Configuration.addDeprecation("hadoop.job.history.user.location", 
      new String[] {MRJobConfig.HISTORY_LOCATION});
    Configuration.addDeprecation("job.end.notification.url", 
      new String[] {MRJobConfig.END_NOTIFICATION_URL});
    Configuration.addDeprecation("job.end.retry.attempts", 
      new String[] {MRJobConfig.END_NOTIFICATION_RETRIES});
    Configuration.addDeprecation("job.end.retry.interval", 
      new String[] {MRJobConfig.END_NOTIFICATION_RETRIE_INTERVAL});
    Configuration.addDeprecation("mapred.committer.job.setup.cleanup.needed", 
      new String[] {MRJobConfig.SETUP_CLEANUP_NEEDED});
    Configuration.addDeprecation("mapred.jar", 
      new String[] {MRJobConfig.JAR});
    Configuration.addDeprecation("mapred.job.id", 
      new String[] {MRJobConfig.ID});
    Configuration.addDeprecation("mapred.job.name", 
      new String[] {MRJobConfig.JOB_NAME});
    Configuration.addDeprecation("mapred.job.priority", 
      new String[] {MRJobConfig.PRIORITY});
    Configuration.addDeprecation("mapred.job.queue.name", 
      new String[] {MRJobConfig.QUEUE_NAME});
    Configuration.addDeprecation("mapred.job.reuse.jvm.num.tasks", 
      new String[] {MRJobConfig.JVM_NUMTASKS_TORUN});
    Configuration.addDeprecation("mapred.map.tasks", 
      new String[] {MRJobConfig.NUM_MAPS});
    Configuration.addDeprecation("mapred.max.tracker.failures", 
      new String[] {MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER});
    Configuration.addDeprecation("mapred.reduce.slowstart.completed.maps", 
      new String[] {MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART});
    Configuration.addDeprecation("mapred.reduce.tasks", 
      new String[] {MRJobConfig.NUM_REDUCES});
    Configuration.addDeprecation("mapred.skip.on", 
      new String[] {MRJobConfig.SKIP_RECORDS});
    Configuration.addDeprecation("mapred.skip.out.dir", 
      new String[] {MRJobConfig.SKIP_OUTDIR});
    Configuration.addDeprecation(
      "mapred.speculative.execution.slowNodeThreshold", 
      new String[] {MRJobConfig.SPECULATIVE_SLOWNODE_THRESHOLD});
    Configuration.addDeprecation(
      "mapred.speculative.execution.slowTaskThreshold", 
      new String[] {MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD});
    Configuration.addDeprecation("mapred.speculative.execution.speculativeCap", 
      new String[] {MRJobConfig.SPECULATIVECAP});
    Configuration.addDeprecation("job.local.dir", 
      new String[] {MRJobConfig.JOB_LOCAL_DIR});
    Configuration.addDeprecation("mapreduce.inputformat.class", 
      new String[] {MRJobConfig.INPUT_FORMAT_CLASS_ATTR});
    Configuration.addDeprecation("mapreduce.map.class", 
      new String[] {MRJobConfig.MAP_CLASS_ATTR});
    Configuration.addDeprecation("mapreduce.combine.class", 
      new String[] {MRJobConfig.COMBINE_CLASS_ATTR});
    Configuration.addDeprecation("mapreduce.reduce.class", 
      new String[] {MRJobConfig.REDUCE_CLASS_ATTR});
    Configuration.addDeprecation("mapreduce.outputformat.class", 
      new String[] {MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR});
    Configuration.addDeprecation("mapreduce.partitioner.class", 
      new String[] {MRJobConfig.PARTITIONER_CLASS_ATTR});
    Configuration.addDeprecation("mapred.job.classpath.archives", 
      new String[] {MRJobConfig.CLASSPATH_ARCHIVES});
    Configuration.addDeprecation("mapred.job.classpath.files", 
      new String[] {MRJobConfig.CLASSPATH_FILES});
    Configuration.addDeprecation("mapred.cache.files", 
      new String[] {MRJobConfig.CACHE_FILES});
    Configuration.addDeprecation("mapred.cache.archives", 
      new String[] {MRJobConfig.CACHE_ARCHIVES});
    Configuration.addDeprecation("mapred.cache.localFiles", 
      new String[] {MRJobConfig.CACHE_LOCALFILES});
    Configuration.addDeprecation("mapred.cache.localArchives", 
      new String[] {MRJobConfig.CACHE_LOCALARCHIVES});
    Configuration.addDeprecation("mapred.cache.files.timestamps", 
      new String[] {MRJobConfig.CACHE_FILE_TIMESTAMPS});
    Configuration.addDeprecation("mapred.cache.archives.timestamps", 
      new String[] {MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS});
    Configuration.addDeprecation("mapred.create.symlink", 
      new String[] {MRJobConfig.CACHE_SYMLINK});
    Configuration.addDeprecation("mapred.working.dir", 
      new String[] {MRJobConfig.WORKING_DIR});
    Configuration.addDeprecation("hadoop.job.history.user.location", 
      new String[] {MRJobConfig.HISTORY_LOCATION});
    Configuration.addDeprecation("user.name", 
      new String[] {MRJobConfig.USER_NAME});
    Configuration.addDeprecation("mapred.output.key.class", 
      new String[] {MRJobConfig.OUTPUT_KEY_CLASS});
    Configuration.addDeprecation("mapred.output.value.class", 
      new String[] {MRJobConfig.OUTPUT_VALUE_CLASS});
    Configuration.addDeprecation("mapred.output.value.groupfn.class", 
      new String[] {MRJobConfig.GROUP_COMPARATOR_CLASS});
    Configuration.addDeprecation("mapred.output.key.comparator.class", 
      new String[] {MRJobConfig.KEY_COMPARATOR});
    Configuration.addDeprecation("io.sort.factor", 
      new String[] {MRJobConfig.IO_SORT_FACTOR});
    Configuration.addDeprecation("io.sort.mb", 
      new String[] {MRJobConfig.IO_SORT_MB});
    Configuration.addDeprecation("keep.failed.task.files", 
      new String[] {MRJobConfig.PRESERVE_FAILED_TASK_FILES});
    Configuration.addDeprecation("keep.task.files.pattern", 
      new String[] {MRJobConfig.PRESERVE_FILES_PATTERN});
    Configuration.addDeprecation("mapred.child.tmp", 
      new String[] {MRJobConfig.TASK_TEMP_DIR});
    Configuration.addDeprecation("mapred.debug.out.lines", 
      new String[] {MRJobConfig.TASK_DEBUGOUT_LINES});
    Configuration.addDeprecation("mapred.merge.recordsBeforeProgress", 
      new String[] {MRJobConfig.RECORDS_BEFORE_PROGRESS});
    Configuration.addDeprecation("mapred.skip.attempts.to.start.skipping", 
      new String[] {MRJobConfig.SKIP_START_ATTEMPTS});
    Configuration.addDeprecation("mapred.task.id", 
      new String[] {MRJobConfig.TASK_ATTEMPT_ID});
    Configuration.addDeprecation("mapred.task.is.map", 
      new String[] {MRJobConfig.TASK_ISMAP});
    Configuration.addDeprecation("mapred.task.partition", 
      new String[] {MRJobConfig.TASK_PARTITION});
    Configuration.addDeprecation("mapred.task.profile", 
      new String[] {MRJobConfig.TASK_PROFILE});
    Configuration.addDeprecation("mapred.task.profile.maps", 
      new String[] {MRJobConfig.NUM_MAP_PROFILES});
    Configuration.addDeprecation("mapred.task.profile.reduces", 
      new String[] {MRJobConfig.NUM_REDUCE_PROFILES});
    Configuration.addDeprecation("mapred.task.timeout", 
      new String[] {MRJobConfig.TASK_TIMEOUT});
    Configuration.addDeprecation("mapred.tip.id", 
      new String[] {MRJobConfig.TASK_ID});
    Configuration.addDeprecation("mapred.work.output.dir", 
      new String[] {MRJobConfig.TASK_OUTPUT_DIR});
    Configuration.addDeprecation("mapred.userlog.limit.kb", 
      new String[] {MRJobConfig.TASK_USERLOG_LIMIT});
    Configuration.addDeprecation("mapred.userlog.retain.hours", 
      new String[] {MRJobConfig.USER_LOG_RETAIN_HOURS});
    Configuration.addDeprecation("mapred.task.profile.params", 
      new String[] {MRJobConfig.TASK_PROFILE_PARAMS});
    Configuration.addDeprecation("io.sort.spill.percent", 
      new String[] {MRJobConfig.MAP_SORT_SPILL_PERCENT});
    Configuration.addDeprecation("map.input.file", 
      new String[] {MRJobConfig.MAP_INPUT_FILE});
    Configuration.addDeprecation("map.input.length", 
      new String[] {MRJobConfig.MAP_INPUT_PATH});
    Configuration.addDeprecation("map.input.start", 
      new String[] {MRJobConfig.MAP_INPUT_START});
    Configuration.addDeprecation("mapred.job.map.memory.mb", 
      new String[] {MRJobConfig.MAP_MEMORY_MB});
    Configuration.addDeprecation("mapred.map.child.env", 
      new String[] {MRJobConfig.MAP_ENV});
    Configuration.addDeprecation("mapred.map.child.java.opts", 
      new String[] {MRJobConfig.MAP_JAVA_OPTS});
    Configuration.addDeprecation("mapred.map.child.ulimit", 
      new String[] {MRJobConfig.MAP_ULIMIT});
    Configuration.addDeprecation("mapred.map.max.attempts", 
      new String[] {MRJobConfig.MAP_MAX_ATTEMPTS});
    Configuration.addDeprecation("mapred.map.task.debug.script", 
      new String[] {MRJobConfig.MAP_DEBUG_SCRIPT});
    Configuration.addDeprecation("mapred.map.tasks.speculative.execution", 
      new String[] {MRJobConfig.MAP_SPECULATIVE});
    Configuration.addDeprecation("mapred.max.map.failures.percent", 
      new String[] {MRJobConfig.MAP_FAILURES_MAX_PERCENT});
    Configuration.addDeprecation("mapred.skip.map.auto.incr.proc.count", 
      new String[] {MRJobConfig.MAP_SKIP_INCR_PROC_COUNT});
    Configuration.addDeprecation("mapred.skip.map.max.skip.records", 
      new String[] {MRJobConfig.MAP_SKIP_MAX_RECORDS});
    Configuration.addDeprecation("min.num.spills.for.combine", 
      new String[] {MRJobConfig.MAP_COMBINE_MIN_SPILLS});
    Configuration.addDeprecation("mapred.compress.map.output", 
      new String[] {MRJobConfig.MAP_OUTPUT_COMPRESS});
    Configuration.addDeprecation("mapred.map.output.compression.codec", 
      new String[] {MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC});
    Configuration.addDeprecation("mapred.mapoutput.key.class", 
      new String[] {MRJobConfig.MAP_OUTPUT_KEY_CLASS});
    Configuration.addDeprecation("mapred.mapoutput.value.class", 
      new String[] {MRJobConfig.MAP_OUTPUT_VALUE_CLASS});
    Configuration.addDeprecation("map.output.key.field.separator", 
      new String[] {MRJobConfig.MAP_OUTPUT_KEY_FIELD_SEPERATOR});
    Configuration.addDeprecation("mapred.map.child.log.level", 
      new String[] {MRJobConfig.MAP_LOG_LEVEL});
    Configuration.addDeprecation("mapred.inmem.merge.threshold", 
      new String[] {MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD});
    Configuration.addDeprecation("mapred.job.reduce.input.buffer.percent", 
      new String[] {MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT});
    Configuration.addDeprecation("mapred.job.reduce.markreset.buffer.percent", 
      new String[] {MRJobConfig.REDUCE_MARKRESET_BUFFER_PERCENT});
    Configuration.addDeprecation("mapred.job.reduce.memory.mb", 
      new String[] {MRJobConfig.REDUCE_MEMORY_MB});
    Configuration.addDeprecation("mapred.job.reduce.total.mem.bytes", 
      new String[] {MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES});
    Configuration.addDeprecation("mapred.job.shuffle.input.buffer.percent", 
      new String[] {MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT});
    Configuration.addDeprecation("mapred.job.shuffle.merge.percent", 
      new String[] {MRJobConfig.SHUFFLE_MERGE_EPRCENT});
    Configuration.addDeprecation("mapred.max.reduce.failures.percent", 
      new String[] {MRJobConfig.REDUCE_FAILURES_MAXPERCENT});
    Configuration.addDeprecation("mapred.reduce.child.env", 
      new String[] {MRJobConfig.REDUCE_ENV});
    Configuration.addDeprecation("mapred.reduce.child.java.opts", 
      new String[] {MRJobConfig.REDUCE_JAVA_OPTS});
    Configuration.addDeprecation("mapred.reduce.child.ulimit", 
      new String[] {MRJobConfig.REDUCE_ULIMIT});
    Configuration.addDeprecation("mapred.reduce.max.attempts", 
      new String[] {MRJobConfig.REDUCE_MAX_ATTEMPTS});
    Configuration.addDeprecation("mapred.reduce.parallel.copies", 
      new String[] {MRJobConfig.SHUFFLE_PARALLEL_COPIES});
    Configuration.addDeprecation("mapred.reduce.task.debug.script", 
      new String[] {MRJobConfig.REDUCE_DEBUG_SCRIPT});
    Configuration.addDeprecation("mapred.reduce.tasks.speculative.execution", 
      new String[] {MRJobConfig.REDUCE_SPECULATIVE});
    Configuration.addDeprecation("mapred.shuffle.connect.timeout", 
      new String[] {MRJobConfig.SHUFFLE_CONNECT_TIMEOUT});
    Configuration.addDeprecation("mapred.shuffle.read.timeout", 
      new String[] {MRJobConfig.SHUFFLE_READ_TIMEOUT});
    Configuration.addDeprecation("mapred.skip.reduce.auto.incr.proc.count", 
      new String[] {MRJobConfig.REDUCE_SKIP_INCR_PROC_COUNT});
    Configuration.addDeprecation("mapred.skip.reduce.max.skip.groups", 
      new String[] {MRJobConfig.REDUCE_SKIP_MAXGROUPS});
    Configuration.addDeprecation("mapred.reduce.child.log.level", 
      new String[] {MRJobConfig.REDUCE_LOG_LEVEL});
    Configuration.addDeprecation("jobclient.completion.poll.interval", 
      new String[] {Job.COMPLETION_POLL_INTERVAL_KEY});
    Configuration.addDeprecation("jobclient.progress.monitor.poll.interval", 
      new String[] {Job.PROGRESS_MONITOR_POLL_INTERVAL_KEY});
    Configuration.addDeprecation("jobclient.output.filter", 
      new String[] {Job.OUTPUT_FILTER});
    Configuration.addDeprecation("mapred.submit.replication", 
      new String[] {Job.SUBMIT_REPLICATION});
    Configuration.addDeprecation("mapred.used.genericoptionsparser", 
      new String[] {Job.USED_GENERIC_PARSER});
    Configuration.addDeprecation("mapred.input.dir", 
      new String[] {
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR});
    Configuration.addDeprecation("mapred.input.pathFilter.class", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        FileInputFormat.PATHFILTER_CLASS});
    Configuration.addDeprecation("mapred.max.split.size", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        FileInputFormat.SPLIT_MAXSIZE});
    Configuration.addDeprecation("mapred.min.split.size", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        FileInputFormat.SPLIT_MINSIZE});
    Configuration.addDeprecation("mapred.output.compress", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        FileOutputFormat.COMPRESS});
    Configuration.addDeprecation("mapred.output.compression.codec", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        FileOutputFormat.COMPRESS_CODEC});
    Configuration.addDeprecation("mapred.output.compression.type", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        FileOutputFormat.COMPRESS_TYPE});
    Configuration.addDeprecation("mapred.output.dir", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        FileOutputFormat.OUTDIR});
    Configuration.addDeprecation("mapred.seqbinary.output.key.class", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        SequenceFileAsBinaryOutputFormat.KEY_CLASS});
    Configuration.addDeprecation("mapred.seqbinary.output.value.class", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
        SequenceFileAsBinaryOutputFormat.VALUE_CLASS});
    Configuration.addDeprecation("sequencefile.filter.class", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        SequenceFileInputFilter.FILTER_CLASS});
    Configuration.addDeprecation("sequencefile.filter.regex", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        SequenceFileInputFilter.FILTER_REGEX});
    Configuration.addDeprecation("sequencefile.filter.frequency", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        SequenceFileInputFilter.FILTER_FREQUENCY});
    Configuration.addDeprecation("mapred.input.dir.mappers", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        MultipleInputs.DIR_MAPPERS});
    Configuration.addDeprecation("mapred.input.dir.formats", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        MultipleInputs.DIR_FORMATS});
    Configuration.addDeprecation("mapred.line.input.format.linespermap", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
        NLineInputFormat.LINES_PER_MAP});
    Configuration.addDeprecation("mapred.binary.partitioner.left.offset", 
      new String[] {org.apache.hadoop.mapreduce.lib.partition.
        BinaryPartitioner.LEFT_OFFSET_PROPERTY_NAME});
    Configuration.addDeprecation("mapred.binary.partitioner.right.offset", 
      new String[] {org.apache.hadoop.mapreduce.lib.partition.
        BinaryPartitioner.RIGHT_OFFSET_PROPERTY_NAME});
    Configuration.addDeprecation("mapred.text.key.comparator.options", 
      new String[] {org.apache.hadoop.mapreduce.lib.partition.
        KeyFieldBasedComparator.COMPARATOR_OPTIONS});
    Configuration.addDeprecation("mapred.text.key.partitioner.options", 
      new String[] {org.apache.hadoop.mapreduce.lib.partition.
        KeyFieldBasedPartitioner.PARTITIONER_OPTIONS});
    Configuration.addDeprecation("mapred.mapper.regex.group", 
      new String[] {org.apache.hadoop.mapreduce.lib.map.RegexMapper.GROUP});
    Configuration.addDeprecation("mapred.mapper.regex", 
      new String[] {org.apache.hadoop.mapreduce.lib.map.RegexMapper.PATTERN});
    Configuration.addDeprecation("create.empty.dir.if.nonexist", 
      new String[] {org.apache.hadoop.mapreduce.lib.jobcontrol.
                    ControlledJob.CREATE_DIR});
    Configuration.addDeprecation("mapred.data.field.separator", 
      new String[] {org.apache.hadoop.mapreduce.lib.fieldsel.
                    FieldSelectionHelper.DATA_FIELD_SEPERATOR});
    Configuration.addDeprecation("map.output.key.value.fields.spec", 
      new String[] {org.apache.hadoop.mapreduce.lib.fieldsel.
                    FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC});
    Configuration.addDeprecation("reduce.output.key.value.fields.spec", 
      new String[] {org.apache.hadoop.mapreduce.lib.fieldsel.
                    FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC});
    Configuration.addDeprecation("mapred.min.split.size.per.node", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
                    CombineFileInputFormat.SPLIT_MINSIZE_PERNODE});
    Configuration.addDeprecation("mapred.min.split.size.per.rack", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
                    CombineFileInputFormat.SPLIT_MINSIZE_PERRACK});
    Configuration.addDeprecation("key.value.separator.in.input.line", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
                    KeyValueLineRecordReader.KEY_VALUE_SEPERATOR});
    Configuration.addDeprecation("mapred.linerecordreader.maxlength", 
      new String[] {org.apache.hadoop.mapreduce.lib.input.
                    LineRecordReader.MAX_LINE_LENGTH});
    Configuration.addDeprecation("mapred.lazy.output.format", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
                    LazyOutputFormat.OUTPUT_FORMAT});
    Configuration.addDeprecation("mapred.textoutputformat.separator", 
      new String[] {org.apache.hadoop.mapreduce.lib.output.
                    TextOutputFormat.SEPERATOR});
    Configuration.addDeprecation("mapred.join.expr", 
      new String[] {org.apache.hadoop.mapreduce.lib.join.
                    CompositeInputFormat.JOIN_EXPR});
    Configuration.addDeprecation("mapred.join.keycomparator", 
      new String[] {org.apache.hadoop.mapreduce.lib.join.
                    CompositeInputFormat.JOIN_COMPARATOR});
    Configuration.addDeprecation("hadoop.pipes.command-file.keep", 
      new String[] {org.apache.hadoop.mapred.pipes.
                    Submitter.PRESERVE_COMMANDFILE});
    Configuration.addDeprecation("hadoop.pipes.executable", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.EXECUTABLE});
    Configuration.addDeprecation("hadoop.pipes.executable.interpretor", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.INTERPRETOR});
    Configuration.addDeprecation("hadoop.pipes.java.mapper", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_MAP});
    Configuration.addDeprecation("hadoop.pipes.java.recordreader", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_RR});
    Configuration.addDeprecation("hadoop.pipes.java.recordwriter", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_RW});
    Configuration.addDeprecation("hadoop.pipes.java.reducer", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.IS_JAVA_REDUCE});
    Configuration.addDeprecation("hadoop.pipes.partitioner", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.PARTITIONER});
    Configuration.addDeprecation("mapred.pipes.user.inputformat", 
      new String[] {org.apache.hadoop.mapred.pipes.Submitter.INPUT_FORMAT});
    
    Configuration.addDeprecation("mapred.tasktracker.indexcache.mb", 
        new String[] {MRConfig.JOB_INDEX_CACHE});
    Configuration.addDeprecation("mapred.tasktracker.memory_calculator_plugin", 
        new String[] {MRConfig.RESOURCE_CALCULATOR_PLUGIN});
    Configuration.addDeprecation("mapred.tasktracker.memorycalculatorplugin", 
        new String[] {MRConfig.RESOURCE_CALCULATOR_PLUGIN});
    Configuration.addDeprecation("hadoop.net.static.resolutions", 
        new String[] {MRConfig.STATIC_RESOLUTIONS});

    Configuration.addDeprecation("mapred.job.tracker", 
        new String[] {MRConfig.MASTER_ADDRESS});
  }
}

