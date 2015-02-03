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
package org.apache.hadoop.mapred.gridmix;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import static org.apache.hadoop.tools.rumen.datatypes.util.MapReduceJobPropertiesParser.extractMaxHeapOpts;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Synthetic job generated from a trace description.
 */
abstract class GridmixJob implements Callable<Job>, Delayed {

  // Gridmix job name format is GRIDMIX<6 digit sequence number>
  public static final String JOB_NAME_PREFIX = "GRIDMIX";
  public static final Log LOG = LogFactory.getLog(GridmixJob.class);

  private static final ThreadLocal<Formatter> nameFormat =
    new ThreadLocal<Formatter>() {
      @Override
      protected Formatter initialValue() {
        final StringBuilder sb =
            new StringBuilder(JOB_NAME_PREFIX.length() + 6);
        sb.append(JOB_NAME_PREFIX);
        return new Formatter(sb);
      }
    };

  private boolean submitted;
  protected final int seq;
  protected final Path outdir;
  protected final Job job;
  protected final JobStory jobdesc;
  protected final UserGroupInformation ugi;
  protected final long submissionTimeNanos;
  private static final ConcurrentHashMap<Integer,List<InputSplit>> descCache =
     new ConcurrentHashMap<Integer,List<InputSplit>>();
  protected static final String GRIDMIX_JOB_SEQ = "gridmix.job.seq";
  protected static final String GRIDMIX_USE_QUEUE_IN_TRACE = 
      "gridmix.job-submission.use-queue-in-trace";
  protected static final String GRIDMIX_DEFAULT_QUEUE = 
      "gridmix.job-submission.default-queue";
  // configuration key to enable/disable High-Ram feature emulation
  static final String GRIDMIX_HIGHRAM_EMULATION_ENABLE = 
    "gridmix.highram-emulation.enable";
  // configuration key to enable/disable task jvm options
  static final String GRIDMIX_TASK_JVM_OPTIONS_ENABLE = 
    "gridmix.task.jvm-options.enable";

  private static void setJobQueue(Job job, String queue) {
    if (queue != null) {
      job.getConfiguration().set(MRJobConfig.QUEUE_NAME, queue);
    }
  }
  
  public GridmixJob(final Configuration conf, long submissionMillis,
      final JobStory jobdesc, Path outRoot, UserGroupInformation ugi, 
      final int seq) throws IOException {
    this.ugi = ugi;
    this.jobdesc = jobdesc;
    this.seq = seq;

    ((StringBuilder)nameFormat.get().out()).setLength(JOB_NAME_PREFIX.length());
    try {
      job = this.ugi.doAs(new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException {

          String jobId = null == jobdesc.getJobID() 
                         ? "<unknown>" 
                         : jobdesc.getJobID().toString();
          Job ret = Job.getInstance(conf, nameFormat.get().format("%06d", seq)
                                          .toString());
          ret.getConfiguration().setInt(GRIDMIX_JOB_SEQ, seq);

          ret.getConfiguration().set(Gridmix.ORIGINAL_JOB_ID, jobId);
          ret.getConfiguration().set(Gridmix.ORIGINAL_JOB_NAME,
                                     jobdesc.getName());
          if (conf.getBoolean(GRIDMIX_USE_QUEUE_IN_TRACE, false)) {
            setJobQueue(ret, jobdesc.getQueueName());
          } else {
            setJobQueue(ret, conf.get(GRIDMIX_DEFAULT_QUEUE));
          }

          // check if the job can emulate compression
          if (canEmulateCompression()) {
            // set the compression related configs if compression emulation is
            // enabled
            if (CompressionEmulationUtil.isCompressionEmulationEnabled(conf)) {
              CompressionEmulationUtil.configureCompressionEmulation(
                  jobdesc.getJobConf(), ret.getConfiguration());
            }
          }
          
          // configure high ram properties if enabled
          if (conf.getBoolean(GRIDMIX_HIGHRAM_EMULATION_ENABLE, true)) {
            configureHighRamProperties(jobdesc.getJobConf(), 
                                       ret.getConfiguration());
          }
          
          // configure task jvm options if enabled
          // this knob can be turned off if there is a mismatch between the
          // target (simulation) cluster and the original cluster. Such a 
          // mismatch can result in job failures (due to memory issues) on the 
          // target (simulated) cluster.
          //
          // TODO If configured, scale the original task's JVM (heap related)
          //      options to suit the target (simulation) cluster
          if (conf.getBoolean(GRIDMIX_TASK_JVM_OPTIONS_ENABLE, true)) {
            configureTaskJVMOptions(jobdesc.getJobConf(), 
                                    ret.getConfiguration());
          }
          
          return ret;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    outdir = new Path(outRoot, "" + seq);
  }
  
  @SuppressWarnings("deprecation")
  protected static void configureTaskJVMOptions(Configuration originalJobConf,
                                                Configuration simulatedJobConf){
    // Get the heap related java opts used for the original job and set the 
    // same for the simulated job.
    //    set task task heap options
    configureTaskJVMMaxHeapOptions(originalJobConf, simulatedJobConf, 
                                   JobConf.MAPRED_TASK_JAVA_OPTS);
    //  set map task heap options
    configureTaskJVMMaxHeapOptions(originalJobConf, simulatedJobConf, 
                                   MRJobConfig.MAP_JAVA_OPTS);

    //  set reduce task heap options
    configureTaskJVMMaxHeapOptions(originalJobConf, simulatedJobConf, 
                                   MRJobConfig.REDUCE_JAVA_OPTS);
  }
  
  // Configures the task's max heap options using the specified key
  private static void configureTaskJVMMaxHeapOptions(Configuration srcConf, 
                                                     Configuration destConf,
                                                     String key) {
    String srcHeapOpts = srcConf.get(key);
    if (srcHeapOpts != null) {
      List<String> srcMaxOptsList = new ArrayList<String>();
      // extract the max heap options and ignore the rest
      extractMaxHeapOpts(srcHeapOpts, srcMaxOptsList, 
                         new ArrayList<String>());
      if (srcMaxOptsList.size() > 0) {
        List<String> destOtherOptsList = new ArrayList<String>();
        // extract the other heap options and ignore the max options in the 
        // destination configuration
        String destHeapOpts = destConf.get(key);
        if (destHeapOpts != null) {
          extractMaxHeapOpts(destHeapOpts, new ArrayList<String>(), 
                             destOtherOptsList);
        }
        
        // the source configuration might have some task level max heap opts set
        // remove these opts from the destination configuration and replace
        // with the options set in the original configuration
        StringBuilder newHeapOpts = new StringBuilder();
        
        for (String otherOpt : destOtherOptsList) {
          newHeapOpts.append(otherOpt).append(" ");
        }
        
        for (String opts : srcMaxOptsList) {
          newHeapOpts.append(opts).append(" ");
        }
        
        // set the final heap opts 
        destConf.set(key, newHeapOpts.toString().trim());
      }
    }
  }

  // Scales the desired job-level configuration parameter. This API makes sure 
  // that the ratio of the job level configuration parameter to the cluster 
  // level configuration parameter is maintained in the simulated run. Hence 
  // the values are scaled from the original cluster's configuration to the 
  // simulated cluster's configuration for higher emulation accuracy.
  // This kind of scaling is useful for memory parameters.
  private static void scaleConfigParameter(Configuration sourceConf, 
                        Configuration destConf, String clusterValueKey, 
                        String jobValueKey, long defaultValue) {
    long simulatedClusterDefaultValue = 
           destConf.getLong(clusterValueKey, defaultValue);
    
    long originalClusterDefaultValue = 
           sourceConf.getLong(clusterValueKey, defaultValue);
    
    long originalJobValue = 
           sourceConf.getLong(jobValueKey, defaultValue);
    
    double scaleFactor = (double)originalJobValue/originalClusterDefaultValue;
    
    long simulatedJobValue = (long)(scaleFactor * simulatedClusterDefaultValue);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("For the job configuration parameter '" + jobValueKey 
                + "' and the cluster configuration parameter '" 
                + clusterValueKey + "', the original job's configuration value"
                + " is scaled from '" + originalJobValue + "' to '" 
                + simulatedJobValue + "' using the default (unit) value of "
                + "'" + originalClusterDefaultValue + "' for the original "
                + " cluster and '" + simulatedClusterDefaultValue + "' for the"
                + " simulated cluster.");
    }
    
    destConf.setLong(jobValueKey, simulatedJobValue);
  }
  
  // Checks if the scaling of original job's memory parameter value is 
  // valid
  @SuppressWarnings("deprecation")
  private static boolean checkMemoryUpperLimits(String jobKey, String limitKey,  
                                                Configuration conf, 
                                                boolean convertLimitToMB) {
    if (conf.get(limitKey) != null) {
      long limit = conf.getLong(limitKey, JobConf.DISABLED_MEMORY_LIMIT);
      // scale only if the max memory limit is set.
      if (limit >= 0) {
        if (convertLimitToMB) {
          limit /= (1024 * 1024); //Converting to MB
        }
        
        long scaledConfigValue = 
               conf.getLong(jobKey, JobConf.DISABLED_MEMORY_LIMIT);
        
        // check now
        if (scaledConfigValue > limit) {
          throw new RuntimeException("Simulated job's configuration" 
              + " parameter '" + jobKey + "' got scaled to a value '" 
              + scaledConfigValue + "' which exceeds the upper limit of '" 
              + limit + "' defined for the simulated cluster by the key '" 
              + limitKey + "'. To disable High-Ram feature emulation, set '" 
              + GRIDMIX_HIGHRAM_EMULATION_ENABLE + "' to 'false'.");
        }
        return true;
      }
    }
    return false;
  }
  
  // Check if the parameter scaling does not exceed the cluster limits.
  @SuppressWarnings("deprecation")
  private static void validateTaskMemoryLimits(Configuration conf, 
                        String jobKey, String clusterMaxKey) {
    if (!checkMemoryUpperLimits(jobKey, 
        JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, conf, true)) {
      checkMemoryUpperLimits(jobKey, clusterMaxKey, conf, false);
    }
  }

  /**
   * Sets the high ram job properties in the simulated job's configuration.
   */
  @SuppressWarnings("deprecation")
  static void configureHighRamProperties(Configuration sourceConf, 
                                         Configuration destConf) {
    // set the memory per map task
    scaleConfigParameter(sourceConf, destConf, 
                         MRConfig.MAPMEMORY_MB, MRJobConfig.MAP_MEMORY_MB, 
                         MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    
    // validate and fail early
    validateTaskMemoryLimits(destConf, MRJobConfig.MAP_MEMORY_MB, 
                             JTConfig.JT_MAX_MAPMEMORY_MB);
    
    // set the memory per reduce task
    scaleConfigParameter(sourceConf, destConf, 
                         MRConfig.REDUCEMEMORY_MB, MRJobConfig.REDUCE_MEMORY_MB,
                         MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    // validate and fail early
    validateTaskMemoryLimits(destConf, MRJobConfig.REDUCE_MEMORY_MB, 
                             JTConfig.JT_MAX_REDUCEMEMORY_MB);
  }
  
  /**
   * Indicates whether this {@link GridmixJob} supports compression emulation.
   */
  protected abstract boolean canEmulateCompression();
  
  protected GridmixJob(final Configuration conf, long submissionMillis, 
                       final String name) throws IOException {
    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    jobdesc = null;
    outdir = null;
    seq = -1;
    ugi = UserGroupInformation.getCurrentUser();

    try {
      job = this.ugi.doAs(new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException {
          Job ret = Job.getInstance(conf, name);
          ret.getConfiguration().setInt(GRIDMIX_JOB_SEQ, seq);
          setJobQueue(ret, conf.get(GRIDMIX_DEFAULT_QUEUE));
          return ret;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }

  public String toString() {
    return job.getJobName();
  }

  public long getDelay(TimeUnit unit) {
    return unit.convert(submissionTimeNanos - System.nanoTime(),
        TimeUnit.NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed other) {
    if (this == other) {
      return 0;
    }
    if (other instanceof GridmixJob) {
      final long otherNanos = ((GridmixJob)other).submissionTimeNanos;
      if (otherNanos < submissionTimeNanos) {
        return 1;
      }
      if (otherNanos > submissionTimeNanos) {
        return -1;
      }
      return id() - ((GridmixJob)other).id();
    }
    final long diff =
      getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS);
    return 0 == diff ? 0 : (diff > 0 ? 1 : -1);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    // not possible unless job is cloned; all jobs should be unique
    return other instanceof GridmixJob && id() == ((GridmixJob)other).id();
  }

  @Override
  public int hashCode() {
    return id();
  }

  int id() {
    return seq;
  }

  Job getJob() {
    return job;
  }

  JobStory getJobDesc() {
    return jobdesc;
  }

  void setSubmitted() {
    submitted = true;
  }
  
  boolean isSubmitted() {
    return submitted;
  }
  
  static void pushDescription(int seq, List<InputSplit> splits) {
    if (null != descCache.putIfAbsent(seq, splits)) {
      throw new IllegalArgumentException("Description exists for id " + seq);
    }
  }

  static List<InputSplit> pullDescription(JobContext jobCtxt) {
    return pullDescription(GridmixJob.getJobSeqId(jobCtxt));
  }
  
  static List<InputSplit> pullDescription(int seq) {
    return descCache.remove(seq);
  }

  static void clearAll() {
    descCache.clear();
  }

  void buildSplits(FilePool inputDir) throws IOException {

  }
  static int getJobSeqId(JobContext job) {
    return job.getConfiguration().getInt(GRIDMIX_JOB_SEQ,-1);
  }

  public static class DraftPartitioner<V> extends Partitioner<GridmixKey,V> {
    public int getPartition(GridmixKey key, V value, int numReduceTasks) {
      return key.getPartition();
    }
  }

  public static class SpecGroupingComparator
      implements RawComparator<GridmixKey> {
    private final DataInputBuffer di = new DataInputBuffer();
    private final byte[] reset = di.getData();
    @Override
    public int compare(GridmixKey g1, GridmixKey g2) {
      final byte t1 = g1.getType();
      final byte t2 = g2.getType();
      if (t1 == GridmixKey.REDUCE_SPEC ||
          t2 == GridmixKey.REDUCE_SPEC) {
        return t1 - t2;
      }
      assert t1 == GridmixKey.DATA;
      assert t2 == GridmixKey.DATA;
      return g1.compareTo(g2);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        final int ret;
        di.reset(b1, s1, l1);
        final int x1 = WritableUtils.readVInt(di);
        di.reset(b2, s2, l2);
        final int x2 = WritableUtils.readVInt(di);
        final int t1 = b1[s1 + x1];
        final int t2 = b2[s2 + x2];
        if (t1 == GridmixKey.REDUCE_SPEC ||
            t2 == GridmixKey.REDUCE_SPEC) {
          ret = t1 - t2;
        } else {
          assert t1 == GridmixKey.DATA;
          assert t2 == GridmixKey.DATA;
          ret =
            WritableComparator.compareBytes(b1, s1, x1, b2, s2, x2);
        }
        di.reset(reset, 0, 0);
        return ret;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class RawBytesOutputFormat<K>
      extends FileOutputFormat<K,GridmixRecord> {

    @Override
    public RecordWriter<K,GridmixRecord> getRecordWriter(
        TaskAttemptContext job) throws IOException {

      Path file = getDefaultWorkFile(job, "");
      final DataOutputStream fileOut;

      fileOut = 
        new DataOutputStream(CompressionEmulationUtil
            .getPossiblyCompressedOutputStream(file, job.getConfiguration()));

      return new RecordWriter<K,GridmixRecord>() {
        @Override
        public void write(K ignored, GridmixRecord value)
            throws IOException {
          // Let the Gridmix record fill itself.
          value.write(fileOut);
        }
        @Override
        public void close(TaskAttemptContext ctxt) throws IOException {
          fileOut.close();
        }
      };
    }
  }
}
