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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.gridmix.emulators.resourceusage.ResourceUsageMatcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Synthetic job generated from a trace description.
 */
class LoadJob extends GridmixJob {

  public static final Logger LOG = LoggerFactory.getLogger(LoadJob.class);

  public LoadJob(final Configuration conf, long submissionMillis, 
                 final JobStory jobdesc, Path outRoot, UserGroupInformation ugi,
                 final int seq) throws IOException {
    super(conf, submissionMillis, jobdesc, outRoot, ugi, seq);
  }

  public Job call() throws IOException, InterruptedException,
                           ClassNotFoundException {
    ugi.doAs(
      new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException, ClassNotFoundException,
                                InterruptedException {
          job.setMapperClass(LoadMapper.class);
          job.setReducerClass(LoadReducer.class);
          job.setNumReduceTasks(jobdesc.getNumberReduces());
          job.setMapOutputKeyClass(GridmixKey.class);
          job.setMapOutputValueClass(GridmixRecord.class);
          job.setSortComparatorClass(LoadSortComparator.class);
          job.setGroupingComparatorClass(SpecGroupingComparator.class);
          job.setInputFormatClass(LoadInputFormat.class);
          job.setOutputFormatClass(RawBytesOutputFormat.class);
          job.setPartitionerClass(DraftPartitioner.class);
          job.setJarByClass(LoadJob.class);
          job.getConfiguration().setBoolean(Job.USED_GENERIC_PARSER, true);
          FileOutputFormat.setOutputPath(job, outdir);
          job.submit();
          return job;
        }
      });

    return job;
  }

  @Override
  protected boolean canEmulateCompression() {
    return true;
  }
  
  /**
   * This is a load matching key comparator which will make sure that the
   * resource usage load is matched even when the framework is in control.
   */
  public static class LoadSortComparator extends GridmixKey.Comparator {
    private ResourceUsageMatcherRunner matcher = null;
    private boolean isConfigured = false;
    
    public LoadSortComparator() {
      super();
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      configure();
      int ret = super.compare(b1, s1, l1, b2, s2, l2);
      if (matcher != null) {
        try {
          matcher.match(); // match the resource usage now
        } catch (Exception e) {}
      }
      return ret;
    }
    
    //TODO Note that the sorter will be instantiated 2 times as follows
    //       1. During the sort/spill in the map phase
    //       2. During the merge in the sort phase
    // We need the handle to the matcher thread only in (2).
    // This logic can be relaxed to run only in (2).
    private void configure() {
      if (!isConfigured) {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        Thread[] threads = new Thread[group.activeCount() * 2];
        group.enumerate(threads, true);
        for (Thread t : threads) {
          if (t != null && (t instanceof ResourceUsageMatcherRunner)) {
            this.matcher = (ResourceUsageMatcherRunner) t;
            isConfigured = true;
            break;
          }
        }
      }
    }
  }
  
  /**
   * This is a progress based resource usage matcher.
   */
  @SuppressWarnings("unchecked")
  static class ResourceUsageMatcherRunner extends Thread 
  implements Progressive {
    private final ResourceUsageMatcher matcher;
    private final BoostingProgress progress;
    private final long sleepTime;
    private static final String SLEEP_CONFIG = 
      "gridmix.emulators.resource-usage.sleep-duration";
    private static final long DEFAULT_SLEEP_TIME = 100; // 100ms
    
    /**
     * This is a progress bar that can be boosted for weaker use-cases.
     */
    private static class BoostingProgress implements Progressive {
      private float boostValue = 0f;
      TaskInputOutputContext context;
      
      BoostingProgress(TaskInputOutputContext context) {
        this.context = context;
      }
      
      void setBoostValue(float boostValue) {
        this.boostValue = boostValue;
      }
      
      @Override
      public float getProgress() {
        return Math.min(1f, context.getProgress() + boostValue);
      }
    }
    
    ResourceUsageMatcherRunner(final TaskInputOutputContext context, 
                               ResourceUsageMetrics metrics) {
      Configuration conf = context.getConfiguration();
      
      // set the resource calculator plugin
      Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(TTConfig.TT_RESOURCE_CALCULATOR_PLUGIN,
                      null, ResourceCalculatorPlugin.class);
      ResourceCalculatorPlugin plugin = 
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
      
      // set the other parameters
      this.sleepTime = conf.getLong(SLEEP_CONFIG, DEFAULT_SLEEP_TIME);
      progress = new BoostingProgress(context);
      
      // instantiate a resource-usage-matcher
      matcher = new ResourceUsageMatcher();
      matcher.configure(conf, plugin, metrics, progress);
    }
    
    protected void match() throws IOException, InterruptedException {
      // match the resource usage
      matcher.matchResourceUsage();
    }
    
    @Override
    public void run() {
      LOG.info("Resource usage matcher thread started.");
      try {
        while (progress.getProgress() < 1) {
          // match
          match();
          
          // sleep for some time
          try {
            Thread.sleep(sleepTime);
          } catch (Exception e) {}
        }
        
        // match for progress = 1
        match();
        LOG.info("Resource usage emulation complete! Matcher exiting");
      } catch (Exception e) {
        LOG.info("Exception while running the resource-usage-emulation matcher"
                 + " thread! Exiting.", e);
      }
    }
    
    @Override
    public float getProgress() {
      return matcher.getProgress();
    }
    
    // boost the progress bar as fasten up the emulation cycles.
    void boost(float value) {
      progress.setBoostValue(value);
    }
  }
  
  // Makes sure that the TaskTracker doesn't kill the map/reduce tasks while
  // they are emulating
  private static class StatusReporter extends Thread {
    private final TaskAttemptContext context;
    private final Progressive progress;
    
    StatusReporter(TaskAttemptContext context, Progressive progress) {
      this.context = context;
      this.progress = progress;
    }
    
    @Override
    public void run() {
      LOG.info("Status reporter thread started.");
      try {
        while (!isInterrupted() && progress.getProgress() < 1) {
          // report progress
          context.progress();

          // sleep for some time
          try {
            Thread.sleep(100); // sleep for 100ms
          } catch (Exception e) {}
        }
        
        LOG.info("Status reporter thread exiting");
      } catch (Exception e) {
        LOG.info("Exception while running the status reporter thread!", e);
      }
    }
  }
  
  public static class LoadMapper
  extends Mapper<NullWritable, GridmixRecord, GridmixKey, GridmixRecord> {

    private double acc;
    private double ratio;
    private final ArrayList<RecordFactory> reduces =
      new ArrayList<RecordFactory>();
    private final Random r = new Random();

    private final GridmixKey key = new GridmixKey();
    private final GridmixRecord val = new GridmixRecord();

    private ResourceUsageMatcherRunner matcher = null;
    private StatusReporter reporter = null;
    
    @Override
    protected void setup(Context ctxt) 
    throws IOException, InterruptedException {
      final Configuration conf = ctxt.getConfiguration();
      final LoadSplit split = (LoadSplit) ctxt.getInputSplit();
      final int maps = split.getMapCount();
      final long[] reduceBytes = split.getOutputBytes();
      final long[] reduceRecords = split.getOutputRecords();

      long totalRecords = 0L;
      final int nReduces = ctxt.getNumReduceTasks();
      if (nReduces > 0) {
        // enable gridmix map output record for compression
        boolean emulateMapOutputCompression = 
          CompressionEmulationUtil.isCompressionEmulationEnabled(conf)
          && conf.getBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false);
        float compressionRatio = 1.0f;
        if (emulateMapOutputCompression) {
          compressionRatio = 
            CompressionEmulationUtil.getMapOutputCompressionEmulationRatio(conf);
          LOG.info("GridMix is configured to use a compression ratio of " 
                   + compressionRatio + " for the map output data.");
          key.setCompressibility(true, compressionRatio);
          val.setCompressibility(true, compressionRatio);
        }
        
        int idx = 0;
        int id = split.getId();
        for (int i = 0; i < nReduces; ++i) {
          final GridmixKey.Spec spec = new GridmixKey.Spec();
          if (i == id) {
            spec.bytes_out = split.getReduceBytes(idx);
            spec.rec_out = split.getReduceRecords(idx);
            spec.setResourceUsageSpecification(
                   split.getReduceResourceUsageMetrics(idx));
            ++idx;
            id += maps;
          }
          
          // set the map output bytes such that the final reduce input bytes 
          // match the expected value obtained from the original job
          long mapOutputBytes = reduceBytes[i];
          if (emulateMapOutputCompression) {
            mapOutputBytes /= compressionRatio;
          }
          reduces.add(new IntermediateRecordFactory(
              new AvgRecordFactory(mapOutputBytes, reduceRecords[i], conf, 
                                   5*1024),
              i, reduceRecords[i], spec, conf));
          totalRecords += reduceRecords[i];
        }
      } else {
        long mapOutputBytes = reduceBytes[0];
        
        // enable gridmix job output compression
        boolean emulateJobOutputCompression = 
          CompressionEmulationUtil.isCompressionEmulationEnabled(conf)
          && conf.getBoolean(FileOutputFormat.COMPRESS, false);

        if (emulateJobOutputCompression) {
          float compressionRatio = 
            CompressionEmulationUtil.getJobOutputCompressionEmulationRatio(conf);
          LOG.info("GridMix is configured to use a compression ratio of " 
                   + compressionRatio + " for the job output data.");
          key.setCompressibility(true, compressionRatio);
          val.setCompressibility(true, compressionRatio);

          // set the output size accordingly
          mapOutputBytes /= compressionRatio;
        }
        reduces.add(new AvgRecordFactory(mapOutputBytes, reduceRecords[0],
                                         conf, 5*1024));
        totalRecords = reduceRecords[0];
      }
      final long splitRecords = split.getInputRecords();
      int missingRecSize = 
        conf.getInt(AvgRecordFactory.GRIDMIX_MISSING_REC_SIZE, 64*1024);
      final long inputRecords = 
        (splitRecords <= 0 && split.getLength() >= 0)
        ? Math.max(1, split.getLength() / missingRecSize)
        : splitRecords;
      ratio = totalRecords / (1.0 * inputRecords);
      acc = 0.0;
      
      matcher = new ResourceUsageMatcherRunner(ctxt, 
                      split.getMapResourceUsageMetrics());
      matcher.setDaemon(true);
      
      // start the status reporter thread
      reporter = new StatusReporter(ctxt, matcher);
      reporter.setDaemon(true);
      reporter.start();
    }

    @Override
    public void map(NullWritable ignored, GridmixRecord rec,
                    Context context) throws IOException, InterruptedException {
      acc += ratio;
      while (acc >= 1.0 && !reduces.isEmpty()) {
        key.setSeed(r.nextLong());
        val.setSeed(r.nextLong());
        final int idx = r.nextInt(reduces.size());
        final RecordFactory f = reduces.get(idx);
        if (!f.next(key, val)) {
          reduces.remove(idx);
          continue;
        }
        context.write(key, val);
        acc -= 1.0;
        
        // match inline
        try {
          matcher.match();
        } catch (Exception e) {
          LOG.debug("Error in resource usage emulation! Message: ", e);
        }
      }
    }

    @Override
    public void cleanup(Context context) 
    throws IOException, InterruptedException {
      LOG.info("Starting the cleanup phase.");
      for (RecordFactory factory : reduces) {
        key.setSeed(r.nextLong());
        while (factory.next(key, val)) {
          // send the progress update (maybe make this a thread)
          context.progress();
          
          context.write(key, val);
          key.setSeed(r.nextLong());
          
          // match inline
          try {
            matcher.match();
          } catch (Exception e) {
            LOG.debug("Error in resource usage emulation! Message: ", e);
          }
        }
      }
      
      // check if the thread will get a chance to run or not
      //  check if there will be a sort&spill->merge phase or not
      //  check if the final sort&spill->merge phase is gonna happen or not
      if (context.getNumReduceTasks() > 0 
          && context.getCounter(TaskCounter.SPILLED_RECORDS).getValue() == 0) {
        LOG.info("Boosting the map phase progress.");
        // add the sort phase progress to the map phase and emulate
        matcher.boost(0.33f);
        matcher.match();
      }
      
      // start the matcher thread since the map phase ends here
      matcher.start();
    }
  }

  public static class LoadReducer
  extends Reducer<GridmixKey,GridmixRecord,NullWritable,GridmixRecord> {

    private final Random r = new Random();
    private final GridmixRecord val = new GridmixRecord();

    private double acc;
    private double ratio;
    private RecordFactory factory;

    private ResourceUsageMatcherRunner matcher = null;
    private StatusReporter reporter = null;
    
    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
      if (!context.nextKey() 
          || context.getCurrentKey().getType() != GridmixKey.REDUCE_SPEC) {
        throw new IOException("Missing reduce spec");
      }
      long outBytes = 0L;
      long outRecords = 0L;
      long inRecords = 0L;
      ResourceUsageMetrics metrics = new ResourceUsageMetrics();
      for (GridmixRecord ignored : context.getValues()) {
        final GridmixKey spec = context.getCurrentKey();
        inRecords += spec.getReduceInputRecords();
        outBytes += spec.getReduceOutputBytes();
        outRecords += spec.getReduceOutputRecords();
        if (spec.getReduceResourceUsageMetrics() != null) {
          metrics = spec.getReduceResourceUsageMetrics();
        }
      }
      if (0 == outRecords && inRecords > 0) {
        LOG.info("Spec output bytes w/o records. Using input record count");
        outRecords = inRecords;
      }
      
      // enable gridmix reduce output record for compression
      Configuration conf = context.getConfiguration();
      if (CompressionEmulationUtil.isCompressionEmulationEnabled(conf)
          && FileOutputFormat.getCompressOutput(context)) {
        float compressionRatio = 
          CompressionEmulationUtil
            .getJobOutputCompressionEmulationRatio(conf);
        LOG.info("GridMix is configured to use a compression ratio of " 
                 + compressionRatio + " for the reduce output data.");
        val.setCompressibility(true, compressionRatio);
        
        // Set the actual output data size to make sure that the actual output 
        // data size is same after compression
        outBytes /= compressionRatio;
      }
      
      factory =
        new AvgRecordFactory(outBytes, outRecords, 
                             context.getConfiguration(), 5*1024);
      ratio = outRecords / (1.0 * inRecords);
      acc = 0.0;
      
      matcher = new ResourceUsageMatcherRunner(context, metrics);
      
      // start the status reporter thread
      reporter = new StatusReporter(context, matcher);
      reporter.start();
    }
    @Override
    protected void reduce(GridmixKey key, Iterable<GridmixRecord> values,
                          Context context) 
    throws IOException, InterruptedException {
      for (GridmixRecord ignored : values) {
        acc += ratio;
        while (acc >= 1.0 && factory.next(null, val)) {
          context.write(NullWritable.get(), val);
          acc -= 1.0;
          
          // match inline
          try {
            matcher.match();
          } catch (Exception e) {
            LOG.debug("Error in resource usage emulation! Message: ", e);
          }
        }
      }
    }
    @Override
    protected void cleanup(Context context)
    throws IOException, InterruptedException {
      val.setSeed(r.nextLong());
      while (factory.next(null, val)) {
        context.write(NullWritable.get(), val);
        val.setSeed(r.nextLong());
        
        // match inline
        try {
          matcher.match();
        } catch (Exception e) {
          LOG.debug("Error in resource usage emulation! Message: ", e);
        }
      }
    }
  }

  static class LoadRecordReader
  extends RecordReader<NullWritable,GridmixRecord> {

    private RecordFactory factory;
    private final Random r = new Random();
    private final GridmixRecord val = new GridmixRecord();

    public LoadRecordReader() { }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext ctxt)
    throws IOException, InterruptedException {
      final LoadSplit split = (LoadSplit)genericSplit;
      final Configuration conf = ctxt.getConfiguration();
      factory = 
        new ReadRecordFactory(split.getLength(), split.getInputRecords(), 
                              new FileQueue(split, conf), conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      val.setSeed(r.nextLong());
      return factory.next(null, val);
    }
    @Override
    public float getProgress() throws IOException {
      return factory.getProgress();
    }
    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }
    @Override
    public GridmixRecord getCurrentValue() {
      return val;
    }
    @Override
    public void close() throws IOException {
      factory.close();
    }
  }

  static class LoadInputFormat
  extends InputFormat<NullWritable,GridmixRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      return pullDescription(jobCtxt);
    }
    @Override
    public RecordReader<NullWritable,GridmixRecord> createRecordReader(
        InputSplit split, final TaskAttemptContext taskContext)
        throws IOException {
      return new LoadRecordReader();
    }
  }

  @Override
  void buildSplits(FilePool inputDir) throws IOException {
    long mapInputBytesTotal = 0L;
    long mapOutputBytesTotal = 0L;
    long mapOutputRecordsTotal = 0L;
    final JobStory jobdesc = getJobDesc();
    if (null == jobdesc) {
      return;
    }
    final int maps = jobdesc.getNumberMaps();
    final int reds = jobdesc.getNumberReduces();
    for (int i = 0; i < maps; ++i) {
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.MAP, i);
      mapInputBytesTotal += info.getInputBytes();
      mapOutputBytesTotal += info.getOutputBytes();
      mapOutputRecordsTotal += info.getOutputRecords();
    }
    final double[] reduceRecordRatio = new double[reds];
    final double[] reduceByteRatio = new double[reds];
    for (int i = 0; i < reds; ++i) {
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.REDUCE, i);
      reduceByteRatio[i] = info.getInputBytes() / (1.0 * mapOutputBytesTotal);
      reduceRecordRatio[i] =
        info.getInputRecords() / (1.0 * mapOutputRecordsTotal);
    }
    final InputStriper striper = new InputStriper(inputDir, mapInputBytesTotal);
    final List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < maps; ++i) {
      final int nSpec = reds / maps + ((reds % maps) > i ? 1 : 0);
      final long[] specBytes = new long[nSpec];
      final long[] specRecords = new long[nSpec];
      final ResourceUsageMetrics[] metrics = new ResourceUsageMetrics[nSpec];
      for (int j = 0; j < nSpec; ++j) {
        final TaskInfo info =
          jobdesc.getTaskInfo(TaskType.REDUCE, i + j * maps);
        specBytes[j] = info.getOutputBytes();
        specRecords[j] = info.getOutputRecords();
        metrics[j] = info.getResourceUsageMetrics();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("SPEC(%d) %d -> %d %d %d %d %d %d %d", id(), i,
                    i + j * maps, info.getOutputRecords(), 
                    info.getOutputBytes(), 
                    info.getResourceUsageMetrics().getCumulativeCpuUsage(),
                    info.getResourceUsageMetrics().getPhysicalMemoryUsage(),
                    info.getResourceUsageMetrics().getVirtualMemoryUsage(),
                    info.getResourceUsageMetrics().getHeapUsage()));
        }
      }
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.MAP, i);
      long possiblyCompressedInputBytes = info.getInputBytes();
      Configuration conf = job.getConfiguration();
      long uncompressedInputBytes =
          CompressionEmulationUtil.getUncompressedInputBytes(
          possiblyCompressedInputBytes, conf);
      splits.add(
        new LoadSplit(striper.splitFor(inputDir, uncompressedInputBytes, 3), 
                      maps, i, uncompressedInputBytes, info.getInputRecords(),
                      info.getOutputBytes(), info.getOutputRecords(),
                      reduceByteRatio, reduceRecordRatio, specBytes, 
                      specRecords, info.getResourceUsageMetrics(),
                      metrics));
    }
    pushDescription(id(), splits);
  }
}
