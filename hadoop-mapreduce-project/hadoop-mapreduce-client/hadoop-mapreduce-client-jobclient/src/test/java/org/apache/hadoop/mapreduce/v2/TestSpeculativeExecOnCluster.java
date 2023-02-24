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

package org.apache.hadoop.mapreduce.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.speculate.ExponentiallySmoothedTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.LegacyTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SimpleExponentialTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test speculation on Mini Cluster.
 */
@Ignore
@RunWith(Parameterized.class)
public class TestSpeculativeExecOnCluster {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpeculativeExecOnCluster.class);

  private static final int NODE_MANAGERS_COUNT = 2;
  private static final boolean ENABLE_SPECULATIVE_MAP = true;
  private static final boolean ENABLE_SPECULATIVE_REDUCE = true;

  private static final int NUM_MAP_DEFAULT = 8 * NODE_MANAGERS_COUNT;
  private static final int NUM_REDUCE_DEFAULT = NUM_MAP_DEFAULT / 2;
  private static final int MAP_SLEEP_TIME_DEFAULT = 60000;
  private static final int REDUCE_SLEEP_TIME_DEFAULT = 10000;
  private static final int MAP_SLEEP_COUNT_DEFAULT = 10000;
  private static final int REDUCE_SLEEP_COUNT_DEFAULT = 1000;

  private static final String MAP_SLEEP_COUNT =
      "mapreduce.sleepjob.map.sleep.count";
  private static final String REDUCE_SLEEP_COUNT =
      "mapreduce.sleepjob.reduce.sleep.count";
  private static final String MAP_SLEEP_TIME =
      "mapreduce.sleepjob.map.sleep.time";
  private static final String REDUCE_SLEEP_TIME =
      "mapreduce.sleepjob.reduce.sleep.time";
  private static final String MAP_SLEEP_CALCULATOR_TYPE =
      "mapreduce.sleepjob.map.sleep.time.calculator";
  private static final String MAP_SLEEP_CALCULATOR_TYPE_DEFAULT = "normal_run";

  private static Map<String, SleepDurationCalculator> mapSleepTypeMapper;


  private static FileSystem localFs;

  static {
    mapSleepTypeMapper = new HashMap<>();
    mapSleepTypeMapper.put("normal_run", new SleepDurationCalcImpl());
    mapSleepTypeMapper.put("stalled_run",
        new StalledSleepDurationCalcImpl());
    mapSleepTypeMapper.put("slowing_run",
        new SlowingSleepDurationCalcImpl());
    mapSleepTypeMapper.put("dynamic_slowing_run",
        new DynamicSleepDurationCalcImpl());
    mapSleepTypeMapper.put("step_stalled_run",
        new StepStalledSleepDurationCalcImpl());
    try {
      localFs = FileSystem.getLocal(new Configuration());
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static final Path TEST_ROOT_DIR =
      new Path("target",
          TestSpeculativeExecOnCluster.class.getName() + "-tmpDir")
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
  private static final Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
  private static final Path TEST_OUT_DIR =
      new Path(TEST_ROOT_DIR, "test.out.dir");

  private MiniMRYarnCluster mrCluster;

  private int myNumMapper;
  private int myNumReduce;
  private int myMapSleepTime;
  private int myReduceSleepTime;
  private int myMapSleepCount;
  private int myReduceSleepCount;
  private String chosenSleepCalc;
  private Class<?> estimatorClass;


  /**
   * The test cases take a long time to run all the estimators against all the
   * cases. We skip the legacy estimators to reduce the execution time.
   */
  private List<String> ignoredTests;


  @Parameterized.Parameters(name = "{index}: TaskEstimator(EstimatorClass {0})")
  public static Collection<Object[]> getTestParameters() {
    List<String> ignoredTests = Arrays.asList(new String[] {
        "stalled_run",
        "slowing_run",
        "step_stalled_run"
    });
    return Arrays.asList(new Object[][] {
        {SimpleExponentialTaskRuntimeEstimator.class, ignoredTests,
            NUM_MAP_DEFAULT, NUM_REDUCE_DEFAULT},
        {LegacyTaskRuntimeEstimator.class, ignoredTests,
            NUM_MAP_DEFAULT, NUM_REDUCE_DEFAULT}
    });
  }

  public TestSpeculativeExecOnCluster(
      Class<? extends TaskRuntimeEstimator> estimatorKlass,
      List<String> testToIgnore,
      Integer numMapper,
      Integer numReduce) {
    this.ignoredTests = testToIgnore;
    this.estimatorClass = estimatorKlass;
    this.myNumMapper = numMapper;
    this.myNumReduce = numReduce;

  }

  @Before
  public void setup() throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(
          TestSpeculativeExecution.class.getName(), NODE_MANAGERS_COUNT);
      Configuration conf = new Configuration();
      mrCluster.init(conf);
      mrCluster.start();

    }

    // workaround the absent public distcache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
    myMapSleepTime = MAP_SLEEP_TIME_DEFAULT;
    myReduceSleepTime = REDUCE_SLEEP_TIME_DEFAULT;
    myMapSleepCount = MAP_SLEEP_COUNT_DEFAULT;
    myReduceSleepCount = REDUCE_SLEEP_COUNT_DEFAULT;
    chosenSleepCalc = MAP_SLEEP_CALCULATOR_TYPE_DEFAULT;
  }

  @After
  public void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
  }

  /**
   * Overrides default behavior of Partitioner for testing.
   */
  public static class SpeculativeSleepJobPartitioner extends
      Partitioner<IntWritable, NullWritable> {
    public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
      return k.get() % numPartitions;
    }
  }

  /**
   * Overrides default behavior of InputSplit for testing.
   */
  public static class EmptySplit extends InputSplit implements Writable {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() {
      return 0L;
    }
    public String[] getLocations() {
      return new String[0];
    }
  }

  /**
   * Input format that sleeps after updating progress.
   */
  public static class SpeculativeSleepInputFormat
      extends InputFormat<IntWritable, IntWritable> {

    public List<InputSplit> getSplits(JobContext jobContext) {
      List<InputSplit> ret = new ArrayList<InputSplit>();
      int numSplits = jobContext.getConfiguration().
          getInt(MRJobConfig.NUM_MAPS, 1);
      for (int i = 0; i < numSplits; ++i) {
        ret.add(new EmptySplit());
      }
      return ret;
    }

    public RecordReader<IntWritable, IntWritable> createRecordReader(
        InputSplit ignored, TaskAttemptContext taskContext)
        throws IOException {
      Configuration conf = taskContext.getConfiguration();
      final int count = conf.getInt(MAP_SLEEP_COUNT, MAP_SLEEP_COUNT_DEFAULT);
      if (count < 0) {
        throw new IOException("Invalid map count: " + count);
      }
      final int redcount = conf.getInt(REDUCE_SLEEP_COUNT,
          REDUCE_SLEEP_COUNT_DEFAULT);
      if (redcount < 0) {
        throw new IOException("Invalid reduce count: " + redcount);
      }
      final int emitPerMapTask = (redcount * taskContext.getNumReduceTasks());

      return new RecordReader<IntWritable, IntWritable>() {
        private int records = 0;
        private int emitCount = 0;
        private IntWritable key = null;
        private IntWritable value = null;
        public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        public boolean nextKeyValue()
            throws IOException {
          if (count == 0) {
            return false;
          }
          key = new IntWritable();
          key.set(emitCount);
          int emit = emitPerMapTask / count;
          if ((emitPerMapTask) % count > records) {
            ++emit;
          }
          emitCount += emit;
          value = new IntWritable();
          value.set(emit);
          return records++ < count;
        }
        public IntWritable getCurrentKey() {
          return key;
        }
        public IntWritable getCurrentValue() {
          return value;
        }
        public void close() throws IOException { }
        public float getProgress() throws IOException {
          return count == 0 ? 100 : records / ((float)count);
        }
      };
    }
  }

  /**
   * Interface used to simulate different progress rates of the tasks.
   */
  public interface SleepDurationCalculator {
    long calcSleepDuration(TaskAttemptID taId, int currCount, int totalCount,
        long defaultSleepDuration);
  }

  /**
   * All tasks have the same progress.
   */
  public static class SleepDurationCalcImpl implements SleepDurationCalculator {

    private double threshold = 1.0;
    private double slowFactor = 1.0;

    SleepDurationCalcImpl() {

    }

    public long calcSleepDuration(TaskAttemptID taId, int currCount,
        int totalCount, long defaultSleepDuration) {
      if (threshold <= ((double) currCount) / totalCount) {
        return (long) (slowFactor * defaultSleepDuration);
      }
      return defaultSleepDuration;
    }
  }

  /**
   * The first attempt of task_0 slows down by a small factor that should not
   * trigger a speculation. An speculated attempt should never beat the
   * original task.
   * A conservative estimator/speculator will speculate another attempt
   * because of the slower progress.
   */
  public static class SlowingSleepDurationCalcImpl implements
      SleepDurationCalculator {

    private double threshold = 0.4;
    private double slowFactor = 1.2;

    SlowingSleepDurationCalcImpl() {

    }

    public long calcSleepDuration(TaskAttemptID taId, int currCount,
        int totalCount, long defaultSleepDuration) {
      if ((taId.getTaskType() == TaskType.MAP)
          && (taId.getTaskID().getId() == 0) && (taId.getId() == 0)) {
        if (threshold <= ((double) currCount) / totalCount) {
          return (long) (slowFactor * defaultSleepDuration);
        }
      }
      return defaultSleepDuration;
    }
  }

  /**
   * The progress of the first Mapper task is stalled by 100 times the other
   * tasks.
   * The speculated attempt should be succeed if the estimator detects
   * the slow down on time.
   */
  public static class StalledSleepDurationCalcImpl implements
      SleepDurationCalculator {

    StalledSleepDurationCalcImpl() {

    }

    public long calcSleepDuration(TaskAttemptID taId, int currCount,
        int totalCount, long defaultSleepDuration) {
      if ((taId.getTaskType() == TaskType.MAP)
          && (taId.getTaskID().getId() == 0) && (taId.getId() == 0)) {
        return 1000 * defaultSleepDuration;
      }
      return defaultSleepDuration;
    }
  }


  /**
   * Emulates the behavior with a step change in the progress.
   */
  public static class StepStalledSleepDurationCalcImpl implements
      SleepDurationCalculator {

    private double threshold = 0.4;
    private double slowFactor = 10000;

    StepStalledSleepDurationCalcImpl() {

    }

    public long calcSleepDuration(TaskAttemptID taId, int currCount,
        int totalCount, long defaultSleepDuration) {
      if ((taId.getTaskType() == TaskType.MAP)
          && (taId.getTaskID().getId() == 0) && (taId.getId() == 0)) {
        if (threshold <= ((double) currCount) / totalCount) {
          return (long) (slowFactor * defaultSleepDuration);
        }
      }
      return defaultSleepDuration;
    }
  }

  /**
   * Dynamically slows down the progress of the first Mapper task.
   * The speculated attempt should be succeed if the estimator detects
   * the slow down on time.
   */
  public static class DynamicSleepDurationCalcImpl implements
      SleepDurationCalculator {

    private double[] thresholds;
    private double[] slowFactors;

    DynamicSleepDurationCalcImpl() {
      thresholds = new double[] {
          0.1, 0.25, 0.4, 0.5, 0.6, 0.65, 0.7, 0.8, 0.9
      };
      slowFactors = new double[] {
          2.0, 4.0, 5.0, 6.0, 10.0, 15.0, 20.0, 25.0, 30.0
      };
    }

    public long calcSleepDuration(TaskAttemptID taId, int currCount,
        int totalCount,
        long defaultSleepDuration) {
      if ((taId.getTaskType() == TaskType.MAP)
          && (taId.getTaskID().getId() == 0) && (taId.getId() == 0)) {
        double currProgress = ((double) currCount) / totalCount;
        double slowFactor = 1.0;
        for (int i = 0; i < thresholds.length; i++) {
          if (thresholds[i] >= currProgress) {
            break;
          }
          slowFactor = slowFactors[i];
        }
        return (long) (slowFactor * defaultSleepDuration);
      }
      return defaultSleepDuration;
    }
  }

  /**
   * Dummy class for testing Speculation. Sleeps for a defined period
   * of time in mapper. Generates fake input for map / reduce
   * jobs. Note that generated number of input pairs is in the order
   * of <code>numMappers * mapSleepTime / 100</code>, so the job uses
   * some disk space.
   * The sleep duration for a given task is going to slowDown to evaluate
   * the estimator
   */
  public static class SpeculativeSleepMapper
      extends Mapper<IntWritable, IntWritable, IntWritable, NullWritable> {
    private long mapSleepDuration = MAP_SLEEP_TIME_DEFAULT;
    private int mapSleepCount = 1;
    private int count = 0;
    private SleepDurationCalculator sleepCalc = new SleepDurationCalcImpl();

    protected void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.mapSleepCount =
          conf.getInt(MAP_SLEEP_COUNT, mapSleepCount);
      this.mapSleepDuration = mapSleepCount == 0 ? 0 :
          conf.getLong(MAP_SLEEP_TIME, MAP_SLEEP_TIME_DEFAULT) / mapSleepCount;
      this.sleepCalc =
          mapSleepTypeMapper.get(conf.get(MAP_SLEEP_CALCULATOR_TYPE,
              MAP_SLEEP_CALCULATOR_TYPE_DEFAULT));

    }

    public void map(IntWritable key, IntWritable value, Context context)
        throws IOException, InterruptedException {
      //it is expected that every map processes mapSleepCount number of records.
      try {
        context.setStatus("Sleeping... (" +
            (mapSleepDuration * (mapSleepCount - count)) + ") ms left");
        long sleepTime = sleepCalc.calcSleepDuration(context.getTaskAttemptID(),
            count, mapSleepCount,
            mapSleepDuration);
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
        throw (IOException) new IOException(
            "Interrupted while sleeping").initCause(ex);
      }
      ++count;
      // output reduceSleepCount * numReduce number of random values, so that
      // each reducer will get reduceSleepCount number of keys.
      int k = key.get();
      for (int i = 0; i < value.get(); ++i) {
        context.write(new IntWritable(k + i), NullWritable.get());
      }
    }
  }

  /**
   * Implementation of the reducer task for testing.
   */
  public static class SpeculativeSleepReducer
      extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> {

    private long reduceSleepDuration = REDUCE_SLEEP_TIME_DEFAULT;
    private int reduceSleepCount = 1;
    private int count = 0;

    protected void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.reduceSleepCount =
          conf.getInt(REDUCE_SLEEP_COUNT, reduceSleepCount);
      this.reduceSleepDuration = reduceSleepCount == 0 ? 0 :
          conf.getLong(REDUCE_SLEEP_TIME, REDUCE_SLEEP_TIME_DEFAULT)
              / reduceSleepCount;
    }

    public void reduce(IntWritable key, Iterable<NullWritable> values,
        Context context)
        throws IOException {
      try {
        context.setStatus("Sleeping... (" +
            (reduceSleepDuration * (reduceSleepCount - count)) + ") ms left");
        Thread.sleep(reduceSleepDuration);
      } catch (InterruptedException ex) {
        throw (IOException) new IOException(
            "Interrupted while sleeping").initCause(ex);
      }
      count++;
    }
  }

  /**
   * A class used to map the estimatopr implementation to the expected
   * test results.
   */
  class EstimatorMetricsPair {

    private Class<?> estimatorClass;
    private int expectedMapTasks;
    private int expectedReduceTasks;
    private boolean speculativeEstimator;

    EstimatorMetricsPair(Class<?> estimatorClass, int mapTasks, int reduceTasks,
        boolean isToSpeculate) {
      this.estimatorClass = estimatorClass;
      this.expectedMapTasks = mapTasks;
      this.expectedReduceTasks = reduceTasks;
      this.speculativeEstimator = isToSpeculate;
    }

    boolean didSpeculate(Counters counters) {
      long launchedMaps = counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
          .getValue();
      long launchedReduce = counters
          .findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES)
          .getValue();
      boolean isSpeculated =
          (launchedMaps > expectedMapTasks
              || launchedReduce > expectedReduceTasks);
      return isSpeculated;
    }

    String getErrorMessage(Counters counters) {
      String msg = "Unexpected tasks running estimator "
          + estimatorClass.getName() + "\n\t";
      long launchedMaps = counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
          .getValue();
      long launchedReduce = counters
          .findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES)
          .getValue();
      if (speculativeEstimator) {
        if (launchedMaps < expectedMapTasks) {
          msg += "maps " + launchedMaps + ", expected: " + expectedMapTasks;
        }
        if (launchedReduce < expectedReduceTasks) {
          msg += ", reduces " + launchedReduce + ", expected: "
              + expectedReduceTasks;
        }
      } else {
        if (launchedMaps > expectedMapTasks) {
          msg += "maps " + launchedMaps + ", expected: " + expectedMapTasks;
        }
        if (launchedReduce > expectedReduceTasks) {
          msg += ", reduces " + launchedReduce + ", expected: "
              + expectedReduceTasks;
        }
      }
      return msg;
    }
  }

  @Test
  public void testExecDynamicSlowingSpeculative() throws Exception {
    /*------------------------------------------------------------------
     * Test that Map/Red speculates because:
     * 1- all tasks have same progress rate except for task_0
     * 2- task_0 slows down by dynamic increasing factor
     * 3- A good estimator should readjust the estimation and the speculator
     *    launches a new task.
     *
     * Expected:
     * A- SimpleExponentialTaskRuntimeEstimator: speculates a successful
     *    attempt to beat the slowing task_0
     * B- LegacyTaskRuntimeEstimator: speculates an attempt
     * C- ExponentiallySmoothedTaskRuntimeEstimator: Fails to detect the slow
     *    down and never speculates but it may speculate other tasks
     *    (mappers or reducers)
     * -----------------------------------------------------------------
     */
    chosenSleepCalc = "dynamic_slowing_run";

    if (ignoredTests.contains(chosenSleepCalc)) {
      return;
    }

    EstimatorMetricsPair[] estimatorPairs = new EstimatorMetricsPair[] {
        new EstimatorMetricsPair(SimpleExponentialTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(LegacyTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(
            ExponentiallySmoothedTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true)
    };

    for (EstimatorMetricsPair specEstimator : estimatorPairs) {
      if (!estimatorClass.equals(specEstimator.estimatorClass)) {
        continue;
      }
      LOG.info("+++ Dynamic Slow Progress testing against " + estimatorClass
          .getName() + " +++");
      Job job = runSpecTest();

      boolean succeeded = job.waitForCompletion(true);
      Assert.assertTrue(
          "Job expected to succeed with estimator " + estimatorClass.getName(),
          succeeded);
      Assert.assertEquals(
          "Job expected to succeed with estimator " + estimatorClass.getName(),
          JobStatus.State.SUCCEEDED, job.getJobState());
      Counters counters = job.getCounters();

      String errorMessage = specEstimator.getErrorMessage(counters);
      boolean didSpeculate = specEstimator.didSpeculate(counters);
      Assert.assertEquals(errorMessage, didSpeculate,
          specEstimator.speculativeEstimator);
      Assert
          .assertEquals("Failed maps higher than 0 " + estimatorClass.getName(),
              0, counters.findCounter(JobCounter.NUM_FAILED_MAPS).getValue());
    }
  }


  @Test
  public void testExecSlowNonSpeculative() throws Exception {
    /*------------------------------------------------------------------
     * Test that Map/Red does not speculate because:
     * 1- all tasks have same progress rate except for task_0
     * 2- task_0 slows down by 0.5 after 50% of the workload
     * 3- A good estimator may adjust the estimation that the task will finish
     *    sooner than a new speculated task.
     *
     * Expected:
     * A- SimpleExponentialTaskRuntimeEstimator: does not speculate because
     *    the new attempt estimated end time is not going to be smaller than the
     *    original end time.
     * B- LegacyTaskRuntimeEstimator: speculates an attempt
     * C- ExponentiallySmoothedTaskRuntimeEstimator: speculates an attempt.
     * -----------------------------------------------------------------
     */
    chosenSleepCalc = "slowing_run";

    if (ignoredTests.contains(chosenSleepCalc)) {
      return;
    }

    EstimatorMetricsPair[] estimatorPairs = new EstimatorMetricsPair[] {
        new EstimatorMetricsPair(SimpleExponentialTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, false),
        new EstimatorMetricsPair(LegacyTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(
            ExponentiallySmoothedTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true)
    };

    for (EstimatorMetricsPair specEstimator : estimatorPairs) {
      if (!estimatorClass.equals(specEstimator.estimatorClass)) {
        continue;
      }
      LOG.info("+++ Linear Slow Progress Non Speculative testing against "
          + estimatorClass.getName() + " +++");
      Job job = runSpecTest();

      boolean succeeded = job.waitForCompletion(true);
      Assert.assertTrue(
          "Job expected to succeed with estimator " + estimatorClass.getName(),
          succeeded);
      Assert.assertEquals(
          "Job expected to succeed with estimator " + estimatorClass.getName(),
          JobStatus.State.SUCCEEDED, job.getJobState());
      Counters counters = job.getCounters();

      String errorMessage = specEstimator.getErrorMessage(counters);
      boolean didSpeculate = specEstimator.didSpeculate(counters);
      Assert.assertEquals(errorMessage, didSpeculate,
          specEstimator.speculativeEstimator);
      Assert
          .assertEquals("Failed maps higher than 0 " + estimatorClass.getName(),
              0, counters.findCounter(JobCounter.NUM_FAILED_MAPS).getValue());
    }
  }

  @Test
  public void testExecStepStalledSpeculative() throws Exception {
    /*------------------------------------------------------------------
     * Test that Map/Red speculates because:
     * 1- all tasks have same progress rate except for task_0
     * 2- task_0 has long sleep duration
     * 3- A good estimator may adjust the estimation that the task will finish
     *    sooner than a new speculated task.
     *
     * Expected:
     * A- SimpleExponentialTaskRuntimeEstimator: speculates
     * B- LegacyTaskRuntimeEstimator: speculates
     * C- ExponentiallySmoothedTaskRuntimeEstimator: speculates
     * -----------------------------------------------------------------
     */
    chosenSleepCalc = "step_stalled_run";
    if (ignoredTests.contains(chosenSleepCalc)) {
      return;
    }
    EstimatorMetricsPair[] estimatorPairs = new EstimatorMetricsPair[] {
        new EstimatorMetricsPair(SimpleExponentialTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(LegacyTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(
            ExponentiallySmoothedTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true)
    };

    for (EstimatorMetricsPair specEstimator : estimatorPairs) {
      if (!estimatorClass.equals(specEstimator.estimatorClass)) {
        continue;
      }
      LOG.info("+++ Stalled Progress testing against "
          + estimatorClass.getName() + " +++");
      Job job = runSpecTest();

      boolean succeeded = job.waitForCompletion(true);
      Assert.assertTrue("Job expected to succeed with estimator "
          + estimatorClass.getName(), succeeded);
      Assert.assertEquals("Job expected to succeed with estimator "
              + estimatorClass.getName(), JobStatus.State.SUCCEEDED,
          job.getJobState());
      Counters counters = job.getCounters();

      String errorMessage = specEstimator.getErrorMessage(counters);
      boolean didSpeculate = specEstimator.didSpeculate(counters);
      Assert.assertEquals(errorMessage, didSpeculate,
          specEstimator.speculativeEstimator);
      Assert.assertEquals("Failed maps higher than 0 "
              + estimatorClass.getName(), 0,
          counters.findCounter(JobCounter.NUM_FAILED_MAPS)
              .getValue());
    }
  }

  @Test
  public void testExecStalledSpeculative() throws Exception {
    /*------------------------------------------------------------------
     * Test that Map/Red speculates because:
     * 1- all tasks have same progress rate except for task_0
     * 2- task_0 has long sleep duration
     * 3- A good estimator may adjust the estimation that the task will finish
     *    sooner than a new speculated task.
     *
     * Expected:
     * A- SimpleExponentialTaskRuntimeEstimator: speculates
     * B- LegacyTaskRuntimeEstimator: speculates
     * C- ExponentiallySmoothedTaskRuntimeEstimator: speculates
     * -----------------------------------------------------------------
     */
    chosenSleepCalc = "stalled_run";

    if (ignoredTests.contains(chosenSleepCalc)) {
      return;
    }
    EstimatorMetricsPair[] estimatorPairs = new EstimatorMetricsPair[] {
        new EstimatorMetricsPair(SimpleExponentialTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(LegacyTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(
            ExponentiallySmoothedTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true)
    };

    for (EstimatorMetricsPair specEstimator : estimatorPairs) {
      if (!estimatorClass.equals(specEstimator.estimatorClass)) {
        continue;
      }
      LOG.info("+++ Stalled Progress testing against "
          + estimatorClass.getName() + " +++");
      Job job = runSpecTest();

      boolean succeeded = job.waitForCompletion(true);
      Assert.assertTrue("Job expected to succeed with estimator "
          + estimatorClass.getName(), succeeded);
      Assert.assertEquals("Job expected to succeed with estimator "
              + estimatorClass.getName(), JobStatus.State.SUCCEEDED,
          job.getJobState());
      Counters counters = job.getCounters();

      String errorMessage = specEstimator.getErrorMessage(counters);
      boolean didSpeculate = specEstimator.didSpeculate(counters);
      Assert.assertEquals(errorMessage, didSpeculate,
          specEstimator.speculativeEstimator);
      Assert.assertEquals("Failed maps higher than 0 "
              + estimatorClass.getName(), 0,
          counters.findCounter(JobCounter.NUM_FAILED_MAPS)
              .getValue());
    }
  }

  @Test
  public void testExecNonSpeculative() throws Exception {
    /*------------------------------------------------------------------
     * Test that Map/Red does not speculate because all tasks progress in the
     *    same rate.
     *
     * Expected:
     * A- SimpleExponentialTaskRuntimeEstimator: does not speculate
     * B- LegacyTaskRuntimeEstimator: speculates
     * C- ExponentiallySmoothedTaskRuntimeEstimator: speculates
     * -----------------------------------------------------------------
     */
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (ignoredTests.contains(chosenSleepCalc)) {
      return;
    }

    EstimatorMetricsPair[] estimatorPairs = new EstimatorMetricsPair[] {
        new EstimatorMetricsPair(LegacyTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true),
        new EstimatorMetricsPair(SimpleExponentialTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, false),
        new EstimatorMetricsPair(
            ExponentiallySmoothedTaskRuntimeEstimator.class,
            myNumMapper, myNumReduce, true)
    };

    for (EstimatorMetricsPair specEstimator : estimatorPairs) {
      if (!estimatorClass.equals(specEstimator.estimatorClass)) {
        continue;
      }
      LOG.info("+++ No Speculation testing against "
          + estimatorClass.getName() + " +++");
      Job job = runSpecTest();

      boolean succeeded = job.waitForCompletion(true);
      Assert.assertTrue("Job expected to succeed with estimator "
          + estimatorClass.getName(), succeeded);
      Assert.assertEquals("Job expected to succeed with estimator "
              + estimatorClass.getName(), JobStatus.State.SUCCEEDED,
          job.getJobState());
      Counters counters = job.getCounters();

      String errorMessage = specEstimator.getErrorMessage(counters);
      boolean didSpeculate = specEstimator.didSpeculate(counters);
      Assert.assertEquals(errorMessage, didSpeculate,
          specEstimator.speculativeEstimator);
    }
  }

  private Job runSpecTest()
      throws IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = mrCluster.getConfig();
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, ENABLE_SPECULATIVE_MAP);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, ENABLE_SPECULATIVE_REDUCE);
    conf.setClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
        estimatorClass,
        TaskRuntimeEstimator.class);
    conf.setLong(MAP_SLEEP_TIME, myMapSleepTime);
    conf.setLong(REDUCE_SLEEP_TIME, myReduceSleepTime);
    conf.setInt(MAP_SLEEP_COUNT, myMapSleepCount);
    conf.setInt(REDUCE_SLEEP_COUNT, myReduceSleepCount);
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0F);
    conf.setInt(MRJobConfig.NUM_MAPS, myNumMapper);
    conf.set(MAP_SLEEP_CALCULATOR_TYPE, chosenSleepCalc);
    Job job = Job.getInstance(conf);
    job.setJarByClass(TestSpeculativeExecution.class);
    job.setMapperClass(SpeculativeSleepMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(SpeculativeSleepReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setInputFormatClass(SpeculativeSleepInputFormat.class);
    job.setPartitionerClass(SpeculativeSleepJobPartitioner.class);
    job.setNumReduceTasks(myNumReduce);
    FileInputFormat.addInputPath(job, new Path("ignored"));
    // Delete output directory if it exists.
    try {
      localFs.delete(TEST_OUT_DIR, true);
    } catch (IOException e) {
      // ignore
    }
    FileOutputFormat.setOutputPath(job, TEST_OUT_DIR);

    // Creates the Job Configuration
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setMaxMapAttempts(2);

    job.submit();

    return job;
  }
}
