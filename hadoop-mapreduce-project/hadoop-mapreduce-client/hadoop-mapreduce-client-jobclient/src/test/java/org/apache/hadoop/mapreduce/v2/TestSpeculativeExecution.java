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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.speculate.LegacyTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSpeculativeExecution {

  /*
   * This class is used to control when speculative execution happens.
   */
  public static class TestSpecEstimator extends LegacyTaskRuntimeEstimator {
    private static final long SPECULATE_THIS = 999999L;

    public TestSpecEstimator() {
      super();
    }

    /*
     * This will only be called if speculative execution is turned on.
     * 
     * If either mapper or reducer speculation is turned on, this will be
     * called.
     * 
     * This will cause speculation to engage for the first mapper or first
     * reducer (that is, attempt ID "*_m_000000_0" or "*_r_000000_0")
     * 
     * If this attempt is killed, the retry will have attempt id 1, so it
     * will not engage speculation again.
     */
    @Override
    public long estimatedRuntime(TaskAttemptId id) {
      if ((id.getTaskId().getId() == 0) && (id.getId() == 0)) {
        return SPECULATE_THIS;
      }
      return super.estimatedRuntime(id);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSpeculativeExecution.class);

  protected static MiniMRYarnCluster mrCluster;

  private static Configuration initialConf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(initialConf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static Path TEST_ROOT_DIR =
          new Path("target",TestSpeculativeExecution.class.getName() + "-tmpDir")
               .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
  private static Path TEST_OUT_DIR = new Path(TEST_ROOT_DIR, "test.out.dir");

  @BeforeClass
  public static void setup() throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestSpeculativeExecution.class.getName(), 4);
      Configuration conf = new Configuration();
      mrCluster.init(conf);
      mrCluster.start();
    }

    // workaround the absent public distcache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
  }

  public static class SpeculativeMapper extends
    Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
      // Make one mapper slower for speculative execution
      TaskAttemptID taid = context.getTaskAttemptID();
      long sleepTime = 100;
      Configuration conf = context.getConfiguration();
      boolean test_speculate_map =
              conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false);

      // IF TESTING MAPPER SPECULATIVE EXECUTION:
      //   Make the "*_m_000000_0" attempt take much longer than the others.
      //   When speculative execution is enabled, this should cause the attempt
      //   to be killed and restarted. At that point, the attempt ID will be
      //   "*_m_000000_1", so sleepTime will still remain 100ms.
      if ( (taid.getTaskType() == TaskType.MAP) && test_speculate_map
            && (taid.getTaskID().getId() == 0) && (taid.getId() == 0)) {
        sleepTime = 10000;
      }
      try{
        Thread.sleep(sleepTime);
      } catch(InterruptedException ie) {
        // Ignore
      }
      context.write(value, new IntWritable(1));
    }
  }

  public static class SpeculativeReducer extends
    Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, 
                           Context context) throws IOException, InterruptedException {
      // Make one reducer slower for speculative execution
      TaskAttemptID taid = context.getTaskAttemptID();
      long sleepTime = 100;
      Configuration conf = context.getConfiguration();
      boolean test_speculate_reduce =
                conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

      // IF TESTING REDUCE SPECULATIVE EXECUTION:
      //   Make the "*_r_000000_0" attempt take much longer than the others.
      //   When speculative execution is enabled, this should cause the attempt
      //   to be killed and restarted. At that point, the attempt ID will be
      //   "*_r_000000_1", so sleepTime will still remain 100ms.
      if ( (taid.getTaskType() == TaskType.REDUCE) && test_speculate_reduce
            && (taid.getTaskID().getId() == 0) && (taid.getId() == 0)) {
        sleepTime = 10000;
      }
      try{
        Thread.sleep(sleepTime);
      } catch(InterruptedException ie) {
        // Ignore
      }
      context.write(key,new IntWritable(0));
    }
  }

  @Test
  public void testSpeculativeExecution() throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
           + " not found. Not running test.");
      return;
    }

    /*------------------------------------------------------------------
     * Test that Map/Red does not speculate if MAP_SPECULATIVE and 
     * REDUCE_SPECULATIVE are both false.
     * -----------------------------------------------------------------
     */
    Job job = runSpecTest(false, false);

    boolean succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    Counters counters = job.getCounters();
    Assert.assertEquals(2, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
            .getValue());
    Assert.assertEquals(2, counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES)
            .getValue());
    Assert.assertEquals(0, counters.findCounter(JobCounter.NUM_FAILED_MAPS)
            .getValue());

    /*----------------------------------------------------------------------
     * Test that Mapper speculates if MAP_SPECULATIVE is true and
     * REDUCE_SPECULATIVE is false.
     * ---------------------------------------------------------------------
     */
    job = runSpecTest(true, false);

    succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    counters = job.getCounters();

    // The long-running map will be killed and a new one started.
    Assert.assertEquals(3, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
            .getValue());
    Assert.assertEquals(2, counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES)
            .getValue());
    Assert.assertEquals(0, counters.findCounter(JobCounter.NUM_FAILED_MAPS)
            .getValue());
    Assert.assertEquals(1, counters.findCounter(JobCounter.NUM_KILLED_MAPS)
        .getValue());

    /*----------------------------------------------------------------------
     * Test that Reducer speculates if REDUCE_SPECULATIVE is true and
     * MAP_SPECULATIVE is false.
     * ---------------------------------------------------------------------
     */
    job = runSpecTest(false, true);

    succeeded = job.waitForCompletion(true);
    Assert.assertTrue(succeeded);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
    counters = job.getCounters();

    // The long-running map will be killed and a new one started.
    Assert.assertEquals(2, counters.findCounter(JobCounter.TOTAL_LAUNCHED_MAPS)
            .getValue());
    Assert.assertEquals(3, counters.findCounter(JobCounter.TOTAL_LAUNCHED_REDUCES)
            .getValue());
  }

  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(TEST_ROOT_DIR, filename);
    FSDataOutputStream os = localFs.create(path);
    os.writeBytes(contents);
    os.close();
    localFs.setPermission(path, new FsPermission("700"));
    return path;
  }
  
  private Job runSpecTest(boolean mapspec, boolean redspec)
      throws IOException, ClassNotFoundException, InterruptedException {

    Path first = createTempFile("specexec_map_input1", "a\nz");
    Path secnd = createTempFile("specexec_map_input2", "a\nz");

    Configuration conf = mrCluster.getConfig();
    conf.setBoolean(MRJobConfig.MAP_SPECULATIVE,mapspec);
    conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE,redspec);
    conf.setClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
            TestSpecEstimator.class,
            TaskRuntimeEstimator.class);

    Job job = Job.getInstance(conf);
    job.setJarByClass(TestSpeculativeExecution.class);
    job.setMapperClass(SpeculativeMapper.class);
    job.setReducerClass(SpeculativeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(2);
    FileInputFormat.setInputPaths(job, first);
    FileInputFormat.addInputPath(job, secnd);
    FileOutputFormat.setOutputPath(job, TEST_OUT_DIR);

    // Delete output directory if it exists.
    try {
      localFs.delete(TEST_OUT_DIR,true);
    } catch (IOException e) {
      // ignore
    }

    // Creates the Job Configuration
    job.addFileToClassPath(APP_JAR); // The AppMaster jar itself.
    job.setMaxMapAttempts(2);

    job.submit();

    return job;
  }
}
