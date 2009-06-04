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

package org.apache.hadoop.mapred;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.UtilsForTests.RandomInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * Test the running task limits - mapred.max.maps.per.node,
 * mapred.max.reduces.per.node, mapred.max.running.maps and
 * mapred.max.running.reduces.
 */
public class TestRunningTaskLimits extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestRunningTaskLimits.class);
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data", "/tmp"), 
             "test-running-task-limits");
  
  /**
   * This test creates a cluster with 1 tasktracker with 3 map and 3 reduce 
   * slots. We then submit a job with a limit of 2 maps and 1 reduce per
   * node, and check that these limits are obeyed in launching tasks.
   */
  public void testPerNodeLimits() throws Exception {
    LOG.info("Running testPerNodeLimits");
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true); // cleanup test dir
    
    // Create a cluster with 1 tasktracker with 3 map slots and 3 reduce slots
    JobConf conf = new JobConf();
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 3);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 3);
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
    
    // Create a job with limits of 3 maps/node and 2 reduces/node 
    JobConf jobConf = createWaitJobConf(mr, "job1", 20, 20);
    jobConf.setMaxMapsPerNode(2);
    jobConf.setMaxReducesPerNode(1);
    
    // Submit the job
    RunningJob rJob = (new JobClient(jobConf)).submitJob(jobConf);
    
    // Wait 20 seconds for it to start up
    UtilsForTests.waitFor(20000);
    
    // Check the number of running tasks
    JobTracker jobTracker = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jobTracker.getJob(rJob.getID());
    assertEquals(2, jip.runningMaps());
    assertEquals(1, jip.runningReduces());
    
    rJob.killJob();
    mr.shutdown();
  }
  
  /**
   * This test creates a cluster with 2 tasktrackers with 3 map and 3 reduce 
   * slots each. We then submit a job with a limit of 5 maps and 3 reduces
   * cluster-wide, and check that these limits are obeyed in launching tasks.
   */
  public void testClusterWideLimits() throws Exception {
    LOG.info("Running testClusterWideLimits");
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true); // cleanup test dir
    
    // Create a cluster with 2 tasktrackers with 3 map and reduce slots each
    JobConf conf = new JobConf();
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 3);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 3);
    MiniMRCluster mr = new MiniMRCluster(2, "file:///", 1, null, null, conf);
    
    // Create a job with limits of 10 maps and 5 reduces on the entire cluster 
    JobConf jobConf = createWaitJobConf(mr, "job1", 20, 20);
    jobConf.setRunningMapLimit(5);
    jobConf.setRunningReduceLimit(3);
    
    // Submit the job
    RunningJob rJob = (new JobClient(jobConf)).submitJob(jobConf);
    
    // Wait 20 seconds for it to start up
    UtilsForTests.waitFor(20000);
    
    // Check the number of running tasks
    JobTracker jobTracker = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jobTracker.getJob(rJob.getID());
    assertEquals(5, jip.runningMaps());
    assertEquals(3, jip.runningReduces());
    
    rJob.killJob();
    mr.shutdown();
  }
  
  /**
   * This test creates a cluster with 2 tasktrackers with 3 map and 3 reduce 
   * slots each. We then submit a job with a limit of 5 maps and 3 reduces
   * cluster-wide, and 2 maps and 2 reduces per node. We should end up with
   * 4 maps and 3 reduces running: the maps hit the per-node limit first,
   * while the reduces hit the cluster-wide limit.
   */
  public void testClusterWideAndPerNodeLimits() throws Exception {
    LOG.info("Running testClusterWideAndPerNodeLimits");
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true); // cleanup test dir
    
    // Create a cluster with 2 tasktrackers with 3 map and reduce slots each
    JobConf conf = new JobConf();
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 3);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 3);
    MiniMRCluster mr = new MiniMRCluster(2, "file:///", 1, null, null, conf);
    
    // Create a job with limits of 10 maps and 5 reduces on the entire cluster 
    JobConf jobConf = createWaitJobConf(mr, "job1", 20, 20);
    jobConf.setRunningMapLimit(5);
    jobConf.setRunningReduceLimit(3);
    jobConf.setMaxMapsPerNode(2);
    jobConf.setMaxReducesPerNode(2);
    
    // Submit the job
    RunningJob rJob = (new JobClient(jobConf)).submitJob(jobConf);
    
    // Wait 20 seconds for it to start up
    UtilsForTests.waitFor(20000);
    
    // Check the number of running tasks
    JobTracker jobTracker = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jobTracker.getJob(rJob.getID());
    assertEquals(4, jip.runningMaps());
    assertEquals(3, jip.runningReduces());
    
    rJob.killJob();
    mr.shutdown();
  }
  
  /**
   * Create a JobConf for a job using the WaitingMapper and IdentityReducer,
   * which will sleep until a signal file is created. In this test we never
   * create the signal file so the job just occupies slots for the duration
   * of the test as they are assigned to it. 
   */
  JobConf createWaitJobConf(MiniMRCluster mr, String jobName,
      int numMaps, int numRed)
  throws IOException {
    JobConf jobConf = mr.createJobConf();
    Path inDir = new Path(TEST_DIR, "input");
    Path outDir = new Path(TEST_DIR, "output-" + jobName);
    String signalFile = new Path(TEST_DIR, "signal").toString();
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    jobConf.setMapperClass(UtilsForTests.WaitingMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    jobConf.setInputFormat(RandomInputFormat.class);
    jobConf.setNumMapTasks(numMaps);
    jobConf.setNumReduceTasks(numRed);
    jobConf.setJar("build/test/mapred/testjar/testjob.jar");
    jobConf.set(UtilsForTests.getTaskSignalParameter(true), signalFile);
    jobConf.set(UtilsForTests.getTaskSignalParameter(false), signalFile);
    // Disable reduce slow start to begin reduces ASAP
    jobConf.setFloat("mapred.reduce.slowstart.completed.maps", 0.0f);
    return jobConf;
  }
}
