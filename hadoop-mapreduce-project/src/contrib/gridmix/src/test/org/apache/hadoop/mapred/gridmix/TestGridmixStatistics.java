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

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.gridmix.Statistics.ClusterStats;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the Gridmix's {@link Statistics} class.
 */
public class TestGridmixStatistics {
  /**
   * Test {@link Statistics.JobStats}.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testJobStats() throws Exception {
    Job job = new Job() {};
    JobStats stats = new JobStats(1, 2, job);
    assertEquals("Incorrect num-maps", 1, stats.getNoOfMaps());
    assertEquals("Incorrect num-reds", 2, stats.getNoOfReds());
    assertTrue("Incorrect job", job == stats.getJob());
    assertNull("Unexpected job status", stats.getJobStatus());
    
    // add a new status
    JobStatus status = new JobStatus();
    stats.updateJobStatus(status);
    assertNotNull("Missing job status", stats.getJobStatus());
    assertTrue("Incorrect job status", status == stats.getJobStatus());
  }
  
  private static JobStory getCustomJobStory(final int numMaps, 
                                            final int numReds) {
    return new JobStory() {
      @Override
      public InputSplit[] getInputSplits() {
        return null;
      }
      @Override
      public JobConf getJobConf() {
        return null;
      }
      @Override
      public JobID getJobID() {
        return null;
      }
      @Override
      public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int arg0, int arg1,
                                                           int arg2) {
        return null;
      }
      @Override
      public String getName() {
        return null;
      }
      @Override
      public int getNumberMaps() {
        return numMaps;
      }
      @Override
      public int getNumberReduces() {
        return numReds;
      }
      @Override
      public Values getOutcome() {
        return null;
      }
      @Override
      public String getQueueName() {
        return null;
      }
      @Override
      public long getSubmissionTime() {
        return 0;
      }
      @Override
      public TaskAttemptInfo getTaskAttemptInfo(TaskType arg0, int arg1, 
                                                int arg2) {
        return null;
      }
      @Override
      public TaskInfo getTaskInfo(TaskType arg0, int arg1) {
        return null;
      }
      @Override
      public String getUser() {
        return null;
      }
    };
  }
  
  /**
   * Test {@link Statistics}.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testStatistics() throws Exception {
    // test job stats generation
    Configuration conf = new Configuration();
    
    // test dummy jobs like data-generation etc
    Job job = new Job(conf) {
    };
    JobStats stats = Statistics.generateJobStats(job, null);
    testJobStats(stats, -1, -1, null, job);
    
    // add a job desc with 2 map and 1 reduce task
    conf.setInt(GridmixJob.GRIDMIX_JOB_SEQ, 1);
    
    // test dummy jobs like data-generation etc
    job = new Job(conf) {
    };
    JobStory zjob = getCustomJobStory(2, 1);
    stats = Statistics.generateJobStats(job, zjob);
    testJobStats(stats, 2, 1, null, job);
    
    // add a job status
    JobStatus jStatus = new JobStatus();
    stats.updateJobStatus(jStatus);
    testJobStats(stats, 2, 1, jStatus, job);
    
    
    // start the statistics
    CountDownLatch startFlag = new CountDownLatch(1); // prevents the collector
                                                      // thread from starting
    Statistics statistics = new Statistics(new JobConf(), 0, startFlag);
    statistics.start();

    testClusterStats(0, 0, 0);
    
    // add to the statistics object
    statistics.addJobStats(stats);
    testClusterStats(2, 1, 1);
    
    // add another job
    JobStory zjob2 = getCustomJobStory(10, 5);
    conf.setInt(GridmixJob.GRIDMIX_JOB_SEQ, 2);
    job = new Job(conf) {
    };
    
    JobStats stats2 = Statistics.generateJobStats(job, zjob2);
    statistics.addJobStats(stats2);
    testClusterStats(12, 6, 2);
    
    // finish off one job
    statistics.add(stats2);
    testClusterStats(2, 1, 1);
    
    // finish off the other job
    statistics.add(stats);
    testClusterStats(0, 0, 0);
    
    statistics.shutdown();
  }
  
  // test the job stats
  private static void testJobStats(JobStats stats, int numMaps, int numReds,
                                   JobStatus jStatus, Job job) {
    assertEquals("Incorrect num map tasks", numMaps, stats.getNoOfMaps());
    assertEquals("Incorrect num reduce tasks", numReds, stats.getNoOfReds());
    
    if (job != null) {
      assertNotNull("Missing job", job);
    }
    // check running job
    assertTrue("Incorrect job", job == stats.getJob());
    
    if (jStatus != null) {
      assertNotNull("Missing job status", jStatus);
    }
    // check job stats
    assertTrue("Incorrect job status", jStatus == stats.getJobStatus());
  }
  
  // test the cluster stats
  private static void testClusterStats(int numSubmittedMapTasks, 
                                       int numSubmittedReduceTasks, 
                                       int numSubmittedJobs) {
    assertEquals("Incorrect count of total number of submitted map tasks", 
                 numSubmittedMapTasks, ClusterStats.getSubmittedMapTasks());
    assertEquals("Incorrect count of total number of submitted reduce tasks", 
                 numSubmittedReduceTasks, 
                 ClusterStats.getSubmittedReduceTasks());
    assertEquals("Incorrect submitted jobs", 
                 numSubmittedJobs, ClusterStats.getRunningJobStats().size());
  }
}
