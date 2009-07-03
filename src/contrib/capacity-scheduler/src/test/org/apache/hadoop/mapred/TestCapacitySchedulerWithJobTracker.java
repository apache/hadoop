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

import java.util.Properties;

import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapreduce.Job;

public class TestCapacitySchedulerWithJobTracker extends
    ClusterWithCapacityScheduler {

  /**
   * Test case which checks if the jobs which 
   * fail initialization are removed from the
   * {@link CapacityTaskScheduler} waiting queue.
   * 
   * @throws Exception
   */
  public void testFailingJobInitalization() throws Exception {
    Properties schedulerProps = new Properties();
    schedulerProps.put("mapred.capacity-scheduler.queue.default.capacity",
        "100");
    Properties clusterProps = new Properties();
    clusterProps.put("mapred.tasktracker.map.tasks.maximum", String.valueOf(1));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(1));
    clusterProps.put("mapred.jobtracker.maxtasks.per.job", String.valueOf(1));
    // cluster capacity 1 maps, 1 reduces
    startCluster(1, clusterProps, schedulerProps);
    JobConf conf = getJobConf();
    conf.setSpeculativeExecution(false);
    conf.setNumTasksToExecutePerJvm(-1);
    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(conf);
    Job job = sleepJob.createJob(3, 3, 1, 1, 1, 1);
    job.waitForCompletion(false);
    assertFalse("The submitted job successfully completed", 
        job.isSuccessful());
    CapacityTaskScheduler scheduler = (CapacityTaskScheduler) getJobTracker()
        .getTaskScheduler();
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    assertEquals("Failed job present in Waiting queue", 0, mgr
        .getWaitingJobCount("default"));
  }

  /**
   * Test case which checks {@link JobTracker} and {@link CapacityTaskScheduler}
   * 
   * Test case submits 2 jobs in two different capacity scheduler queues.
   * And checks if the jobs successfully complete.
   * 
   * @throws Exception
   */
  public void testJobTrackerIntegration() throws Exception {

    Properties schedulerProps = new Properties();
    String[] queues = new String[] { "Q1", "Q2" };
    Job jobs[] = new Job[2];
    for (String q : queues) {
      schedulerProps.put(CapacitySchedulerConf
          .toFullPropertyName(q, "capacity"), "50");
      schedulerProps.put(CapacitySchedulerConf.toFullPropertyName(q,
          "minimum-user-limit-percent"), "100");
    }

    Properties clusterProps = new Properties();
    clusterProps.put("mapred.tasktracker.map.tasks.maximum", String.valueOf(2));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(2));
    clusterProps.put("mapred.queue.names", queues[0] + "," + queues[1]);
    startCluster(2, clusterProps, schedulerProps);

    JobConf conf = getJobConf();
    conf.setSpeculativeExecution(false);
    conf.set("mapred.committer.job.setup.cleanup.needed", "false");
    conf.setNumTasksToExecutePerJvm(-1);
    conf.setQueueName(queues[0]);
    SleepJob sleepJob1 = new SleepJob();
    sleepJob1.setConf(conf);
    jobs[0] = sleepJob1.createJob(1, 1, 1, 1, 1, 1);
    jobs[0].submit();

    JobConf conf2 = getJobConf();
    conf2.setSpeculativeExecution(false);
    conf2.setNumTasksToExecutePerJvm(-1);
    conf2.setQueueName(queues[1]);
    SleepJob sleepJob2 = new SleepJob();
    sleepJob2.setConf(conf2);
    jobs[1] = sleepJob2.createJob(3, 3, 5, 3, 5, 3);
    jobs[1].waitForCompletion(false);
    assertTrue("Sleep job submitted to queue 1 is not successful", jobs[0]
        .isSuccessful());
    assertTrue("Sleep job submitted to queue 2 is not successful", jobs[1]
        .isSuccessful());
  }
}
