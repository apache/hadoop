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
import org.apache.hadoop.mapred.ControlledMapReduceJob.ControlledMapReduceJobRunner;

/**
 * End to end tests based on MiniMRCluster to verify that queue capacities are
 * honored. Automates the tests related to queue capacities: submits jobs to
 * different queues simultaneously and ensures that capacities are honored
 */
public class TestQueueCapacities extends ClusterWithCapacityScheduler {

  /**
   * Test single queue.
   * 
   * <p>
   * 
   * Submit a job with more M/R tasks than total capacity. Full queue capacity
   * should be utilized and remaining M/R tasks should wait for slots to be
   * available.
   * 
   * @throws Exception
   */
  public void testSingleQueue()
      throws Exception {

    Properties schedulerProps = new Properties();
    schedulerProps.put(
        "mapred.capacity-scheduler.queue.default.guaranteed-capacity", "100");
    Properties clusterProps = new Properties();
    clusterProps
        .put("mapred.tasktracker.map.tasks.maximum", String.valueOf(3));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(3));
    // cluster capacity 12 maps, 12 reduces
    startCluster(4, clusterProps, schedulerProps);

    ControlledMapReduceJobRunner jobRunner =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
            getJobConf(), 16, 16);
    jobRunner.start();
    ControlledMapReduceJob controlledJob = jobRunner.getJob();
    JobID myJobID = jobRunner.getJobID();
    JobInProgress myJob = getJobTracker().getJob(myJobID);

    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 12);

    // Wait till the cluster reaches steady state. This confirms that the rest
    // of the tasks are not running and waiting for slots
    // to be freed.
    waitTillAllSlotsAreOccupied(true);

    LOG.info("Trying to finish 2 maps");
    controlledJob.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 2);
    assertTrue("Number of maps finished", myJob.finishedMaps() == 2);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 12);
    waitTillAllSlotsAreOccupied(true);

    LOG.info("Trying to finish 2 more maps");
    controlledJob.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 4);
    assertTrue("Number of maps finished", myJob.finishedMaps() == 4);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 12);
    waitTillAllSlotsAreOccupied(true);

    LOG.info("Trying to finish the last 12 maps");
    controlledJob.finishNTasks(true, 12);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 16);
    assertTrue("Number of maps finished", myJob.finishedMaps() == 16);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 0);
    ControlledMapReduceJob.haveAllTasksFinished(myJob, true);

    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, false, 12);
    waitTillAllSlotsAreOccupied(false);

    LOG.info("Trying to finish 4 reduces");
    controlledJob.finishNTasks(false, 4);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, false, 4);
    assertTrue("Number of reduces finished", myJob.finishedReduces() == 4);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, false, 12);
    waitTillAllSlotsAreOccupied(false);

    LOG.info("Trying to finish the last 12 reduces");
    controlledJob.finishNTasks(false, 12);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, false, 16);
    assertTrue("Number of reduces finished", myJob.finishedReduces() == 16);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, false, 0);
    ControlledMapReduceJob.haveAllTasksFinished(myJob, false);

    jobRunner.join();
  }

  /**
   * Test single queue with multiple jobs.
   * 
   * @throws Exception
   */
  public void testSingleQueueMultipleJobs()
      throws Exception {

    Properties schedulerProps = new Properties();
    schedulerProps.put(
        "mapred.capacity-scheduler.queue.default.guaranteed-capacity", "100");
    Properties clusterProps = new Properties();
    clusterProps
        .put("mapred.tasktracker.map.tasks.maximum", String.valueOf(3));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(0));
    // cluster capacity 12 maps, 0 reduces
    startCluster(4, clusterProps, schedulerProps);

    singleQMultipleJobs1();
    singleQMultipleJobs2();
  }

  /**
   * Test multiple queues.
   * 
   * These tests use 4 queues default, Q2, Q3 and Q4 with guaranteed capacities
   * 10, 20, 30, 40 respectively), user limit 100%, priority not respected, one
   * user per queue. Reclaim time 5 minutes.
   * 
   * @throws Exception
   */
  public void testMultipleQueues()
      throws Exception {
    Properties schedulerProps = new Properties();
    String[] queues = new String[] { "default", "Q2", "Q3", "Q4" };
    int GC = 0;
    for (String q : queues) {
      GC += 10;
      schedulerProps.put(CapacitySchedulerConf.toFullPropertyName(q,
          "guaranteed-capacity"), String.valueOf(GC)); // TODO: use strings
      schedulerProps.put(CapacitySchedulerConf.toFullPropertyName(q,
          "minimum-user-limit-percent"), String.valueOf(100));
      schedulerProps.put(CapacitySchedulerConf.toFullPropertyName(q,
          "reclaim-time-limit"), String.valueOf(300));
    }

    Properties clusterProps = new Properties();
    clusterProps
        .put("mapred.tasktracker.map.tasks.maximum", String.valueOf(2));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(2));
    clusterProps.put("mapred.queue.names", queues[0] + "," + queues[1] + ","
        + queues[2] + "," + queues[3]);

    // cluster capacity 10 maps, 10 reduces and 4 queues with capacities 1, 2,
    // 3, 4 respectively.
    startCluster(5, clusterProps, schedulerProps);

    multipleQsWithOneQBeyondCapacity(queues);
    multipleQueuesWithinCapacities(queues);
  }

  /**
   * Submit a job with more M/R tasks than total queue capacity and then submit
   * another job. First job utilizes all the slots. When the second job is
   * submitted, the tasks of the second job wait for slots to be available. As
   * the tasks of the first jobs finish and there are no more tasks pending, the
   * tasks of the second job start running on the freed up slots.
   * 
   * @throws Exception
   */
  private void singleQMultipleJobs1()
      throws Exception {

    ControlledMapReduceJobRunner jobRunner1 =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
            getJobConf(), 16, 0);
    ControlledMapReduceJobRunner jobRunner2 =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
            getJobConf(), 12, 0);
    jobRunner1.start();
    ControlledMapReduceJob controlledJob1 = jobRunner1.getJob();
    JobID jobID1 = jobRunner1.getJobID();
    JobInProgress jip1 = getJobTracker().getJob(jobID1);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 12);

    // Confirm that the rest of the tasks are not running and waiting for slots
    // to be freed.
    waitTillAllSlotsAreOccupied(true);

    // Now start the second job.
    jobRunner2.start();
    JobID jobID2 = jobRunner2.getJobID();
    ControlledMapReduceJob controlledJob2 = jobRunner2.getJob();
    JobInProgress jip2 = getJobTracker().getJob(jobID2);

    LOG.info("Trying to finish 2 map");
    controlledJob1.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 2);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 2);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 12);
    waitTillAllSlotsAreOccupied(true);

    LOG.info("Trying to finish 2 more maps");
    controlledJob1.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 4);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 4);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 12);
    waitTillAllSlotsAreOccupied(true);

    // All tasks of Job1 started running/finished. Now job2 should start
    LOG.info("Trying to finish 2 more maps");
    controlledJob1.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 6);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 6);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 10);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip2, true, 2);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 10);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 2);

    LOG.info("Trying to finish 10 more maps and hence job1");
    controlledJob1.finishNTasks(true, 10);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 16);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 16);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip2, true, 12);
    controlledJob1.finishJob();
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 0);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 12);

    // Finish job2 also
    controlledJob2.finishJob();
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip2, true, 12);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 0);

    jobRunner1.join();
    jobRunner2.join();
  }

  /**
   * Submit a job with less M/R tasks than total capacity and another job with
   * more M/R tasks than the remaining capacity. First job should utilize the
   * required slots and other job should utilize the available slots and its
   * remaining tasks wait for slots to become free.
   * 
   * @throws Exception
   */
  private void singleQMultipleJobs2()
      throws Exception {

    ControlledMapReduceJobRunner jobRunner1 =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
            getJobConf(), 8, 0);
    ControlledMapReduceJobRunner jobRunner2 =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
            getJobConf(), 12, 0);
    jobRunner1.start();
    ControlledMapReduceJob controlledJob1 = jobRunner1.getJob();
    JobID jobID1 = jobRunner1.getJobID();
    JobInProgress jip1 = getJobTracker().getJob(jobID1);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 8);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 8);

    // Now start the second job.
    jobRunner2.start();
    JobID jobID2 = jobRunner2.getJobID();
    ControlledMapReduceJob controlledJob2 = jobRunner2.getJob();
    JobInProgress jip2 = getJobTracker().getJob(jobID2);

    ControlledMapReduceJob.waitTillNTasksStartRunning(jip2, true, 4);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 8);
    // The rest of the tasks of job2 should wait.
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 4);

    LOG.info("Trying to finish 2 maps of job1");
    controlledJob1.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 2);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 2);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip1, true, 6);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip2, true, 6);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 6);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 6);

    LOG.info("Trying to finish 6 more maps of job1");
    controlledJob1.finishNTasks(true, 6);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip1, true, 8);
    assertTrue("Number of maps finished", jip1.finishedMaps() == 8);
    ControlledMapReduceJob.waitTillNTasksStartRunning(jip2, true, 12);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(jip1, true, 0);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 12);

    // Finish job2 also
    controlledJob2.finishJob();

    ControlledMapReduceJob.waitTillNTotalTasksFinish(jip2, true, 12);
    ControlledMapReduceJob.assertNumTasksRunning(jip2, true, 0);

    jobRunner1.join();
    jobRunner2.join();
  }

  /**
   * Test to verify running of tasks in a queue going over its capacity. In
   * queue default, user U1 starts a job J1, having more M/R tasks than the
   * total slots. M/R tasks of job J1 should start running on all the nodes (100
   * % utilization).
   * 
   * @throws Exception
   */
  private void multipleQsWithOneQBeyondCapacity(String[] queues)
      throws Exception {

    JobConf conf = getJobConf();
    conf.setQueueName(queues[0]);
    conf.setUser("U1");
    ControlledMapReduceJobRunner jobRunner =
        ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(conf, 15,
            0);
    jobRunner.start();
    ControlledMapReduceJob controlledJob = jobRunner.getJob();
    JobID myJobID = jobRunner.getJobID();
    JobInProgress myJob = getJobTracker().getJob(myJobID);

    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 10);

    // Confirm that the rest of the tasks are not running and waiting for slots
    // to be freed.
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(myJob, true, 10);

    LOG.info("Trying to finish 3 maps");
    controlledJob.finishNTasks(true, 3);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 3);
    assertTrue("Number of maps finished", myJob.finishedMaps() == 3);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 10);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(myJob, true, 10);

    LOG.info("Trying to finish 2 more maps");
    controlledJob.finishNTasks(true, 2);
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 5);
    assertTrue("Number of maps finished", myJob.finishedMaps() == 5);
    ControlledMapReduceJob.waitTillNTasksStartRunning(myJob, true, 10);
    waitTillAllSlotsAreOccupied(true);
    ControlledMapReduceJob.assertNumTasksRunning(myJob, true, 10);

    // Finish job
    controlledJob.finishJob();
    ControlledMapReduceJob.waitTillNTotalTasksFinish(myJob, true, 15);
    ControlledMapReduceJob.assertNumTasksRunning(myJob, true, 0);
    jobRunner.join();
  }

  /**
   * Test to verify queue capacities across multiple queues. In this test, jobs
   * are submitted to different queues - all below the queue's capacity and
   * verifies that all the jobs are running. This will test code paths related
   * to job initialization, considering multiple queues for scheduling jobs etc.
   * 
   * <p>
   * 
   * One user per queue. Four jobs are submitted to the four queues such that
   * they exactly fill up the queues. No queue should be beyond capacity. All
   * jobs should be running.
   * 
   * @throws Exception
   */
  private void multipleQueuesWithinCapacities(String[] queues)
      throws Exception {
    String[] users = new String[] { "U1", "U2", "U3", "U4" };
    ControlledMapReduceJobRunner[] jobRunners =
        new ControlledMapReduceJobRunner[4];
    ControlledMapReduceJob[] controlledJobs = new ControlledMapReduceJob[4];
    JobInProgress[] jips = new JobInProgress[4];

    // Initialize all the jobs
    // Start all the jobs in parallel
    JobConf conf = getJobConf();
    int numTasks = 1;
    for (int i = 0; i < 4; i++) {
      conf.setQueueName(queues[i]);
      conf.setUser(users[i]);
      jobRunners[i] =
          ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
              getJobConf(), numTasks, numTasks);
      jobRunners[i].start();
      controlledJobs[i] = jobRunners[i].getJob();
      JobID jobID = jobRunners[i].getJobID();
      jips[i] = getJobTracker().getJob(jobID);
      // Wait till all the jobs start running all of their tasks
      ControlledMapReduceJob.waitTillNTasksStartRunning(jips[i], true,
          numTasks);
      ControlledMapReduceJob.waitTillNTasksStartRunning(jips[i], false,
          numTasks);
      numTasks += 1;
    }

    // Ensure steady state behavior
    waitTillAllSlotsAreOccupied(true);
    waitTillAllSlotsAreOccupied(false);
    numTasks = 1;
    for (int i = 0; i < 4; i++) {
      ControlledMapReduceJob.assertNumTasksRunning(jips[i], true, numTasks);
      ControlledMapReduceJob.assertNumTasksRunning(jips[i], false, numTasks);
      numTasks += 1;
    }

    // Finish the jobs and join them
    numTasks = 1;
    for (int i = 0; i < 4; i++) {
      controlledJobs[i].finishJob();
      ControlledMapReduceJob
          .waitTillNTotalTasksFinish(jips[i], true, numTasks);
      ControlledMapReduceJob.assertNumTasksRunning(jips[i], true, 0);
      ControlledMapReduceJob.waitTillNTotalTasksFinish(jips[i], false,
          numTasks);
      ControlledMapReduceJob.assertNumTasksRunning(jips[i], false, 0);
      jobRunners[i].join();
      numTasks += 1;
    }
  }
}
