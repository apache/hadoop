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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CapacityTestUtils.ControlledInitializationPoller;
import org.apache.hadoop.mapred.CapacityTestUtils.FakeJobInProgress;
import org.apache.hadoop.mapred.CapacityTestUtils.FakeTaskTrackerManager;
import static org.apache.hadoop.mapred.CapacityTestUtils.*;
import org.junit.After;
import org.junit.Test;

/**
 * Test the Queue-Refresh feature.
 */
public class TestRefreshOfQueues {

  private static final Log LOG =
      LogFactory.getLog(org.apache.hadoop.mapred.TestRefreshOfQueues.class);

  String queueConfigPath =
      System.getProperty("test.build.extraconf", "build/test/extraconf");
  File queueConfigFile =
      new File(queueConfigPath, QueueManager.QUEUE_CONF_FILE_NAME);

  private CapacityTaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;

  /**
   * Remove the queueConfigFile once the test is done.
   */
  @After
  public void tearDown() {
    if (queueConfigFile.exists()) {
      queueConfigFile.delete();
    }
  }

  /**
   * Sets up the scheduler, TaskTrackerManager, QueueManager, initializer and
   * starts the scheduler.
   * 
   * @throws IOException
   */
  private void setupAndStartSchedulerFramework(int numTTs, int numMapsPerTT,
      int numReducesPerTT)
      throws IOException {
    scheduler = new CapacityTaskScheduler();
    taskTrackerManager =
        new FakeTaskTrackerManager(numTTs, numMapsPerTT, numReducesPerTT);
    taskTrackerManager.setQueueManager(new QueueManager());
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.setConf(new Configuration());
    ControlledInitializationPoller controlledInitializationPoller =
        new ControlledInitializationPoller(scheduler.jobQueuesManager,
            taskTrackerManager);
    scheduler.setInitializationPoller(controlledInitializationPoller);
    taskTrackerManager.addJobInProgressListener(scheduler.jobQueuesManager);
    scheduler.start();
  }

  /**
   * Helper method that ensures TaskScheduler is locked before calling
   * {@link QueueManager#refreshQueues(Configuration, 
   *    org.apache.hadoop.mapred.TaskScheduler.QueueRefresher)}.
   */
  private static void refreshQueues(QueueManager qm, Configuration conf,
      TaskScheduler ts) throws IOException {
    synchronized (ts) {
      qm.refreshQueues(conf, ts.getQueueRefresher());
    }
  }

  /**
   * @throws Throwable
   */
  @Test
  public void testRefreshOfQueuesSanity()
      throws Throwable {

    JobQueueInfo[] queues = TestQueueManagerRefresh.getSimpleQueueHierarchy();

    Properties[] props = new Properties[3];
    for (int i = 0; i < props.length; i++) {
      props[i] = queues[i].getProperties();
      props[i].setProperty(CapacitySchedulerConf.CAPACITY_PROPERTY,
          String.valueOf(i + 10));
      props[i].setProperty(CapacitySchedulerConf.MAX_CAPACITY_PROPERTY,
          String.valueOf(i + 15));
      props[i].setProperty(CapacitySchedulerConf.SUPPORTS_PRIORITY_PROPERTY,
          String.valueOf(false));
      props[i].setProperty(
          CapacitySchedulerConf.MAXIMUM_INITIALIZED_JOBS_PER_USER_PROPERTY,
          String.valueOf(i + 11));
      props[i].setProperty(
          CapacitySchedulerConf.MINIMUM_USER_LIMIT_PERCENT_PROPERTY,
          String.valueOf(i + 16));
    }

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    setupAndStartSchedulerFramework(0, 0, 0);

    Map<String, AbstractQueue> allQueues = getAllQueues(scheduler);

    // Verify the configuration.
    for (int i = 0; i < queues.length; i++) {
      String qName = queues[i].getQueueName();
      LOG.info("Queue name : " + qName);
      QueueSchedulingContext qsc =
          allQueues.get(qName).getQueueSchedulingContext();
      LOG.info("Context for queue " + qName + " is : " + qsc);
      assertEquals(i + 10, qsc.getCapacityPercent(), 0);
      assertEquals(i + 15, qsc.getMaxCapacityPercent(), 0);
      assertEquals(Boolean.valueOf(false),
          Boolean.valueOf(qsc.supportsPriorities()));
      assertEquals(i + 16, qsc.getUlMin());
    }

    // change configuration
    for (int i = 0; i < props.length; i++) {
      props[i] = queues[i].getProperties();
      props[i].setProperty(CapacitySchedulerConf.CAPACITY_PROPERTY,
          String.valueOf(i + 20));
      props[i].setProperty(CapacitySchedulerConf.MAX_CAPACITY_PROPERTY,
          String.valueOf(i + 25));
      props[i].setProperty(CapacitySchedulerConf.SUPPORTS_PRIORITY_PROPERTY,
          String.valueOf(false));
      props[i].setProperty(
          CapacitySchedulerConf.MAXIMUM_INITIALIZED_JOBS_PER_USER_PROPERTY,
          String.valueOf(i + 5));
      props[i].setProperty(
          CapacitySchedulerConf.MINIMUM_USER_LIMIT_PERCENT_PROPERTY,
          String.valueOf(i + 10));
    }

    // Re-write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    // Now do scheduler refresh.
    refreshQueues(taskTrackerManager.getQueueManager(), null, scheduler);

    allQueues = getAllQueues(scheduler);

    for (int i = 0; i < queues.length; i++) {
      String qName = queues[i].getQueueName();
      LOG.info("Queue name : " + qName);
      QueueSchedulingContext qsc =
        allQueues.get(qName).getQueueSchedulingContext();
      assertEquals(qName, qsc.getQueueName());
      LOG.info("Context for queue " + qName + " is : " + qsc);
      assertEquals(i + 20, qsc.getCapacityPercent(), 0);
      assertEquals(i + 25, qsc.getMaxCapacityPercent(), 0);
      assertEquals(Boolean.valueOf(false),
          Boolean.valueOf(qsc.supportsPriorities()));
    }
  }

  /**
   * @throws Throwable
   */
   @Test
  public void testSuccessfulCapacityRefresh()
      throws Throwable {

    JobQueueInfo[] queues = TestQueueManagerRefresh.getSimpleQueueHierarchy();

    queues[0].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(100));
    queues[1].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));
    queues[2].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
      queueConfigFile.getAbsolutePath(), new JobQueueInfo[]{queues[0]});

    setupAndStartSchedulerFramework(2, 2, 2);

    FakeJobInProgress job1 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[1].getQueueName(), "user");
    FakeJobInProgress job2 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[2].getQueueName(), "user");

    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
//===========================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0002_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0002_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0002_m_000002_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0002_r_000002_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);
//============================================
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000002_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000002_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

    taskTrackerManager.killJob(job1.getJobID());
    taskTrackerManager.killJob(job2.getJobID());

    // change configuration
    queues[1].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(25));
    queues[2].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(75));

    // Re-write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
      queueConfigFile.getAbsolutePath(), new JobQueueInfo[]{queues[0]});

    refreshQueues(taskTrackerManager.getQueueManager(), null, scheduler);

    job1 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[1].getQueueName(), "user");
    job2 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 4, 4,
        queues[2].getQueueName(), "user");

    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0003_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0003_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);


    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0004_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0004_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);


    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0004_m_000002_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0004_r_000002_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0004_m_000003_0 on tt2");
    expectedStrings.put(REDUCE, "attempt_test_0004_r_000003_0 on tt2");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt2",
      expectedStrings);

  }

  /**
   * Test to verify that the refresh of the scheduler fails when modified
   * configuration overflows 100%
   * 
   * @throws Throwable
   */
   @Test
  public void testFailingCapacityRefresh()
      throws Throwable {

    JobQueueInfo[] queues = TestQueueManagerRefresh.getSimpleQueueHierarchy();

    queues[0].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(100));
    queues[1].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(70));
    queues[2].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    try {
      setupAndStartSchedulerFramework(2, 2, 2);
      fail("Scheduler should have failed to start!");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains(
          String.format(QueueHierarchyBuilder.TOTAL_CAPACITY_OVERFLOWN_MSG,
              queues[1].getQueueName() + "," + queues[2].getQueueName(),
              Float.valueOf(120.0f))));
    }

    // Rectify the properties and start the scheduler
    queues[1].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    setupAndStartSchedulerFramework(2, 2, 2);

    // Now change configuration.
    queues[1].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(35));
    queues[2].getProperties().setProperty(
        CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(95));

    // Re-write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    try {
      refreshQueues(taskTrackerManager.getQueueManager(), null, scheduler);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains(
          String.format(QueueHierarchyBuilder.TOTAL_CAPACITY_OVERFLOWN_MSG,
              queues[1].getQueueName() + "," + queues[2].getQueueName(),
              Float.valueOf(130.0f))));
    }
  }

  /**
   * @throws Throwable
   */
   @Test
  public void testRefreshUserLimits()
      throws Throwable {

    JobQueueInfo[] queues = TestQueueManagerRefresh.getSimpleQueueHierarchy();

    queues[0].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(100));
    queues[1].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));
    queues[2].getProperties().setProperty(
      CapacitySchedulerConf.CAPACITY_PROPERTY, String.valueOf(50));

    queues[2].getProperties().setProperty(
      CapacitySchedulerConf.MINIMUM_USER_LIMIT_PERCENT_PROPERTY,
      String.valueOf(100));

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
      queueConfigFile.getAbsolutePath(), new JobQueueInfo[]{queues[0]});

    setupAndStartSchedulerFramework(1, 2, 2);

    FakeJobInProgress job1 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[2].getQueueName(), "user1");
    FakeJobInProgress job2 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[2].getQueueName(), "user2");

    Map<String, String> expectedStrings = new HashMap<String, String>();
    expectedStrings.put(MAP, "attempt_test_0001_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
    
    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0001_m_000002_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0001_r_000002_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    assertNull(scheduler.assignTasks(taskTrackerManager.getTaskTracker("tt1")));
    taskTrackerManager.killJob(job1.getJobID());
    taskTrackerManager.killJob(job2.getJobID());

    // change configuration
    queues[2].getProperties().setProperty(
      CapacitySchedulerConf.MINIMUM_USER_LIMIT_PERCENT_PROPERTY,
      String.valueOf(50));

    // Re-write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
      queueConfigFile.getAbsolutePath(), new JobQueueInfo[]{queues[0]});

    refreshQueues(taskTrackerManager.getQueueManager(), null, scheduler);

    job1 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[1].getQueueName(), "user1");
    job2 =
      taskTrackerManager.submitJobAndInit(
        JobStatus.PREP, 2, 2,
        queues[2].getQueueName(), "user2");

    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0003_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0003_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);

    expectedStrings.clear();
    expectedStrings.put(MAP, "attempt_test_0004_m_000001_0 on tt1");
    expectedStrings.put(REDUCE, "attempt_test_0004_r_000001_0 on tt1");
    checkMultipleTaskAssignment(
      taskTrackerManager, scheduler, "tt1",
      expectedStrings);
  }

  /**
   * Get a map of all {@link AbstractQueue}s.
   * 
   * @param sched
   * @return
   */
  private static Map<String, AbstractQueue> getAllQueues(
      CapacityTaskScheduler sched) {
    AbstractQueue rootQueue = sched.getRoot();
    HashMap<String, AbstractQueue> allQueues =
        new HashMap<String, AbstractQueue>();
    List<AbstractQueue> allQueuesList = new ArrayList<AbstractQueue>();
    allQueuesList.addAll(rootQueue.getDescendentJobQueues());
    allQueuesList.addAll(rootQueue.getDescendantContainerQueues());
    for (AbstractQueue q : allQueuesList) {
      LOG.info("Putting in allQueues list " + q.getName());
      allQueues.put(q.getName(), q);
    }
    return allQueues;
  }
}
