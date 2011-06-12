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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.junit.After;
import org.junit.Test;

/**
 * Test the refresh feature of QueueManager.
 */
public class TestQueueManagerRefresh {

  private static final Log LOG =
      LogFactory.getLog(TestQueueManagerRefresh.class);

  String queueConfigPath =
      System.getProperty("test.build.extraconf", "build/test/extraconf");
  File queueConfigFile =
      new File(queueConfigPath, QueueManager.QUEUE_CONF_FILE_NAME);

  /**
   * Remove the configuration file after the test's done.
   */
  @After
  public void tearDown() {
    if (queueConfigFile.exists()) {
      queueConfigFile.delete();
    }
  }

  /**
   * @return a simple hierarchy of JobQueueInfos
   */
  static JobQueueInfo[] getSimpleQueueHierarchy() {
    int numQs = 3;
    JobQueueInfo[] queues = new JobQueueInfo[numQs];
    queues[0] =
        newJobQueueInfo(new ArrayList<JobQueueInfo>(), null, "q1",
            QueueState.UNDEFINED, null);
    queues[1] =
        newJobQueueInfo(new ArrayList<JobQueueInfo>(), null, "q1:q2",
            QueueState.RUNNING, null);
    queues[2] =
        newJobQueueInfo(new ArrayList<JobQueueInfo>(), null, "q1:q3",
            QueueState.RUNNING, null);
    queues[0].addChild(queues[1]);
    queues[0].addChild(queues[2]);
    return queues;
  }

  /**
   * Test to verify that the refresh of queue properties fails if a new queue is
   * added.
   * 
   * @throws Exception
   */
  @Test
  public void testRefreshWithAddedQueues()
      throws Exception {

    JobQueueInfo[] queues = getSimpleQueueHierarchy();

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    QueueManager qManager = new QueueManager();

    JobQueueInfo newQueue =
        newJobQueueInfo(new ArrayList<JobQueueInfo>(), null, "q4",
            QueueState.UNDEFINED, null);
    queues[0].addChild(newQueue);

    // Rewrite the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    testRefreshFailureWithChangeOfHierarchy(qManager);

  }

  /**
   * Test to verify that the refresh of queue properties fails if queues are
   * removed.
   * 
   * @throws Exception
   */
  @Test
  public void testRefreshWithRemovedQueues()
      throws Exception {

    JobQueueInfo[] queues = getSimpleQueueHierarchy();

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    QueueManager qManager = new QueueManager();

    // Remove queue[2]
    JobQueueInfo q2 = queues[2];
    queues[0].removeChild(q2);

    // Rewrite the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    testRefreshFailureWithChangeOfHierarchy(qManager);
  }

  /**
   * @param originalQManager
   * @throws Exception
   */
  private void testRefreshFailureWithChangeOfHierarchy(
      QueueManager originalQManager)
      throws Exception {

    // Make sure that isHierarchySame returns false.
    QueueManager modifiedQueueManager = new QueueManager();
    assertFalse("Hierarchy changed after refresh!",
        originalQManager.getRoot().isHierarchySameAs(
            modifiedQueueManager.getRoot()));

    // Refresh the QueueManager and make sure it fails.
    try {
      originalQManager.refreshQueues(null, null);
      fail("Queue-refresh should have failed!");
    } catch (Exception e) {
      // Refresh failed as expected. Check the error message.
      assertTrue(
          "Exception message should point to a change in queue hierarchy!",
          e.getMessage().contains(
              QueueManager.MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY));
    }

    // Make sure that the old configuration is retained.
    List<JobQueueInfo> rootQueues =
        originalQManager.getRoot().getJobQueueInfo().getChildren();
    assertTrue(rootQueues.size() == 1);
  }

  /**
   * Test to verify that the refresh of queue properties fails if scheduler
   * fails to reload itself.
   * 
   * @throws Exception
   */
  // @Test
  public void testRefreshWithSchedulerFailure()
      throws Exception {
    JobQueueInfo[] queues = getSimpleQueueHierarchy();

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    QueueManager qManager = new QueueManager();

    // No change in configuration. Just Refresh the QueueManager and make sure
    // it fails.
    try {
      qManager.refreshQueues(null,
          new MyTaskScheduler().new MyFailingQueueRefresher());
      fail("Queue-refresh should have failed!");
    } catch (Exception e) {
      // Refresh failed as expected. Check the error message.
      assertTrue(
          "Exception message should point to a refresh-failure in scheduler!",
          e.getMessage().contains(
              QueueManager.MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE));
    }
  }

  /**
   * Test to verify that the refresh of scheduler properties passes smoothly.
   * 
   * @throws Exception
   */
  @Test
  public void testRefreshOfSchedulerProperties()
      throws Exception {
    JobQueueInfo[] queues = getSimpleQueueHierarchy();

    // Set some scheduler properties
    for (JobQueueInfo jqi : queues) {
      Properties props = new Properties();
      props.setProperty("testing.property", "testing.value."
          + jqi.getQueueName());
      jqi.setProperties(props);
    }

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    QueueManager qManager = new QueueManager();

    MyTaskScheduler myScheduler = new MyTaskScheduler();

    qManager.refreshQueues(null, myScheduler.new MyQueueRefresher());

    // Verify that the scheduler props are set correctly by scheduler-refresh.
    Map<String, Properties> schedProps = myScheduler.getSchedulerProperties();
    for (JobQueueInfo jqi : queues) {
      String expectedVal = "testing.value." + jqi.getQueueName();
      Properties qProperties = schedProps.get(jqi.getQueueName());
      assertNotNull("Properties should not be null for the SchedulerQueue "
          + jqi.getQueueName(), qProperties);
      String observedVal = qProperties.getProperty("testing.property");
      assertEquals("Properties for the SchedulerQueue " + jqi.getQueueName()
          + " are not reloaded properly!", expectedVal, observedVal);
    }
  }

  /**
   * Test to verify that the scheduling information per queue in the
   * {@link QueueManager} is retained across queue-refresh.
   * 
   * @throws Exception
   */
  @Test
  public void testSchedulingInfoAfterRefresh()
      throws Exception {

    JobQueueInfo[] queues = getSimpleQueueHierarchy();

    // write the configuration file
    QueueManagerTestUtils.writeQueueConfigurationFile(
        queueConfigFile.getAbsolutePath(), new JobQueueInfo[] { queues[0] });

    QueueManager qManager = new QueueManager();

    // Set some scheduling information for the queues in the QueueManager.
    for (String qName : qManager.getLeafQueueNames()) {
      qManager.setSchedulerInfo(qName, new String(
          "scheduling-information-for-queue-" + qName));
    }

    qManager.refreshQueues(null, null);

    // Verify that the scheduling information is retained across refresh.
    for (String qName : qManager.getLeafQueueNames()) {
      assertEquals("scheduling-information-for-queue-" + qName,
          qManager.getSchedulerInfo(qName));
    }
  }

  static class MyTaskScheduler extends TaskScheduler {

    Map<String, Properties> schedulerPropsMap =
        new HashMap<String, Properties>();

    Map<String, Properties> getSchedulerProperties() {
      return schedulerPropsMap;
    }

    class MyQueueRefresher extends QueueRefresher {

      private void updateSchedulerProps(JobQueueInfo jqi) {
        LOG.info("Updating properties for SchedulerQueue "
            + jqi.getQueueName());
        LOG.info("Putting " + jqi.getProperties() + " in "
            + jqi.getQueueName());
        schedulerPropsMap.put(jqi.getQueueName(), jqi.getProperties());
        for (JobQueueInfo child : jqi.getChildren()) {
          updateSchedulerProps(child);
        }
      }

      @Override
      void refreshQueues(List<JobQueueInfo> newRootQueues) {
        LOG.info("Refreshing scheduler's properties");
        for (JobQueueInfo jqi : newRootQueues) {
          updateSchedulerProps(jqi);
        }
      }
    }

    class MyFailingQueueRefresher extends QueueRefresher {
      @Override
      void refreshQueues(List<JobQueueInfo> newRootQueues)
          throws Throwable {
        throw new IOException("Scheduler cannot refresh the queues!");
      }
    }

    @Override
    public List<Task> assignTasks(TaskTracker taskTracker) {
      return null;
    }

    @Override
    public Collection<JobInProgress> getJobs(String queueName) {
      return null;
    }
  }

  static JobQueueInfo newJobQueueInfo(List<JobQueueInfo> children,
      Properties props, String queueName, QueueState state,
      String schedulingInfo) {
    JobQueueInfo jqi = new JobQueueInfo();
    jqi.setChildren(children);
    if (props != null) {
      jqi.setProperties(props);
    }
    jqi.setQueueName(queueName);
    jqi.setQueueState(state.getStateName());
    jqi.setSchedulingInfo(schedulingInfo);
    return jqi;
  }
}
