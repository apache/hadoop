/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestCapacitySchedulerNewQueueAutoCreation.MAX_MEMORY;

public class TestAutoCreatedQueueDeletionPolicy
    extends TestCapacitySchedulerAutoCreatedQueueBase {
  private CapacitySchedulerConfiguration csConf;
  private CapacityScheduler cs;
  private final AutoCreatedQueueDeletionPolicy policy = new
      AutoCreatedQueueDeletionPolicy();

  private CapacitySchedulerQueueManager autoQueueHandler;

  public static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  public static final QueuePath ROOT_A = new QueuePath("root", "a");
  public static final QueuePath ROOT_A_A1 = QueuePath.createFromQueues("root", "a", "a1");
  public static final QueuePath ROOT_B = new QueuePath("root", "b");

  /*
    Create the following structure:
             root
          /       \
        a          b
      /
    a1
  */
  @BeforeEach
  public void setUp() throws Exception {
    csConf = new CapacitySchedulerConfiguration();
    csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    // By default, set 3 queues, a/b, and a.a1
    csConf.setQueues(ROOT, new String[]{"a", "b"});
    csConf.setNonLabeledQueueWeight(ROOT, 1f);
    csConf.setNonLabeledQueueWeight(ROOT_A, 1f);
    csConf.setNonLabeledQueueWeight(ROOT_B, 1f);
    csConf.setQueues(ROOT_A, new String[]{"a1"});
    csConf.setNonLabeledQueueWeight(ROOT_A_A1, 1f);
    csConf.setAutoQueueCreationV2Enabled(ROOT, true);
    csConf.setAutoQueueCreationV2Enabled(ROOT_A, true);
    csConf.setAutoQueueCreationV2Enabled(PARENT_QUEUE_PATH, true);
    // Test for auto deletion when expired
    csConf.setAutoExpiredDeletionTime(1);
  }

  @AfterEach
  public void tearDown() {
    if (mockRM != null) {
      mockRM.stop();
    }
  }

  @Test
  public void testEditSchedule() throws Exception {
    prepareForSchedule();
    // Make sure e not null
    AbstractCSQueue e = (AbstractCSQueue) cs.
        getQueue("root.e");
    Assertions.assertNotNull(e);
    Assertions.assertTrue(e.isDynamicQueue());

    // Make sure e1 not null
    AbstractCSQueue e1 =  (AbstractCSQueue)cs.
        getQueue("root.e.e1");
    Assertions.assertNotNull(e1);
    Assertions.assertTrue(e1.isDynamicQueue());
    // signal it because of without submit created
    e1.setLastSubmittedTimestamp(Time.monotonicNow());

    ApplicationAttemptId user0AppAttemptId =
        submitApp(cs, USER0, USER0, "root.e");

    // Wait user0 created successfully.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.e.user_0") != null, 100,
        2000);
    // Make sure user0 not null
    AbstractCSQueue user0 = (AbstractCSQueue) cs
        .getQueue("root.e.user_0");
    Assertions.assertNotNull(user0);
    Assertions.assertTrue(user0.isDynamicQueue());
    // Make app finished
    AppAttemptRemovedSchedulerEvent event =
        new AppAttemptRemovedSchedulerEvent(user0AppAttemptId,
            RMAppAttemptState.FINISHED, false);
    cs.handle(event);
    AppRemovedSchedulerEvent rEvent = new AppRemovedSchedulerEvent(
        user0AppAttemptId.getApplicationId(), RMAppState.FINISHED);
    cs.handle(rEvent);

    // There are no apps in user0
    Assertions.assertEquals(user0.getNumApplications(), 0);

    // Wait the time expired.
    long l1 = user0.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l1)/1000;
      return duration > cs.
          getConfiguration().getAutoExpiredDeletionTime();
    }, 100, 2000);

    long l2 = e1.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l2)/1000;
      return duration > cs.
          getConfiguration().getAutoExpiredDeletionTime();
    }, 100, 2000);

    policy.editSchedule();
    // Make sure user_0 , e1 queue
    // will be scheduled to mark for deletion
    // because it is expired for deletion.
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 2);
    Assertions.assertTrue(policy.
        getMarkedForDeletion().contains("root.e.user_0"));
    Assertions.assertTrue(policy.
        getMarkedForDeletion().contains("root.e.e1"));
    // Make sure the send for deletion is empty for first mark.
    Assertions.assertEquals(policy.getSentForDeletion().size(), 0);

    // Make sure user_0 , e1 queue will be scheduled to send for deletion
    policy.prepareForAutoDeletion();
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 0);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 2);

    // Make sure e1, user0 not null before trigger remove.
    e1 = (AbstractCSQueue) cs.getQueue("root.e.e1");
    Assertions.assertNotNull(e1);
    user0 =  (AbstractCSQueue)cs.getQueue("root.e.user_0");
    Assertions.assertNotNull(user0);

    // Make sure e1, user0 will be null after trigger remove.
    policy.triggerAutoDeletionForExpiredQueues();
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 0);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 0);

    // Wait e1, user0 auto deleted.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.e.e1") == null,
        100, 2000);
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.e.user_0") == null,
        100, 2000);
    e1 = (AbstractCSQueue) cs.getQueue("root.e.e1");
    Assertions.assertNull(e1);
    user0 =  (AbstractCSQueue)cs.getQueue("root.e.user_0");
    Assertions.assertNull(user0);

    // Make sure e is not null, before schedule.
    e = (AbstractCSQueue) cs.getQueue("root.e");
    Assertions.assertNotNull(e);

    // Expired for e
    // Wait e marked for deletion.
    long l3 = e.getLastSubmittedTimestamp();
    GenericTestUtils.waitFor(() -> {
      long duration = (Time.monotonicNow() - l3)/1000;
      return duration > cs.
          getConfiguration().getAutoExpiredDeletionTime();
    }, 100, 2000);
    policy.editSchedule();
    e = (AbstractCSQueue) cs.getQueue("root.e");
    Assertions.assertNotNull(e);
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 1);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 0);
    Assertions.assertTrue(policy.getMarkedForDeletion().contains("root.e"));

    // Make sure e queue will be scheduled to send for deletion
    policy.prepareForAutoDeletion();
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 0);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 1);

    // Make sure e not null before trigger remove.
    e = (AbstractCSQueue) cs.getQueue("root.e");
    Assertions.assertNotNull(e);

    // Make sure e will be null after trigger remove.
    policy.triggerAutoDeletionForExpiredQueues();
    // Wait e1 auto deleted.
    GenericTestUtils.waitFor(()-> cs.getQueue(
        "root.e") == null, 100, 2000);
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 0);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 0);
    e = (AbstractCSQueue) cs.getQueue("root.e");
    Assertions.assertNull(e);
  }

  public void prepareForSchedule() throws Exception{
    startScheduler();

    policy.editSchedule();
    // There are no queues should be scheduled
    Assertions.assertEquals(policy.getMarkedForDeletion().size(), 0);
    Assertions.assertEquals(policy.getSentForDeletion().size(), 0);

    createQueue("root.e.e1");
  }

  protected void startScheduler() throws Exception {
    try (RMNodeLabelsManager mgr = new NullRMNodeLabelsManager()) {
      mgr.init(csConf);
      mockRM = new MockRM(csConf) {
        protected RMNodeLabelsManager createNodeLabelManager() {
          return mgr;
        }
      };

      cs = (CapacityScheduler) mockRM.getResourceScheduler();
      cs.updatePlacementRules();
      // Policy for new auto created queue's auto deletion when expired
      policy.init(cs.getConfiguration(), cs.getRMContext(), cs);
      mockRM.start();
      cs.start();
      autoQueueHandler = cs.getCapacitySchedulerQueueManager();
      mockRM.registerNode("h1:1234", MAX_MEMORY * GB);
    }
  }

  protected AbstractLeafQueue createQueue(String queuePath) throws YarnException,
      IOException {
    return autoQueueHandler.createQueue(new QueuePath(queuePath));
  }
}

