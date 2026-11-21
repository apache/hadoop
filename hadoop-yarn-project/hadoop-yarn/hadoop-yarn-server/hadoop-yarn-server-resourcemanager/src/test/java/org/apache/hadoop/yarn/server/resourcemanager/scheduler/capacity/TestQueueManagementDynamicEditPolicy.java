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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GatedGuaranteedOrZeroCapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CapacitySchedulerConfiguration.AUTO_CREATED_QUEUE_MANAGEMENT_POLICY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQueueManagementDynamicEditPolicy extends
    TestCapacitySchedulerAutoCreatedQueueBase {
   private QueueManagementDynamicEditPolicy policy = new
       QueueManagementDynamicEditPolicy();

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    policy.init(cs.getConfiguration(), cs.getRMContext(), cs);
  }

  @Test
  public void testEditSchedule() throws Exception {

    try {
      policy.editSchedule();
      assertEquals(2, policy.getManagedParentQueues().size());

      CSQueue parentQueue = cs.getQueue(PARENT_QUEUE);

      GuaranteedOrZeroCapacityOverTimePolicy autoCreatedQueueManagementPolicy =
          (GuaranteedOrZeroCapacityOverTimePolicy) ((ManagedParentQueue)
              parentQueue)
              .getAutoCreatedQueueManagementPolicy();
      assertEquals(0f, autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), EPSILON);

      //submit app1 as USER1
      ApplicationId user1AppId = submitApp(mockRM, parentQueue, USER1, USER1, 1,
          1);
      Map<String, Float> expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(1);
      validateInitialQueueEntitlement(parentQueue, USER1,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //submit another app2 as USER2
      ApplicationId user2AppId = submitApp(mockRM, parentQueue, USER2, USER2, 2,
          1);
      expectedAbsChildQueueCapacity =
          populateExpectedAbsCapacityByLabelForParentQueue(2);
      validateInitialQueueEntitlement(parentQueue, USER2,
          expectedAbsChildQueueCapacity, accessibleNodeLabelsOnC);

      //validate total activated abs capacity
      assertEquals(0.2f, autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), EPSILON);

      //submit user_3 app. This cant be scheduled since there is no capacity
      submitApp(mockRM, parentQueue, USER3, USER3, 3, 1);
      final CSQueue user3LeafQueue = cs.getQueue(USER3);
      validateCapacities((AutoCreatedLeafQueue) user3LeafQueue, 0.0f, 0.0f,
          1.0f, 1.0f);

      assertEquals(autoCreatedQueueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(NO_LABEL), 0.2f, EPSILON);

      //deactivate USER2 queue
      cs.killAllAppsInQueue(USER2);
      mockRM.waitForState(user2AppId, RMAppState.KILLED);

      //deactivate USER1 queue
      cs.killAllAppsInQueue(USER1);
      mockRM.waitForState(user1AppId, RMAppState.KILLED);

      policy.editSchedule();
      waitForPolicyState(0.1f, autoCreatedQueueManagementPolicy, NO_LABEL,
          1000);

      validateCapacities((AutoCreatedLeafQueue) user3LeafQueue, 0.5f, 0.1f,
          1.0f, 1.0f);

      validateCapacitiesByLabel((ManagedParentQueue) parentQueue, (AutoCreatedLeafQueue) user3LeafQueue,
          NODEL_LABEL_GPU);

    } finally {
      cleanupQueue(USER1);
      cleanupQueue(USER2);
      cleanupQueue(USER3);
    }
  }

  @Test
  @Timeout(value = 60)
  public void testMonitorAndReinitializeDoNotDeadlock() throws Exception {
    // Switch the parent queue to the gated policy.
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(cs.getConfiguration());
    conf.set(QueuePrefixes.getQueuePrefix(C)
            + AUTO_CREATED_QUEUE_MANAGEMENT_POLICY,
        GatedGuaranteedOrZeroCapacityOverTimePolicy.class.getName());
    cs.reinitialize(conf, mockRM.getRMContext());

    // Finish node registrations first, so no scheduler event is pending on
    // the parent queue lock when the threads below block.
    mockRM.drainEvents();

    ManagedParentQueue parentQueue =
        (ManagedParentQueue) cs.getQueue(PARENT_QUEUE);
    GatedGuaranteedOrZeroCapacityOverTimePolicy gatedPolicy =
        (GatedGuaranteedOrZeroCapacityOverTimePolicy)
            parentQueue.getAutoCreatedQueueManagementPolicy();

    FutureTask<Void> monitor = new FutureTask<>(() -> {
      policy.editSchedule();
      return null;
    });
    FutureTask<Void> reinit = new FutureTask<>(() -> {
      parentQueue.reinitialize(parentQueue, cs.getClusterResource());
      return null;
    });
    Thread monitorThread = new Thread(monitor, "queue-management-monitor");
    Thread reinitThread = new Thread(reinit, "queue-reinitialize");
    monitorThread.setDaemon(true);
    reinitThread.setDaemon(true);

    try {
      // The monitor pauses while holding the policy's write lock.
      gatedPolicy.gateThread(monitorThread);
      monitorThread.start();
      assertTrue(gatedPolicy.awaitGate(10, TimeUnit.SECONDS),
          "Monitor thread did not reach the policy gate");

      // Reinitialize takes the parent queue's write lock, then blocks.
      reinitThread.start();
      GenericTestUtils.waitFor(
          () -> reinitThread.getState() == Thread.State.WAITING, 10, 10000);

      // If deadlock happens, the monitor waits on the parent queue lock
      // while reinitialize waits on the policy lock, and both get() calls
      // time out.
      gatedPolicy.release();
      monitor.get(10, TimeUnit.SECONDS);
      reinit.get(10, TimeUnit.SECONDS);
    } finally {
      gatedPolicy.release();
    }
  }

  private void waitForPolicyState(float expectedVal,
      GuaranteedOrZeroCapacityOverTimePolicy queueManagementPolicy, String
      nodeLabel, int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (Math.abs(expectedVal - queueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(nodeLabel)) > EPSILON) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }
}
