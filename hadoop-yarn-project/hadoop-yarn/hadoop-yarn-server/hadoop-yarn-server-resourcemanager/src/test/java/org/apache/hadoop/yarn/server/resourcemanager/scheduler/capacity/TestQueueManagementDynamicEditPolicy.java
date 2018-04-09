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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;
import static org.junit.Assert.assertEquals;

public class TestQueueManagementDynamicEditPolicy extends
    TestCapacitySchedulerAutoCreatedQueueBase {
   private QueueManagementDynamicEditPolicy policy = new
       QueueManagementDynamicEditPolicy();

  @Before
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

  private void waitForPolicyState(float expectedVal,
      GuaranteedOrZeroCapacityOverTimePolicy queueManagementPolicy, String
      nodeLabel, int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (Float.compare(expectedVal, queueManagementPolicy
          .getAbsoluteActivatedChildQueueCapacity(nodeLabel)) > EPSILON) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }
}
