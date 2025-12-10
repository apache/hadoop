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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test to verify that FairSharePolicy correctly handles queues with
 * pending applications waiting for AM allocation.
 *
 * This test ensures that queues with demand=0 but getNumRunnableApps() > 0
 * are not treated as empty and can progress to minShare comparison.
 */
public class TestFairSharePolicyPendingApps {

  /**
   * Test that a queue with pending apps (demand=0, getNumRunnableApps() > 0)
   * is not treated as empty and can progress to Stage 2 (minShare comparison).
   */
  @Test
  public void testQueueWithPendingAppsNotTreatedAsEmpty() {
    // Queue with pending apps but no running containers
    FSLeafQueue queueWithPendingApps = mock(FSLeafQueue.class);
    when(queueWithPendingApps.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(queueWithPendingApps.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(queueWithPendingApps.getMinShare()).thenReturn(Resources.createResource(256 * 1024, 1));
    when(queueWithPendingApps.getNumRunnableApps()).thenReturn(6); // Has pending apps
    when(queueWithPendingApps.getWeight()).thenReturn(1.0f);
    when(queueWithPendingApps.getName()).thenReturn("queueWithPending");
    when(queueWithPendingApps.getStartTime()).thenReturn(1000L);

    // Active queue with demand > 0
    FSLeafQueue activeQueue = mock(FSLeafQueue.class);
    when(activeQueue.getDemand()).thenReturn(Resources.createResource(10240, 10));
    when(activeQueue.getResourceUsage()).thenReturn(Resources.createResource(10240, 10));
    when(activeQueue.getMinShare()).thenReturn(Resources.createResource(128 * 1024, 1));
    when(activeQueue.getNumRunnableApps()).thenReturn(5);
    when(activeQueue.getWeight()).thenReturn(1.0f);
    when(activeQueue.getName()).thenReturn("activeQueue");
    when(activeQueue.getStartTime()).thenReturn(2000L);

    FairSharePolicy policy = new FairSharePolicy();
    int result = policy.getComparator().compare(queueWithPendingApps, activeQueue);

    // Queue with pending apps should NOT be assigned lowest priority
    // compareDemand() should return 0, allowing progression to Stage 2
    // Result depends on Stage 2 (minShare comparison) or later stages
    assertNotEquals("Queue with pending apps should not get lowest priority from compareDemand()",
        1, result);
  }

  /**
   * Test that a truly empty queue (getNumRunnableApps() == 0) gets lower
   * priority compared to a queue with pending apps.
   */
  @Test
  public void testTrulyEmptyQueueGetsLowerPriority() {
    // Queue with pending apps
    FSLeafQueue queueWithPendingApps = mock(FSLeafQueue.class);
    when(queueWithPendingApps.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(queueWithPendingApps.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(queueWithPendingApps.getMinShare()).thenReturn(Resources.createResource(256 * 1024, 1));
    when(queueWithPendingApps.getNumRunnableApps()).thenReturn(6); // Has pending apps
    when(queueWithPendingApps.getWeight()).thenReturn(1.0f);
    when(queueWithPendingApps.getName()).thenReturn("queueWithPending");
    when(queueWithPendingApps.getStartTime()).thenReturn(1000L);

    // Truly empty queue (no apps at all)
    FSLeafQueue emptyQueue = mock(FSLeafQueue.class);
    when(emptyQueue.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(emptyQueue.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(emptyQueue.getMinShare()).thenReturn(Resources.createResource(128 * 1024, 1));
    when(emptyQueue.getNumRunnableApps()).thenReturn(0); // Truly empty
    when(emptyQueue.getWeight()).thenReturn(1.0f);
    when(emptyQueue.getName()).thenReturn("emptyQueue");
    when(emptyQueue.getStartTime()).thenReturn(2000L);

    FairSharePolicy policy = new FairSharePolicy();
    int result = policy.getComparator().compare(queueWithPendingApps, emptyQueue);

    // Queue with pending apps should get higher priority than truly empty queue
    assertTrue("Queue with pending apps should have higher priority than truly empty queue",
        result < 0);
  }

  /**
   * Test that minShare comparison works correctly for queues with pending apps.
   */
  @Test
  public void testMinShareComparisonForQueueWithPendingApps() {
    // Queue with pending apps, below minShare
    FSLeafQueue queueBelowMinShare = mock(FSLeafQueue.class);
    when(queueBelowMinShare.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(queueBelowMinShare.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(queueBelowMinShare.getMinShare()).thenReturn(Resources.createResource(256 * 1024, 1));
    when(queueBelowMinShare.getNumRunnableApps()).thenReturn(6);
    when(queueBelowMinShare.getWeight()).thenReturn(1.0f);
    when(queueBelowMinShare.getName()).thenReturn("queueBelowMinShare");
    when(queueBelowMinShare.getStartTime()).thenReturn(1000L);

    // Queue above minShare
    FSLeafQueue queueAboveMinShare = mock(FSLeafQueue.class);
    when(queueAboveMinShare.getDemand()).thenReturn(Resources.createResource(200 * 1024, 10));
    when(queueAboveMinShare.getResourceUsage()).thenReturn(Resources.createResource(200 * 1024, 10));
    when(queueAboveMinShare.getMinShare()).thenReturn(Resources.createResource(128 * 1024, 1));
    when(queueAboveMinShare.getNumRunnableApps()).thenReturn(5);
    when(queueAboveMinShare.getWeight()).thenReturn(1.0f);
    when(queueAboveMinShare.getName()).thenReturn("queueAboveMinShare");
    when(queueAboveMinShare.getStartTime()).thenReturn(2000L);

    FairSharePolicy policy = new FairSharePolicy();
    int result = policy.getComparator().compare(queueBelowMinShare, queueAboveMinShare);

    // Queue below minShare should get higher priority (negative result)
    assertTrue("Queue below minShare with pending apps should get higher priority",
        result < 0);
  }

  /**
   * Test that two queues with pending apps are compared by later stages
   * (not blocked at compareDemand).
   */
  @Test
  public void testTwoQueuesWithPendingAppsComparedByLaterStages() {
    // First queue with pending apps
    FSLeafQueue queue1 = mock(FSLeafQueue.class);
    when(queue1.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(queue1.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(queue1.getMinShare()).thenReturn(Resources.createResource(256 * 1024, 1));
    when(queue1.getNumRunnableApps()).thenReturn(6);
    when(queue1.getWeight()).thenReturn(1.0f);
    when(queue1.getName()).thenReturn("queue1");
    when(queue1.getStartTime()).thenReturn(1000L);

    // Second queue with pending apps
    FSLeafQueue queue2 = mock(FSLeafQueue.class);
    when(queue2.getDemand()).thenReturn(Resources.createResource(0, 0));
    when(queue2.getResourceUsage()).thenReturn(Resources.createResource(0, 0));
    when(queue2.getMinShare()).thenReturn(Resources.createResource(128 * 1024, 1));
    when(queue2.getNumRunnableApps()).thenReturn(3);
    when(queue2.getWeight()).thenReturn(1.0f);
    when(queue2.getName()).thenReturn("queue2");
    when(queue2.getStartTime()).thenReturn(2000L);

    FairSharePolicy policy = new FairSharePolicy();
    int result = policy.getComparator().compare(queue1, queue2);

    // Both queues have pending apps, so compareDemand() should return 0
    // The comparison should proceed to Stage 2 (minShare) or later
    // queue1 has higher minShare (256GB > 128GB), both at 0% usage
    // So queue1 should get higher priority (negative result)
    assertTrue("Queues with pending apps should be compared by minShare, not blocked at compareDemand",
        result < 0);
  }
}
