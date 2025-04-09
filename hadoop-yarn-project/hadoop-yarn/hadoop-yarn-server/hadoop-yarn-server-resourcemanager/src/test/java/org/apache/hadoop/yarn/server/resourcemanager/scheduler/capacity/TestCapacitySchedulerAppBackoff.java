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
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.PlacementTargets.allocationTag;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.targetIn;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.APP_BACKOFF_MISSED_THRESHOLD;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.BACKOFF_ENABLED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.APP_BACKOFF_INTERVAL_MS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DEFAULT_APP_BACKOFF_MISSED_THRESHOLD;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DEFAULT_APP_BACKOFF_INTERVAL_MS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A3;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B3;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCapacitySchedulerAppBackoff {

  @Test
  public void testAppBackoffConfUpdate() throws Exception {
    // Setup initial queue configuration
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    conf.setAppBackoffEnabled(A1, true);
    conf.setAppBackoffIntervalMs(A1, 10000L);
    conf.setAppBackoffMissedThreshold(A1, 50L);
    conf.setAppBackoffEnabled(B2, true);

    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Verify initial configuration
    assertTrue(cs.getConfiguration().isAppBackoffEnabled(A1));
    assertEquals(10000L, cs.getConfiguration().getAppBackoffIntervalMs(A1));
    assertEquals(50L, cs.getConfiguration().getAppBackoffMissedThreshold(A1));
    assertTrue(cs.getConfiguration().isAppBackoffEnabled(B2));
    assertEquals(DEFAULT_APP_BACKOFF_INTERVAL_MS,
        cs.getConfiguration().getAppBackoffIntervalMs(B2));
    assertEquals(DEFAULT_APP_BACKOFF_MISSED_THRESHOLD,
        cs.getConfiguration().getAppBackoffMissedThreshold(B2));
    assertFalse(cs.getConfiguration().isAppBackoffEnabled(A2));
    assertFalse(cs.getConfiguration().isAppBackoffEnabled(A3));
    assertFalse(cs.getConfiguration().isAppBackoffEnabled(B1));
    assertFalse(cs.getConfiguration().isAppBackoffEnabled(B3));

    // Update configuration: enabled backoff
    conf.setBoolean(PREFIX + BACKOFF_ENABLED, true);
    conf.setLong(PREFIX + APP_BACKOFF_MISSED_THRESHOLD, 5L);
    conf.setLong(PREFIX + APP_BACKOFF_INTERVAL_MS, 5000L);
    // Disabled for A1
    conf.setAppBackoffEnabled(A1, false);

    // Reinitialize the scheduler with updated configuration
    cs.reinitialize(conf, rm.getRMContext());

    // Verify updated configuration
    CapacitySchedulerConfiguration newConf = cs.getConfiguration();
    assertTrue(newConf.isAppBackoffEnabled(B2));
    assertEquals(5L, newConf.getAppBackoffMissedThreshold(B2));
    assertEquals(5000L, newConf.getAppBackoffIntervalMs(B2));
    assertFalse(newConf.isAppBackoffEnabled(A1));
    assertTrue(newConf.isAppBackoffEnabled(A2));
    assertTrue(newConf.isAppBackoffEnabled(A3));
    assertTrue(newConf.isAppBackoffEnabled(B1));
    assertTrue(newConf.isAppBackoffEnabled(B3));

    rm.stop();
  }

  @Test
  public void testSchedulingWithAppBackoffEnabled() throws Exception {
    // Setup backoff conf for queue A1
    long appBackoffIntervalMs = 100L;
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);
    enabledMultiNodesPlacement(conf);
    conf.setAppBackoffEnabled(A1, true);
    conf.setAppBackoffIntervalMs(A1, appBackoffIntervalMs);
    conf.setAppBackoffMissedThreshold(A1, 3L);

    // Register a node
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB);

    // Submit an application in queue A1
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app1")
            .withUser("user")
            .withAcls(null)
            .withQueue(A1.getLeafName())
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm1);

    // Submit a request that cannot be satisfied due to the
    // placement-constraint condition
    PlacementConstraint pc = targetIn("node",
        allocationTag("hbase-master")).build();
    SchedulingRequest schedulingRequest = SchedulingRequest.newInstance(
        1, Priority.newInstance(1), ExecutionTypeRequest.newInstance(), null,
        ResourceSizing.newInstance(1, Resource.newInstance(2 * GB, 1)), pc);
    am.addSchedulingRequest(ImmutableList.of(schedulingRequest));
    am.doHeartbeat();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    AbstractLeafQueue queueA1 =
        (AbstractLeafQueue) cs.getQueue(A1.getLeafName());
    FiCaSchedulerApp schedulerApp =
        cs.getApplicationAttempt(am.getApplicationAttemptId());

    // Simulate missed scheduling opportunities
    for (int i = 0; i < 3; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(nm1.getNodeId())));
    }
    assertFalse(queueA1.isAppInBackoffState(app.getApplicationId()));
    assertEquals(3L, schedulerApp.getAppMissedSchedulingOpportunities());

    // Make the app enter backoff state when it reaches the missed threshold
    cs.handle(new NodeUpdateSchedulerEvent(
        rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

    // Verify app is in backoff state
    assertTrue(queueA1.isAppInBackoffState(app.getApplicationId()));
    assertEquals(0L, schedulerApp.getAppMissedSchedulingOpportunities());

    // Wait for the backoff interval to expire
    GenericTestUtils.waitFor(
        () -> !queueA1.isAppInBackoffState(app.getApplicationId()),
        appBackoffIntervalMs, appBackoffIntervalMs * 2);

    // Verify app is no longer in backoff state after the backoff interval
    assertFalse(queueA1.isAppInBackoffState(app.getApplicationId()));

    // Simulate another missed scheduling opportunity
    cs.handle(new NodeUpdateSchedulerEvent(
        rm.getRMContext().getRMNodes().get(nm1.getNodeId())));
    assertFalse(queueA1.isAppInBackoffState(app.getApplicationId()));
    assertEquals(1L, schedulerApp.getAppMissedSchedulingOpportunities());

    // Request another request which can be allocated at first
    am.allocate("*", 2 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(
        rm.getRMContext().getRMNodes().get(nm1.getNodeId())));

    // new request should be allocated and app is not in backoff state
    assertFalse(queueA1.isAppInBackoffState(schedulerApp.getApplicationId()));
    assertEquals(0L, schedulerApp.getAppMissedSchedulingOpportunities());

    rm.stop();
  }

  private void enabledMultiNodesPlacement(CapacitySchedulerConfiguration conf) {
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    conf.setBoolean(MULTI_NODE_PLACEMENT_ENABLED, true);
    conf.setBoolean(PREFIX + MULTI_NODE_PLACEMENT_ENABLED, true);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
        "resource-based");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        "resource-based");
    String policyName =
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME +
            ".resource-based.class";
    conf.set(policyName, ResourceUsageMultiNodeLookupPolicy.class.getName());
    conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
        true);
  }
}
