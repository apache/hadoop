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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerPreemptionUtils {
  private CapacityScheduler scheduler;
  private CapacitySchedulerPreemptionContext preemptionContext;
  private RMNode rmNode;
  private RMContainer container;
  private ApplicationAttemptId attemptId;
  private Map<String, Resource> demand;
  private Resource budget;
  private Map<ApplicationAttemptId, Set<RMContainer>> selected;
  private Map<ApplicationAttemptId, Set<RMContainer>> candidates;

  @BeforeEach
  public void setup() {
    // These tests use node tracking without starting the scheduler service.
    scheduler = new CapacityScheduler();
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getNodeLabelManager())
        .thenReturn(mock(RMNodeLabelsManager.class));
    scheduler.setRMContext(rmContext);
    preemptionContext = mock(CapacitySchedulerPreemptionContext.class);
    when(preemptionContext.getScheduler()).thenReturn(scheduler);
    rmNode = MockNodes.newNodeInfo(1, Resource.newInstance(8192, 8));
    attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    container = mock(RMContainer.class);
    when(container.getApplicationAttemptId()).thenReturn(attemptId);
    when(container.getAllocatedNode()).thenReturn(rmNode.getNodeID());
    when(container.getAllocatedResource())
        .thenReturn(Resource.newInstance(1, 1));
    demand = new HashMap<>();
    demand.put(RMNodeLabelsManager.NO_LABEL, Resource.newInstance(10, 10));
    budget = Resource.newInstance(10, 10);
    selected = new HashMap<>();
    candidates = new HashMap<>();
  }

  @AfterEach
  public void cleanup() {
    ClusterMetrics.destroy();
  }

  @Test
  public void testSkipContainerOnRemovedNode() {
    FiCaSchedulerNode node = new FiCaSchedulerNode(rmNode, false);
    scheduler.getNodeTracker().addNode(node);
    scheduler.getNodeTracker().removeNode(node.getNodeID());

    assertFalse(selectContainer(container));
    assertUnchanged();
  }

  @Test
  public void testContinueSelectingAfterMissingNode() {
    assertFalse(selectContainer(container));
    assertUnchanged();

    RMNode remainingNode = MockNodes.newNodeInfo(1,
        Resource.newInstance(8192, 8), 1000, "remaining-node");
    scheduler.getNodeTracker().addNode(
        new FiCaSchedulerNode(remainingNode, false));
    RMContainer nextContainer = mock(RMContainer.class);
    when(nextContainer.getApplicationAttemptId()).thenReturn(attemptId);
    when(nextContainer.getAllocatedNode())
        .thenReturn(remainingNode.getNodeID());
    when(nextContainer.getAllocatedResource())
        .thenReturn(Resource.newInstance(1, 1));

    assertTrue(selectContainer(nextContainer));
    assertEquals(Resource.newInstance(9, 9),
        demand.get(RMNodeLabelsManager.NO_LABEL));
    assertEquals(Resource.newInstance(9, 9), budget);
    assertEquals(Collections.singleton(nextContainer), selected.get(attemptId));
    assertEquals(Collections.singleton(nextContainer),
        candidates.get(attemptId));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "x"})
  public void testSelectContainerOnExistingNode(String partition) {
    FiCaSchedulerNode node = new FiCaSchedulerNode(rmNode, false,
        Collections.singleton(partition));
    scheduler.getNodeTracker().addNode(node);
    demand.clear();
    demand.put(partition, Resource.newInstance(10, 10));

    assertTrue(selectContainer(container));
    assertEquals(Resource.newInstance(9, 9), demand.get(partition));
    assertEquals(Resource.newInstance(9, 9), budget);
    assertEquals(Collections.singleton(container), selected.get(attemptId));
    assertEquals(Collections.singleton(container), candidates.get(attemptId));
  }

  @Test
  public void testSkipContainerDuringNodeResourceUpdate() throws Exception {
    FiCaSchedulerNode node = spy(new FiCaSchedulerNode(rmNode, false));
    scheduler.getNodeTracker().addNode(node);
    Resource newResource = Resource.newInstance(16384, 16);
    CountDownLatch nodeRemoved = new CountDownLatch(1);
    CountDownLatch resumeUpdate = new CountDownLatch(1);
    // Pause the real update after removal from the tracker, before re-adding.
    doAnswer(invocation -> {
      nodeRemoved.countDown();
      assertTrue(resumeUpdate.await(10, TimeUnit.SECONDS));
      return invocation.callRealMethod();
    }).when(node).updateTotalResource(newResource);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> update = executor.submit(() -> scheduler.updateNodeResource(
          rmNode, ResourceOption.newInstance(newResource, -1)));
      assertTrue(nodeRemoved.await(10, TimeUnit.SECONDS));
      assertNull(scheduler.getSchedulerNode(rmNode.getNodeID()));
      assertFalse(selectContainer(container));
      assertUnchanged();

      resumeUpdate.countDown();
      update.get(10, TimeUnit.SECONDS);
      assertSame(node, scheduler.getSchedulerNode(rmNode.getNodeID()));
      assertEquals(newResource, node.getTotalResource());
    } finally {
      resumeUpdate.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private boolean selectContainer(RMContainer candidate) {
    return CapacitySchedulerPreemptionUtils
        .tryPreemptContainerAndDeductResToObtain(
            new DefaultResourceCalculator(), preemptionContext, demand,
            candidate, Resource.newInstance(100, 100), selected, candidates,
            budget, false);
  }

  private void assertUnchanged() {
    assertEquals(1, demand.size());
    assertEquals(Resource.newInstance(10, 10),
        demand.get(RMNodeLabelsManager.NO_LABEL));
    assertEquals(Resource.newInstance(10, 10), budget);
    assertTrue(selected.isEmpty());
    assertTrue(candidates.isEmpty());
  }
}
