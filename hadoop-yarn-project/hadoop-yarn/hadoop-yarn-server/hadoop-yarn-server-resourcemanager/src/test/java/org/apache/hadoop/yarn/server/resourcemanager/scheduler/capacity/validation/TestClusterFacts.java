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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueKind;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolutionInputs.DynamicQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ClusterFacts#capture} copies the queues, labels and resources of a
 * running scheduler.
 */
public class TestClusterFacts {
  private static final String P = CapacitySchedulerConfiguration.PREFIX;

  @Test
  public void testCaptureFromRunningScheduler() throws Exception {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new YarnConfiguration(), false);
    conf.set(P + "root.queues", "a,p");
    conf.set(P + "root.a.capacity", "1w");
    conf.set(P + "root.p.capacity", "1w");
    conf.set(P + "root.a.state", "STOPPED");
    conf.set(P + "root.p.auto-queue-creation-v2.enabled", "true");
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      rm.getRMContext().getNodeLabelManager().addToCluserNodeLabels(
          Arrays.asList(NodeLabel.newInstance("x", false)));
      rm.registerNode("h1:1234", 8 * 1024, 8);
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.p.d"));

      ClusterFacts facts = ClusterFacts.capture(cs);
      assertEquals(Resource.newInstance(8 * 1024, 8),
          facts.getClusterResource());
      assertEquals(Collections.singleton("x"),
          facts.getNodeLabels());
      assertEquals(Resource.newInstance(8 * 1024, 8),
          facts.getResourceByLabel(""));
      assertEquals(Arrays.asList("root", "root.a", "root.p", "root.p.d"),
          new ArrayList<>(facts.getQueues().keySet()));
      assertEquals(QueueKind.PARENT, facts.getQueue("root").getKind());
      assertEquals(QueueState.STOPPED, facts.getQueue("root.a").getState());
      assertFalse(facts.getQueue("root.a").isDynamic());
      ClusterFacts.QueueFacts dynamic = facts.getQueue("root.p.d");
      assertEquals(QueueKind.LEAF, dynamic.getKind());
      assertTrue(dynamic.isDynamic());
      assertEquals(0, dynamic.getRunningApplications());
      assertEquals(cs.getQueue("root.p.d").getMaximumAllocation(),
          dynamic.getMaximumAllocation());
      assertFalse(facts.isHierarchyChecksSkipped());

      // The dynamic queue is resolved with its template, so the unchanged
      // configuration validates cleanly
      assertTrue(new CSConfigValidator().validate(
          ConfigSnapshot.of(cs.getConfiguration()), facts)
          .getIssues().isEmpty());
    } finally {
      rm.stop();
    }
  }

  /**
   * Validation classifies the dynamic queues of the facts like the scheduler
   * classifies its live queues: AQC v1 leaves, AQC v2 queues, and no
   * ReservationQueue.
   */
  @Test
  public void testDynamicQueueClassificationMatchesScheduler()
      throws Exception {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(new YarnConfiguration(), false);
    conf.set(P + "root.queues", "m,p,plan");
    conf.set(P + "root.m.capacity", "30");
    conf.set(P + "root.p.capacity", "30");
    conf.set(P + "root.plan.capacity", "40");
    conf.set(P + "root.m.auto-create-child-queue.enabled", "true");
    conf.set(P + "root.m.leaf-queue-template.capacity", "10");
    conf.set(P + "root.p.auto-queue-creation-v2.enabled", "true");
    conf.set(P + "root.plan.reservable", "true");
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.m.u1"));
      cs.getCapacitySchedulerQueueManager().createQueue(
          new QueuePath("root.p.d"));
      assertTrue(cs.getQueue("root.m.u1") instanceof AutoCreatedLeafQueue);
      assertTrue(cs.getQueue("root.plan.plan-default")
          instanceof ReservationQueue);

      List<String> live = new ArrayList<>();
      for (CSQueue queue : cs.getCapacitySchedulerQueueManager().getQueues()
          .values()) {
        DynamicQueue dynamic = DynamicQueue.of(queue.getQueuePathObject(),
            queue instanceof AutoCreatedLeafQueue, queue.isDynamicQueue(),
            queue instanceof AbstractLeafQueue);
        if (dynamic != null) {
          live.add(describe(dynamic));
        }
      }
      List<String> validated = new ArrayList<>();
      for (DynamicQueue dynamic
          : ValidationContext.dynamicQueues(ClusterFacts.capture(cs))) {
        validated.add(describe(dynamic));
      }
      Collections.sort(live);
      Collections.sort(validated);
      assertEquals(Arrays.asList("root.m.u1 v1 leaf", "root.p.d v2 leaf"),
          validated);
      assertEquals(live, validated);
    } finally {
      rm.stop();
    }
  }

  private static String describe(DynamicQueue queue) {
    return queue.getPath().getFullPath()
        + (queue.isLegacyAutoCreated() ? " v1" : " v2")
        + (queue.isLeaf() ? " leaf" : " parent");
  }
}
