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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSchedulingRequestContainerAllocation {
  private final int GB = 1024;

  private YarnConfiguration conf;

  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }

  @Test
  public void testIntraAppAntiAffinity() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.setBoolean(CapacitySchedulerConfiguration.SCHEDULING_REQUEST_ALLOWED,
        true);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("mapper"), "mapper");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 5 containers allocated (1 AM + 1 node each).
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(5, schedulerApp.getLiveContainers().size());

    // Similarly, app1 asks 10 anti-affinity containers at different priority,
    // it should be satisfied as well.
    // app1 asks for 10 anti-affinity containers for the same app. It should
    // only get 4 containers allocated because we only have 4 nodes.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("reducer"), "reducer");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 9 containers allocated (1 AM + 8 containers).
    Assert.assertEquals(9, schedulerApp.getLiveContainers().size());

    // Test anti-affinity to both of "mapper/reducer", we should only get no
    // container allocated
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(10, Resource.newInstance(2048, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("reducer2"), "mapper");
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 10 containers allocated (1 AM + 9 containers).
    Assert.assertEquals(9, schedulerApp.getLiveContainers().size());

    rm1.close();
  }

  @Test
  public void testIntraAppAntiAffinityWithMultipleTags() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());
    csConf.setBoolean(CapacitySchedulerConfiguration.SCHEDULING_REQUEST_ALLOWED,
        true);

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
        Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
        "tag_1_1", "tag_1_2");

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 3 containers allocated (1 AM + 2 task).
    FiCaSchedulerApp schedulerApp = cs.getApplicationAttempt(
        am1.getApplicationAttemptId());
    Assert.assertEquals(3, schedulerApp.getLiveContainers().size());

    // app1 asks for 1 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2. With allocation_tag = tag_2_1/tag_2_2
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(2), 1L, ImmutableSet.of("tag_2_1", "tag_2_2"),
        "tag_1_1", "tag_1_2");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 4 containers allocated (1 AM + 2 task (first request) +
    // 1 task (2nd request).
    Assert.assertEquals(4, schedulerApp.getLiveContainers().size());

    // app1 asks for 10 anti-affinity containers for the same app. anti-affinity
    // to tag_1_1/tag_1_2/tag_2_1/tag_2_2. With allocation_tag = tag_3
    am1.allocateIntraAppAntiAffinity(
        ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)),
        Priority.newInstance(3), 1L, ImmutableSet.of("tag_3"),
        "tag_1_1", "tag_1_2", "tag_2_1", "tag_2_2");

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        cs.handle(new NodeUpdateSchedulerEvent(rmNodes[j]));
      }
    }

    // App1 should get 1 more containers allocated
    // 1 AM + 2 task (first request) + 1 task (2nd request) +
    // 1 task (3rd request)
    Assert.assertEquals(5, schedulerApp.getLiveContainers().size());

    rm1.close();
  }

  @Test
  public void testSchedulingRequestDisabledByDefault() throws Exception {
    Configuration csConf = TestUtils.getConfigurationWithMultipleQueues(
        new Configuration());

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();

    // 4 NMs.
    MockNM[] nms = new MockNM[4];
    RMNode[] rmNodes = new RMNode[4];
    for (int i = 0; i < 4; i++) {
      nms[i] = rm1.registerNode("192.168.0." + i + ":1234", 10 * GB);
      rmNodes[i] = rm1.getRMContext().getRMNodes().get(nms[i].getNodeId());
    }

    // app1 -> c
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nms[0]);

    // app1 asks for 2 anti-affinity containers for the same app.
    boolean caughtException = false;
    try {
      // Since feature is disabled by default, we should expect exception.
      am1.allocateIntraAppAntiAffinity(
          ResourceSizing.newInstance(2, Resource.newInstance(1024, 1)),
          Priority.newInstance(1), 1L, ImmutableSet.of("tag_1_1", "tag_1_2"),
          "tag_1_1", "tag_1_2");
    } catch (Exception e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
    rm1.close();
  }
}
