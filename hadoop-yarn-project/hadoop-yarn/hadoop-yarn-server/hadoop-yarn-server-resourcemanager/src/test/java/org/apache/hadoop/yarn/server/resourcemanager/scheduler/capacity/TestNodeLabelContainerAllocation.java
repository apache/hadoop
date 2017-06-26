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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestNodeLabelContainerAllocation {
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
  
  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, toSet("x"));
    conf.setCapacityByLabel(A, "x", 100);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    conf.setAccessibleNodeLabels(B, toSet("y", "z"));
    conf.setCapacityByLabel(B, "y", 100);
    conf.setCapacityByLabel(B, "z", 100);

    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);
    conf.setAccessibleNodeLabels(C, RMNodeLabelsManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    conf.setQueues(A, new String[] {"a1"});
    conf.setCapacity(A1, 100);
    conf.setMaximumCapacity(A1, 100);
    conf.setCapacityByLabel(A1, "x", 100);
    
    final String B1 = B + ".b1";
    conf.setQueues(B, new String[] {"b1"});
    conf.setCapacity(B1, 100);
    conf.setMaximumCapacity(B1, 100);
    conf.setCapacityByLabel(B1, "y", 100);
    conf.setCapacityByLabel(B1, "z", 100);

    final String C1 = C + ".c1";
    conf.setQueues(C, new String[] {"c1"});
    conf.setCapacity(C1, 100);
    conf.setMaximumCapacity(C1, 100);
    
    return conf;
  }
  
  private void checkTaskContainersHost(ApplicationAttemptId attemptId,
      ContainerId containerId, ResourceManager rm, String host) {
    YarnScheduler scheduler = rm.getRMContext().getScheduler();
    SchedulerAppReport appReport = scheduler.getSchedulerAppInfo(attemptId);

    Assert.assertTrue(appReport.getLiveContainers().size() > 0);
    for (RMContainer c : appReport.getLiveContainers()) {
      if (c.getContainerId().equals(containerId)) {
        Assert.assertEquals(host, c.getAllocatedNode().getHost());
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  
  @Test (timeout = 300000)
  public void testContainerAllocationWithSingleUserLimits() throws Exception {
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    // A has only 10% of x, so it can only allocate one container in label=empty
    ContainerId containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "");
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
          RMContainerState.ALLOCATED));
    // Cannot allocate 2nd label=empty container
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "");
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
          RMContainerState.ALLOCATED));

    // A has default user limit = 100, so it can use all resource in label = x
    // We can allocate floor(8000 / 1024) = 7 containers
    for (int id = 3; id <= 8; id++) {
      containerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), id);
      am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x");
      Assert.assertTrue(rm1.waitForState(nm1, containerId,
          RMContainerState.ALLOCATED));
    }
    rm1.close();
  }
  
  @Test(timeout = 300000)
  public void testContainerAllocateWithComplexLabels() throws Exception {
    /*
     * Queue structure:
     *                      root (*)
     *                  ________________
     *                 /                \
     *               a x(100%), y(50%)   b y(50%), z(100%)
     *               ________________    ______________
     *              /                   /              \
     *             a1 (x,y)         b1(no)              b2(y,z)
     *               100%                          y = 100%, z = 100%
     *                           
     * Node structure:
     * h1 : x
     * h2 : y
     * h3 : y
     * h4 : z
     * h5 : NO
     * 
     * Total resource:
     * x: 4G
     * y: 6G
     * z: 2G
     * *: 2G
     * 
     * Resource of
     * a1: x=4G, y=3G, NO=0.2G
     * b1: NO=0.9G (max=1G)
     * b2: y=3, z=2G, NO=0.9G (max=1G)
     * 
     * Each node can only allocate two containers
     */

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("y"), NodeId.newInstance("h4", 0),
        toSet("z"), NodeId.newInstance("h5", 0),
        RMNodeLabelsManager.EMPTY_STRING_SET));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getComplexConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 2048);
    MockNM nm2 = rm1.registerNode("h2:1234", 2048);
    MockNM nm3 = rm1.registerNode("h3:1234", 2048);
    MockNM nm4 = rm1.registerNode("h4:1234", 2048);
    MockNM nm5 = rm1.registerNode("h5:1234", 2048);
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(1024, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container (label = y). can be allocated on nm2 
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2L);
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h2");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h5
    RMApp app2 = rm1.submitApp(1024, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm5);

    // request a container for AM, will succeed
    // and now b1's queue capacity will be used, cannot allocate more containers
    // (Maximum capacity reached)
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertFalse(rm1.waitForState(nm5, containerId,
        RMContainerState.ALLOCATED));
    
    // launch an app to queue b2
    RMApp app3 = rm1.submitApp(1024, "app", "user", null, "b2");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm5);

    // request a container. try to allocate on nm1 (label = x) and nm3 (label =
    // y,z). Will successfully allocate on nm3
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");
    
    // try to allocate container (request label = z) on nm4 (label = y,z). 
    // Will successfully allocate on nm4 only.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "z");
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 3L);
    Assert.assertTrue(rm1.waitForState(nm4, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h4");

    rm1.close();
  }

  @Test (timeout = 120000)
  public void testContainerAllocateWithLabels() throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm3);

    // request a container.
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "x");
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm3);

    // request a container.
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>(), "y");
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    // launch an app to queue c1 (label = ""), and check all container will
    // be allocated in h3
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // request a container.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }
  
  @Test (timeout = 120000)
  public void testContainerAllocateWithDefaultQueueLabels() throws Exception {
    // This test is pretty much similar to testContainerAllocateWithLabel.
    // Difference is, this test doesn't specify label expression in ResourceRequest,
    // instead, it uses default queue label expression

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8000); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 8000); // label = y
    MockNM nm3 = rm1.registerNode("h3:1234", 8000); // label = <empty>
    
    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(200, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // request a container.
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am2.getApplicationAttemptId(), containerId, rm1,
        "h2");
    
    // launch an app to queue c1 (label = ""), and check all container will
    // be allocated in h3
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // request a container.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertFalse(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkTaskContainersHost(am3.getApplicationAttemptId(), containerId, rm1,
        "h3");

    rm1.close();
  }

  @Test (timeout = 120000)
  public void testContainerReservationWithLabels() throws Exception {
    // This test is pretty much similar to testContainerAllocateWithLabel.
    // Difference is, this test doesn't specify label expression in
    // ResourceRequest,
    // instead, it uses default queue label expression

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y",
        "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(
        TestUtils.getConfigurationWithDefaultQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = x
    rm1.registerNode("h2:1234", 8 * GB); // label = y
    rm1.registerNode("h3:1234", 8 * GB); // label = x

    ContainerId containerId;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // request a container.
    am1.allocate("*", 4 * GB, 2, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a1");

    // Do node heartbeats 2 times
    // First time will allocate container for app1, second time will reserve
    // container for app1
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    checkTaskContainersHost(am1.getApplicationAttemptId(), containerId, rm1,
        "h1");

    // Check if a 4G container allocated for app1, and 4G is reserved
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(am1
        .getApplicationAttemptId());
    Assert.assertEquals(2, schedulerApp1.getLiveContainers().size());
    Assert.assertTrue(schedulerApp1.getReservedContainers().size() > 0);
    Assert.assertEquals(9 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed("x").getMemorySize());
    Assert.assertEquals(4 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getReserved("x").getMemorySize());
    Assert.assertEquals(4 * GB,
        leafQueue.getQueueResourceUsage().getReserved("x").getMemorySize());

    // Cancel asks of app2 and re-kick RM
    am1.allocate("*", 4 * GB, 0, new ArrayList<ContainerId>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    Assert.assertEquals(5 * GB, cs.getRootQueue().getQueueResourceUsage()
        .getUsed("x").getMemorySize());
    Assert.assertEquals(0, cs.getRootQueue().getQueueResourceUsage()
        .getReserved("x").getMemorySize());
    Assert.assertEquals(0, leafQueue.getQueueResourceUsage().getReserved("x")
        .getMemorySize());
    rm1.close();
  }

  private void checkPendingResource(MockRM rm, int priority,
      ApplicationAttemptId attemptId, int memory) {
    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    FiCaSchedulerApp app = cs.getApplicationAttempt(attemptId);
    PendingAsk ask =
        app.getAppSchedulingInfo().getPendingAsk(
            TestUtils.toSchedulerKey(priority), "*");
    Assert.assertEquals(memory,
        ask.getPerAllocationResource().getMemorySize() * ask
            .getCount());
  }
  
  private void checkLaunchedContainerNumOnNode(MockRM rm, NodeId nodeId,
      int numContainers) {
    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    SchedulerNode node = cs.getSchedulerNode(nodeId);
    Assert.assertEquals(numContainers, node.getNumContainers());
  }

  /**
   * JIRA YARN-4140, In Resource request set node label will be set only on ANY
   * reqest. RACK/NODE local and default requests label expression need to be
   * updated. This testcase is to verify the label expression is getting changed
   * based on ANY requests.
   *
   * @throws Exception
   */
  @Test
  public void testResourceRequestUpdateNodePartitions() throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(NodeLabel.newInstance("x"),
        NodeLabel.newInstance("y", false), NodeLabel.newInstance("z", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));
    // inject node label manager
    MockRM rm1 = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm2 = rm1.registerNode("h2:1234", 40 * GB); // label = y
    // launch an app to queue b1 (label = y), AM container should be launched in
    // nm2
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    // Creating request set when request before ANY is not having label and any
    // is having label
    List<ResourceRequest> resourceRequest = new ArrayList<ResourceRequest>();
    resourceRequest.add(am1.createResourceReq("/default-rack", 1024, 3, 1,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest.add(am1.createResourceReq("*", 1024, 3, 5, "y"));
    resourceRequest.add(am1.createResourceReq("h1:1234", 1024, 3, 2,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest.add(am1.createResourceReq("*", 1024, 2, 3, "y"));
    resourceRequest.add(am1.createResourceReq("h2:1234", 1024, 2, 4, null));
    resourceRequest.add(am1.createResourceReq("*", 1024, 4, 3, null));
    resourceRequest.add(am1.createResourceReq("h2:1234", 1024, 4, 4, null));
    am1.allocate(resourceRequest, new ArrayList<ContainerId>());
    CapacityScheduler cs =
        (CapacityScheduler) rm1.getRMContext().getScheduler();
    FiCaSchedulerApp app =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 2, "y");
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 3, "y");
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 4,
        RMNodeLabelsManager.NO_LABEL);

    // Previous any request was Y trying to update with z and the
    // request before ANY label is null
    List<ResourceRequest> newReq = new ArrayList<ResourceRequest>();
    newReq.add(am1.createResourceReq("h2:1234", 1024, 3, 4, null));
    newReq.add(am1.createResourceReq("*", 1024, 3, 5, "z"));
    newReq.add(am1.createResourceReq("h1:1234", 1024, 3, 4, null));
    newReq.add(am1.createResourceReq("*", 1024, 4, 5, "z"));
    am1.allocate(newReq, new ArrayList<ContainerId>());

    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 3, "z");
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 4, "z");
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 2, "y");

    // Request before ANY and ANY request is set as NULL. Request should be set
    // with Empty Label
    List<ResourceRequest> resourceRequest1 = new ArrayList<ResourceRequest>();
    resourceRequest1.add(am1.createResourceReq("/default-rack", 1024, 3, 1,
        null));
    resourceRequest1.add(am1.createResourceReq("*", 1024, 3, 5, null));
    resourceRequest1.add(am1.createResourceReq("h1:1234", 1024, 3, 2,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest1.add(am1.createResourceReq("/default-rack", 1024, 2, 1,
        null));
    resourceRequest1.add(am1.createResourceReq("*", 1024, 2, 3,
        RMNodeLabelsManager.NO_LABEL));
    resourceRequest1.add(am1.createResourceReq("h2:1234", 1024, 2, 4, null));
    am1.allocate(resourceRequest1, new ArrayList<ContainerId>());

    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 3,
        RMNodeLabelsManager.NO_LABEL);
    checkNodePartitionOfRequestedPriority(app.getAppSchedulingInfo(), 2,
        RMNodeLabelsManager.NO_LABEL);
  }

  private void checkNodePartitionOfRequestedPriority(AppSchedulingInfo info,
      int priority, String expectedPartition) {
    for (SchedulerRequestKey key : info.getSchedulerKeys()) {
      if (key.getPriority().getPriority() == priority) {
        Assert.assertEquals("Expected partition is " + expectedPartition,
            expectedPartition,
            info.getSchedulingPlacementSet(key)
                .getPrimaryRequestedNodePartition());
      }
    }
  }

  @Test
  public void testPreferenceOfNeedyAppsTowardsNodePartitions() throws Exception {
    /**
     * Test case: Submit two application to a queue (app1 first then app2), app1
     * asked for no-label, app2 asked for label=x, when node1 has label=x
     * doing heart beat, app2 will get allocation first, even if app2 submits later
     * than app1
     */
    
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x"), NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>

    // launch an app to queue b1 (label = y), AM container should be launched in nm2
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    // launch another app to queue b1 (label = y), AM container should be launched in nm2
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // request container and nm1 do heartbeat (nm2 has label=y), note that app1
    // request non-labeled container, and app2 request labeled container, app2
    // will get allocated first even if app1 submitted first.  
    am1.allocate("*", 1 * GB, 8, new ArrayList<ContainerId>());
    am2.allocate("*", 1 * GB, 8, new ArrayList<ContainerId>(), "y");
    
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    
    // Do node heartbeats many times
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
    }
    
    // App2 will get preference to be allocated on node1, and node1 will be all
    // used by App2.
    FiCaSchedulerApp schedulerApp1 = cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 = cs.getApplicationAttempt(am2.getApplicationAttemptId());
    // app1 get nothing in nm1 (partition=y)
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(), schedulerApp1);
    checkNumOfContainersInAnAppOnGivenNode(9, nm2.getNodeId(), schedulerApp1);
    // app2 get all resource in nm1 (partition=y)
    checkNumOfContainersInAnAppOnGivenNode(8, nm1.getNodeId(), schedulerApp2);
    checkNumOfContainersInAnAppOnGivenNode(1, nm2.getNodeId(), schedulerApp2);
    
    rm1.close();
  }
  
  private void checkNumOfContainersInAnAppOnGivenNode(int expectedNum,
      NodeId nodeId, FiCaSchedulerApp app) {
    int num = 0;
    for (RMContainer container : app.getLiveContainers()) {
      if (container.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    Assert.assertEquals(expectedNum, num);
  }
  
  @Test
  public void
      testPreferenceOfNeedyPrioritiesUnderSameAppTowardsNodePartitions()
          throws Exception {
    /**
     * Test case: Submit one application, it asks label="" in priority=1 and
     * label="x" in priority=2, when a node with label=x heartbeat, priority=2
     * will get allocation first even if there're pending resource in priority=1
     */
    
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x"), NodeLabel.newInstance("y", false)));
    // Makes y to be non-exclusive node labels
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>

    // launch an app to queue b1 (label = y), AM container should be launched in nm3
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    // request containers from am2, priority=1 asks for "" and priority=2 asks
    // for "y", "y" container should be allocated first
    am1.allocate("*", 1 * GB, 1, 1, new ArrayList<ContainerId>(), "");
    am1.allocate("*", 1 * GB, 1, 2, new ArrayList<ContainerId>(), "y");

    // Do a node heartbeat once
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    cs.handle(new NodeUpdateSchedulerEvent(
        rm1.getRMContext().getRMNodes().get(nm1.getNodeId())));
    
    // Check pending resource for am2, priority=1 doesn't get allocated before
    // priority=2 allocated
    checkPendingResource(rm1, 1, am1.getApplicationAttemptId(), 1 * GB);
    checkPendingResource(rm1, 2, am1.getApplicationAttemptId(), 0 * GB);
    
    rm1.close();
  }
  
  @Test
  public void testNonLabeledResourceRequestGetPreferrenceToNonLabeledNode()
      throws Exception {
    /**
     * Test case: Submit one application, it asks 6 label="" containers, NM1
     * with label=y and NM2 has no label, NM1/NM2 doing heartbeat together. Even
     * if NM1 has idle resource, containers are all allocated to NM2 since
     * non-labeled request should get allocation on non-labeled nodes first.
     */
    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB); // label = y
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>
    
    ContainerId nextContainerId;

    // launch an app to queue b1 (label = y), AM container should be launched in nm3
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    // request containers from am2, priority=1 asks for "" * 6 (id from 4 to 9),
    // nm2/nm3 do
    // heartbeat at the same time, check containers are always allocated to nm3.
    // This is to verify when there's resource available in non-labeled
    // partition, non-labeled resource should allocate to non-labeled partition
    // first.
    am1.allocate("*", 1 * GB, 6, 1, new ArrayList<ContainerId>(), "");
    for (int i = 2; i < 2 + 6; i++) {
      nextContainerId =
          ContainerId.newContainerId(am1.getApplicationAttemptId(), i);
      Assert.assertTrue(rm1.waitForState(Arrays.asList(nm1, nm2),
          nextContainerId, RMContainerState.ALLOCATED));
    }
    // no more container allocated on nm1
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 0);
    // all 7 (1 AM container + 6 task container) containers allocated on nm2
    checkLaunchedContainerNumOnNode(rm1, nm2.getNodeId(), 7);   
    
    rm1.close();
  }

  @Test
  public void testPreferenceOfQueuesTowardsNodePartitions()
      throws Exception {
    /**
     * Test case: have a following queue structure:
     * 
     * <pre>
     *            root
     *         /   |   \
     *        a     b    c
     *       / \   / \  /  \
     *      a1 a2 b1 b2 c1 c2
     *     (x)    (x)   (x)
     * </pre>
     * 
     * Only a1, b1, c1 can access label=x, and their default label=x Each each
     * has one application, asks for 5 containers. NM1 has label=x
     * 
     * NM1/NM2 doing heartbeat for 15 times, it should allocate all 15
     * containers with label=x
     */
    
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);
    
    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 33);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 33);
    csConf.setQueues(A, new String[] {"a1", "a2"});
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 33);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 33);
    csConf.setQueues(B, new String[] {"b1", "b2"});
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    csConf.setCapacity(C, 34);
    csConf.setAccessibleNodeLabels(C, toSet("x"));
    csConf.setCapacityByLabel(C, "x", 34);
    csConf.setQueues(C, new String[] {"c1", "c2"});
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    csConf.setCapacity(A1, 50);
    csConf.setCapacityByLabel(A1, "x", 100);
    csConf.setDefaultNodeLabelExpression(A1, "x");
    
    final String A2 = A + ".a2";
    csConf.setCapacity(A2, 50);
    csConf.setCapacityByLabel(A2, "x", 0);
    
    final String B1 = B + ".b1";
    csConf.setCapacity(B1, 50);
    csConf.setCapacityByLabel(B1, "x", 100);
    csConf.setDefaultNodeLabelExpression(B1, "x");
    
    final String B2 = B + ".b2";
    csConf.setCapacity(B2, 50);
    csConf.setCapacityByLabel(B2, "x", 0);
    
    final String C1 = C + ".c1";
    csConf.setCapacity(C1, 50);
    csConf.setCapacityByLabel(C1, "x", 100);
    csConf.setDefaultNodeLabelExpression(C1, "x");
    
    final String C2 = C + ".c2";
    csConf.setCapacity(C2, 50);
    csConf.setCapacityByLabel(C2, "x", 0);
    
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 20 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 100 * GB); // label = <empty>

    // app1 -> a1
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);
    
    // app2 -> a2
    RMApp app2 = rm1.submitApp(1 * GB, "app", "user", null, "a2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);
    
    // app3 -> b1
    RMApp app3 = rm1.submitApp(1 * GB, "app", "user", null, "b1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm1);
    
    // app4 -> b2
    RMApp app4 = rm1.submitApp(1 * GB, "app", "user", null, "b2");
    MockAM am4 = MockRM.launchAndRegisterAM(app4, rm1, nm2);
    
    // app5 -> c1
    RMApp app5 = rm1.submitApp(1 * GB, "app", "user", null, "c1");
    MockAM am5 = MockRM.launchAndRegisterAM(app5, rm1, nm1);
    
    // app6 -> b2
    RMApp app6 = rm1.submitApp(1 * GB, "app", "user", null, "c2");
    MockAM am6 = MockRM.launchAndRegisterAM(app6, rm1, nm2);
    
    // Each application request 5 * 1GB container
    am1.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am2.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am3.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am4.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am5.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    am6.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());
    
    // NM1 do 15 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    for (int i = 0; i < 15; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    
    // NM1 get 15 new containers (total is 18, 15 task containers and 3 AM
    // containers)
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 18);

    // Check pending resource each application
    // APP1/APP3/APP5 get satisfied, and APP2/APP2/APP3 get nothing.
    checkPendingResource(rm1, 1, am1.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am2.getApplicationAttemptId(), 5 * GB);
    checkPendingResource(rm1, 1, am3.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am4.getApplicationAttemptId(), 5 * GB);
    checkPendingResource(rm1, 1, am5.getApplicationAttemptId(), 0 * GB);
    checkPendingResource(rm1, 1, am6.getApplicationAttemptId(), 5 * GB);

    rm1.close();
  }
  
  @Test
  public void testQueuesWithoutAccessUsingPartitionedNodes() throws Exception {
    /**
     * Test case: have a following queue structure:
     * 
     * <pre>
     *            root
     *         /      \
     *        a        b
     *        (x)
     * </pre>
     * 
     * Only a can access label=x, two nodes in the cluster, n1 has x and n2 has
     * no-label.
     * 
     * When user-limit-factor=5, submit one application in queue b and request
     * for infinite containers should be able to use up all cluster resources.
     */
    
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);
    
    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 50);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 100);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, new HashSet<String>());
    csConf.setUserLimitFactor(B, 5);
    
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = <empty>

    // app1 -> b
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "b");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);
    
    // Each application request 50 * 1GB container
    am1.allocate("*", 1 * GB, 50, new ArrayList<ContainerId>());
    
    // NM1 do 50 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());
    
    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());
    
    // How much cycles we waited to be allocated when available resource only on
    // partitioned node
    int cycleWaited = 0;
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      if (schedulerNode1.getNumContainers() == 0) {
        cycleWaited++;
      }
    }
    // We will will 10 cycles before get allocated on partitioned node
    // NM2 can allocate 10 containers totally, exclude already allocated AM
    // container, we will wait 9 to fulfill non-partitioned node, and need wait
    // one more cycle before allocating to non-partitioned node 
    Assert.assertEquals(10, cycleWaited);
    
    // Both NM1/NM2 launched 10 containers, cluster resource is exhausted
    checkLaunchedContainerNumOnNode(rm1, nm1.getNodeId(), 10);
    checkLaunchedContainerNumOnNode(rm1, nm2.getNodeId(), 10);

    rm1.close();
  }
  
  @Test
  public void testAMContainerAllocationWillAlwaysBeExclusive()
      throws Exception {
    /**
     * Test case: Submit one application without partition, trying to allocate a
     * node has partition=x, it should fail to allocate since AM container will
     * always respect exclusivity for partitions
     */
    
    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(TestUtils.getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    String nodeIdStr = "h1:1234";
    MockNM nm1 = rm1.registerNode(nodeIdStr, 8 * GB); // label = x

    // launch an app to queue b1 (label = y), AM container should be launched in nm3
    RMApp app = rm1.submitApp(1 * GB, "app", "user", null, "b1");
   
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    
    // Heartbeat for many times, app1 should get nothing
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    Assert.assertTrue(
        "Scheduler diagnostics should have reason for not assigning the node",
        app.getDiagnostics().toString().contains(
            CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_IN_IGNORE_EXCLUSIVE_MODE));

    Assert.assertTrue(
        "Scheduler diagnostics should have last processed node information",
        app.getDiagnostics().toString().contains(
            CSAMContainerLaunchDiagnosticsConstants.LAST_NODE_PROCESSED_MSG
                + nodeIdStr + " ( Partition : [x]"));
    Assert.assertEquals(0, cs.getSchedulerNode(nm1.getNodeId())
        .getNumContainers());
    
    rm1.close();
  }

  @Test(timeout=60000)
  public void
      testQueueMaxCapacitiesWillNotBeHonoredWhenNotRespectingExclusivity()
          throws Exception {
    /**
     * Test case: have a following queue structure:
     * 
     * <pre>
     *            root
     *         /      \
     *        a        b
     *        (x)     (x)
     * </pre>
     * 
     * a/b can access x, both of them has max-capacity-on-x = 50
     * 
     * When doing non-exclusive allocation, app in a (or b) can use 100% of x
     * resource.
     */

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 50);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 50);
    csConf.setMaximumCapacityByLabel(A, "x", 50);
    csConf.setUserLimit(A, 200);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 50);
    csConf.setMaximumCapacityByLabel(B, "x", 50);
    csConf.setUserLimit(B, 200);

    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = <empty>

    // app1 -> a
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);

    // app1 asks for 10 partition= containers
    am1.allocate("*", 1 * GB, 10, new ArrayList<ContainerId>());

    // NM1 do 50 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());

    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
    
    // app1 gets all resource in partition=x
    Assert.assertEquals(10, schedulerNode1.getNumContainers());

    // check non-exclusive containers of LeafQueue is correctly updated
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a");
    Assert.assertFalse(leafQueue.getIgnoreExclusivityRMContainers().containsKey(
        "y"));
    Assert.assertEquals(10,
        leafQueue.getIgnoreExclusivityRMContainers().get("x").size());

    // completes all containers of app1, ignoreExclusivityRMContainers should be
    // updated as well.
    cs.handle(new AppAttemptRemovedSchedulerEvent(
        am1.getApplicationAttemptId(), RMAppAttemptState.FINISHED, false));
    Assert.assertFalse(leafQueue.getIgnoreExclusivityRMContainers().containsKey(
        "x"));

    rm1.close();
  }
  
  private void checkQueueUsedCapacity(String queueName, CapacityScheduler cs,
      String nodePartition, float usedCapacity, float absoluteUsedCapacity) {
    float epsilon = 1e-6f;
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertNotNull("Failed to get queue=" + queueName, queue);

    Assert.assertEquals(usedCapacity, queue.getQueueCapacities()
        .getUsedCapacity(nodePartition), epsilon);
    Assert.assertEquals(absoluteUsedCapacity, queue.getQueueCapacities()
        .getAbsoluteUsedCapacity(nodePartition), epsilon);
  }
  
  private void doNMHeartbeat(MockRM rm, NodeId nodeId, int nHeartbeat) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nodeId);
    for (int i = 0; i < nHeartbeat; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }
  }

  private void waitSchedulerNodeJoined(MockRM rm, int expectedNodeNum)
      throws InterruptedException {
    int totalWaitTick = 100; // wait 10 sec at most.
    while (expectedNodeNum > rm.getResourceScheduler().getNumClusterNodes()
        && totalWaitTick > 0) {
      Thread.sleep(100);
      totalWaitTick--;
    }
  }

  private void waitSchedulerNodeHasUpdatedLabels(CapacityScheduler cs,
      MockNM nm, String partition) throws InterruptedException {
    FiCaSchedulerNode node = cs.getNode(nm.getNodeId());
    int totalWaitTick = 20; // wait 2 sec at most.
    while (!node.getLabels().contains(partition)
        && totalWaitTick > 0) {
      Thread.sleep(100);
      totalWaitTick--;
    }
  }

  @Test
  public void testQueueUsedCapacitiesUpdate()
          throws Exception {
    /**
     * Test case: have a following queue structure:
     * 
     * <pre>
     *            root
     *         /      \
     *        a        b
     *       / \      (x)
     *      a1  a2
     *     (x)  (x)
     * </pre>
     * 
     * Both a/b can access x, we need to verify when
     * <pre>
     * 1) container allocated/released in both partitioned/non-partitioned node, 
     * 2) clusterResource updates
     * 3) queue guaranteed resource changed
     * </pre>
     * 
     * used capacity / absolute used capacity of queues are correctly updated.
     */

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    /**
     * Initially, we set A/B's resource 50:50
     */
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 50);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 50);
    
    csConf.setQueues(A, new String[] { "a1", "a2" });
    
    final String A1 = A + ".a1";
    csConf.setCapacity(A1, 50);
    csConf.setAccessibleNodeLabels(A1, toSet("x"));
    csConf.setCapacityByLabel(A1, "x", 50);
    
    final String A2 = A + ".a2";
    csConf.setCapacity(A2, 50);
    csConf.setAccessibleNodeLabels(A2, toSet("x"));
    csConf.setCapacityByLabel(A2, "x", 50);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 50);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 50);

    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    /*
     * Before we adding any node to the cluster, used-capacity/abs-used-capacity
     * should be 0
     */
    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0f, 0f);
    
    MockNM nm1 = rm.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm.registerNode("h2:1234", 10 * GB); // label = <empty>
    
    /*
     * After we adding nodes to the cluster, and before starting to use them,
     * used-capacity/abs-used-capacity should be 0
     */
    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0f, 0f);

    // app1 -> a1
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // app1 asks for 1 partition= containers
    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>());
    
    doNMHeartbeat(rm, nm2.getNodeId(), 10);
    
    // Now check usage, app1 uses:
    //   a1: used(no-label) = 80%
    //       abs-used(no-label) = 20%
    //   a: used(no-label) = 40%
    //       abs-used(no-label) = 20%
    //   root: used(no-label) = 20%
    //       abs-used(no-label) = 20%
    checkQueueUsedCapacity("a", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a", cs, "", 0.4f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "", 0.2f, 0.2f);
    
    // app1 asks for 2 partition=x containers
    am1.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    
    // Now check usage, app1 uses:
    //   a1: used(x) = 80%
    //       abs-used(x) = 20%
    //   a: used(x) = 40%
    //       abs-used(x) = 20%
    //   root: used(x) = 20%
    //       abs-used(x) = 20%
    checkQueueUsedCapacity("a", cs, "x", 0.4f, 0.2f);
    checkQueueUsedCapacity("a", cs, "", 0.4f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "x", 0.8f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("a2", cs, "", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.2f, 0.2f);
    checkQueueUsedCapacity("root", cs, "", 0.2f, 0.2f);
    
    // submit an app to a2, uses 1 NON_PARTITIONED container and 1 PARTITIONED
    // container
    // app2 -> a2
    RMApp app2 = rm.submitApp(1 * GB, "app", "user", null, "a2");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

    // app1 asks for 1 partition= containers
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    
    // Now check usage, app1 uses:
    //   a2: used(x) = 40%
    //       abs-used(x) = 10%
    //   a: used(x) = 20%
    //       abs-used(x) = 10%
    //   root: used(x) = 10%
    //       abs-used(x) = 10%
    checkQueueUsedCapacity("a", cs, "x", 0.6f, 0.3f);
    checkQueueUsedCapacity("a", cs, "", 0.6f, 0.3f);
    checkQueueUsedCapacity("a1", cs, "x", 0.8f, 0.2f);
    checkQueueUsedCapacity("a1", cs, "", 0.8f, 0.2f);
    checkQueueUsedCapacity("a2", cs, "x", 0.4f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "", 0.4f, 0.1f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.3f, 0.3f);
    checkQueueUsedCapacity("root", cs, "", 0.3f, 0.3f);
    
    // Add nm3/nm4, double resource for both partitioned/non-partitioned
    // resource, used capacity should be 1/2 of before
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h3", 0), toSet("x")));
    MockNM nm3 = rm.registerNode("h3:1234", 10 * GB); // label = x
    MockNM nm4 = rm.registerNode("h4:1234", 10 * GB); // label = <empty>

    waitSchedulerNodeJoined(rm, 4);
    waitSchedulerNodeHasUpdatedLabels(cs, nm3, "x");
    waitSchedulerNodeHasUpdatedLabels(cs, nm4, "");

    checkQueueUsedCapacity("a", cs, "x", 0.3f, 0.15f);
    checkQueueUsedCapacity("a", cs, "", 0.3f, 0.15f);
    checkQueueUsedCapacity("a1", cs, "x", 0.4f, 0.1f);
    checkQueueUsedCapacity("a1", cs, "", 0.4f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "x", 0.2f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.2f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("root", cs, "", 0.15f, 0.15f);
    
    // Reinitialize queue, makes A's capacity double, and B's capacity to be 0
    csConf.setCapacity(A, 100); // was 50
    csConf.setCapacityByLabel(A, "x", 100); // was 50
    csConf.setCapacity(B, 0); // was 50
    csConf.setCapacityByLabel(B, "x", 0); // was 50
    cs.reinitialize(csConf, rm.getRMContext());
    
    checkQueueUsedCapacity("a", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("a", cs, "", 0.15f, 0.15f);
    checkQueueUsedCapacity("a1", cs, "x", 0.2f, 0.1f);
    checkQueueUsedCapacity("a1", cs, "", 0.2f, 0.1f);
    checkQueueUsedCapacity("a2", cs, "x", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.15f, 0.15f);
    checkQueueUsedCapacity("root", cs, "", 0.15f, 0.15f);
    
    // Release all task containers from a1, check usage
    am1.allocate(null, Arrays.asList(
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2),
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3),
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 4)));
    checkQueueUsedCapacity("a", cs, "x", 0.05f, 0.05f);
    checkQueueUsedCapacity("a", cs, "", 0.10f, 0.10f);
    checkQueueUsedCapacity("a1", cs, "x", 0.0f, 0.0f);
    checkQueueUsedCapacity("a1", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "x", 0.1f, 0.05f);
    checkQueueUsedCapacity("a2", cs, "", 0.1f, 0.05f);
    checkQueueUsedCapacity("b", cs, "x", 0f, 0f);
    checkQueueUsedCapacity("b", cs, "", 0f, 0f);
    checkQueueUsedCapacity("root", cs, "x", 0.05f, 0.05f);
    checkQueueUsedCapacity("root", cs, "", 0.10f, 0.10f);

    rm.close();
  }

  @Test
  public void testOrderOfAllocationOnPartitions()
          throws Exception {
    /**
     * Test case: have a following queue structure:
     * 
     * <pre>
     *                root
     *          ________________
     *         /     |     \    \
     *        a (x)  b (x)  c    d
     * </pre>
     * 
     * Both a/b can access x, we need to verify when
     * <pre>
     * When doing allocation on partitioned nodes,
     *    - Queue has accessibility to the node will go first
     *    - When accessibility is same
     *      - Queue has less used_capacity on given partition will go first
     *      - When used_capacity is same
     *        - Queue has more abs_capacity will go first
     * </pre>
     * 
     * used capacity / absolute used capacity of queues are correctly updated.
     */

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b", "c", "d" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 25);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 30);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 25);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 70);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    csConf.setAccessibleNodeLabels(C, Collections.<String> emptySet());
    csConf.setCapacity(C, 25);
    
    final String D = CapacitySchedulerConfiguration.ROOT + ".d";
    csConf.setAccessibleNodeLabels(D, Collections.<String> emptySet());
    csConf.setCapacity(D, 25);

    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm.registerNode("h2:1234", 10 * GB); // label = <empty>
    
    // app1 -> a
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);
    
    // app2 -> b
    RMApp app2 = rm.submitApp(1 * GB, "app", "user", null, "b");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);
    
    // app3 -> c
    RMApp app3 = rm.submitApp(1 * GB, "app", "user", null, "c");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm2);
    
    // app4 -> d
    RMApp app4 = rm.submitApp(1 * GB, "app", "user", null, "d");
    MockAM am4 = MockRM.launchAndRegisterAM(app4, rm, nm2);

    // Test case 1
    // Both a/b has used_capacity(x) = 0, when doing exclusive allocation, b
    // will go first since b has more capacity(x)
    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    
    // Test case 2
    // Do another allocation, a will go first since it has 0 use_capacity(x) and
    // b has 1/7 used_capacity(x)
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    // Test case 3
    // Just like above, when doing non-exclusive allocation, b will go first as well.
    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    // Test case 4
    // After b allocated, we should be able to allocate non-exlusive container in a
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    
    // Test case 5
    // b/c/d asks non-exclusive container together, b will go first irrelated to
    // used_capacity(x)
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "");
    am3.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "");
    am4.allocate("*", 1 * GB, 2, new ArrayList<ContainerId>(), "");
    doNMHeartbeat(rm, nm1.getNodeId(), 2);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    // Test case 6
    // After b allocated, c will go first by lexicographic order
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(0, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    // Test case 7
    // After c allocated, d will go first because it has less used_capacity(x)
    // than c
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));
    
    // Test case 8
    // After d allocated, c will go first, c/d has same use_capacity(x), so compare c/d's lexicographic order
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am3.getApplicationAttemptId()));
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am4.getApplicationAttemptId()));

  }

  @Test
  public void testOrderOfAllocationOnPartitionsWhenAccessibilityIsAll()
      throws Exception {
    /**
     * Test case: have a following queue structure:
     *
     * <pre>
     *             root
     *          __________
     *         /          \
     *        a (*)      b (x)
     * </pre>
     *
     * Both queues a/b can access x, we need to verify whether * accessibility
     * is considered in ordering of queues
     */

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 25);
    csConf.setAccessibleNodeLabels(A, toSet("*"));
    csConf.setCapacityByLabel(A, "x", 60);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 75);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 40);

    // set node -> label
    mgr.addToCluserNodeLabels(
        ImmutableSet.of(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 = rm.registerNode("h1:1234", 10 * GB); // label = x

    // app1 -> a
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a", "x");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // app2 -> b
    RMApp app2 = rm.submitApp(1 * GB, "app", "user", null, "b", "x");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

    // Both a/b has used_capacity(x) = 0, when doing exclusive allocation, a
    // will go first since a has more capacity(x)
    am1.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    am2.allocate("*", 1 * GB, 1, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 1);
    checkNumOfContainersInAnAppOnGivenNode(2, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));
  }

  @Test
  public void testParentQueueMaxCapsAreRespected() throws Exception {
    /*
     * Queue tree:
     *          Root
     *        /     \
     *       A       B
     *      / \
     *     A1 A2
     *
     * A has 50% capacity and 50% max capacity (of label=x)
     * A1/A2 has 50% capacity and 100% max capacity (of label=x)
     * Cluster has one node (label=x) with resource = 24G.
     * So we can at most use 12G resources under queueA.
     */
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a",
        "b"});
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(A, 10);
    csConf.setAccessibleNodeLabels(A, toSet("x"));
    csConf.setCapacityByLabel(A, "x", 50);
    csConf.setMaximumCapacityByLabel(A, "x", 50);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(B, 90);
    csConf.setAccessibleNodeLabels(B, toSet("x"));
    csConf.setCapacityByLabel(B, "x", 50);
    csConf.setMaximumCapacityByLabel(B, "x", 50);

    // Define 2nd-level queues
    csConf.setQueues(A, new String[] { "a1",
        "a2"});

    final String A1 = A + ".a1";
    csConf.setCapacity(A1, 50);
    csConf.setAccessibleNodeLabels(A1, toSet("x"));
    csConf.setCapacityByLabel(A1, "x", 50);
    csConf.setMaximumCapacityByLabel(A1, "x", 100);
    csConf.setUserLimitFactor(A1, 100.0f);

    final String A2 = A + ".a2";
    csConf.setCapacity(A2, 50);
    csConf.setAccessibleNodeLabels(A2, toSet("x"));
    csConf.setCapacityByLabel(A2, "x", 50);
    csConf.setMaximumCapacityByLabel(A2, "x", 100);
    csConf.setUserLimitFactor(A2, 100.0f);

    // set node -> label
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    MockNM nm1 =
        new MockNM("h1:1234", 24 * GB, rm.getResourceTrackerService());
    nm1.registerNode();

    // Launch app1 in a1, resource usage is 1GB (am) + 4GB * 2 = 9GB
    RMApp app1 = rm.submitApp(1 * GB, "app", "user", null, "a1", "x");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    am1.allocate("*", 4 * GB, 2, new ArrayList<ContainerId>(), "x");
    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    checkNumOfContainersInAnAppOnGivenNode(3, nm1.getNodeId(),
        cs.getApplicationAttempt(am1.getApplicationAttemptId()));

    // Try to launch app2 in a2, asked 2GB, should success
    RMApp app2 = rm.submitApp(2 * GB, "app", "user", null, "a2", "x");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);

    // am2 asks more resources, cannot success because current used = 9G (app1)
    // + 2G (app2) = 11G, and queue's max capacity = 12G
    am2.allocate("*", 2 * GB, 2, new ArrayList<ContainerId>(), "x");

    doNMHeartbeat(rm, nm1.getNodeId(), 10);
    checkNumOfContainersInAnAppOnGivenNode(1, nm1.getNodeId(),
        cs.getApplicationAttempt(am2.getApplicationAttemptId()));
  }

  @Test
  public void testQueueMetricsWithLabels() throws Exception {
    /**
     * Test case: have a following queue structure:
     *
     * <pre>
     *            root
     *         /      \
     *        a        b
     *        (x)     (x)
     * </pre>
     *
     * a/b can access x, both of them has max-capacity-on-x = 50
     *
     * When doing non-exclusive allocation, app in a (or b) can use 100% of x
     * resource.
     */

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String queueA = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(queueA, 25);
    csConf.setAccessibleNodeLabels(queueA, toSet("x"));
    csConf.setCapacityByLabel(queueA, "x", 50);
    csConf.setMaximumCapacityByLabel(queueA, "x", 50);
    final String queueB = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(queueB, 75);
    csConf.setAccessibleNodeLabels(queueB, toSet("x"));
    csConf.setCapacityByLabel(queueB, "x", 50);
    csConf.setMaximumCapacityByLabel(queueB, "x", 50);

    // set node -> label
    mgr.addToCluserNodeLabels(
        ImmutableSet.of(NodeLabel.newInstance("x", false)));
    mgr.addToCluserNodeLabels(
        ImmutableSet.of(NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = y
    // app1 -> a
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a", "x");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // app1 asks for 5 partition=x containers
    am1.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>(), "x");
    // NM1 do 50 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());

    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    // app1 gets all resource in partition=x
    Assert.assertEquals(5, schedulerNode1.getNumContainers());

    SchedulerNodeReport reportNm1 = rm1.getResourceScheduler()
        .getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(5 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm2 = rm1.getResourceScheduler()
        .getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals(10 * GB,
        reportNm2.getAvailableResource().getMemorySize());

    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a");
    assertEquals(5 * GB, leafQueue.getMetrics().getAvailableMB());
    assertEquals(0 * GB, leafQueue.getMetrics().getAllocatedMB());

    // Kill all apps in queue a
    cs.killAllAppsInQueue("a");
    rm1.waitForState(app1.getApplicationId(), RMAppState.KILLED);
    rm1.waitForAppRemovedFromScheduler(app1.getApplicationId());

    assertEquals(0 * GB, leafQueue.getMetrics().getUsedAMResourceMB());
    assertEquals(0, leafQueue.getMetrics().getUsedAMResourceVCores());
    rm1.close();
  }

  @Test
  public void testQueueMetricsWithLabelsOnDefaultLabelNode() throws Exception {
    /**
     * Test case: have a following queue structure:
     *
     * <pre>
     *            root
     *         /      \
     *        a        b
     *        (x)     (x)
     * </pre>
     *
     * a/b can access x, both of them has max-capacity-on-x = 50
     *
     * When doing non-exclusive allocation, app in a (or b) can use 100% of x
     * resource.
     */

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        this.conf);

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "a", "b" });
    csConf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String queueA = CapacitySchedulerConfiguration.ROOT + ".a";
    csConf.setCapacity(queueA, 25);
    csConf.setAccessibleNodeLabels(queueA, toSet("x"));
    csConf.setCapacityByLabel(queueA, "x", 50);
    csConf.setMaximumCapacityByLabel(queueA, "x", 50);
    final String queueB = CapacitySchedulerConfiguration.ROOT + ".b";
    csConf.setCapacity(queueB, 75);
    csConf.setAccessibleNodeLabels(queueB, toSet("x"));
    csConf.setCapacityByLabel(queueB, "x", 50);
    csConf.setMaximumCapacityByLabel(queueB, "x", 50);

    // set node -> label
    mgr.addToCluserNodeLabels(
        ImmutableSet.of(NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm1 = new MockRM(csConf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm1.getRMContext().setNodeLabelManager(mgr);
    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 10 * GB); // label = x
    MockNM nm2 = rm1.registerNode("h2:1234", 10 * GB); // label = <no_label>
    // app1 -> a
    RMApp app1 = rm1.submitApp(1 * GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm2);

    // app1 asks for 3 partition= containers
    am1.allocate("*", 1 * GB, 3, new ArrayList<ContainerId>());

    // NM1 do 50 heartbeats
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());

    SchedulerNode schedulerNode1 = cs.getSchedulerNode(nm1.getNodeId());
    for (int i = 0; i < 50; i++) {
      cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    }

    // app1 gets all resource in partition=x (non-exclusive)
    Assert.assertEquals(3, schedulerNode1.getNumContainers());

    SchedulerNodeReport reportNm1 = rm1.getResourceScheduler()
        .getNodeReport(nm1.getNodeId());
    Assert.assertEquals(3 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(7 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm2 = rm1.getResourceScheduler()
        .getNodeReport(nm2.getNodeId());
    Assert.assertEquals(1 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals(9 * GB,
        reportNm2.getAvailableResource().getMemorySize());

    LeafQueue leafQueue = (LeafQueue) cs.getQueue("a");
    double delta = 0.0001;
    // 3GB is used from label x quota. 1.5 GB is remaining from default label.
    // 2GB is remaining from label x.
    assertEquals(6.5 * GB, leafQueue.getMetrics().getAvailableMB(), delta);
    assertEquals(1 * GB, leafQueue.getMetrics().getAllocatedMB());

    // app1 asks for 1 default partition container
    am1.allocate("*", 1 * GB, 5, new ArrayList<ContainerId>());

    // NM2 do couple of heartbeats
    RMNode rmNode2 = rm1.getRMContext().getRMNodes().get(nm2.getNodeId());

    SchedulerNode schedulerNode2 = cs.getSchedulerNode(nm2.getNodeId());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode2));

    // app1 gets all resource in default partition
    Assert.assertEquals(2, schedulerNode2.getNumContainers());

    // 3GB is used from label x quota. 2GB used from default label.
    // So total 2.5 GB is remaining.
    assertEquals(2.5 * GB, leafQueue.getMetrics().getAvailableMB(), delta);
    assertEquals(2 * GB, leafQueue.getMetrics().getAllocatedMB());

    rm1.close();
  }
}
