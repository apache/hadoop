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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestCapacitySchedulerNodeLabelUpdate {
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
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "z", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("x", "y", "z"));
    conf.setCapacityByLabel(A, "x", 100);
    conf.setCapacityByLabel(A, "y", 100);
    conf.setCapacityByLabel(A, "z", 100);
    
    return conf;
  }

  private Configuration getConfigurationWithSubQueueLabels(
      Configuration config) {
    CapacitySchedulerConfiguration conf2 =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf2.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});
    conf2.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);
    conf2.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "y", 100);

    final String a = CapacitySchedulerConfiguration.ROOT + ".a";
    final String b = CapacitySchedulerConfiguration.ROOT + ".b";
    final String aa1 = a + ".a1";
    final String aa2 = a + ".a2";
    final String aa3 = a + ".a3";
    final String aa4 = a + ".a4";
    conf2.setQueues(a, new String[] {"a1", "a2", "a3", "a4"});
    conf2.setCapacity(a, 50);
    conf2.setCapacity(b, 50);
    conf2.setCapacity(aa1, 40);
    conf2.setCapacity(aa2, 20);
    conf2.setCapacity(aa3, 20);
    conf2.setCapacity(aa4, 20);
    conf2.setAccessibleNodeLabels(a, ImmutableSet.of("x", "y", "z"));
    conf2.setAccessibleNodeLabels(aa1, ImmutableSet.of("x", "y"));
    conf2.setAccessibleNodeLabels(aa2, ImmutableSet.of("y"));
    conf2.setAccessibleNodeLabels(aa3, ImmutableSet.of("x", "y", "z"));
    conf2.setAccessibleNodeLabels(aa4, ImmutableSet.of("x", "y"));
    conf2.setCapacityByLabel(a, "x", 50);
    conf2.setCapacityByLabel(a, "y", 50);
    conf2.setCapacityByLabel(a, "z", 50);
    conf2.setCapacityByLabel(b, "x", 50);
    conf2.setCapacityByLabel(b, "y", 50);
    conf2.setCapacityByLabel(b, "z", 50);
    conf2.setCapacityByLabel(aa1, "x", 50);
    conf2.setCapacityByLabel(aa3, "x", 25);
    conf2.setCapacityByLabel(aa4, "x", 25);
    conf2.setCapacityByLabel(aa1, "y", 25);
    conf2.setCapacityByLabel(aa2, "y", 25);
    conf2.setCapacityByLabel(aa4, "y", 50);
    conf2.setCapacityByLabel(aa3, "z", 50);
    conf2.setCapacityByLabel(aa4, "z", 50);
    return conf2;
  }

  private Set<String> toSet(String... elements) {
    Set<String> set = Sets.newHashSet(elements);
    return set;
  }
  
  private void checkUsedResource(MockRM rm, String queueName, int memory) {
    checkUsedResource(rm, queueName, memory, RMNodeLabelsManager.NO_LABEL);
  }

  private void checkAMUsedResource(MockRM rm, String queueName, int memory) {
    checkAMUsedResource(rm, queueName, memory, RMNodeLabelsManager.NO_LABEL);
  }

  private void checkUsedCapacity(MockRM rm, String queueName, int capacity,
      int total) {
    checkUsedCapacity(rm, queueName, capacity, total,
        RMNodeLabelsManager.NO_LABEL);
  }

  private void checkUsedResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = scheduler.getQueue(queueName);
    Assert.assertEquals(memory, queue.getQueueResourceUsage().getUsed(label)
        .getMemorySize());
  }

  private void checkUsedCapacity(MockRM rm, String queueName, int capacity,
      int total, String label) {
    float epsillon = 0.0001f;
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = scheduler.getQueue(queueName);
    Assert.assertEquals((float)capacity/total,
        queue.getQueueCapacities().getUsedCapacity(label), epsillon);
  }

  private void checkAMUsedResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = scheduler.getQueue(queueName);
    Assert.assertEquals(memory, queue.getQueueResourceUsage().getAMUsed(label)
        .getMemorySize());
  }

  private void checkUserUsedResource(MockRM rm, String queueName,
      String userName, String partition, int memory) {
    CapacityScheduler scheduler = (CapacityScheduler) rm.getResourceScheduler();
    LeafQueue queue = (LeafQueue) scheduler.getQueue(queueName);
    UsersManager.User user = queue.getUser(userName);
    Assert.assertEquals(memory,
        user.getResourceUsage().getUsed(partition).getMemorySize());
  }

  @Test(timeout = 60000)
  public void testRequestContainerAfterNodePartitionUpdated()
      throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y",
        "z"));

    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 2048);
    MockNM nm2 = rm.registerNode("h2:1234", 2048);
    MockNM nm3 = rm.registerNode("h3:1234", 2048);

    ContainerId containerId;
    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm3);
    ApplicationResourceUsageReport appResourceUsageReport =
        rm.getResourceScheduler().getAppResourceUsageReport(
            am1.getApplicationAttemptId());
    Assert.assertEquals(1024, appResourceUsageReport.getUsedResources()
        .getMemorySize());
    Assert.assertEquals(1, appResourceUsageReport.getUsedResources()
        .getVirtualCores());
    // request a container.
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    containerId = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm.waitForState(nm1, containerId, RMContainerState.ALLOCATED);
    appResourceUsageReport =
        rm.getResourceScheduler().getAppResourceUsageReport(
            am1.getApplicationAttemptId());
    Assert.assertEquals(2048, appResourceUsageReport.getUsedResources()
        .getMemorySize());
    Assert.assertEquals(2, appResourceUsageReport.getUsedResources()
        .getVirtualCores());
    LeafQueue queue =
        (LeafQueue) ((CapacityScheduler) rm.getResourceScheduler())
            .getQueue("a");
    ArrayList<UserInfo> users = queue.getUsersManager().getUsersInfo();
    for (UserInfo userInfo : users) {
      if (userInfo.getUsername().equals("user")) {
        ResourceInfo resourcesUsed = userInfo.getResourcesUsed();
        Assert.assertEquals(2048, resourcesUsed.getMemorySize());
        Assert.assertEquals(2, resourcesUsed.getvCores());
      }
    }
    rm.stop();
  }

  @Test
  public void testResourceUsageWhenNodeUpdatesPartition()
      throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    
    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 8000);
    MockNM nm2 = rm.registerNode("h2:1234", 8000);
    MockNM nm3 = rm.registerNode("h3:1234", 8000);

    ContainerId containerId1;
    ContainerId containerId2;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm3);

    // request a container.
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    containerId1 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    containerId2 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));
    
    // check used resource:
    // queue-a used x=1G, ""=1G
    checkUsedResource(rm, "a", 1024, "x");
    checkUsedResource(rm, "a", 1024);
    checkUsedCapacity(rm, "a", 1024, 8000, "x");
    checkUsedCapacity(rm, "a", 1024, 8000);
    
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    FiCaSchedulerApp app = cs.getApplicationAttempt(am1.getApplicationAttemptId());
    
    // change h1's label to z
    mgr.replaceLabelsOnNode(ImmutableMap.of(nm1.getNodeId(), toSet("z")));
    cs.handle(new NodeLabelsUpdateSchedulerEvent(ImmutableMap.of(nm1.getNodeId(),
        toSet("z"))));
    Thread.sleep(100);
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 1024, "z");
    checkUsedResource(rm, "a", 1024);
    checkUsedCapacity(rm, "a", 0, 8000, "x");
    checkUsedCapacity(rm, "a", 1024, 8000, "z");
    checkUsedCapacity(rm, "a", 1024, 8000);
    checkUsedResource(rm, "root", 0, "x");
    checkUsedResource(rm, "root", 1024, "z");
    checkUsedResource(rm, "root", 1024);
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "z", 1024);
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("x").getMemorySize());
    Assert.assertEquals(1024,
        app.getAppAttemptResourceUsage().getUsed("z").getMemorySize());
    
    // change h1's label to y
    mgr.replaceLabelsOnNode(ImmutableMap.of(nm1.getNodeId(), toSet("y")));
    cs.handle(new NodeLabelsUpdateSchedulerEvent(ImmutableMap.of(nm1.getNodeId(),
        toSet("y"))));
    Thread.sleep(100);
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 1024, "y");
    checkUsedResource(rm, "a", 0, "z");
    checkUsedResource(rm, "a", 1024);
    checkUsedCapacity(rm, "a", 0, 8000, "x");
    checkUsedCapacity(rm, "a", 1024, 16000, "y");
    checkUsedCapacity(rm, "a", 0, 8000, "z");
    checkUsedCapacity(rm, "a", 1024, 8000);
    checkUsedResource(rm, "root", 0, "x");
    checkUsedResource(rm, "root", 1024, "y");
    checkUsedResource(rm, "root", 0, "z");
    checkUsedResource(rm, "root", 1024);
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "y", 1024);
    checkUserUsedResource(rm, "a", "user", "z", 0);
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("x").getMemorySize());
    Assert.assertEquals(1024,
        app.getAppAttemptResourceUsage().getUsed("y").getMemorySize());
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("z").getMemorySize());
    
    // change h1's label to no label
    Set<String> emptyLabels = new HashSet<>();
    Map<NodeId,Set<String>> map = ImmutableMap.of(nm1.getNodeId(),
        emptyLabels);
    mgr.replaceLabelsOnNode(map);
    cs.handle(new NodeLabelsUpdateSchedulerEvent(map));
    Thread.sleep(100);
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 0, "y");
    checkUsedResource(rm, "a", 0, "z");
    checkUsedResource(rm, "a", 2048);
    checkUsedCapacity(rm, "a", 0, 8000, "x");
    checkUsedCapacity(rm, "a", 0, 8000, "y");
    checkUsedCapacity(rm, "a", 0, 8000, "z");
    checkUsedCapacity(rm, "a", 2048, 16000);
    checkUsedResource(rm, "root", 0, "x");
    checkUsedResource(rm, "root", 0, "y");
    checkUsedResource(rm, "root", 0, "z");
    checkUsedResource(rm, "root", 2048);
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "y", 0);
    checkUserUsedResource(rm, "a", "user", "z", 0);
    checkUserUsedResource(rm, "a", "user", "", 2048);
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("x").getMemorySize());
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("y").getMemorySize());
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getUsed("z").getMemorySize());
    Assert.assertEquals(2048,
        app.getAppAttemptResourceUsage().getUsed("").getMemorySize());

    // Finish the two containers, we should see used resource becomes 0
    cs.completedContainer(cs.getRMContainer(containerId2),
        ContainerStatus.newInstance(containerId2, ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL);
    cs.completedContainer(cs.getRMContainer(containerId1),
        ContainerStatus.newInstance(containerId1, ContainerState.COMPLETE, "",
            ContainerExitStatus.KILLED_BY_RESOURCEMANAGER),
        RMContainerEventType.KILL);
    
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 0, "y");
    checkUsedResource(rm, "a", 0, "z");
    checkUsedResource(rm, "a", 0);
    checkUsedCapacity(rm, "a", 0, 8000, "x");
    checkUsedCapacity(rm, "a", 0, 8000, "y");
    checkUsedCapacity(rm, "a", 0, 8000, "z");
    checkUsedCapacity(rm, "a", 0, 16000);
    checkUsedResource(rm, "root", 0, "x");
    checkUsedResource(rm, "root", 0, "y");
    checkUsedResource(rm, "root", 0, "z");
    checkUsedResource(rm, "root", 0);
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "y", 0);
    checkUserUsedResource(rm, "a", "user", "z", 0);
    checkUserUsedResource(rm, "a", "user", "", 0);

    rm.close();
  }

  @Test(timeout = 300000)
  public void testMoveApplicationWithLabel() throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of("x", "y", "z"));
    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));
    mgr.addLabelsToNode(
        ImmutableMap.of(NodeId.newInstance("h4", 0), toSet("z")));
    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithSubQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 4096 * 2);
    MockNM nm2 = rm.registerNode("h2:1234", 4096 * 2);
    MockNM nm3 = rm.registerNode("h3:1234", 4096 * 2);
    MockNM nm4 = rm.registerNode("h4:1234", 4096 * 2);
    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a1");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm3);
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    ContainerId container1 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    rm.waitForState(nm1, container1, RMContainerState.ALLOCATED, 10 * 1000);
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "y");
    ContainerId container2 =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    rm.waitForState(nm2, container2, RMContainerState.ALLOCATED, 10 * 1000);
    CapacityScheduler scheduler =
        ((CapacityScheduler) rm.getResourceScheduler());
    try {
      scheduler.preValidateMoveApplication(app1.getApplicationId(), "a2");
      scheduler.moveApplication(app1.getApplicationId(), "a2");
      fail("Should throw exception since target queue doesnt have "
          + "required labels");
    } catch (Exception e) {
      Assert.assertTrue("Yarn Exception should be thrown",
          e instanceof YarnException);
      Assert.assertEquals("Specified queue=a2 can't satisfy "
          + "following apps label expressions =[x] accessible "
          + "node labels =[y]", e.getMessage());
    }
    try {
      scheduler.moveApplication(app1.getApplicationId(), "a3");
      scheduler.moveApplication(app1.getApplicationId(), "a4");
      // Check move to queue with accessible label ANY
      scheduler.moveApplication(app1.getApplicationId(), "b");
    } catch (Exception e) {
      fail("Should not throw exception since target queue has "
          + "required labels");
    }
    rm.stop();
  }

  @Test (timeout = 60000)
  public void testComplexResourceUsageWhenNodeUpdatesPartition()
      throws Exception {
    /*
     * This test is similar to testResourceUsageWhenNodeUpdatesPartition, this
     * will include multiple applications, multiple users and multiple
     * containers running on a single node, size of each container is 1G
     *
     * Node 1
     * ------
     * App1-container3
     * App2-container2
     * App2-Container3
     *
     * Node 2
     * ------
     * App2-container1
     * App1-container1
     * App1-container2
     */
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    
    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 80000);
    MockNM nm2 = rm.registerNode("h2:1234", 80000);

    // app1
    RMApp app1 = rm.submitApp(GB, "app", "u1", null, "a");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    // c2 on n1, c3 on n2
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    ContainerId containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>());
    containerId =
        ContainerId.newContainerId(am1.getApplicationAttemptId(), 3);
    Assert.assertTrue(rm.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    
    // app2
    RMApp app2 = rm.submitApp(GB, "app", "u2", null, "a");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

    // c2/c3 on n1
    am2.allocate("*", GB, 2, new ArrayList<ContainerId>(), "x");
    containerId =
        ContainerId.newContainerId(am2.getApplicationAttemptId(), 3);
    Assert.assertTrue(rm.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    
    // check used resource:
    // queue-a used x=1G, ""=1G
    checkUsedResource(rm, "a", 3 * GB, "x");
    checkUsedResource(rm, "a", 3 * GB);

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    FiCaSchedulerApp application1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp application2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    // change h1's label to z
    cs.handle(new NodeLabelsUpdateSchedulerEvent(ImmutableMap.of(nm1.getNodeId(),
        toSet("z"))));
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 3 * GB, "z");
    checkUsedResource(rm, "a", 3 * GB);
    checkUsedResource(rm, "root", 0, "x");
    checkUsedResource(rm, "root", 3 * GB, "z");
    checkUsedResource(rm, "root", 3 * GB);
    checkUserUsedResource(rm, "a", "u1", "x", 0 * GB);
    checkUserUsedResource(rm, "a", "u1", "z", 1 * GB);
    checkUserUsedResource(rm, "a", "u1", "", 2 * GB);
    checkUserUsedResource(rm, "a", "u2", "x", 0 * GB);
    checkUserUsedResource(rm, "a", "u2", "z", 2 * GB);
    checkUserUsedResource(rm, "a", "u2", "", 1 * GB);
    Assert.assertEquals(0,
        application1.getAppAttemptResourceUsage().getUsed("x").getMemorySize());
    Assert.assertEquals(1 * GB,
        application1.getAppAttemptResourceUsage().getUsed("z").getMemorySize());
    Assert.assertEquals(2 * GB,
        application1.getAppAttemptResourceUsage().getUsed("").getMemorySize());
    Assert.assertEquals(0,
        application2.getAppAttemptResourceUsage().getUsed("x").getMemorySize());
    Assert.assertEquals(2 * GB,
        application2.getAppAttemptResourceUsage().getUsed("z").getMemorySize());
    Assert.assertEquals(1 * GB,
        application2.getAppAttemptResourceUsage().getUsed("").getMemorySize());

    rm.close();
  }

  @Test
  public void testAMResourceLimitNodeUpdatePartition() throws Exception {
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 64);
    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    rm.registerNode("h1:1234", 6400);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(
        ImmutableSet.of("x", "y", "z"));

    // .1 percentage of 6400 will be for am
    checkAMResourceLimit(rm, "a", 640, "");
    checkAMResourceLimit(rm, "a", 0, "x");
    checkAMResourceLimit(rm, "a", 0, "y");
    checkAMResourceLimit(rm, "a", 0, "z");

    mgr.replaceLabelsOnNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    rm.drainEvents();

    checkAMResourceLimit(rm, "a", 640, "x");
    checkAMResourceLimit(rm, "a", 0, "y");
    checkAMResourceLimit(rm, "a", 0, "z");
    checkAMResourceLimit(rm, "a", 0, "");

    // Switch
    mgr.replaceLabelsOnNode(
        ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));
    rm.drainEvents();

    checkAMResourceLimit(rm, "a", 0, "x");
    checkAMResourceLimit(rm, "a", 640, "y");
    checkAMResourceLimit(rm, "a", 0, "z");
    checkAMResourceLimit(rm, "a", 0, "");
  }

  @Test(timeout = 60000)
  public void testAMResourceUsageWhenNodeUpdatesPartition()
      throws Exception {
    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));

    // set mapping:
    // h1 -> x
    // h2 -> y
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("y")));

    // inject node label manager
    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };

    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 8000);
    rm.registerNode("h2:1234", 8000);
    rm.registerNode("h3:1234", 8000);

    ContainerId containerId2;

    // launch an app to queue a1 (label = x), and check all container will
    // be allocated in h1
    RMApp app1 = rm.submitApp(GB, "app", "user", null, "a", "x");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

    // request a container.
    am1.allocate("*", GB, 1, new ArrayList<ContainerId>(), "x");
    ContainerId.newContainerId(am1.getApplicationAttemptId(), 1);
    containerId2 = ContainerId.newContainerId(am1.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm.waitForState(nm1, containerId2,
        RMContainerState.ALLOCATED));

    // check used resource:
    // queue-a used x=2G
    checkUsedResource(rm, "a", 2048, "x");
    checkAMUsedResource(rm, "a", 1024, "x");

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    FiCaSchedulerApp app = cs.getApplicationAttempt(am1.getApplicationAttemptId());

    // change h1's label to z
    cs.handle(new NodeLabelsUpdateSchedulerEvent(
        ImmutableMap.of(nm1.getNodeId(), toSet("z"))));

    // Now the resources also should change from x to z. Verify AM and normal
    // used resource are successfully changed.
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 2048, "z");
    checkAMUsedResource(rm, "a", 0, "x");
    checkAMUsedResource(rm, "a", 1024, "z");
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "z", 2048);
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getAMUsed("x").getMemorySize());
    Assert.assertEquals(1024,
        app.getAppAttemptResourceUsage().getAMUsed("z").getMemorySize());

    // change h1's label to no label
    Set<String> emptyLabels = new HashSet<>();
    Map<NodeId,Set<String>> map = ImmutableMap.of(nm1.getNodeId(),
        emptyLabels);
    cs.handle(new NodeLabelsUpdateSchedulerEvent(map));
    checkUsedResource(rm, "a", 0, "x");
    checkUsedResource(rm, "a", 0, "z");
    checkUsedResource(rm, "a", 2048);
    checkAMUsedResource(rm, "a", 0, "x");
    checkAMUsedResource(rm, "a", 0, "z");
    checkAMUsedResource(rm, "a", 1024);
    checkUserUsedResource(rm, "a", "user", "x", 0);
    checkUserUsedResource(rm, "a", "user", "z", 0);
    checkUserUsedResource(rm, "a", "user", "", 2048);
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getAMUsed("x").getMemorySize());
    Assert.assertEquals(0,
        app.getAppAttemptResourceUsage().getAMUsed("z").getMemorySize());
    Assert.assertEquals(1024,
        app.getAppAttemptResourceUsage().getAMUsed("").getMemorySize());

    rm.close();
  }

  @Test(timeout = 30000)
  public void testBlacklistAMDisableLabel() throws Exception {
    conf.setBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
        true);
    conf.setFloat(
        YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD,
        0.5f);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h2", 0), toSet("x"),
        NodeId.newInstance("h3", 0), toSet("x"), NodeId.newInstance("h6", 0),
        toSet("x")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h4", 0), toSet("y"),
        NodeId.newInstance("h5", 0), toSet("y"), NodeId.newInstance("h7", 0),
        toSet("y")));

    MockRM rm = new MockRM(getConfigurationWithQueueLabels(conf)) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return mgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(mgr);
    rm.start();
    // Nodes in label default h1,h8,h9
    // Nodes in label x h2,h3,h6
    // Nodes in label y h4,h5,h7
    MockNM nm1 = rm.registerNode("h1:1234", 2048);
    MockNM nm2 = rm.registerNode("h2:1234", 2048);
    rm.registerNode("h3:1234", 2048);
    rm.registerNode("h4:1234", 2048);
    rm.registerNode("h5:1234", 2048);
    rm.registerNode("h6:1234", 2048);
    rm.registerNode("h7:1234", 2048);
    rm.registerNode("h8:1234", 2048);
    rm.registerNode("h9:1234", 2048);

    // Submit app with AM container launched on default partition i.e. h1.
    RMApp app = rm.submitApp(GB, "app", "user", null, "a");
    MockRM.launchAndRegisterAM(app, rm, nm1);
    RMAppAttempt appAttempt = app.getCurrentAppAttempt();
    // Add default node blacklist from default
    appAttempt.getAMBlacklistManager().addNode("h1");
    ResourceBlacklistRequest blacklistUpdates =
        appAttempt.getAMBlacklistManager().getBlacklistUpdates();
    Assert.assertEquals(1, blacklistUpdates.getBlacklistAdditions().size());
    Assert.assertEquals(0, blacklistUpdates.getBlacklistRemovals().size());
    // Adding second node from default parition
    appAttempt.getAMBlacklistManager().addNode("h8");
    blacklistUpdates = appAttempt.getAMBlacklistManager().getBlacklistUpdates();
    Assert.assertEquals(0, blacklistUpdates.getBlacklistAdditions().size());
    Assert.assertEquals(2, blacklistUpdates.getBlacklistRemovals().size());

    // Submission in label x
    RMApp applabel = rm.submitApp(GB, "app", "user", null, "a", "x");
    MockRM.launchAndRegisterAM(applabel, rm, nm2);
    RMAppAttempt appAttemptlabelx = applabel.getCurrentAppAttempt();
    appAttemptlabelx.getAMBlacklistManager().addNode("h2");
    ResourceBlacklistRequest blacklistUpdatesOnx =
        appAttemptlabelx.getAMBlacklistManager().getBlacklistUpdates();
    Assert.assertEquals(1, blacklistUpdatesOnx.getBlacklistAdditions().size());
    Assert.assertEquals(0, blacklistUpdatesOnx.getBlacklistRemovals().size());
    // Adding second node from default parition
    appAttemptlabelx.getAMBlacklistManager().addNode("h3");
    blacklistUpdatesOnx =
        appAttempt.getAMBlacklistManager().getBlacklistUpdates();
    Assert.assertEquals(0, blacklistUpdatesOnx.getBlacklistAdditions().size());
    Assert.assertEquals(2, blacklistUpdatesOnx.getBlacklistRemovals().size());

    rm.close();
  }

  private void checkAMResourceLimit(MockRM rm, String queuename, int memory,
      String label) throws InterruptedException {
    Assert.assertEquals(memory,
        waitForResourceUpdate(rm, queuename, memory, label, 3000L));
  }

  private long waitForResourceUpdate(MockRM rm, String queuename, long memory,
      String label, long timeout) throws InterruptedException {
    long start = System.currentTimeMillis();
    long memorySize = 0;
    while (System.currentTimeMillis() - start < timeout) {
      CapacityScheduler scheduler =
          (CapacityScheduler) rm.getResourceScheduler();
      CSQueue queue = scheduler.getQueue(queuename);
      memorySize =
          queue.getQueueResourceUsage().getAMLimit(label).getMemorySize();
      if (memory == memorySize) {
        return memorySize;
      }
      Thread.sleep(100);
    }
    return memorySize;
  }
}
