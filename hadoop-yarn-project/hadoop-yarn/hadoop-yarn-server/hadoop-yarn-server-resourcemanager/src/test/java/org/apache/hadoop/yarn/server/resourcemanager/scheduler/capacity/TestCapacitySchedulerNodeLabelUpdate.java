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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
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
    LeafQueue.User user = queue.getUser(userName);
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
    ArrayList<UserInfo> users = queue.getUsers();
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

  @Test (timeout = 60000)
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
    cs.handle(new NodeLabelsUpdateSchedulerEvent(ImmutableMap.of(nm1.getNodeId(),
        toSet("z"))));

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
}
