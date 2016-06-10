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
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestRMRestart;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestWorkPreservingRMRestartForNodeLabel {
  private Configuration conf;
  private static final int GB = 1024; // 1024 MB
  
  RMNodeLabelsManager mgr;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
      ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
  }
  
  @SuppressWarnings("unchecked")
  private <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  private void checkRMContainerLabelExpression(ContainerId containerId,
      MockRM rm, String labelExpression) {
    RMContainer container =
        rm.getRMContext().getScheduler().getRMContainer(containerId);
    Assert.assertNotNull("Cannot find RMContainer=" + containerId, container);
    Assert.assertEquals(labelExpression,
        container.getNodeLabelExpression());
  }
  
  @SuppressWarnings("rawtypes")
  public static void waitForNumContainersToRecover(int num, MockRM rm,
      ApplicationAttemptId attemptId) throws Exception {
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    SchedulerApplicationAttempt attempt =
        scheduler.getApplicationAttempt(attemptId);
    while (attempt == null) {
      System.out.println("Wait for scheduler attempt " + attemptId
          + " to be created");
      Thread.sleep(200);
      attempt = scheduler.getApplicationAttempt(attemptId);
    }
    while (attempt.getLiveContainers().size() < num) {
      System.out.println("Wait for " + num
          + " containers to recover. currently: "
          + attempt.getLiveContainers().size());
      Thread.sleep(200);
    }
  }
  
  private void checkAppResourceUsage(String partition, ApplicationId appId,
      MockRM rm, int expectedMemUsage) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    FiCaSchedulerApp app =
        cs.getSchedulerApplications().get(appId).getCurrentAppAttempt();
    Assert.assertEquals(expectedMemUsage, app.getAppAttemptResourceUsage()
        .getUsed(partition).getMemorySize());
  }
  
  private void checkQueueResourceUsage(String partition, String queueName, MockRM rm, int expectedMemUsage) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(expectedMemUsage, queue.getQueueResourceUsage()
        .getUsed(partition).getMemorySize());
  }

  @Test
  public void testWorkPreservingRestartForNodeLabel() throws Exception {
    // This test is pretty much similar to testContainerAllocateWithLabel.
    // Difference is, this test doesn't specify label expression in ResourceRequest,
    // instead, it uses default queue label expression

    // set node -> label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);
    
    conf = TestUtils.getConfigurationWithDefaultQueueLabels(conf);
    
    // inject node label manager
    MockRM rm1 =
        new MockRM(conf,
            memStore) {
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
    Assert.assertTrue(rm1.waitForState(nm1, containerId,
        RMContainerState.ALLOCATED));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 1), rm1, "x");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2), rm1, "x");

    // launch an app to queue b1 (label = y), and check all container will
    // be allocated in h2
    RMApp app2 = rm1.submitApp(200, "app", "user", null, "b1");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    // request a container.
    am2.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am2.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm2, containerId,
        RMContainerState.ALLOCATED));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 1), rm1, "y");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 2), rm1, "y");
    
    // launch an app to queue c1 (label = ""), and check all container will
    // be allocated in h3
    RMApp app3 = rm1.submitApp(200, "app", "user", null, "c1");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm1, nm3);

    // request a container.
    am3.allocate("*", 1024, 1, new ArrayList<ContainerId>());
    containerId = ContainerId.newContainerId(am3.getApplicationAttemptId(), 2);
    Assert.assertTrue(rm1.waitForState(nm3, containerId,
        RMContainerState.ALLOCATED));
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 1), rm1, "");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 2), rm1, "");
    
    // Re-start RM
    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));
    MockRM rm2 =
        new MockRM(conf,
            memStore) {
          @Override
          public RMNodeLabelsManager createNodeLabelManager() {
            return mgr;
          }
        };
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());
    nm3.setResourceTrackerService(rm2.getResourceTrackerService());
    
    // recover app
    NMContainerStatus app1c1 =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "x");
    NMContainerStatus app1c2 =
        TestRMRestart.createNMContainerStatus(am1.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "x");
    nm1.registerNode(Arrays.asList(app1c1, app1c2), null);
    waitForNumContainersToRecover(2, rm2, am1.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 1), rm1, "x");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am1.getApplicationAttemptId(), 2), rm1, "x");
    
    NMContainerStatus app2c1 =
        TestRMRestart.createNMContainerStatus(am2.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "y");
    NMContainerStatus app2c2 =
        TestRMRestart.createNMContainerStatus(am2.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "y");
    nm2.registerNode(Arrays.asList(app2c1, app2c2), null);
    waitForNumContainersToRecover(2, rm2, am2.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 1), rm1, "y");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am2.getApplicationAttemptId(), 2), rm1, "y");
    
    NMContainerStatus app3c1 =
        TestRMRestart.createNMContainerStatus(am3.getApplicationAttemptId(), 1,
          ContainerState.RUNNING, "");
    NMContainerStatus app3c2 =
        TestRMRestart.createNMContainerStatus(am3.getApplicationAttemptId(), 2,
          ContainerState.RUNNING, "");
    nm3.registerNode(Arrays.asList(app3c1, app3c2), null);
    waitForNumContainersToRecover(2, rm2, am3.getApplicationAttemptId());
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 1), rm1, "");
    checkRMContainerLabelExpression(ContainerId.newContainerId(
        am3.getApplicationAttemptId(), 2), rm1, "");
    
    // Check recovered resource usage
    checkAppResourceUsage("x", app1.getApplicationId(), rm1, 2 * GB);
    checkAppResourceUsage("y", app2.getApplicationId(), rm1, 2 * GB);
    checkAppResourceUsage("", app3.getApplicationId(), rm1, 2 * GB);
    checkQueueResourceUsage("x", "a1", rm1, 2 * GB);
    checkQueueResourceUsage("y", "b1", rm1, 2 * GB);
    checkQueueResourceUsage("", "c1", rm1, 2 * GB);
    checkQueueResourceUsage("x", "a", rm1, 2 * GB);
    checkQueueResourceUsage("y", "b", rm1, 2 * GB);
    checkQueueResourceUsage("", "c", rm1, 2 * GB);
    checkQueueResourceUsage("x", "root", rm1, 2 * GB);
    checkQueueResourceUsage("y", "root", rm1, 2 * GB);
    checkQueueResourceUsage("", "root", rm1, 2 * GB);


    rm1.close();
    rm2.close();
  }
}