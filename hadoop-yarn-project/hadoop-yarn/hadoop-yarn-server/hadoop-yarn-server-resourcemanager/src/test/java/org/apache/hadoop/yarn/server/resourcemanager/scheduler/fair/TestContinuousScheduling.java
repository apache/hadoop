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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@Deprecated
public class TestContinuousScheduling extends FairSchedulerTestBase {
  private ControlledClock mockClock;
  private static int delayThresholdTimeMs = 1000;

  @SuppressWarnings("deprecation")
  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(
        FairSchedulerConfiguration.CONTINUOUS_SCHEDULING_ENABLED, true);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_NODE_MS,
        delayThresholdTimeMs);
    conf.setInt(FairSchedulerConfiguration.LOCALITY_DELAY_RACK_MS,
        delayThresholdTimeMs);
    return conf;
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setup() {
    mockClock = new ControlledClock();
    conf = createConfiguration();
    resourceManager = new MockRM(conf);
    resourceManager.start();

    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
    scheduler.setClock(mockClock);

    assertTrue(scheduler.isContinuousSchedulingEnabled());
    assertEquals(
        FairSchedulerConfiguration.DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS,
        scheduler.getContinuousSchedulingSleepMs());
    assertEquals(mockClock, scheduler.getClock());
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  @Test (timeout = 60000)
  public void testBasic() throws InterruptedException {
    // Add one node
    String host = "127.0.0.1";
    RMNode node1 = MockNodes.newNodeInfo(
        1, Resources.createResource(4096, 4), 1, host);
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeUpdateEvent);

    ApplicationAttemptId appAttemptId =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(appAttemptId);

    scheduler.addApplication(appAttemptId.getApplicationId(), "queue11", "user11", false);
    scheduler.addApplicationAttempt(appAttemptId, false, false);
    List<ResourceRequest> ask = new ArrayList<>();
    ask.add(createResourceRequest(1024, 1, ResourceRequest.ANY, 1, 1, true));
    scheduler.allocate(
        appAttemptId, ask, null, new ArrayList<ContainerId>(),
        null, null, NULL_UPDATE_REQUESTS);
    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);

    triggerSchedulingAttempt();
    checkAppConsumption(app, Resources.createResource(1024, 1));
  }

  @Test (timeout = 10000)
  public void testSortedNodes() throws Exception {
    // Add two nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    // available resource
    Assert.assertEquals(scheduler.getClusterResource().getMemorySize(), 16 * 1024);
    Assert.assertEquals(scheduler.getClusterResource().getVirtualCores(), 16);

    // send application request
    ApplicationAttemptId appAttemptId =
        createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    createMockRMApp(appAttemptId);

    scheduler.addApplication(appAttemptId.getApplicationId(),
        "queue11", "user11", false);
    scheduler.addApplicationAttempt(appAttemptId, false, false);
    List<ResourceRequest> ask = new ArrayList<>();
    ResourceRequest request =
        createResourceRequest(1024, 1, ResourceRequest.ANY, 1, 1, true);
    ask.add(request);
    scheduler.allocate(appAttemptId, ask, null, new ArrayList<ContainerId>(), null, null, NULL_UPDATE_REQUESTS);
    triggerSchedulingAttempt();

    FSAppAttempt app = scheduler.getSchedulerApp(appAttemptId);
    checkAppConsumption(app, Resources.createResource(1024, 1));

    // another request
    request =
        createResourceRequest(1024, 1, ResourceRequest.ANY, 2, 1, true);
    ask.clear();
    ask.add(request);
    scheduler.allocate(appAttemptId, ask, null, new ArrayList<ContainerId>(), null, null, NULL_UPDATE_REQUESTS);
    triggerSchedulingAttempt();

    checkAppConsumption(app, Resources.createResource(2048,2));

    // 2 containers should be assigned to 2 nodes
    Set<NodeId> nodes = new HashSet<NodeId>();
    Iterator<RMContainer> it = app.getLiveContainers().iterator();
    while (it.hasNext()) {
      nodes.add(it.next().getContainer().getNodeId());
    }
    Assert.assertEquals(2, nodes.size());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithNodeRemoved() throws Exception {
    // Disable continuous scheduling, will invoke continuous
    // scheduling once manually
    scheduler = new FairScheduler();
    conf = super.createConfiguration();
    resourceManager = new MockRM(conf);

    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager()
        .rollMasterKey();

    scheduler.setRMContext(resourceManager.getRMContext());
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !scheduler.isContinuousSchedulingEnabled());
    scheduler.init(conf);
    scheduler.start();

    // Add two nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 2,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    Assert.assertEquals("We should have two alive nodes.",
        2, scheduler.getNumClusterNodes());

    // Remove one node
    NodeRemovedSchedulerEvent removeNode1
        = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(removeNode1);
    Assert.assertEquals("We should only have one alive node.",
        1, scheduler.getNumClusterNodes());

    // Invoke the continuous scheduling once
    try {
      scheduler.continuousSchedulingAttempt();
    } catch (Exception e) {
      fail("Exception happened when doing continuous scheduling. " +
          e.toString());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testInterruptedException()
          throws Exception {
    // Disable continuous scheduling, will invoke continuous
    // scheduling once manually
    scheduler = new FairScheduler();
    conf = super.createConfiguration();
    resourceManager = new MockRM(conf);

    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    resourceManager.getRMContext().getStateStore().start();

    // to initialize the master key
    resourceManager.getRMContext().getContainerTokenSecretManager()
        .rollMasterKey();

    scheduler.setRMContext(resourceManager.getRMContext());
    scheduler.init(conf);
    scheduler.start();
    FairScheduler spyScheduler = spy(scheduler);
    Assert.assertTrue("Continuous scheduling should be disabled.",
        !spyScheduler.isContinuousSchedulingEnabled());
    // Add one node
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(8 * 1024, 8), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    spyScheduler.handle(nodeEvent1);
    Assert.assertEquals("We should have one alive node.",
        1, spyScheduler.getNumClusterNodes());
    InterruptedException ie = new InterruptedException();
    doThrow(new YarnRuntimeException(ie)).when(spyScheduler).
        attemptScheduling(isA(FSSchedulerNode.class));
    // Invoke the continuous scheduling once
    try {
      spyScheduler.continuousSchedulingAttempt();
      fail("Expected InterruptedException to stop schedulingThread");
    } catch (InterruptedException e) {
      Assert.assertEquals(ie, e);
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSchedulerThreadLifeCycle() throws InterruptedException {
    scheduler.start();

    Thread schedulingThread = scheduler.schedulingThread;
    assertTrue(schedulingThread.isAlive());
    scheduler.stop();

    int numRetries = 100;
    while (numRetries-- > 0 && schedulingThread.isAlive()) {
      Thread.sleep(50);
    }

    assertNotEquals("The Scheduling thread is still alive", 0, numRetries);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestNodeAvailableResourceComparatorTransitivity() {
    ClusterNodeTracker<FSSchedulerNode> clusterNodeTracker =
        scheduler.getNodeTracker();

    List<RMNode> rmNodes =
        MockNodes.newNodes(2, 4000, Resource.newInstance(4096, 4));
    for (RMNode rmNode : rmNodes) {
      clusterNodeTracker.addNode(new FSSchedulerNode(rmNode, false));
    }

    // To simulate unallocated resource changes
    new Thread() {
      @Override
      public void run() {
        for (int j = 0; j < 100; j++) {
          for (FSSchedulerNode node : clusterNodeTracker.getAllNodes()) {
            int i = ThreadLocalRandom.current().nextInt(-30, 30);
            synchronized (scheduler) {
              node.deductUnallocatedResource(Resource.newInstance(i * 1024, i));
            }
          }
        }
      }
    }.start();

    try {
      scheduler.continuousSchedulingAttempt();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFairSchedulerContinuousSchedulingInitTime() throws Exception {
    scheduler.start();

    int priorityValue;
    Priority priority;
    FSAppAttempt fsAppAttempt;
    ResourceRequest request1;
    ResourceRequest request2;
    ApplicationAttemptId id11;

    priorityValue = 1;
    id11 = createAppAttemptId(1, 1);
    createMockRMApp(id11);
    priority = Priority.newInstance(priorityValue);
    scheduler.addApplication(id11.getApplicationId(), "root.queue1", "user1",
        false);
    scheduler.addApplicationAttempt(id11, false, false);
    fsAppAttempt = scheduler.getApplicationAttempt(id11);

    String hostName = "127.0.0.1";
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(16 * 1024, 16), 1,
        hostName);
    List<ResourceRequest> ask1 = new ArrayList<>();
    request1 =
        createResourceRequest(1024, 8, node1.getRackName(), priorityValue, 1,
        true);
    request2 =
        createResourceRequest(1024, 8, ResourceRequest.ANY, priorityValue, 1,
        true);
    ask1.add(request1);
    ask1.add(request2);
    scheduler.allocate(id11, ask1, null, new ArrayList<ContainerId>(), null, null,
        NULL_UPDATE_REQUESTS);

    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    FSSchedulerNode node = scheduler.getSchedulerNode(node1.getNodeID());
    // Tick the time and let the fsApp startTime different from initScheduler
    // time
    mockClock.tickSec(delayThresholdTimeMs / 1000);
    scheduler.attemptScheduling(node);
    Map<SchedulerRequestKey, Long> lastScheduledContainer =
        fsAppAttempt.getLastScheduledContainer();
    long initSchedulerTime =
        lastScheduledContainer.get(TestUtils.toSchedulerKey(priority));
    assertEquals(delayThresholdTimeMs, initSchedulerTime);
  }

  @SuppressWarnings("deprecation")
  private void triggerSchedulingAttempt() throws InterruptedException {
    Thread.sleep(
        2 * scheduler.getConf().getContinuousSchedulingSleepMs());
  }
}
