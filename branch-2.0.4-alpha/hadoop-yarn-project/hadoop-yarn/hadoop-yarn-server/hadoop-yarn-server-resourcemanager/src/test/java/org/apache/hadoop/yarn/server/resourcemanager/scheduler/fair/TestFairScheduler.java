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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestFairScheduler {

  private class MockClock implements Clock {
    private long time = 0;
    @Override
    public long getTime() {
      return time;
    }

    public void tick(int seconds) {
      time = time + seconds * 1000;
    }

  }

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp")).getAbsolutePath();

  final static String ALLOC_FILE = new File(TEST_DIR,
      "test-queues").getAbsolutePath();

  private FairScheduler scheduler;
  private ResourceManager resourceManager;
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private int APP_ID = 1; // Incrementing counter for schedling apps
  private int ATTEMPT_ID = 1; // Incrementing counter for scheduling attempts

  // HELPER METHODS
  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    Configuration conf = createConfiguration();
    // All tests assume only one assignment per node update
    conf.set(FairSchedulerConfiguration.ASSIGN_MULTIPLE, "false");
    resourceManager = new ResourceManager();
    resourceManager.init(conf);
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());
  }

  @After
  public void tearDown() {
    scheduler = null;
    resourceManager = null;
  }

  private Configuration createConfiguration() {
    Configuration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FairScheduler.class,
        ResourceScheduler.class);
    return conf;
  }

  private ApplicationAttemptId createAppAttemptId(int appId, int attemptId) {
    ApplicationAttemptId attId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    ApplicationId appIdImpl = recordFactory.newRecordInstance(ApplicationId.class);
    appIdImpl.setId(appId);
    attId.setAttemptId(attemptId);
    attId.setApplicationId(appIdImpl);
    return attId;
  }


  private ResourceRequest createResourceRequest(int memory, String host, int priority, int numContainers) {
    ResourceRequest request = recordFactory.newRecordInstance(ResourceRequest.class);
    request.setCapability(Resources.createResource(memory));
    request.setHostName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  /**
   * Creates a single container priority-1 request and submits to
   * scheduler.
   */
  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId, String userId) {
    return createSchedulingRequest(memory, queueId, userId, 1);
  }

  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId, String userId, int numContainers) {
    return createSchedulingRequest(memory, queueId, userId, numContainers, 1);
  }

  private ApplicationAttemptId createSchedulingRequest(int memory, String queueId, String userId, int numContainers, int priority) {
    ApplicationAttemptId id = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(id, queueId, userId);
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request = createResourceRequest(memory, "*", priority, numContainers);
    ask.add(request);
    scheduler.allocate(id, ask,  new ArrayList<ContainerId>());
    return id;
  }
  
  private void createSchedulingRequestExistingApplication(int memory, int priority, ApplicationAttemptId attId) {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest request = createResourceRequest(memory, "*", priority, 1);
    ask.add(request);
    scheduler.allocate(attId, ask,  new ArrayList<ContainerId>());
  }

  // TESTS

  @Test
  public void testAggregateCapacityTracking() throws Exception {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    assertEquals(1024, scheduler.getClusterCapacity().getMemory());

    // Add another node
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(512));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    assertEquals(1536, scheduler.getClusterCapacity().getMemory());

    // Remove the first node
    NodeRemovedSchedulerEvent nodeEvent3 = new NodeRemovedSchedulerEvent(node1);
    scheduler.handle(nodeEvent3);
    assertEquals(512, scheduler.getClusterCapacity().getMemory());
  }

  @Test
  public void testSimpleFairShareCalculation() {
    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(10 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "queue1", "user1");
    createSchedulingRequest(10 * 1024, "queue2", "user1");

    scheduler.update();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    for (FSLeafQueue p : queues) {
      if (!p.getName().equals("root.default")) {
        assertEquals(5120, p.getFairShare().getMemory());
      }
    }
  }
  
  @Test
  public void testSimpleHierarchicalFairShareCalculation() {
    // Add one big node (only care about aggregate capacity)
    int capacity = 10 * 24;
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(capacity));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Have two queues which want entire cluster capacity
    createSchedulingRequest(10 * 1024, "queue1", "user1");
    createSchedulingRequest(10 * 1024, "parent.queue2", "user1");
    createSchedulingRequest(10 * 1024, "parent.queue3", "user1");

    scheduler.update();

    QueueManager queueManager = scheduler.getQueueManager();
    Collection<FSLeafQueue> queues = queueManager.getLeafQueues();
    assertEquals(4, queues.size());
    
    FSLeafQueue queue1 = queueManager.getLeafQueue("queue1");
    FSLeafQueue queue2 = queueManager.getLeafQueue("parent.queue2");
    FSLeafQueue queue3 = queueManager.getLeafQueue("parent.queue3");
    assertEquals(capacity / 2, queue1.getFairShare().getMemory());
    assertEquals(capacity / 4, queue2.getFairShare().getMemory());
    assertEquals(capacity / 4, queue3.getFairShare().getMemory());
  }
  
  @Test
  public void testHierarchicalQueuesSimilarParents() {
    QueueManager queueManager = scheduler.getQueueManager();
    FSLeafQueue leafQueue = queueManager.getLeafQueue("parent.child");
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    Assert.assertNotNull(leafQueue);
    Assert.assertEquals("root.parent.child", leafQueue.getName());

    FSLeafQueue leafQueue2 = queueManager.getLeafQueue("parent");
    Assert.assertNull(leafQueue2);
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue3 = queueManager.getLeafQueue("parent.child.grandchild");
    Assert.assertNull(leafQueue3);
    Assert.assertEquals(2, queueManager.getLeafQueues().size());
    
    FSLeafQueue leafQueue4 = queueManager.getLeafQueue("parent.sister");
    Assert.assertNotNull(leafQueue4);
    Assert.assertEquals("root.parent.sister", leafQueue4.getName());
    Assert.assertEquals(3, queueManager.getLeafQueues().size());
  }

  @Test (timeout = 5000)
  public void testSimpleContainerAllocation() {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Add another node
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(512));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    createSchedulingRequest(512, "queue1", "user1", 2);

    scheduler.update();

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Asked for less than min_allocation.
    assertEquals(YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);

    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
      getResourceUsage().getMemory());
  }

  @Test (timeout = 5000)
  public void testSimpleContainerReservation() throws InterruptedException {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue 1 requests full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    // Make sure queue 1 is allocated app capacity
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue1").
        getResourceUsage().getMemory());

    // Now queue 2 requests likewise
    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue2", "user1", 1);
    scheduler.update();
    scheduler.handle(updateEvent);

    // Make sure queue 2 is waiting with a reservation
    assertEquals(0, scheduler.getQueueManager().getQueue("queue2").
      getResourceUsage().getMemory());
    assertEquals(1024, scheduler.applications.get(attId).getCurrentReservation().getMemory());

    // Now another node checks in with capacity
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    scheduler.handle(updateEvent2);

    // Make sure this goes to queue 2
    assertEquals(1024, scheduler.getQueueManager().getQueue("queue2").
        getResourceUsage().getMemory());

    // The old reservation should still be there...
    assertEquals(1024, scheduler.applications.get(attId).getCurrentReservation().getMemory());
    // ... but it should disappear when we update the first node.
    scheduler.handle(updateEvent);
    assertEquals(0, scheduler.applications.get(attId).getCurrentReservation().getMemory());

  }

  @Test
  public void testUserAsDefaultQueue() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "true");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(
        createAppAttemptId(1, 1), "default", "user1");
    scheduler.handle(appAddedEvent);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1")
        .getAppSchedulables().size());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("default")
        .getAppSchedulables().size());

    conf.set(FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE, "false");
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    AppAddedSchedulerEvent appAddedEvent2 = new AppAddedSchedulerEvent(
        createAppAttemptId(2, 1), "default", "user2");
    scheduler.handle(appAddedEvent2);
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1")
        .getAppSchedulables().size());
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("default")
        .getAppSchedulables().size());
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user2")
        .getAppSchedulables().size());
  }

  @Test
  public void testFairShareWithMinAlloc() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(3 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    createSchedulingRequest(2 * 1024, "queueA", "user1");
    createSchedulingRequest(2 * 1024, "queueB", "user1");

    scheduler.update();

    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(1024, p.getFairShare().getMemory());
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(2048, p.getFairShare().getMemory());
      }
    }
  }

  /**
   * Make allocation requests and ensure they are reflected in queue demand.
   */
  @Test
  public void testQueueDemandCalculation() throws Exception {
    ApplicationAttemptId id11 = createAppAttemptId(1, 1);
    scheduler.addApplication(id11, "root.queue1", "user1");
    ApplicationAttemptId id21 = createAppAttemptId(2, 1);
    scheduler.addApplication(id21, "root.queue2", "user1");
    ApplicationAttemptId id22 = createAppAttemptId(2, 2);
    scheduler.addApplication(id22, "root.queue2", "user1");

    int minReqSize = YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB;
    
    // First ask, queue1 requests 1 large (minReqSize * 2).
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ResourceRequest request1 = createResourceRequest(minReqSize * 2, "*", 1, 1);
    ask1.add(request1);
    scheduler.allocate(id11, ask1, new ArrayList<ContainerId>());

    // Second ask, queue2 requests 1 large + (2 * minReqSize)
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ResourceRequest request2 = createResourceRequest(2 * minReqSize, "foo", 1, 1);
    ResourceRequest request3 = createResourceRequest(minReqSize, "bar", 1, 2);
    ask2.add(request2);
    ask2.add(request3);
    scheduler.allocate(id21, ask2, new ArrayList<ContainerId>());

    // Third ask, queue2 requests 1 large
    List<ResourceRequest> ask3 = new ArrayList<ResourceRequest>();
    ResourceRequest request4 = createResourceRequest(2 * minReqSize, "*", 1, 1);
    ask3.add(request4);
    scheduler.allocate(id22, ask3, new ArrayList<ContainerId>());

    scheduler.update();

    assertEquals(2 * minReqSize, scheduler.getQueueManager().getQueue("root.queue1")
        .getDemand().getMemory());
    assertEquals(2 * minReqSize + 2 * minReqSize + (2 * minReqSize), scheduler
        .getQueueManager().getQueue("root.queue2").getDemand()
        .getMemory());
  }

  @Test
  public void testAppAdditionAndRemoval() throws Exception {
    AppAddedSchedulerEvent appAddedEvent1 = new AppAddedSchedulerEvent(
        createAppAttemptId(1, 1), "default", "user1");
    scheduler.handle(appAddedEvent1);

    // Scheduler should have two queues (the default and the one created for user1)
    assertEquals(2, scheduler.getQueueManager().getLeafQueues().size());

    // That queue should have one app
    assertEquals(1, scheduler.getQueueManager().getLeafQueue("user1")
        .getAppSchedulables().size());

    AppRemovedSchedulerEvent appRemovedEvent1 = new AppRemovedSchedulerEvent(
        createAppAttemptId(1, 1), RMAppAttemptState.FINISHED);

    // Now remove app
    scheduler.handle(appRemovedEvent1);

    // Queue should have no apps
    assertEquals(0, scheduler.getQueueManager().getLeafQueue("user1")
        .getAppSchedulables().size());
  }

  @Test
  public void testAllocationFileParsing() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give queue A a minimum of 1024 M
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    // Give queue B a minimum of 2048 M
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048</minResources>");
    out.println("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
    out.println("</queue>");
    // Give queue C no minimum
    out.println("<queue name=\"queueC\">");
    out.println("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
    out.println("</queue>");
    // Give queue D a limit of 3 running apps
    out.println("<queue name=\"queueD\">");
    out.println("<maxRunningApps>3</maxRunningApps>");
    out.println("</queue>");
    // Give queue E a preemption timeout of one minute
    out.println("<queue name=\"queueE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</queue>");
    // Set default limit of apps per queue to 15
    out.println("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
    // Set default limit of apps per user to 5
    out.println("<userMaxAppsDefault>5</userMaxAppsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>10</maxRunningApps>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120"
        + "</defaultMinSharePreemptionTimeout>");
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    assertEquals(6, queueManager.getLeafQueues().size()); // 5 in file + default queue
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));

    assertEquals(Resources.createResource(1024),
        queueManager.getMinResources("root.queueA"));
    assertEquals(Resources.createResource(2048),
        queueManager.getMinResources("root.queueB"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueC"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueD"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueE"));

    assertEquals(15, queueManager.getQueueMaxApps("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueA"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueB"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueC"));
    assertEquals(3, queueManager.getQueueMaxApps("root.queueD"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueE"));
    assertEquals(10, queueManager.getUserMaxApps("user1"));
    assertEquals(5, queueManager.getUserMaxApps("user2"));

    // Unspecified queues should get default ACL
    Map<QueueACL, AccessControlList> aclsA = queueManager.getQueueAcls("root.queueA");
    assertTrue(aclsA.containsKey(QueueACL.ADMINISTER_QUEUE));
    assertEquals("*", aclsA.get(QueueACL.ADMINISTER_QUEUE).getAclString());
    assertTrue(aclsA.containsKey(QueueACL.SUBMIT_APPLICATIONS));
    assertEquals("*", aclsA.get(QueueACL.SUBMIT_APPLICATIONS).getAclString());

    // Queue B ACL
    Map<QueueACL, AccessControlList> aclsB = queueManager.getQueueAcls("root.queueB");
    assertTrue(aclsB.containsKey(QueueACL.ADMINISTER_QUEUE));
    assertEquals("alice,bob admins", aclsB.get(QueueACL.ADMINISTER_QUEUE).getAclString());

    // Queue c ACL
    Map<QueueACL, AccessControlList> aclsC = queueManager.getQueueAcls("root.queueC");
    assertTrue(aclsC.containsKey(QueueACL.SUBMIT_APPLICATIONS));
    assertEquals("alice,bob admins", aclsC.get(QueueACL.SUBMIT_APPLICATIONS).getAclString());

    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root." + 
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueB"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueC"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueD"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(60000, queueManager.getMinSharePreemptionTimeout("root.queueE"));
    assertEquals(300000, queueManager.getFairSharePreemptionTimeout());
  }

  @Test
  public void testHierarchicalQueueAllocationFileParsing() throws IOException, SAXException, 
      AllocationConfigurationException, ParserConfigurationException {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048</minResources>");
    out.println("<queue name=\"queueC\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();
    
    Collection<FSLeafQueue> leafQueues = queueManager.getLeafQueues();
    Assert.assertEquals(4, leafQueues.size());
    Assert.assertNotNull(queueManager.getLeafQueue("queueA"));
    Assert.assertNotNull(queueManager.getLeafQueue("queueB.queueC"));
    Assert.assertNotNull(queueManager.getLeafQueue("queueB.queueD"));
    Assert.assertNotNull(queueManager.getLeafQueue("default"));
    // Make sure querying for queues didn't create any new ones:
    Assert.assertEquals(4, leafQueues.size());
  }
  
  @Test
  public void testBackwardsCompatibleAllocationFileParsing() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give queue A a minimum of 1024 M
    out.println("<pool name=\"queueA\">");
    out.println("<minResources>1024</minResources>");
    out.println("</pool>");
    // Give queue B a minimum of 2048 M
    out.println("<pool name=\"queueB\">");
    out.println("<minResources>2048</minResources>");
    out.println("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
    out.println("</pool>");
    // Give queue C no minimum
    out.println("<pool name=\"queueC\">");
    out.println("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
    out.println("</pool>");
    // Give queue D a limit of 3 running apps
    out.println("<pool name=\"queueD\">");
    out.println("<maxRunningApps>3</maxRunningApps>");
    out.println("</pool>");
    // Give queue E a preemption timeout of one minute
    out.println("<pool name=\"queueE\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of apps per queue to 15
    out.println("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
    // Set default limit of apps per user to 5
    out.println("<userMaxAppsDefault>5</userMaxAppsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>10</maxRunningApps>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120"
        + "</defaultMinSharePreemptionTimeout>");
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    assertEquals(6, queueManager.getLeafQueues().size()); // 5 in file + default queue
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));

    assertEquals(Resources.createResource(1024),
        queueManager.getMinResources("root.queueA"));
    assertEquals(Resources.createResource(2048),
        queueManager.getMinResources("root.queueB"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueC"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueD"));
    assertEquals(Resources.createResource(0),
        queueManager.getMinResources("root.queueE"));

    assertEquals(15, queueManager.getQueueMaxApps("root." + YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueA"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueB"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueC"));
    assertEquals(3, queueManager.getQueueMaxApps("root.queueD"));
    assertEquals(15, queueManager.getQueueMaxApps("root.queueE"));
    assertEquals(10, queueManager.getUserMaxApps("user1"));
    assertEquals(5, queueManager.getUserMaxApps("user2"));

    // Unspecified queues should get default ACL
    Map<QueueACL, AccessControlList> aclsA = queueManager.getQueueAcls("queueA");
    assertTrue(aclsA.containsKey(QueueACL.ADMINISTER_QUEUE));
    assertEquals("*", aclsA.get(QueueACL.ADMINISTER_QUEUE).getAclString());
    assertTrue(aclsA.containsKey(QueueACL.SUBMIT_APPLICATIONS));
    assertEquals("*", aclsA.get(QueueACL.SUBMIT_APPLICATIONS).getAclString());

    // Queue B ACL
    Map<QueueACL, AccessControlList> aclsB = queueManager.getQueueAcls("root.queueB");
    assertTrue(aclsB.containsKey(QueueACL.ADMINISTER_QUEUE));
    assertEquals("alice,bob admins", aclsB.get(QueueACL.ADMINISTER_QUEUE).getAclString());

    // Queue c ACL
    Map<QueueACL, AccessControlList> aclsC = queueManager.getQueueAcls("root.queueC");
    assertTrue(aclsC.containsKey(QueueACL.SUBMIT_APPLICATIONS));
    assertEquals("alice,bob admins", aclsC.get(QueueACL.SUBMIT_APPLICATIONS).getAclString());

    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root." +
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueB"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueC"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueD"));
    assertEquals(120000, queueManager.getMinSharePreemptionTimeout("root.queueA"));
    assertEquals(60000, queueManager.getMinSharePreemptionTimeout("root.queueE"));
    assertEquals(300000, queueManager.getFairSharePreemptionTimeout());
  }

  @Test (timeout = 5000)
  public void testIsStarvedForMinShare() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>2048</minResources>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 3 * 1024. Node update gives this all to A
    createSchedulingRequest(3 * 1024, "queueA", "user1");
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    // Queue B arrives and wants 1 * 1024
    createSchedulingRequest(1 * 1024, "queueB", "user1");
    scheduler.update();
    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    // Queue A should be above min share, B below.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(false, scheduler.isStarvedForMinShare(p));
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(true, scheduler.isStarvedForMinShare(p));
      }
    }

    // Node checks in again, should allocate for B
    scheduler.handle(nodeEvent2);
    // Now B should have min share ( = demand here)
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueB")) {
        assertEquals(false, scheduler.isStarvedForMinShare(p));
      }
    }
  }

  @Test (timeout = 5000)
  public void testIsStarvedForFairShare() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.75</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    // Add one big node (only care about aggregate capacity)
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Queue A wants 3 * 1024. Node update gives this all to A
    createSchedulingRequest(3 * 1024, "queueA", "user1");
    scheduler.update();
    NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(nodeEvent2);

    // Queue B arrives and wants 1 * 1024
    createSchedulingRequest(1 * 1024, "queueB", "user1");
    scheduler.update();
    Collection<FSLeafQueue> queues = scheduler.getQueueManager().getLeafQueues();
    assertEquals(3, queues.size());

    // Queue A should be above fair share, B below.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueA")) {
        assertEquals(false, scheduler.isStarvedForFairShare(p));
      }
      else if (p.getName().equals("root.queueB")) {
        assertEquals(true, scheduler.isStarvedForFairShare(p));
      }
    }

    // Node checks in again, should allocate for B
    scheduler.handle(nodeEvent2);
    // B should not be starved for fair share, since entire demand is
    // satisfied.
    for (FSLeafQueue p : queues) {
      if (p.getName().equals("root.queueB")) {
        assertEquals(false, scheduler.isStarvedForFairShare(p));
      }
    }
  }

  @Test (timeout = 5000)
  /**
   * Make sure containers are chosen to be preempted in the correct order. Right
   * now this means decreasing order of priority.
   */
  public void testChoiceOfPreemptedContainers() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE + ".allocation.file", ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    // Create four nodes
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    assertEquals(1, scheduler.applications.get(app1).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app2).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app3).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app4).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app5).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app6).getLiveContainers().size());

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 1);
    ApplicationAttemptId app11 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 2);
    ApplicationAttemptId app12 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 3);

    scheduler.update();

    // We should be able to claw back one container from A and B each.
    // Make sure it is lowest priority container.
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    assertEquals(1, scheduler.applications.get(app1).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app2).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app3).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app4).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app5).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app6).getLiveContainers().size());

    // We should be able to claw back another container from A and B each.
    // Make sure it is lowest priority container.
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    assertEquals(1, scheduler.applications.get(app1).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app2).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app3).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app4).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app5).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app6).getLiveContainers().size());

    // Now A and B are below fair share, so preemption shouldn't do anything
    scheduler.preemptResources(scheduler.getQueueManager().getLeafQueues(),
        Resources.createResource(2 * 1024));
    assertEquals(1, scheduler.applications.get(app1).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app2).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app3).getLiveContainers().size());
    assertEquals(1, scheduler.applications.get(app4).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app5).getLiveContainers().size());
    assertEquals(0, scheduler.applications.get(app6).getLiveContainers().size());
  }

  @Test (timeout = 5000)
  /**
   * Tests the timing of decision to preempt tasks.
   */
  public void testPreemptionDecision() throws Exception {
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    MockClock clock = new MockClock();
    scheduler.setClock(clock);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queueA\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueD\">");
    out.println("<weight>.25</weight>");
    out.println("<minResources>1024</minResources>");
    out.println("</queue>");
    out.print("<defaultMinSharePreemptionTimeout>5</defaultMinSharePreemptionTimeout>");
    out.print("<fairSharePreemptionTimeout>10</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();

    // Create four nodes
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);

    RMNode node3 = MockNodes.newNodeInfo(1, Resources.createResource(2 * 1024));
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);


    // Queue A and B each request three containers
    ApplicationAttemptId app1 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 1);
    ApplicationAttemptId app2 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 2);
    ApplicationAttemptId app3 =
        createSchedulingRequest(1 * 1024, "queueA", "user1", 1, 3);

    ApplicationAttemptId app4 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 1);
    ApplicationAttemptId app5 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 2);
    ApplicationAttemptId app6 =
        createSchedulingRequest(1 * 1024, "queueB", "user1", 1, 3);

    scheduler.update();

    // Sufficient node check-ins to fully schedule containers
    for (int i = 0; i < 2; i++) {
      NodeUpdateSchedulerEvent nodeUpdate1 = new NodeUpdateSchedulerEvent(node1);
      scheduler.handle(nodeUpdate1);

      NodeUpdateSchedulerEvent nodeUpdate2 = new NodeUpdateSchedulerEvent(node2);
      scheduler.handle(nodeUpdate2);

      NodeUpdateSchedulerEvent nodeUpdate3 = new NodeUpdateSchedulerEvent(node3);
      scheduler.handle(nodeUpdate3);
    }

    // Now new requests arrive from queues C and D
    ApplicationAttemptId app7 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 1);
    ApplicationAttemptId app8 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 2);
    ApplicationAttemptId app9 =
        createSchedulingRequest(1 * 1024, "queueC", "user1", 1, 3);

    ApplicationAttemptId app10 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 1);
    ApplicationAttemptId app11 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 2);
    ApplicationAttemptId app12 =
        createSchedulingRequest(1 * 1024, "queueD", "user1", 1, 3);

    scheduler.update();

    FSLeafQueue schedC =
        scheduler.getQueueManager().getLeafQueue("queueC");
    FSLeafQueue schedD =
        scheduler.getQueueManager().getLeafQueue("queueD");

    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(schedC, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.none(), scheduler.resToPreempt(schedD, clock.getTime())));
    // After minSharePreemptionTime has passed, they should want to preempt min
    // share.
    clock.tick(6);
    assertTrue(Resources.equals(
        Resources.createResource(1024), scheduler.resToPreempt(schedC, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.createResource(1024), scheduler.resToPreempt(schedD, clock.getTime())));

    // After fairSharePreemptionTime has passed, they should want to preempt
    // fair share.
    scheduler.update();
    clock.tick(6);
    assertTrue(Resources.equals(
        Resources.createResource(1536), scheduler.resToPreempt(schedC, clock.getTime())));
    assertTrue(Resources.equals(
        Resources.createResource(1536), scheduler.resToPreempt(schedD, clock.getTime())));
  }
  
  @Test (timeout = 5000)
  public void testMultipleContainersWaitingForReservation() {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    // Request full capacity of node
    createSchedulingRequest(1024, "queue1", "user1", 1);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);

    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue2", "user2", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue3", "user3", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // One container should get reservation and the other should get nothing
    assertEquals(1024,
        scheduler.applications.get(attId1).getCurrentReservation().getMemory());
    assertEquals(0,
        scheduler.applications.get(attId2).getCurrentReservation().getMemory());
  }

  @Test (timeout = 5000)
  public void testUserMaxRunningApps() throws Exception {
    // Set max running apps
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningApps>1</maxRunningApps>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();
    
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(8192));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Request for app 1
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    // App 1 should be running
    assertEquals(1, scheduler.applications.get(attId1).getLiveContainers().size());
    
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // App 2 should not be running
    assertEquals(0, scheduler.applications.get(attId2).getLiveContainers().size());
    
    // Request another container for app 1
    createSchedulingRequestExistingApplication(1024, 1, attId1);
    
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // Request should be fulfilled
    assertEquals(2, scheduler.applications.get(attId1).getLiveContainers().size());
  }
  
  @Test (timeout = 5000)
  public void testReservationWhileMultiplePriorities() {
    // Add a node
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);

    ApplicationAttemptId attId = createSchedulingRequest(1024, "queue1",
        "user1", 1, 2);
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent);
    
    FSSchedulerApp app = scheduler.applications.get(attId);
    assertEquals(1, app.getLiveContainers().size());
    
    ContainerId containerId = scheduler.applications.get(attId)
        .getLiveContainers().iterator().next().getContainerId();

    // Cause reservation to be created
    createSchedulingRequestExistingApplication(1024, 2, attId);
    scheduler.update();
    scheduler.handle(updateEvent);

    assertEquals(1, app.getLiveContainers().size());
    
    // Create request at higher priority
    createSchedulingRequestExistingApplication(1024, 1, attId);
    scheduler.update();
    scheduler.handle(updateEvent);
    
    assertEquals(1, app.getLiveContainers().size());
    // Reserved container should still be at lower priority
    for (RMContainer container : app.getReservedContainers()) {
      assertEquals(2, container.getReservedPriority().getPriority());
    }
    
    // Complete container
    scheduler.allocate(attId, new ArrayList<ResourceRequest>(),
        Arrays.asList(containerId));
    
    // Schedule at opening
    scheduler.update();
    scheduler.handle(updateEvent);
    
    // Reserved container (at lower priority) should be run
    Collection<RMContainer> liveContainers = app.getLiveContainers();
    assertEquals(1, liveContainers.size());
    for (RMContainer liveContainer : liveContainers) {
      Assert.assertEquals(2, liveContainer.getContainer().getPriority().getPriority());
    }
  }
  
  @Test
  public void testAclSubmitApplication() throws Exception {
    // Set acl's
    Configuration conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"queue1\">");
    out.println("<aclSubmitApps>norealuserhasthisname</aclSubmitApps>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.initialize();
    
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname", 1);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "norealuserhasthisname2", 1);

    FSSchedulerApp app1 = scheduler.applications.get(attId1);
    assertNotNull("The application was not allowed", app1);
    FSSchedulerApp app2 = scheduler.applications.get(attId2);
    assertNull("The application was allowed", app2);
  }
  
  @Test (timeout = 5000)
  public void testMultipleNodesSingleRackRequest() throws Exception {
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    RMNode node2 = MockNodes.newNodeInfo(1, Resources.createResource(1024));
    RMNode node3 = MockNodes.newNodeInfo(2, Resources.createResource(1024));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    
    ApplicationAttemptId appId = createAppAttemptId(this.APP_ID++, this.ATTEMPT_ID++);
    scheduler.addApplication(appId, "queue1", "user1");
    
    // 1 request with 2 nodes on the same rack. another request with 1 node on
    // a different rack
    List<ResourceRequest> asks = new ArrayList<ResourceRequest>();
    asks.add(createResourceRequest(1024, node1.getHostName(), 1, 1));
    asks.add(createResourceRequest(1024, node2.getHostName(), 1, 1));
    asks.add(createResourceRequest(1024, node3.getHostName(), 1, 1));
    asks.add(createResourceRequest(1024, node1.getRackName(), 1, 1));
    asks.add(createResourceRequest(1024, node3.getRackName(), 1, 1));
    asks.add(createResourceRequest(1024, RMNode.ANY, 1, 2));

    scheduler.allocate(appId, asks, new ArrayList<ContainerId>());
    
    // node 1 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent1 = new NodeUpdateSchedulerEvent(node1);
    scheduler.handle(updateEvent1);
    // should assign node local
    assertEquals(1, scheduler.applications.get(appId).getLiveContainers().size());

    // node 2 checks in
    scheduler.update();
    NodeUpdateSchedulerEvent updateEvent2 = new NodeUpdateSchedulerEvent(node2);
    scheduler.handle(updateEvent2);
    // should assign rack local
    assertEquals(2, scheduler.applications.get(appId).getLiveContainers().size());
  }
  
  @Test (timeout = 5000)
  public void testFifoWithinQueue() throws Exception {
    RMNode node1 = MockNodes.newNodeInfo(1, Resources.createResource(3072));
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    
    // Even if submitted at exact same time, apps will be deterministically
    // ordered by name.
    ApplicationAttemptId attId1 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    ApplicationAttemptId attId2 = createSchedulingRequest(1024, "queue1",
        "user1", 2);
    FSSchedulerApp app1 = scheduler.applications.get(attId1);
    FSSchedulerApp app2 = scheduler.applications.get(attId2);
    
    FSLeafQueue queue1 = scheduler.getQueueManager().getLeafQueue("queue1");
    queue1.setSchedulingMode(SchedulingMode.FIFO);
    
    scheduler.update();

    // First two containers should go to app 1, third should go to app 2.
    // Because tests set assignmultiple to false, each heartbeat assigns a single
    // container.
    
    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node1);

    scheduler.handle(updateEvent);
    assertEquals(1, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(0, app2.getLiveContainers().size());
    
    scheduler.handle(updateEvent);
    assertEquals(2, app1.getLiveContainers().size());
    assertEquals(1, app2.getLiveContainers().size());
  }
}
