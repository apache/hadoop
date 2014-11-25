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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestFifoScheduler {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);
  
  private final int GB = 1024;
  private static YarnConfiguration conf;

  @BeforeClass
  public static void setup() {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, 
        FifoScheduler.class, ResourceScheduler.class);
  }

  @Test (timeout = 30000)
  public void testConfValidation() throws Exception {
    FifoScheduler scheduler = new FifoScheduler();
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 2048);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 1024);
    try {
      scheduler.serviceInit(conf);
      fail("Exception is expected because the min memory allocation is" +
        " larger than the max memory allocation.");
    } catch (YarnRuntimeException e) {
      // Exception is expected.
      assertTrue("The thrown exception is not the expected one.",
        e.getMessage().startsWith(
          "Invalid resource scheduler memory"));
    }
  }
  
  @Test
  public void testAllocateContainerOnNodeWithoutOffSwitchSpecified()
      throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // add request for containers
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    requests.add(am1.createResourceReq("127.0.0.1", 1 * GB, 1, 1));
    requests.add(am1.createResourceReq("/default-rack", 1 * GB, 1, 1));
    am1.allocate(requests, null); // send the request

    try {
      // kick the schedule
      nm1.nodeHeartbeat(true);
    } catch (NullPointerException e) {
      Assert.fail("NPE when allocating container on node but "
          + "forget to set off-switch request should be handled");
    }
  }

  @Test
  public void test() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("127.0.0.2:5678", 4 * GB);

    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 4GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());

    RMApp app2 = rm.submitApp(2048);
    // kick the scheduling, 2GB given to AM, remaining 2 GB on nm2
    nm2.nodeHeartbeat(true);
    RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
    MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();
    SchedulerNodeReport report_nm2 = rm.getResourceScheduler().getNodeReport(
        nm2.getNodeId());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request
    // add request for containers
    am2.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 3 * GB, 0, 1);
    AllocateResponse alloc2Response = am2.schedule(); // send the request

    // kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      alloc1Response = am1.schedule();
    }
    while (alloc2Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(1000);
      alloc2Response = am2.schedule();
    }
    // kick the scheduler, nothing given remaining 2 GB.
    nm2.nodeHeartbeat(true);

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    List<Container> allocated2 = alloc2Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated2.size());
    Assert.assertEquals(3 * GB, allocated2.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated2.get(0).getNodeId());
    
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    report_nm2 = rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getAvailableResource().getMemory());

    Assert.assertEquals(6 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm2.getUsedResource().getMemory());

    Container c1 = allocated1.get(0);
    Assert.assertEquals(GB, c1.getResource().getMemory());
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0);
    nm1.containerStatus(containerStatus);
    int waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1
        && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(1000);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses().size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, report_nm1.getUsedResource().getMemory());

    rm.stop();
  }

  @Test
  public void testNodeUpdateBeforeAppAttemptInit() throws Exception {
    FifoScheduler scheduler = new FifoScheduler();
    MockRM rm = new MockRM(conf);
    scheduler.setRMContext(rm.getRMContext());
    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rm.getRMContext());

    RMNode node = MockNodes.newNodeInfo(1,
            Resources.createResource(1024, 4), 1, "127.0.0.1");
    scheduler.handle(new NodeAddedSchedulerEvent(node));

    ApplicationId appId = ApplicationId.newInstance(0, 1);
    scheduler.addApplication(appId, "queue1", "user1", false);

    NodeUpdateSchedulerEvent updateEvent = new NodeUpdateSchedulerEvent(node);
    try {
      scheduler.handle(updateEvent);
    } catch (NullPointerException e) {
        Assert.fail();
    }

    ApplicationAttemptId attId = ApplicationAttemptId.newInstance(appId, 1);
    scheduler.addApplicationAttempt(attId, false, false);

    rm.stop();
  }

  private void testMinimumAllocation(YarnConfiguration conf, int testAlloc)
      throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = rm.submitApp(testAlloc);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());

    int checkAlloc =
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    Assert.assertEquals(checkAlloc, report_nm1.getUsedResource().getMemory());

    rm.stop();
  }

  @Test
  public void testDefaultMinimumAllocation() throws Exception {
    // Test with something lesser than default
    testMinimumAllocation(
        new YarnConfiguration(TestFifoScheduler.conf),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB / 2);
  }

  @Test
  public void testNonDefaultMinimumAllocation() throws Exception {
    // Set custom min-alloc to test tweaking it
    int allocMB = 1536;
    YarnConfiguration conf = new YarnConfiguration(TestFifoScheduler.conf);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, allocMB);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        allocMB * 10);
    // Test for something lesser than this.
    testMinimumAllocation(conf, allocMB / 2);
  }

  @Test (timeout = 50000)
  public void testReconnectedNode() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setQueues("default", new String[] {"default"});
    conf.setCapacity("default", 100);
    FifoScheduler fs = new FifoScheduler();
    fs.init(conf);
    fs.start();
    // mock rmContext to avoid NPE.
    RMContext context = mock(RMContext.class);
    fs.reinitialize(conf, null);
    fs.setRMContext(context);

    RMNode n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, "127.0.0.2");
    RMNode n2 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2, "127.0.0.3");

    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n2));
    fs.handle(new NodeUpdateSchedulerEvent(n1));
    Assert.assertEquals(6 * GB, fs.getRootQueueMetrics().getAvailableMB());

    // reconnect n1 with downgraded memory
    n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1, "127.0.0.2");
    fs.handle(new NodeRemovedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeUpdateSchedulerEvent(n1));

    Assert.assertEquals(4 * GB, fs.getRootQueueMetrics().getAvailableMB());
    fs.stop();
  }
  
  @Test (timeout = 50000)
  public void testBlackListNodes() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    int rack_num_0 = 0;
    int rack_num_1 = 1;
    // Add 4 nodes in 2 racks
    
    // host_0_0 in rack0
    String host_0_0 = "127.0.0.1";
    RMNode n1 =
        MockNodes.newNodeInfo(rack_num_0, MockNodes.newResource(4 * GB), 1, host_0_0);
    fs.handle(new NodeAddedSchedulerEvent(n1));
    
    // host_0_1 in rack0
    String host_0_1 = "127.0.0.2";
    RMNode n2 =
        MockNodes.newNodeInfo(rack_num_0, MockNodes.newResource(4 * GB), 1, host_0_1);
    fs.handle(new NodeAddedSchedulerEvent(n2));
    
    // host_1_0 in rack1
    String host_1_0 = "127.0.0.3";
    RMNode n3 =
        MockNodes.newNodeInfo(rack_num_1, MockNodes.newResource(4 * GB), 1, host_1_0);
    fs.handle(new NodeAddedSchedulerEvent(n3));
    
    // host_1_1 in rack1
    String host_1_1 = "127.0.0.4";
    RMNode n4 =
        MockNodes.newNodeInfo(rack_num_1, MockNodes.newResource(4 * GB), 1, host_1_1);
    fs.handle(new NodeAddedSchedulerEvent(n4));
    
    // Add one application
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);
    SchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appId1, "queue", "user");
    fs.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    fs.handle(attemptEvent);

    List<ContainerId> emptyId = new ArrayList<ContainerId>();
    List<ResourceRequest> emptyAsk = new ArrayList<ResourceRequest>();

    // Allow rack-locality for rack_1, but blacklist host_1_0
    
    // Set up resource requests
    // Ask for a 1 GB container for app 1
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        "rack1", BuilderUtils.newResource(GB, 1), 1));
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask1, emptyId, Collections.singletonList(host_1_0), null);
    
    // Trigger container assignment
    fs.handle(new NodeUpdateSchedulerEvent(n3));
    
    // Get the allocation for the application and verify no allocation on blacklist node
    Allocation allocation1 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    
    Assert.assertEquals("allocation1", 0, allocation1.getContainers().size());

    // verify host_1_1 can get allocated as not in blacklist
    fs.handle(new NodeUpdateSchedulerEvent(n4));
    Allocation allocation2 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("allocation2", 1, allocation2.getContainers().size());
    List<Container> containerList = allocation2.getContainers();
    for (Container container : containerList) {
      Assert.assertEquals("Container is allocated on n4",
          container.getNodeId(), n4.getNodeID());
    }
    
    // Ask for a 1 GB container again for app 1
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    // this time, rack0 is also in blacklist, so only host_1_1 is available to
    // be assigned
    ask2.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask2, emptyId, Collections.singletonList("rack0"), null);
    
    // verify n1 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n1));
    Allocation allocation3 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("allocation3", 0, allocation3.getContainers().size());
    
    // verify n2 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n2));
    Allocation allocation4 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("allocation4", 0, allocation4.getContainers().size());
    
    // verify n3 is not qualified to be allocated
    fs.handle(new NodeUpdateSchedulerEvent(n3));
    Allocation allocation5 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("allocation5", 0, allocation5.getContainers().size());
    
    fs.handle(new NodeUpdateSchedulerEvent(n4));
    Allocation allocation6 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("allocation6", 1, allocation6.getContainers().size());
    
    containerList = allocation6.getContainers();
    for (Container container : containerList) {
      Assert.assertEquals("Container is allocated on n4", 
          container.getNodeId(), n4.getNodeID());
    }

    rm.stop();
  }
  
  @Test (timeout = 50000)
  public void testHeadroom() throws Exception {
    
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    // Add a node
    RMNode n1 =
        MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1, "127.0.0.2");
    fs.handle(new NodeAddedSchedulerEvent(n1));
    
    // Add two applications
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);
    SchedulerEvent appEvent =
        new AppAddedSchedulerEvent(appId1, "queue", "user");
    fs.handle(appEvent);
    SchedulerEvent attemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId1, false);
    fs.handle(attemptEvent);

    ApplicationId appId2 = BuilderUtils.newApplicationId(200, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);
    SchedulerEvent appEvent2 =
        new AppAddedSchedulerEvent(appId2, "queue", "user");
    fs.handle(appEvent2);
    SchedulerEvent attemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, false);
    fs.handle(attemptEvent2);

    List<ContainerId> emptyId = new ArrayList<ContainerId>();
    List<ResourceRequest> emptyAsk = new ArrayList<ResourceRequest>();

    // Set up resource requests
    
    // Ask for a 1 GB container for app 1
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask1, emptyId, null, null);

    // Ask for a 2 GB container for app 2
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ask2.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(2 * GB, 1), 1));
    fs.allocate(appAttemptId2, ask2, emptyId, null, null);
    
    // Trigger container assignment
    fs.handle(new NodeUpdateSchedulerEvent(n1));
    
    // Get the allocation for the applications and verify headroom
    Allocation allocation1 = fs.allocate(appAttemptId1, emptyAsk, emptyId, null, null);
    Assert.assertEquals("Allocation headroom", 1 * GB,
        allocation1.getResourceLimit().getMemory());

    Allocation allocation2 = fs.allocate(appAttemptId2, emptyAsk, emptyId, null, null);
    Assert.assertEquals("Allocation headroom", 1 * GB,
        allocation2.getResourceLimit().getMemory());

    rm.stop();
  }
  
  @Test
  public void testResourceOverCommit() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 4 * GB);
    
    RMApp app1 = rm.submitApp(2048);
    // kick the scheduling, 2 GB given to AM1, remaining 2GB on nm1
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    // check node report, 2 GB used and 2 GB available
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(2 * GB, report_nm1.getAvailableResource().getMemory());

    // add request for containers
    am1.addRequests(new String[] { "127.0.0.1", "127.0.0.2" }, 2 * GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler, 2 GB given to AM1, resource remaining 0
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());
    
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    // check node report, 4 GB used and 0 GB available
    Assert.assertEquals(0, report_nm1.getAvailableResource().getMemory());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemory());

    // check container is assigned with 2 GB.
    Container c1 = allocated1.get(0);
    Assert.assertEquals(2 * GB, c1.getResource().getMemory());
    
    // update node resource to 2 GB, so resource is over-consumed.
    Map<NodeId, ResourceOption> nodeResourceMap = 
        new HashMap<NodeId, ResourceOption>();
    nodeResourceMap.put(nm1.getNodeId(), 
        ResourceOption.newInstance(Resource.newInstance(2 * GB, 1), -1));
    UpdateNodeResourceRequest request = 
        UpdateNodeResourceRequest.newInstance(nodeResourceMap);
    AdminService as = rm.adminService;
    as.updateNodeResource(request);
    
    // Now, the used resource is still 4 GB, and available resource is minus value.
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(4 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(-2 * GB, report_nm1.getAvailableResource().getMemory());
    
    // Check container can complete successfully in case of resource over-commitment.
    ContainerStatus containerStatus = BuilderUtils.newContainerStatus(
        c1.getId(), ContainerState.COMPLETE, "", 0);
    nm1.containerStatus(containerStatus);
    int waitCount = 0;
    while (attempt1.getJustFinishedContainers().size() < 1
        && waitCount++ != 20) {
      LOG.info("Waiting for containers to be finished for app 1... Tried "
          + waitCount + " times already..");
      Thread.sleep(100);
    }
    Assert.assertEquals(1, attempt1.getJustFinishedContainers().size());
    Assert.assertEquals(1, am1.schedule().getCompletedContainersStatuses().size());
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(2 * GB, report_nm1.getUsedResource().getMemory());
    // As container return 2 GB back, the available resource becomes 0 again.
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());
    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestFifoScheduler t = new TestFifoScheduler();
    t.test();
    t.testDefaultMinimumAllocation();
    t.testNonDefaultMinimumAllocation();
    t.testReconnectedNode();
  }
}
