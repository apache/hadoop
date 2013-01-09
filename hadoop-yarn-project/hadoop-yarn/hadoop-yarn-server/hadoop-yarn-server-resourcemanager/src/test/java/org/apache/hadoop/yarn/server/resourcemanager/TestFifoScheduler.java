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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

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
  
  @Test
  public void test() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 6 * GB);
    MockNM nm2 = rm.registerNode("h2:5678", 4 * GB);

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
    am1.addRequests(new String[] { "h1", "h2" }, GB, 1, 1);
    AMResponse am1Response = am1.schedule(); // send the request
    // add request for containers
    am2.addRequests(new String[] { "h1", "h2" }, 3 * GB, 0, 1);
    AMResponse am2Response = am2.schedule(); // send the request

    // kick the scheduler, 1 GB and 3 GB given to AM1 and AM2, remaining 0
    nm1.nodeHeartbeat(true);
    while (am1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(1000);
      am1Response = am1.schedule();
    }
    while (am2Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(1000);
      am2Response = am2.schedule();
    }
    // kick the scheduler, nothing given remaining 2 GB.
    nm2.nodeHeartbeat(true);

    List<Container> allocated1 = am1Response.getAllocatedContainers();
    Assert.assertEquals(1, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());
    Assert.assertEquals(nm1.getNodeId(), allocated1.get(0).getNodeId());

    List<Container> allocated2 = am2Response.getAllocatedContainers();
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
    c1.setState(ContainerState.COMPLETE);
    nm1.containerStatus(c1);
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

  private void testMinimumAllocation(YarnConfiguration conf, int testAlloc)
      throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("h1:1234", 6 * GB);

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
    // Test for something lesser than this.
    testMinimumAllocation(conf, allocMB / 2);
  }

  @Test
  public void testReconnectedNode() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    conf.setQueues("default", new String[] {"default"});
    conf.setCapacity("default", 100);
    FifoScheduler fs = new FifoScheduler();
    fs.reinitialize(conf, null);

    RMNode n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1);
    RMNode n2 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 2);

    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n2));
    List<ContainerStatus> emptyList = new ArrayList<ContainerStatus>();
    fs.handle(new NodeUpdateSchedulerEvent(n1, emptyList, emptyList));
    Assert.assertEquals(6 * GB, fs.getRootQueueMetrics().getAvailableMB());

    // reconnect n1 with downgraded memory
    n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(2 * GB), 1);
    fs.handle(new NodeRemovedSchedulerEvent(n1));
    fs.handle(new NodeAddedSchedulerEvent(n1));
    fs.handle(new NodeUpdateSchedulerEvent(n1, emptyList, emptyList));

    Assert.assertEquals(4 * GB, fs.getRootQueueMetrics().getAvailableMB());
  }
  
  @Test
  public void testHeadroom() throws Exception {
    
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    FifoScheduler fs = (FifoScheduler) rm.getResourceScheduler();

    // Add a node
    RMNode n1 = MockNodes.newNodeInfo(0, MockNodes.newResource(4 * GB), 1);
    fs.handle(new NodeAddedSchedulerEvent(n1));
    
    // Add two applications
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);
    SchedulerEvent event1 = new AppAddedSchedulerEvent(appAttemptId1, "queue",
        "user");
    fs.handle(event1);

    ApplicationId appId2 = BuilderUtils.newApplicationId(200, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);
    SchedulerEvent event2 = new AppAddedSchedulerEvent(appAttemptId2, "queue",
        "user");
    fs.handle(event2);

    List<ContainerStatus> emptyStatus = new ArrayList<ContainerStatus>();
    List<ContainerId> emptyId = new ArrayList<ContainerId>();
    List<ResourceRequest> emptyAsk = new ArrayList<ResourceRequest>();

    // Set up resource requests
    
    // Ask for a 1 GB container for app 1
    List<ResourceRequest> ask1 = new ArrayList<ResourceRequest>();
    ask1.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0), "*",
        BuilderUtils.newResource(GB, 1), 1));
    fs.allocate(appAttemptId1, ask1, emptyId);

    // Ask for a 2 GB container for app 2
    List<ResourceRequest> ask2 = new ArrayList<ResourceRequest>();
    ask2.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0), "*",
        BuilderUtils.newResource(2 * GB, 1), 1));
    fs.allocate(appAttemptId2, ask2, emptyId);
    
    // Trigger container assignment
    fs.handle(new NodeUpdateSchedulerEvent(n1, emptyStatus, emptyStatus));
    
    // Get the allocation for the applications and verify headroom
    Allocation allocation1 = fs.allocate(appAttemptId1, emptyAsk, emptyId);
    Assert.assertEquals("Allocation headroom", 1 * GB,
        allocation1.getResourceLimit().getMemory());

    Allocation allocation2 = fs.allocate(appAttemptId2, emptyAsk, emptyId);
    Assert.assertEquals("Allocation headroom", 1 * GB,
        allocation2.getResourceLimit().getMemory());

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
