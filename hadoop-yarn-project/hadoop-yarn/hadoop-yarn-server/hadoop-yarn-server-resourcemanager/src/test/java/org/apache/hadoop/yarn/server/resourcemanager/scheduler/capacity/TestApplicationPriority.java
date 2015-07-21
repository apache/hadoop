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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationPriority {
  private static final Log LOG = LogFactory
      .getLog(TestApplicationPriority.class);
  private final int GB = 1024;

  private YarnConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
  }

  @Test
  public void testApplicationOrderingWithPriority() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue q = (LeafQueue) cs.getQueue("default");
    Assert.assertNotNull(q);

    String host = "127.0.0.1";
    RMNode node = MockNodes.newNodeInfo(0, MockNodes.newResource(16 * GB), 1,
        host);
    cs.handle(new NodeAddedSchedulerEvent(node));

    // add app 1 start
    ApplicationId appId1 = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId1 = BuilderUtils.newApplicationAttemptId(
        appId1, 1);

    RMAppAttemptMetrics attemptMetric1 = new RMAppAttemptMetrics(appAttemptId1,
        rm.getRMContext());
    RMAppImpl app1 = mock(RMAppImpl.class);
    when(app1.getApplicationId()).thenReturn(appId1);
    RMAppAttemptImpl attempt1 = mock(RMAppAttemptImpl.class);
    when(attempt1.getAppAttemptId()).thenReturn(appAttemptId1);
    when(attempt1.getRMAppAttemptMetrics()).thenReturn(attemptMetric1);
    when(app1.getCurrentAppAttempt()).thenReturn(attempt1);

    rm.getRMContext().getRMApps().put(appId1, app1);

    SchedulerEvent addAppEvent1 = new AppAddedSchedulerEvent(appId1, "default",
        "user", null, Priority.newInstance(5));
    cs.handle(addAppEvent1);
    SchedulerEvent addAttemptEvent1 = new AppAttemptAddedSchedulerEvent(
        appAttemptId1, false);
    cs.handle(addAttemptEvent1);
    // add app1 end

    // add app2 begin
    ApplicationId appId2 = BuilderUtils.newApplicationId(100, 2);
    ApplicationAttemptId appAttemptId2 = BuilderUtils.newApplicationAttemptId(
        appId2, 1);

    RMAppAttemptMetrics attemptMetric2 = new RMAppAttemptMetrics(appAttemptId2,
        rm.getRMContext());
    RMAppImpl app2 = mock(RMAppImpl.class);
    when(app2.getApplicationId()).thenReturn(appId2);
    RMAppAttemptImpl attempt2 = mock(RMAppAttemptImpl.class);
    when(attempt2.getAppAttemptId()).thenReturn(appAttemptId2);
    when(attempt2.getRMAppAttemptMetrics()).thenReturn(attemptMetric2);
    when(app2.getCurrentAppAttempt()).thenReturn(attempt2);

    rm.getRMContext().getRMApps().put(appId2, app2);

    SchedulerEvent addAppEvent2 = new AppAddedSchedulerEvent(appId2, "default",
        "user", null, Priority.newInstance(8));
    cs.handle(addAppEvent2);
    SchedulerEvent addAttemptEvent2 = new AppAttemptAddedSchedulerEvent(
        appAttemptId2, false);
    cs.handle(addAttemptEvent2);
    // add app end

    // Now, the first assignment will be for app2 since app2 is of highest
    // priority
    assertEquals(q.getApplications().size(), 2);
    assertEquals(q.getApplications().iterator().next()
        .getApplicationAttemptId(), appAttemptId2);

    rm.stop();
  }

  @Test
  public void testApplicationPriorityAllocation() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // add request for containers
    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 7);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler, 7 containers will be allocated for App1
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemory());

    // check node report, 15 GB used (1 AM and 7 containers) and 1 GB available
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(15 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(1 * GB, report_nm1.getAvailableResource().getMemory());

    // Submit the second app App2 with priority 8 (Higher than App1)
    Priority appPriority2 = Priority.newInstance(8);
    RMApp app2 = rm.submitApp(1 * GB, appPriority2);

    // kick the scheduler, 1 GB which was free is given to AM of App2
    nm1.nodeHeartbeat(true);
    MockAM am2 = rm.sendAMLaunched(app2.getCurrentAppAttempt()
        .getAppAttemptId());
    am2.registerAppAttempt();

    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    // kill 2 containers to free up some space
    int counter = 0;
    for (Container c : allocated1) {
      if (++counter > 2) {
        break;
      }
      cs.killContainer(schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check node report, 12 GB used and 4 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(12 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(4 * GB, report_nm1.getAvailableResource().getMemory());

    // add request for containers App1
    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 10);
    am1.schedule(); // send the request for App1

    // add request for containers App2
    am2.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 2 * GB, 1, 3);
    AllocateResponse alloc1Response4 = am2.schedule(); // send the request

    // kick the scheduler, since App2 priority is more than App1, it will get
    // remaining cluster space.
    nm1.nodeHeartbeat(true);
    while (alloc1Response4.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 2...");
      Thread.sleep(100);
      alloc1Response4 = am2.schedule();
    }

    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    rm.stop();
  }

  @Test
  public void testPriorityWithPendingApplications() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 8 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    // kick the scheduler, 1 GB given to AM1, remaining 7GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // add request for containers
    am1.addRequests(new String[]{"127.0.0.1", "127.0.0.2"}, 1 * GB, 1, 7);
    AllocateResponse alloc1Response = am1.schedule(); // send the request

    // kick the scheduler, 7 containers will be allocated for App1
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(100);
      alloc1Response = am1.schedule();
    }

    List<Container> allocated1 = alloc1Response.getAllocatedContainers();
    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemory());

    // check node report, 8 GB used (1 AM and 7 containers) and 0 GB available
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(8 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(0 * GB, report_nm1.getAvailableResource().getMemory());

    // Submit the second app App2 with priority 7
    Priority appPriority2 = Priority.newInstance(7);
    RMApp app2 = rm.submitApp(1 * GB, appPriority2);

    // Submit the third app App3 with priority 8
    Priority appPriority3 = Priority.newInstance(8);
    RMApp app3 = rm.submitApp(1 * GB, appPriority3);

    // Submit the second app App4 with priority 6
    Priority appPriority4 = Priority.newInstance(6);
    RMApp app4 = rm.submitApp(1 * GB, appPriority4);

    // Only one app can run as AM resource limit restricts it. Kill app1,
    // If app3 (highest priority among rest) gets active, it indicates that
    // priority is working with pendingApplications.
    rm.killApp(app1.getApplicationId());

    // kick the scheduler, app3 (high among pending) gets free space
    nm1.nodeHeartbeat(true);
    MockAM am3 = rm.sendAMLaunched(app3.getCurrentAppAttempt()
        .getAppAttemptId());
    am3.registerAppAttempt();

    // check node report, 1 GB used and 7 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(1 * GB, report_nm1.getUsedResource().getMemory());
    Assert.assertEquals(7 * GB, report_nm1.getAvailableResource().getMemory());

    rm.stop();
  }

  @Test
  public void testMaxPriorityValidation() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    Priority maxPriority = Priority.newInstance(10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(15);
    rm.registerNode("127.0.0.1:1234", 8 * GB);
    RMApp app1 = rm.submitApp(1 * GB, appPriority1);

    // Application submission should be successful and verify priority
    Assert.assertEquals(app1.getApplicationSubmissionContext().getPriority(),
        maxPriority);
    rm.stop();
  }
}
