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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationPriority {
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
    assertThat(q.getApplications()).hasSize(2);
    assertThat(q.getApplications().iterator().next().getApplicationAttemptId())
        .isEqualTo(appAttemptId2);

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
    MockRMAppSubmissionData data1 = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority1)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // allocate 7 containers for App1
    List<Container> allocated1 = am1.allocateAndWaitForContainers("127.0.0.1",
        7, 2 * GB, nm1);

    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(2 * GB, allocated1.get(0).getResource().getMemorySize());

    // check node report, 15 GB used (1 AM and 7 containers) and 1 GB available
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(15 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(1 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // Submit the second app App2 with priority 8 (Higher than App1)
    Priority appPriority2 = Priority.newInstance(8);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority2)
        .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);

    // kick the scheduler, 1 GB which was free is given to AM of App2
    MockAM am2 = MockRM.launchAM(app2, rm, nm1);
    am2.registerAppAttempt();

    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    // kill 2 containers of App1 to free up some space
    int counter = 0;
    for (Container c : allocated1) {
      if (++counter > 2) {
        break;
      }
      cs.markContainerForKillable(
          schedulerAppAttempt.getRMContainer(c.getId()));
    }

    // check node report, 12 GB used and 4 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(12 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(4 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // send updated request for App1
    am1.allocate("127.0.0.1", 2 * GB, 10, new ArrayList<ContainerId>());

    // kick the scheduler, since App2 priority is more than App1, it will get
    // remaining cluster space.
    List<Container> allocated2 = am2.allocateAndWaitForContainers("127.0.0.1",
        2, 2 * GB, nm1);

    // App2 has got 2 containers now.
    Assert.assertEquals(2, allocated2.size());

    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        report_nm1.getAvailableResource().getMemorySize());

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
    MockRMAppSubmissionData data3 = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority1)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data3);

    // kick the scheduler, 1 GB given to AM1, remaining 7GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // kick the scheduler, 7 containers will be allocated for App1
    List<Container> allocated1 = am1.allocateAndWaitForContainers("127.0.0.1",
        7, 1 * GB, nm1);

    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(1 * GB, allocated1.get(0).getResource().getMemorySize());

    // check node report, 8 GB used (1 AM and 7 containers) and 0 GB available
    SchedulerNodeReport report_nm1 = rm.getResourceScheduler().getNodeReport(
        nm1.getNodeId());
    Assert.assertEquals(8 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // Submit the second app App2 with priority 7
    Priority appPriority2 = Priority.newInstance(7);
    MockRMAppSubmissionData data2 = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm).withAppPriority(appPriority2).build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);

    // Submit the third app App3 with priority 8
    Priority appPriority3 = Priority.newInstance(8);
    MockRMAppSubmissionData data1 = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm).withAppPriority(appPriority3).build();
    RMApp app3 = MockRMAppSubmitter.submit(rm, data1);

    // Submit the second app App4 with priority 6
    Priority appPriority4 = Priority.newInstance(6);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority4)
        .build();
    RMApp app4 = MockRMAppSubmitter.submit(rm, data);

    // Only one app can run as AM resource limit restricts it. Kill app1,
    // If app3 (highest priority among rest) gets active, it indicates that
    // priority is working with pendingApplications.
    rm.killApp(app1.getApplicationId());
    rm.waitForState(am1.getApplicationAttemptId(), RMAppAttemptState.KILLED);

    // kick the scheduler, app3 (high among pending) gets free space
    MockAM am3 = MockRM.launchAM(app3, rm, nm1);
    am3.registerAppAttempt();

    // check node report, 1 GB used and 7 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(1 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(7 * GB,
        report_nm1.getAvailableResource().getMemorySize());

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
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm).withAppPriority(appPriority1).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);

    // Application submission should be successful and verify priority
    Assert.assertEquals(app1.getApplicationSubmissionContext().getPriority(),
        maxPriority);
    rm.stop();
  }

  @Test
  public void testUpdatePriorityAtRuntime() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * GB);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm).withAppPriority(appPriority1).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Change the priority of App1 to 8
    Priority appPriority2 = Priority.newInstance(8);
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(app1.getUser());
    cs.updateApplicationPriority(appPriority2, app1.getApplicationId(), null,
        ugi);

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    // Verify whether the new priority is updated
    Assert.assertEquals(appPriority2, schedulerAppAttempt.getPriority());
  }

  @Test
  public void testUpdateInvalidPriorityAtRuntime() throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * GB);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority1)
        .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Change the priority of App1 to 15
    Priority appPriority2 = Priority.newInstance(15);
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(app1.getUser());
    cs.updateApplicationPriority(appPriority2, app1.getApplicationId(), null,
        ugi);

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttempt = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();

    // Verify whether priority 15 is reset to 10
    Priority appPriority3 = Priority.newInstance(10);
    Assert.assertEquals(appPriority3, schedulerAppAttempt.getPriority());
    rm.stop();
  }

  @Test(timeout = 180000)
  public void testRMRestartWithChangeInPriority() throws Exception {
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
        false);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);

    // PHASE 1: create state in an RM

    // start RM
    MockRM rm1 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();

    MockNM nm1 = new MockNM("127.0.0.1:1234", 15120,
        rm1.getResourceTrackerService());
    nm1.registerNode();

    Priority appPriority1 = Priority.newInstance(5);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm1).withAppPriority(appPriority1).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm1, nm1);
    am1.registerAppAttempt();

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();

    // Change the priority of App1 to 8
    Priority appPriority2 = Priority.newInstance(8);
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(app1.getUser());
    cs.updateApplicationPriority(appPriority2, app1.getApplicationId(), null,
        ugi);

    // let things settle down
    Thread.sleep(1000);

    // create new RM to represent restart and recover state
    MockRM rm2 = new MockRM(conf, memStore);

    // start new RM
    rm2.start();
    // change NM to point to new RM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // Verify RM Apps after this restart
    Assert.assertEquals(1, rm2.getRMContext().getRMApps().size());

    // get scheduler app
    RMApp loadedApp = rm2.getRMContext().getRMApps()
        .get(app1.getApplicationId());

    // Verify whether priority 15 is reset to 10
    Assert.assertEquals(appPriority2, loadedApp.getApplicationPriority());

    rm2.stop();
    rm1.stop();
  }

  @Test
  public void testApplicationPriorityAllocationWithChangeInPriority()
      throws Exception {

    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    // Set Max Application Priority as 10
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);
    MockRM rm = new MockRM(conf);
    rm.start();

    Priority appPriority1 = Priority.newInstance(5);
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 16 * GB);
    MockRMAppSubmissionData data1 = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm).withAppPriority(appPriority1).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);

    // kick the scheduler, 1 GB given to AM1, remaining 15GB on nm1
    MockAM am1 = MockRM.launchAM(app1, rm, nm1);
    am1.registerAppAttempt();

    // add request for containers and wait for containers to be allocated.
    int NUM_CONTAINERS = 7;
    List<Container> allocated1 = am1.allocateAndWaitForContainers("127.0.0.1",
        NUM_CONTAINERS, 2 * GB, nm1);

    Assert.assertEquals(7, allocated1.size());
    Assert.assertEquals(2 * GB,
        allocated1.get(0).getResource().getMemorySize());

    // check node report, 15 GB used (1 AM and 7 containers) and 1 GB available
    SchedulerNodeReport report_nm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(15 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(1 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // Submit the second app App2 with priority 8 (Higher than App1)
    Priority appPriority2 = Priority.newInstance(8);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(1 * GB, rm)
        .withAppPriority(appPriority2)
        .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data);

    // kick the scheduler, 1 GB which was free is given to AM of App2
    MockAM am2 = MockRM.launchAM(app2, rm, nm1);
    am2.registerAppAttempt();

    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // get scheduler
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // get scheduler app
    FiCaSchedulerApp schedulerAppAttemptApp1 = cs.getSchedulerApplications()
        .get(app1.getApplicationId()).getCurrentAppAttempt();
    // kill 2 containers to free up some space
    int counter = 0;
    for (Iterator<Container> iterator = allocated1.iterator(); iterator
        .hasNext();) {
      Container c = iterator.next();
      if (++counter > 2) {
        break;
      }
      cs.markContainerForKillable(
          schedulerAppAttemptApp1.getRMContainer(c.getId()));
      iterator.remove();
    }

    // check node report, 12 GB used and 4 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(12 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(4 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // add request for containers App1
    am1.allocate("127.0.0.1", 2 * GB, 10, new ArrayList<ContainerId>());

    // add request for containers App2 and wait for containers to get allocated
    List<Container> allocated2 = am2.allocateAndWaitForContainers("127.0.0.1",
        2, 2 * GB, nm1);

    Assert.assertEquals(2, allocated2.size());
    // check node report, 16 GB used and 0 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(16 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // kill 1 more
    counter = 0;
    for (Iterator<Container> iterator = allocated1.iterator(); iterator
        .hasNext();) {
      Container c = iterator.next();
      if (++counter > 1) {
        break;
      }
      cs.markContainerForKillable(
          schedulerAppAttemptApp1.getRMContainer(c.getId()));
      iterator.remove();
    }

    // check node report, 14 GB used and 2 GB available
    report_nm1 = rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(14 * GB, report_nm1.getUsedResource().getMemorySize());
    Assert.assertEquals(2 * GB,
        report_nm1.getAvailableResource().getMemorySize());

    // Change the priority of App1 to 3 (lowest)
    Priority appPriority3 = Priority.newInstance(3);
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(app2.getUser());
    cs.updateApplicationPriority(appPriority3, app2.getApplicationId(), null,
        ugi);

    // add request for containers App2
    am2.allocate("127.0.0.1", 2 * GB, 3, new ArrayList<ContainerId>());

    // add request for containers App1 and wait for containers to get allocated
    // since priority is more for App1 now, App1 will get a container.
    List<Container> allocated3 = am1.allocateAndWaitForContainers("127.0.0.1",
        1, 2 * GB, nm1);

    Assert.assertEquals(1, allocated3.size());
    // Now App1 will have 5 containers and 1 AM. App2 will have 2 containers.
    Assert.assertEquals(6, schedulerAppAttemptApp1.getLiveContainers().size());
    rm.stop();
  }

  /**
   * <p>
   * Test case verifies the order of applications activated after RM Restart.
   * </p>
   * <li>App-1 and app-2 submitted and scheduled and running with a priority
   * 5 and 6 Respectively</li>
   * <li>App-3 submitted and scheduled with a priority 7. This
   * is not activated since AMResourceLimit is reached</li>
   * <li>RM restarted</li>
   * <li>App-1 get activated nevertheless of AMResourceLimit</li>
   * <li>App-2 and app-3 put in pendingOrderingPolicy</li>
   * <li>After NM registration, app-3 is activated</li>
   * <p>
   * Expected Output : App-2 must get activated since app-2 was running earlier
   * </p>
   * @throws Exception
   */
  @Test
  public void testOrderOfActivatingThePriorityApplicationOnRMRestart()
      throws Exception {
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);

    MockRM rm1 = new MockRM(conf);
    MemoryRMStateStore memStore = (MemoryRMStateStore) rm1.getRMStateStore();
    rm1.start();

    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 16384, rm1.getResourceTrackerService());
    nm1.registerNode();
    rm1.drainEvents();

    ResourceScheduler scheduler = rm1.getRMContext().getScheduler();
    LeafQueue defaultQueue =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue("default");
    int memory = (int) (defaultQueue.getAMResourceLimit().getMemorySize() / 2);

    // App-1 with priority 5 submitted and running
    Priority appPriority1 = Priority.newInstance(5);
    MockRMAppSubmissionData data2 = MockRMAppSubmissionData.Builder
        .createWithMemory(memory, rm1).withAppPriority(appPriority1).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm1, data2);
    MockAM am1 = MockRM.launchAM(app1, rm1, nm1);
    am1.registerAppAttempt();

    // App-2 with priority 6 submitted and running
    Priority appPriority2 = Priority.newInstance(6);
    MockRMAppSubmissionData data1 = MockRMAppSubmissionData.Builder
        .createWithMemory(memory, rm1)
        .withAppPriority(appPriority2)
        .build();
    RMApp app2 = MockRMAppSubmitter.submit(rm1, data1);
    MockAM am2 = MockRM.launchAM(app2, rm1, nm1);
    am2.registerAppAttempt();

    rm1.drainEvents();
    Assert.assertEquals(2, defaultQueue.getNumActiveApplications());
    Assert.assertEquals(0, defaultQueue.getNumPendingApplications());

    // App-3 with priority 7 submitted and scheduled. But not activated since
    // AMResourceLimit threshold
    Priority appPriority3 = Priority.newInstance(7);
    MockRMAppSubmissionData data = MockRMAppSubmissionData.Builder
        .createWithMemory(memory, rm1)
        .withAppPriority(appPriority3)
        .build();
    RMApp app3 = MockRMAppSubmitter.submit(rm1, data);

    rm1.drainEvents();
    Assert.assertEquals(2, defaultQueue.getNumActiveApplications());
    Assert.assertEquals(1, defaultQueue.getNumPendingApplications());

    Iterator<FiCaSchedulerApp> iterator =
        defaultQueue.getOrderingPolicy().getSchedulableEntities().iterator();
    FiCaSchedulerApp fcApp2 = iterator.next();
    Assert.assertEquals(app2.getCurrentAppAttempt().getAppAttemptId(),
        fcApp2.getApplicationAttemptId());

    FiCaSchedulerApp fcApp1 = iterator.next();
    Assert.assertEquals(app1.getCurrentAppAttempt().getAppAttemptId(),
        fcApp1.getApplicationAttemptId());

    iterator = defaultQueue.getPendingApplications().iterator();
    FiCaSchedulerApp fcApp3 = iterator.next();
    Assert.assertEquals(app3.getCurrentAppAttempt().getAppAttemptId(),
        fcApp3.getApplicationAttemptId());

    // create new RM to represent restart and recover state
    MockRM rm2 = new MockRM(conf, memStore);

    // start new RM
    rm2.start();
    // change NM to point to new RM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());

    // Verify RM Apps after this restart
    Assert.assertEquals(3, rm2.getRMContext().getRMApps().size());

    rm2.drainEvents();
    scheduler = rm2.getRMContext().getScheduler();
    defaultQueue =
        (LeafQueue) ((CapacityScheduler) scheduler).getQueue("default");

    // wait for all applications to get added to scheduler
    int count = 50;
    while (count-- > 0) {
      if (defaultQueue.getNumPendingApplications() == 3) {
        break;
      }
      Thread.sleep(50);
    }

    // Before NM registration, AMResourceLimit threshold is 0. So no
    // applications get activated.
    Assert.assertEquals(0, defaultQueue.getNumActiveApplications());
    Assert.assertEquals(3, defaultQueue.getNumPendingApplications());

    // NM resync to new RM
    nm1.registerNode();
    rm2.drainEvents();

    // wait for activating applications
    count = 50;
    while (count-- > 0) {
      if (defaultQueue.getNumActiveApplications() == 2) {
        break;
      }
      Thread.sleep(50);
    }

    Assert.assertEquals(2, defaultQueue.getNumActiveApplications());
    Assert.assertEquals(1, defaultQueue.getNumPendingApplications());

    // verify for order of activated applications iterator
    iterator =
        defaultQueue.getOrderingPolicy().getSchedulableEntities().iterator();
    fcApp2 = iterator.next();
    Assert.assertEquals(app2.getCurrentAppAttempt().getAppAttemptId(),
        fcApp2.getApplicationAttemptId());

    fcApp1 = iterator.next();
    Assert.assertEquals(app1.getCurrentAppAttempt().getAppAttemptId(),
        fcApp1.getApplicationAttemptId());

    // verify for pending application iterator. It should be app-3 attempt
    iterator = defaultQueue.getPendingApplications().iterator();
    fcApp3 = iterator.next();
    Assert.assertEquals(app3.getCurrentAppAttempt().getAppAttemptId(),
        fcApp3.getApplicationAttemptId());

    rm2.stop();
    rm1.stop();
  }

  @Test(timeout = 120000)
  public void testUpdatePriorityOnPendingAppAndKillAttempt() throws Exception {
    int maxPriority = 10;
    int appPriority = 5;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        maxPriority);
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue defaultQueue = (LeafQueue) cs.getQueue("default");

    // Update priority and kill application with no resource
    MockRMAppSubmissionData data3 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppPriority(Priority.newInstance(appPriority)).build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data3);
    Collection<FiCaSchedulerApp> appsPending =
        ((LeafQueue) defaultQueue).getPendingApplications();
    Collection<FiCaSchedulerApp> activeApps =
        ((LeafQueue) defaultQueue).getOrderingPolicy().getSchedulableEntities();

    // Verify app is in pending state
    Assert.assertEquals("Pending apps should be 1", 1, appsPending.size());
    Assert.assertEquals("Active apps should be 0", 0, activeApps.size());

    // kill app1 which is pending
    killAppAndVerifyOrderingPolicy(rm, defaultQueue, 0, 0, app1);

    // Check ordering policy size when resource is added
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 8096, rm.getResourceTrackerService());
    nm1.registerNode();
    MockRMAppSubmissionData data2 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppPriority(Priority.newInstance(appPriority)).build();
    RMApp app2 = MockRMAppSubmitter.submit(rm, data2);
    Assert.assertEquals("Pending apps should be 0", 0, appsPending.size());
    Assert.assertEquals("Active apps should be 1", 1, activeApps.size());
    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppPriority(Priority.newInstance(appPriority)).build();
    RMApp app3 = MockRMAppSubmitter.submit(rm, data1);
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(1024, rm)
            .withAppPriority(Priority.newInstance(appPriority)).build();
    RMApp app4 = MockRMAppSubmitter.submit(rm, data);
    Assert.assertEquals("Pending apps should be 2", 2, appsPending.size());
    Assert.assertEquals("Active apps should be 1", 1, activeApps.size());
    // kill app3, pending apps should reduce to 1
    killAppAndVerifyOrderingPolicy(rm, defaultQueue, 1, 1, app3);
    // kill app2, running apps is killed and pending added to running
    killAppAndVerifyOrderingPolicy(rm, defaultQueue, 0, 1, app2);
    // kill app4, all apps are killed and both policy size should be zero
    killAppAndVerifyOrderingPolicy(rm, defaultQueue, 0, 0, app4);
    rm.stop();
  }

  private void killAppAndVerifyOrderingPolicy(MockRM rm, CSQueue defaultQueue,
      int appsPendingExpected, int activeAppsExpected, RMApp app)
      throws YarnException {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(app.getUser());
    cs.updateApplicationPriority(Priority.newInstance(2),
        app.getApplicationId(), null, ugi);
    SchedulerEvent removeAttempt;
    removeAttempt = new AppAttemptRemovedSchedulerEvent(
        app.getCurrentAppAttempt().getAppAttemptId(), RMAppAttemptState.KILLED,
        false);
    cs.handle(removeAttempt);
    rm.drainEvents();
    Collection<FiCaSchedulerApp> appsPending =
        ((LeafQueue) defaultQueue).getPendingApplications();
    Collection<FiCaSchedulerApp> activeApps =
        ((LeafQueue) defaultQueue).getApplications();
    Assert.assertEquals("Pending apps should be " + appsPendingExpected,
        appsPendingExpected, appsPending.size());
    Assert.assertEquals("Active apps should be " + activeAppsExpected,
        activeAppsExpected, activeApps.size());
  }

}
