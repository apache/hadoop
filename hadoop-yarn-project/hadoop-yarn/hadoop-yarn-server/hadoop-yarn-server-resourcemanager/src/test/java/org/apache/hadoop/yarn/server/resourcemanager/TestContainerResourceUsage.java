/*
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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestContainerResourceUsage {

  private YarnConfiguration conf;

  @Before
  public void setup() throws UnknownHostException {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
  }

  @After
  public void tearDown() {
  }

  @Test (timeout = 60000)
  public void testUsageWithOneAttemptAndOneContainer() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    
    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm.registerNode();

    RMApp app0 = rm.submitApp(200);

    RMAppMetrics rmAppMetrics = app0.getRMAppMetrics();
    Assert.assertTrue(
        "Before app submittion, memory seconds should have been 0 but was "
                          + rmAppMetrics.getMemorySeconds(),
        rmAppMetrics.getMemorySeconds() == 0);
    Assert.assertTrue(
        "Before app submission, vcore seconds should have been 0 but was "
                          + rmAppMetrics.getVcoreSeconds(),
        rmAppMetrics.getVcoreSeconds() == 0);

    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();

    nm.nodeHeartbeat(true);
    MockAM am0 = rm.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();

    RMContainer rmContainer =
        rm.getResourceScheduler()
           .getRMContainer(attempt0.getMasterContainer().getId());

    // Allow metrics to accumulate.
    Thread.sleep(1000);

    rmAppMetrics = app0.getRMAppMetrics();
    Assert.assertTrue(
        "While app is running, memory seconds should be >0 but is "
            + rmAppMetrics.getMemorySeconds(),
        rmAppMetrics.getMemorySeconds() > 0);
    Assert.assertTrue(
        "While app is running, vcore seconds should be >0 but is "
            + rmAppMetrics.getVcoreSeconds(),
        rmAppMetrics.getVcoreSeconds() > 0);

    MockRM.finishAMAndVerifyAppState(app0, rm, nm, am0);

    AggregateAppResourceUsage ru = calculateContainerResourceMetrics(rmContainer);
    rmAppMetrics = app0.getRMAppMetrics();

    Assert.assertEquals("Unexcpected MemorySeconds value",
        ru.getMemorySeconds(), rmAppMetrics.getMemorySeconds());
    Assert.assertEquals("Unexpected VcoreSeconds value",
        ru.getVcoreSeconds(), rmAppMetrics.getVcoreSeconds());

    rm.stop();
  }

  @Test (timeout = 60000)
  public void testUsageWithMultipleContainersAndRMRestart() throws Exception {
    // Set max attempts to 1 so that when the first attempt fails, the app
    // won't try to start a new one.
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    MockRM rm0 = new MockRM(conf, memStore);
    rm0.start();
    MockNM nm =
        new MockNM("127.0.0.1:1234", 65536, rm0.getResourceTrackerService());
    nm.registerNode();

    RMApp app0 = rm0.submitApp(200);

    rm0.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();
    ApplicationAttemptId attemptId0 = attempt0.getAppAttemptId();
    rm0.waitForState(attemptId0, RMAppAttemptState.SCHEDULED);

    nm.nodeHeartbeat(true);
    rm0.waitForState(attemptId0, RMAppAttemptState.ALLOCATED);
    MockAM am0 = rm0.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();

    int NUM_CONTAINERS = 2;
    am0.allocate("127.0.0.1" , 1000, NUM_CONTAINERS,
                  new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    List<Container> conts = am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() != NUM_CONTAINERS) {
      nm.nodeHeartbeat(true);
      conts.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }

    // launch the 2nd and 3rd containers.
    for (Container c : conts) {
      nm.nodeHeartbeat(attempt0.getAppAttemptId(),
                       c.getId().getId(), ContainerState.RUNNING);
      rm0.waitForState(nm, c.getId(), RMContainerState.RUNNING);
    }

    // Get the RMContainers for all of the live containers, to be used later
    // for metrics calculations and comparisons.
    Collection<RMContainer> rmContainers =
        rm0.scheduler
            .getSchedulerAppInfo(attempt0.getAppAttemptId())
              .getLiveContainers();

    // Give the metrics time to accumulate.
    Thread.sleep(1000);

    // Stop all non-AM containers
    for (Container c : conts) {
      if (c.getId().getId() == 1) continue;
      nm.nodeHeartbeat(attempt0.getAppAttemptId(),
                       c.getId().getId(), ContainerState.COMPLETE);
      rm0.waitForState(nm, c.getId(), RMContainerState.COMPLETED);
    }

    // After all other containers have completed, manually complete the master
    // container in order to trigger a save to the state store of the resource
    // usage metrics. This will cause the attempt to fail, and, since the max
    // attempt retries is 1, the app will also fail. This is intentional so
    // that all containers will complete prior to saving.
    ContainerId cId = ContainerId.newInstance(attempt0.getAppAttemptId(), 1);
    nm.nodeHeartbeat(attempt0.getAppAttemptId(),
                 cId.getId(), ContainerState.COMPLETE);
    rm0.waitForState(nm, cId, RMContainerState.COMPLETED);

    // Check that the container metrics match those from the app usage report.
    long memorySeconds = 0;
    long vcoreSeconds = 0;
    for (RMContainer c : rmContainers) {
      AggregateAppResourceUsage ru = calculateContainerResourceMetrics(c);
      memorySeconds += ru.getMemorySeconds();
      vcoreSeconds += ru.getVcoreSeconds();
    }

    RMAppMetrics metricsBefore = app0.getRMAppMetrics();
    Assert.assertEquals("Unexcpected MemorySeconds value",
        memorySeconds, metricsBefore.getMemorySeconds());
    Assert.assertEquals("Unexpected VcoreSeconds value",
        vcoreSeconds, metricsBefore.getVcoreSeconds());

    // create new RM to represent RM restart. Load up the state store.
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    RMApp app0After =
        rm1.getRMContext().getRMApps().get(app0.getApplicationId());

    // Compare container resource usage metrics from before and after restart.
    RMAppMetrics metricsAfter = app0After.getRMAppMetrics();
    Assert.assertEquals("Vcore seconds were not the same after RM Restart",
        metricsBefore.getVcoreSeconds(), metricsAfter.getVcoreSeconds());
    Assert.assertEquals("Memory seconds were not the same after RM Restart",
        metricsBefore.getMemorySeconds(), metricsAfter.getMemorySeconds());

    rm0.stop();
    rm0.close();
    rm1.stop();
    rm1.close();
  }

  @Test(timeout = 60000)
  public void testUsageAfterAMRestartWithMultipleContainers() throws Exception {
    amRestartTests(false);
  }

  @Test(timeout = 60000)
  public void testUsageAfterAMRestartKeepContainers() throws Exception {
    amRestartTests(true);
  }

  private void amRestartTests(boolean keepRunningContainers)
      throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();

    RMApp app =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE", false, keepRunningContainers);
    MockNM nm = 
        new MockNM("127.0.0.1:1234", 10240, rm.getResourceTrackerService());
    nm.registerNode();

    MockAM am0 = MockRM.launchAndRegisterAM(app, rm, nm);
    int NUM_CONTAINERS = 1;
    // allocate NUM_CONTAINERS containers
    am0.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
      new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);

    // wait for containers to be allocated.
    List<Container> containers =
        am0.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    while (containers.size() != NUM_CONTAINERS) {
      nm.nodeHeartbeat(true);
      containers.addAll(am0.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }   

    // launch the 2nd container.
    ContainerId containerId2 =
        ContainerId.newInstance(am0.getApplicationAttemptId(), 2);
    nm.nodeHeartbeat(am0.getApplicationAttemptId(),
                      containerId2.getId(), ContainerState.RUNNING);
    rm.waitForState(nm, containerId2, RMContainerState.RUNNING);

    // Capture the containers here so the metrics can be calculated after the
    // app has completed.
    Collection<RMContainer> rmContainers =
        rm.scheduler
            .getSchedulerAppInfo(am0.getApplicationAttemptId())
              .getLiveContainers();

    // fail the first app attempt by sending CONTAINER_FINISHED event without
    // registering.
    ContainerId amContainerId =
        app.getCurrentAppAttempt().getMasterContainer().getId();
    nm.nodeHeartbeat(am0.getApplicationAttemptId(),
                      amContainerId.getId(), ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FAILED);

    long memorySeconds = 0;
    long vcoreSeconds = 0;

    // Calculate container usage metrics for first attempt.
    if (keepRunningContainers) {
      // Only calculate the usage for the one container that has completed.
      for (RMContainer c : rmContainers) {
        if (c.getContainerId().equals(amContainerId)) {
          AggregateAppResourceUsage ru = calculateContainerResourceMetrics(c);
          memorySeconds += ru.getMemorySeconds();
          vcoreSeconds += ru.getVcoreSeconds();
        } else {
          // The remaining container should be RUNNING.
          Assert.assertTrue("After first attempt failed, remaining container "
                        + "should still be running. ",
                        c.getContainerState().equals(ContainerState.RUNNING));
        }
      }
    } else {
      // If keepRunningContainers is false, all live containers should now
      // be completed. Calculate the resource usage metrics for all of them.
      for (RMContainer c : rmContainers) { 
        AggregateAppResourceUsage ru = calculateContainerResourceMetrics(c);
        memorySeconds += ru.getMemorySeconds();
        vcoreSeconds += ru.getVcoreSeconds();
      }
    }

    // wait for app to start a new attempt.
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);

    // assert this is a new AM.
    RMAppAttempt attempt2 = app.getCurrentAppAttempt();
    Assert.assertFalse(attempt2.getAppAttemptId()
                               .equals(am0.getApplicationAttemptId()));

    // launch the new AM
    nm.nodeHeartbeat(true);
    MockAM am1 = rm.sendAMLaunched(attempt2.getAppAttemptId());
    am1.registerAppAttempt();
    
    // allocate NUM_CONTAINERS containers
    am1.allocate("127.0.0.1", 1024, NUM_CONTAINERS,
      new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);

    // wait for containers to be allocated.
    containers =
        am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    while (containers.size() != NUM_CONTAINERS) {
      nm.nodeHeartbeat(true);
      containers.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(200);
    }

    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    
    // Capture running containers for later use by metrics calculations.
    rmContainers = rm.scheduler.getSchedulerAppInfo(attempt2.getAppAttemptId())
                               .getLiveContainers();
      
    // complete container by sending the container complete event which has
    // earlier attempt's attemptId
    amContainerId = app.getCurrentAppAttempt().getMasterContainer().getId();
    nm.nodeHeartbeat(am0.getApplicationAttemptId(),
                      amContainerId.getId(), ContainerState.COMPLETE);
    
    MockRM.finishAMAndVerifyAppState(app, rm, nm, am1);

    // Calculate container usage metrics for second attempt.
    for (RMContainer c : rmContainers) {
      AggregateAppResourceUsage ru = calculateContainerResourceMetrics(c);
      memorySeconds += ru.getMemorySeconds();
      vcoreSeconds += ru.getVcoreSeconds();
    }
    
    RMAppMetrics rmAppMetrics = app.getRMAppMetrics();

    Assert.assertEquals("Unexcpected MemorySeconds value",
        memorySeconds, rmAppMetrics.getMemorySeconds());
    Assert.assertEquals("Unexpected VcoreSeconds value",
        vcoreSeconds, rmAppMetrics.getVcoreSeconds());

    rm.stop();
    return;
  }

  private AggregateAppResourceUsage calculateContainerResourceMetrics(
      RMContainer rmContainer) {
    Resource resource = rmContainer.getContainer().getResource();
    long usedMillis =
        rmContainer.getFinishTime() - rmContainer.getCreationTime();
    long memorySeconds = resource.getMemory()
                          * usedMillis / DateUtils.MILLIS_PER_SECOND;
    long vcoreSeconds = resource.getVirtualCores()
                          * usedMillis / DateUtils.MILLIS_PER_SECOND;
    return new AggregateAppResourceUsage(memorySeconds, vcoreSeconds);
  }
}