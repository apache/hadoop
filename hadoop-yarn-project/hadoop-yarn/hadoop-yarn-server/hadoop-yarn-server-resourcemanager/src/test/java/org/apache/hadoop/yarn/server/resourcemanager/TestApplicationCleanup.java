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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationCleanup {

  private static final Log LOG = LogFactory
    .getLog(TestApplicationCleanup.class);
  
  private YarnConfiguration conf;
  
  @Before
  public void setup() throws UnknownHostException {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    conf = new YarnConfiguration();
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    Assert.assertTrue(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS > 1);
  }

  @SuppressWarnings("resource")
  @Test
  public void testAppCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 2;
    am.allocate("127.0.0.1" , 1000, request, 
        new ArrayList<ContainerId>());
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    int waitCount = 0;
    while (contReceived < request && waitCount++ < 200) {
      LOG.info("Got " + contReceived + " containers. Waiting to get "
               + request);
      Thread.sleep(100);
      conts = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      contReceived += conts.size();
      nm1.nodeHeartbeat(true);
    }
    Assert.assertEquals(request, contReceived);
    
    am.unregisterAppAttempt();
    NodeHeartbeatResponse resp = nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1,
        ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);

    //currently only containers are cleaned via this
    //AM container is cleaned via container launcher
    resp = nm1.nodeHeartbeat(true);
    List<ContainerId> containersToCleanup = resp.getContainersToCleanup();
    List<ApplicationId> appsToCleanup = resp.getApplicationsToCleanup();
    int numCleanedContainers = containersToCleanup.size();
    int numCleanedApps = appsToCleanup.size();
    waitCount = 0;
    while ((numCleanedContainers < 2 || numCleanedApps < 1)
        && waitCount++ < 200) {
      LOG.info("Waiting to get cleanup events.. cleanedConts: "
          + numCleanedContainers + " cleanedApps: " + numCleanedApps);
      Thread.sleep(100);
      resp = nm1.nodeHeartbeat(true);
      List<ContainerId> deltaContainersToCleanup =
          resp.getContainersToCleanup();
      List<ApplicationId> deltaAppsToCleanup = resp.getApplicationsToCleanup();
      // Add the deltas to the global list
      containersToCleanup.addAll(deltaContainersToCleanup);
      appsToCleanup.addAll(deltaAppsToCleanup);
      // Update counts now
      numCleanedContainers = containersToCleanup.size();
      numCleanedApps = appsToCleanup.size();
    }
    
    Assert.assertEquals(1, appsToCleanup.size());
    Assert.assertEquals(app.getApplicationId(), appsToCleanup.get(0));
    Assert.assertEquals(1, numCleanedApps);
    Assert.assertEquals(2, numCleanedContainers);

    rm.stop();
  }

  @SuppressWarnings("resource")
  @Test
  public void testContainerCleanup() throws Exception {

    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    final DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm = new MockRM() {
      @Override
      protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        return new SchedulerEventDispatcher(this.scheduler) {
          @Override
          public void handle(SchedulerEvent event) {
            scheduler.handle(event);
          }
        };
      }

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();

    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 2;
    am.allocate("127.0.0.1" , 1000, request, 
        new ArrayList<ContainerId>());
    dispatcher.await();
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    int contReceived = conts.size();
    int waitCount = 0;
    while (contReceived < request && waitCount++ < 200) {
      LOG.info("Got " + contReceived + " containers. Waiting to get "
               + request);
      Thread.sleep(100);
      conts = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      dispatcher.await();
      contReceived += conts.size();
      nm1.nodeHeartbeat(true);
    }
    Assert.assertEquals(request, contReceived);

    // Release a container.
    ArrayList<ContainerId> release = new ArrayList<ContainerId>();
    release.add(conts.get(0).getId());
    am.allocate(new ArrayList<ResourceRequest>(), release);
    dispatcher.await();

    // Send one more heartbeat with a fake running container. This is to
    // simulate the situation that can happen if the NM reports that container
    // is running in the same heartbeat when the RM asks it to clean it up.
    Map<ApplicationId, List<ContainerStatus>> containerStatuses =
        new HashMap<ApplicationId, List<ContainerStatus>>();
    ArrayList<ContainerStatus> containerStatusList =
        new ArrayList<ContainerStatus>();
    containerStatusList.add(BuilderUtils.newContainerStatus(conts.get(0)
      .getId(), ContainerState.RUNNING, "nothing", 0));
    containerStatuses.put(app.getApplicationId(), containerStatusList);

    NodeHeartbeatResponse resp = nm1.nodeHeartbeat(containerStatuses, true);
    waitForContainerCleanup(dispatcher, nm1, resp);

    // Now to test the case when RM already gave cleanup, and NM suddenly
    // realizes that the container is running.
    LOG.info("Testing container launch much after release and "
        + "NM getting cleanup");
    containerStatuses.clear();
    containerStatusList.clear();
    containerStatusList.add(BuilderUtils.newContainerStatus(conts.get(0)
      .getId(), ContainerState.RUNNING, "nothing", 0));
    containerStatuses.put(app.getApplicationId(), containerStatusList);

    resp = nm1.nodeHeartbeat(containerStatuses, true);
    // The cleanup list won't be instantaneous as it is given out by scheduler
    // and not RMNodeImpl.
    waitForContainerCleanup(dispatcher, nm1, resp);

    rm.stop();
  }

  protected void waitForContainerCleanup(DrainDispatcher dispatcher, MockNM nm,
      NodeHeartbeatResponse resp) throws Exception {
    int waitCount = 0, cleanedConts = 0;
    List<ContainerId> contsToClean;
    do {
      dispatcher.await();
      contsToClean = resp.getContainersToCleanup();
      cleanedConts += contsToClean.size();
      if (cleanedConts >= 1) {
        break;
      }
      Thread.sleep(100);
      resp = nm.nodeHeartbeat(true);
    } while(waitCount++ < 200);

    if (contsToClean.isEmpty()) {
      LOG.error("Failed to get any containers to cleanup");
    } else {
      LOG.info("Got cleanup for " + contsToClean.get(0));
    }
    Assert.assertEquals(1, cleanedConts);
  }

  private void waitForAppCleanupMessageRecved(MockNM nm, ApplicationId appId)
      throws Exception {
    while (true) {
      NodeHeartbeatResponse response = nm.nodeHeartbeat(true);
      if (response.getApplicationsToCleanup() != null
          && response.getApplicationsToCleanup().size() == 1
          && appId.equals(response.getApplicationsToCleanup().get(0))) {
        return;
      }

      LOG.info("Haven't got application=" + appId.toString()
          + " in cleanup list from node heartbeat response, "
          + "sleep for a while before next heartbeat");
      Thread.sleep(1000);
    }
  }
  
  private MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }
  
  @SuppressWarnings("resource")
  @Test (timeout = 60000)
  public void testAppCleanupWhenRMRestartedAfterAppFinished() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(app0.getApplicationId(), RMAppState.FAILED);

    // start new RM
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();
    
    // nm1 register to rm2, and do a heartbeat
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode(Arrays.asList(app0.getApplicationId()));
    rm2.waitForState(app0.getApplicationId(), RMAppState.FAILED);
    
    // wait for application cleanup message received
    waitForAppCleanupMessageRecved(nm1, app0.getApplicationId());
    
    rm1.stop();
    rm2.stop();
  }
  
  @SuppressWarnings("resource")
  @Test(timeout = 60000)
  public void testAppCleanupWhenRMRestartedBeforeAppFinished() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 1024, rm1.getResourceTrackerService());
    nm1.registerNode();
    MockNM nm2 =
        new MockNM("127.0.0.1:5678", 1024, rm1.getResourceTrackerService());
    nm2.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);

    // alloc another container on nm2
    AllocateResponse allocResponse =
        am0.allocate(Arrays.asList(ResourceRequest.newInstance(
            Priority.newInstance(1), "*", Resource.newInstance(1024, 0), 1)),
            null);
    while (null == allocResponse.getAllocatedContainers()
        || allocResponse.getAllocatedContainers().isEmpty()) {
      nm2.nodeHeartbeat(true);
      allocResponse = am0.allocate(null, null);
      Thread.sleep(1000);
    }

    // start new RM
    MockRM rm2 = new MockRM(conf, memStore);
    rm2.start();

    // nm1/nm2 register to rm2, and do a heartbeat
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode(Arrays.asList(NMContainerStatus.newInstance(
      ContainerId.newContainerId(am0.getApplicationAttemptId(), 1),
      ContainerState.COMPLETE, Resource.newInstance(1024, 1), "", 0,
      Priority.newInstance(0), 1234)), Arrays.asList(app0.getApplicationId()));
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.registerNode(Arrays.asList(app0.getApplicationId()));

    // assert app state has been saved.
    rm2.waitForState(app0.getApplicationId(), RMAppState.FAILED);

    // wait for application cleanup message received on NM1
    waitForAppCleanupMessageRecved(nm1, app0.getApplicationId());

    // wait for application cleanup message received on NM2
    waitForAppCleanupMessageRecved(nm2, app0.getApplicationId());

    rm1.stop();
    rm2.stop();
  }

  @SuppressWarnings("resource")
  @Test (timeout = 60000)
  public void testContainerCleanupWhenRMRestartedAppNotRegistered() throws
      Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    final DrainDispatcher dispatcher = new DrainDispatcher();
    MockRM rm1 = new MockRM(conf, memStore) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    rm1.waitForState(app0.getApplicationId(), RMAppState.RUNNING);

    // start new RM
    final DrainDispatcher dispatcher2 = new DrainDispatcher();
    MockRM rm2 = new MockRM(conf, memStore) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher2;
      }
    };
    rm2.start();

    // nm1 register to rm2, and do a heartbeat
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm1.registerNode(Arrays.asList(app0.getApplicationId()));
    rm2.waitForState(app0.getApplicationId(), RMAppState.ACCEPTED);

    // Add unknown container for application unknown to scheduler
    NodeHeartbeatResponse response = nm1.nodeHeartbeat(am0
        .getApplicationAttemptId(), 2, ContainerState.RUNNING);

    waitForContainerCleanup(dispatcher2, nm1, response);

    rm1.stop();
    rm2.stop();
  }

  @Test (timeout = 60000)
  public void testAppCleanupWhenNMReconnects() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // start RM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();

    // create app and launch the AM
    RMApp app0 = rm1.submitApp(200);
    MockAM am0 = launchAM(app0, rm1, nm1);
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm1.waitForState(app0.getApplicationId(), RMAppState.FAILED);

    // wait for application cleanup message received
    waitForAppCleanupMessageRecved(nm1, app0.getApplicationId());

    // reconnect NM with application still active
    nm1.registerNode(Arrays.asList(app0.getApplicationId()));
    waitForAppCleanupMessageRecved(nm1, app0.getApplicationId());

    rm1.stop();
  }

  // The test verifies processing of NMContainerStatuses which are sent during
  // NM registration.
  // 1. Start the cluster-RM,NM,Submit app with 1024MB,Launch & register AM
  // 2. AM sends ResourceRequest for 1 container with memory 2048MB.
  // 3. Verify for number of container allocated by RM
  // 4. Verify Memory Usage by cluster, it should be 3072. AM memory + requested
  // memory. 1024 + 2048=3072
  // 5. Re-register NM by sending completed container status
  // 6. Verify for Memory Used, it should be 1024
  // 7. Send AM heatbeat to RM. Allocated response should contain completed
  // container.
  @Test(timeout = 60000)
  public void testProcessingNMContainerStatusesOnNMRestart() throws Exception {
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 1);
    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    // 1. Start the cluster-RM,NM,Submit app with 1024MB,Launch & register AM
    MockRM rm1 = new MockRM(conf, memStore);
    rm1.start();
    int nmMemory = 8192;
    int amMemory = 1024;
    int containerMemory = 2048;
    MockNM nm1 =
        new MockNM("127.0.0.1:1234", nmMemory, rm1.getResourceTrackerService());
    nm1.registerNode();

    RMApp app0 = rm1.submitApp(amMemory);
    MockAM am0 = MockRM.launchAndRegisterAM(app0, rm1, nm1);

    // 2. AM sends ResourceRequest for 1 container with memory 2048MB.
    int noOfContainers = 1;
    List<Container> allocateContainers =
        am0.allocateAndWaitForContainers(noOfContainers, containerMemory, nm1);

    // 3. Verify for number of container allocated by RM
    Assert.assertEquals(noOfContainers, allocateContainers.size());
    Container container = allocateContainers.get(0);

    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.RUNNING);
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), container.getId()
        .getContainerId(), ContainerState.RUNNING);

    rm1.waitForState(app0.getApplicationId(), RMAppState.RUNNING);

    // 4. Verify Memory Usage by cluster, it should be 3072. AM memory +
    // requested memory. 1024 + 2048=3072
    ResourceScheduler rs = rm1.getRMContext().getScheduler();
    int allocatedMB = rs.getRootQueueMetrics().getAllocatedMB();
    Assert.assertEquals(amMemory + containerMemory, allocatedMB);

    // 5. Re-register NM by sending completed container status
    List<NMContainerStatus> nMContainerStatusForApp =
        createNMContainerStatusForApp(am0);
    nm1.registerNode(nMContainerStatusForApp,
        Arrays.asList(app0.getApplicationId()));

    waitForClusterMemory(nm1, rs, amMemory);

    // 6. Verify for Memory Used, it should be 1024
    Assert.assertEquals(amMemory, rs.getRootQueueMetrics().getAllocatedMB());

    // 7. Send AM heatbeat to RM. Allocated response should contain completed
    // container
    AllocateRequest req =
        AllocateRequest.newInstance(0, 0F, new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>(), null);
    AllocateResponse allocate = am0.allocate(req);
    List<ContainerStatus> completedContainersStatuses =
        allocate.getCompletedContainersStatuses();
    Assert.assertEquals(noOfContainers, completedContainersStatuses.size());

    // Application clean up should happen Cluster memory used is 0
    nm1.nodeHeartbeat(am0.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    waitForClusterMemory(nm1, rs, 0);

    rm1.stop();
  }

  private void waitForClusterMemory(MockNM nm1, ResourceScheduler rs,
      int clusterMemory) throws Exception, InterruptedException {
    int counter = 0;
    while (rs.getRootQueueMetrics().getAllocatedMB() != clusterMemory) {
      nm1.nodeHeartbeat(true);

      Thread.sleep(100);
      if (counter++ == 50) {
        Assert.fail("Wait for cluster memory is timed out.Expected="
            + clusterMemory + " Actual="
            + rs.getRootQueueMetrics().getAllocatedMB());
      }
    }
  }

  public static List<NMContainerStatus> createNMContainerStatusForApp(MockAM am) {
    List<NMContainerStatus> list = new ArrayList<NMContainerStatus>();
    NMContainerStatus amContainer =
        createNMContainerStatus(am.getApplicationAttemptId(), 1,
            ContainerState.RUNNING, 1024);
    NMContainerStatus completedContainer =
        createNMContainerStatus(am.getApplicationAttemptId(), 2,
            ContainerState.COMPLETE, 2048);
    list.add(amContainer);
    list.add(completedContainer);
    return list;
  }

  public static NMContainerStatus createNMContainerStatus(
      ApplicationAttemptId appAttemptId, int id, ContainerState containerState,
      int memory) {
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, id);
    NMContainerStatus containerReport =
        NMContainerStatus.newInstance(containerId, containerState,
            Resource.newInstance(memory, 1), "recover container", 0,
            Priority.newInstance(0), 0);
    return containerReport;
  }

  public static void main(String[] args) throws Exception {
    TestApplicationCleanup t = new TestApplicationCleanup();
    t.testAppCleanup();
  }
}
