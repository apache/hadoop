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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMDeadLockTriggerByApp {

  private static final int INTERVAL = 1;
  private static final int LOOP = 5000;
  private static final float CHECK_DEAD_LOCK_RATIO = 2.0f;
  private static final int NODE_COUNT = 1;

  private boolean deadLock = false;
  private String errString = null;

  private Configuration conf = null;
  private MiniYARNCluster yarnCluster = null;

  private List<NodeReport> nodeReports = null;
  private ApplicationId appId = null;
  private ApplicationAttemptId attemptId = null;

  private YarnClient yarnClient = null;
  private AMRMClient<ContainerRequest> amClient = null;

  private ResourceManager rm;
  private NodeManager nm;

  // thread for allocate container
  private Thread allocateThread = new AllocateTread();

  // thread for add log aggregation report
  private Thread addLogAggReportThread = new AddLogAggregationReportThread();

  // thread for get application report
  private Thread getAppReportThread = new GetApplicationReportThread();

  @Before
  public void setup() throws Exception {
    createClusterAndStartApplication();
  }

  void createClusterAndStartApplication()
      throws Exception {
    this.conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.setInt(YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.setInt(YarnConfiguration.NM_VCORES, LOOP);
    conf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 512 * LOOP);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, this.INTERVAL);

    this.yarnCluster = new MiniYARNCluster(
        TestAMRMClient.class.getName(), NODE_COUNT, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    this.yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    assertTrue("All node managers did not connect to the RM within the allotted 5-second timeout",
        yarnCluster.waitForNodeManagersToConnect(5000L));
    this.nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    assertEquals("Not all node managers were reported running", NODE_COUNT, nodeReports.size());

    // get rm and nm info
    this.rm = yarnCluster.getResourceManager(0);
    this.nm = yarnCluster.getNodeManager(0);

    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    this.appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource>emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    GenericTestUtils.waitFor(() -> {
      try {
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
          this.attemptId = appReport.getCurrentApplicationAttemptId();
          RMAppAttempt appAttempt = rm.getRMContext().getRMApps()
              .get(attemptId.getApplicationId()).getCurrentAppAttempt();
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            return true;
          }
        }
      } catch (Exception e) {
        fail("Application launch failed.");
      }
      return false;
    }, 1000, 10000);

    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));

    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    RMAppAttempt appAttempt = rm.getRMContext().getRMApps()
        .get(attemptId.getApplicationId()).getCurrentAppAttempt();
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(ClientRMProxy.getAMRMTokenService(conf));

    // create AMRMClient
    this.amClient = AMRMClient.createAMRMClient();
    amClient.init(conf);
    amClient.start();
    amClient.registerApplicationMaster("Host", 10000, "");
  }

  @After
  public void teardown() throws YarnException, IOException {
    if (allocateThread != null) {
      allocateThread.interrupt();
      allocateThread = null;
    }
    if (addLogAggReportThread != null) {
      addLogAggReportThread.interrupt();
      addLogAggReportThread = null;
    }
    if (getAppReportThread != null) {
      getAppReportThread.interrupt();
      getAppReportThread = null;
    }

    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    this.yarnClient = null;

    if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
      amClient.stop();
    }
    this.amClient = null;

    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
    this.yarnCluster = null;
  }

  @Test(timeout = 60000)
  public void testRMDeadLockTriggerByApp() throws InterruptedException {
    // start all thread
    allocateThread.start();
    addLogAggReportThread.start();
    getAppReportThread.start();

    this.deadLock = checkAsyncDispatcherDeadLock();
    Assert.assertFalse("There is dead lock!", deadLock);
    Assert.assertNull(errString);
  }

  private boolean checkAsyncDispatcherDeadLock() throws InterruptedException {
    Event lastEvent = null;
    Event currentEvent;
    int counter = 0;
    for (int i = 0; i < LOOP * CHECK_DEAD_LOCK_RATIO; i++) {
      currentEvent = ((AsyncDispatcher) rm.getRmDispatcher()).getHeadEvent();
      if (currentEvent != null && (currentEvent == lastEvent)) {
        if (counter++ > LOOP * CHECK_DEAD_LOCK_RATIO / 2) {
          return true;
        }
      } else {
        counter = 0;
        lastEvent = currentEvent;
      }
      Thread.sleep(INTERVAL);
    }
    return false;
  }

  class AllocateTread extends Thread {

    @Override
    public void run() {
      ContainerId amContainerId = rm.getRMContext().getRMApps().get(appId)
          .getAppAttempts().get(attemptId).getMasterContainer().getId();
      ContainerRequest request = setupContainerAskForRM();
      try {
        for (int i = 0; i < LOOP; i++) {
          amClient.addContainerRequest(request);
          for (ContainerId containerId : nm.getNMContext().getContainers().keySet()) {
            // release all container except am container
            if (!amContainerId.equals(containerId)) {
              amClient.releaseAssignedContainer(containerId);
            }
          }
          amClient.allocate(0.1f);
          Thread.sleep(INTERVAL);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  class AddLogAggregationReportThread extends Thread {

    @Override
    public void run() {
      LogAggregationReport report = LogAggregationReport
          .newInstance(appId, LogAggregationStatus.RUNNING, "");
      try {
        for (int i = 0; i < LOOP; i++) {
          if (nm.getNMContext().getLogAggregationStatusForApps().size() == 0) {
            nm.getNMContext().getLogAggregationStatusForApps().add(report);
          }
          Thread.sleep(INTERVAL);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  class GetApplicationReportThread extends Thread {

    @Override
    public void run() {
      try {
        for (int i = 0; i < LOOP; i++) {
          yarnClient.getApplicationReport(appId);
          Thread.sleep(INTERVAL);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  private ContainerRequest setupContainerAskForRM() {
    Priority pri = Priority.newInstance(1);
    ContainerRequest request =
        new ContainerRequest(Resource.newInstance(512, 1), null, null, pri, 0, true, null,
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true), "");
    return request;
  }
}
