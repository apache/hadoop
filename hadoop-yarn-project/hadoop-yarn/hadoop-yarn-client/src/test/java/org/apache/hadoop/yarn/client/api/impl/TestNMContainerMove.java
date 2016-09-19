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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerMoveRequest;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.statements.Fail;

/**
 * Tests end-to-end workflow of container relocation
 */
public class TestNMContainerMove {
  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;
  YarnClientImpl yarnClient = null;
  AMRMClientImpl<ContainerRequest> rmClient = null;
  NMClientImpl nmClient = null;
  
  int nodeCount = 3;
  List<NodeReport> nodeReports = null;
  static NodeId node1;
  static NodeId node2;
  static NodeId node3;
  static String rack1;
  static String rack2;
  static String rack3;
  ApplicationAttemptId attemptId = null;
  NMTokenCache nmTokenCache = null;
  
  @Before
  public void setup() throws YarnException, IOException {
    // start minicluster
    conf = new YarnConfiguration();
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        13);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 4000);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.set(YarnConfiguration.RM_SCHEDULER, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".scheduler.fifo.FifoScheduler");
    yarnCluster =
        new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    assertNotNull(yarnCluster);
    assertEquals(STATE.STARTED, yarnCluster.getServiceState());
    
    // start rm client
    yarnClient = (YarnClientImpl) YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    assertNotNull(yarnClient);
    assertEquals(STATE.STARTED, yarnClient.getServiceState());
    
    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    node1 = nodeReports.get(0).getNodeId();
    node2 = nodeReports.get(1).getNodeId();
    node3 = nodeReports.get(2).getNodeId();
    rack1 = nodeReports.get(0).getRackName();
    rack2 = nodeReports.get(1).getRackName();
    rack3 = nodeReports.get(2).getRackName();
    
    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Priority.newInstance(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    // unmanaged AM
    appContext.setUnmanagedAM(true);
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);
    
    // wait for app to start
    int iterationsLeft = 30;
    RMAppAttempt appAttempt = null;
    while (iterationsLeft > 0) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
                .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
      sleep(1000);
      --iterationsLeft;
    }
    if (iterationsLeft == 0) {
      fail("Application hasn't bee started");
    }
    
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    
    //creating an instance NMTokenCase
    nmTokenCache = new NMTokenCache();
    
    // start am rm client
    rmClient =
        (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest>createAMRMClient();
    
    //setting an instance NMTokenCase
    rmClient.setNMTokenCache(nmTokenCache);
    rmClient.init(conf);
    rmClient.start();
    assertNotNull(rmClient);
    assertEquals(STATE.STARTED, rmClient.getServiceState());
    
    // start am nm client
    nmClient = (NMClientImpl) NMClient.createNMClient();
    
    //propagating the AMRMClient NMTokenCache instance
    nmClient.setNMTokenCache(rmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();
    assertNotNull(nmClient);
    assertEquals(STATE.STARTED, nmClient.getServiceState());
  }
  
  @After
  public void tearDown() {
    rmClient.stop();
    yarnClient.stop();
    yarnCluster.stop();
  }
  
  @Test(timeout = 200000)
  public void testNMContainerMove()
      throws YarnException, IOException {
    // registering application master
    rmClient.registerApplicationMaster("Host", 10000, "");
    
    // reserving two regular containers with RM
    Resource capability1 = Resource.newInstance(2048, 1);
    Resource capability2 = Resource.newInstance(1024, 1);
    String[] nodes = new String[]{node1.getHost()};
    String[] racks = new String[]{rack1};
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    
    ContainerRequest request1 = new ContainerRequest(capability1, nodes, racks, priority1);
    ContainerRequest request2 = new ContainerRequest(capability2, nodes, racks, priority2);
    
    rmClient.addContainerRequest(request1);
    rmClient.addContainerRequest(request2);
    
    List<Container> containers = allocate(2);
    
    // select the container with capability1 to be the origin container
    Container originContainer = null;
    for (Container container : containers) {
      if (container.getResource().equals(capability1)) {
        originContainer = container;
      }
    }
    
    // request the origin container to be replaced to another node of another host
    // as the test normally runs in local mode, we do not automatically get a
    // different target node, so loop until it happens
    NodeId targetNodeId = originContainer.getNodeId();
    Container targetContainer = null;
    while (originContainer.getNodeId().equals(targetNodeId)) {
      ContainerMoveRequest moveRequest = ContainerMoveRequest.newInstance(priority1,
          originContainer.getId(), targetNodeId.getHost());
      rmClient.addContainerMoveRequest(moveRequest);
      targetContainer = allocate(1).get(0);
      targetNodeId = targetContainer.getNodeId();
    }
    
    // start the two containers allocated before
    for (Container container : containers) {
      Credentials cred = new Credentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      cred.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens =
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
      clc.setCommands(Collections.singletonList("sleep 1m"));
      clc.setTokens(securityTokens);
      try {
        nmClient.startContainer(container, clc);
      } catch (YarnException e) {
        throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e).initCause(e));
      }
    }
    
    int exitStatus1 = 0;
    try {
      exitStatus1 = nmClient.getContainerStatus(
          originContainer.getId(), originContainer.getNodeId()).getExitStatus();
      // status of the origin container befor relocation
      assertTrue(exitStatus1 == ContainerExitStatus.INVALID);
    } catch (Exception e) {
      fail("Unexpected status of the origin container before relocation: "
          + exitStatus1);
    }
    
    // do the actual container relocation
    Credentials cred = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    cred.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
    clc.setTokens(securityTokens);
    try {
      nmClient.startContainer(targetContainer, null);
    } catch (YarnException e) {
      throw (AssertionError)
          (new AssertionError("Exception is not expected: " + e).initCause(e));
    }
    
    try {
      // sleep until the container statuses catch up
      Thread.sleep(1000);
      exitStatus1 = nmClient.getContainerStatus(
          originContainer.getId(), originContainer.getNodeId()).getExitStatus();
      // status of the origin container after relocation
      // the container may stop succesfully before it can be killed
      assertTrue(exitStatus1 == ContainerExitStatus.KILLED_BY_APPMASTER);
    } catch (Exception e) {
      fail("Unexpected status of the origin container after relocation: " +  exitStatus1);
    }
    int exitStatus2 = 0;
    try {
      exitStatus2 = nmClient.getContainerStatus(
          targetContainer.getId(), targetContainer.getNodeId()).getExitStatus();
      // status of the target container after relocation
      assertTrue(exitStatus2 == ContainerExitStatus.INVALID || exitStatus2 ==
          ContainerExitStatus.SUCCESS);
    } catch (Exception e) {
      fail("Unexpected status of the target container after relocation: " +
          exitStatus2);
    }
    
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);
    // stop the running containers on close
    nmClient.cleanupRunningContainersOnStop(true);
    assertTrue(nmClient.getCleanupRunningContainers().get());
    nmClient.stop();
  }
  
  private List<Container> allocate(int expectedNumOfContainers) throws YarnException, IOException {
    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 2;
    List<Container> containers = new ArrayList<Container>();
    
    while (allocatedContainerCount < expectedNumOfContainers
        && iterationsLeft > 0) {
      AllocateResponse allocResponse = rmClient.allocate(0.1f);
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      containers.addAll(allocResponse.getAllocatedContainers());
      if (!allocResponse.getNMTokens().isEmpty()) {
        for (NMToken token : allocResponse.getNMTokens()) {
          rmClient.getNMTokenCache().setToken(token.getNodeId().toString(),
              token.getToken());
        }
      }
      if (allocatedContainerCount < expectedNumOfContainers) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(1000);
      }
      --iterationsLeft;
    }
    assertEquals(expectedNumOfContainers, allocatedContainerCount);
    return containers;
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}