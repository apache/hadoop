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
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerMoveRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests container relocation logic between <code>ApplicationMaster</code>
 * and <code>ResourceManager</code>
 */
public class TestRMContainerMove {
  private static final Log LOG = LogFactory.getLog(TestRMContainerMove.class);
  
  static Configuration conf = null;
  static MiniYARNCluster yarnCluster = null;
  static YarnClient yarnClient = null;
  static List<NodeReport> nodeReports = null;
  static ApplicationAttemptId attemptId = null;
  static int nodeCount = 3;
  
  static final int rolling_interval_sec = 13;
  static final long am_expire_ms = 4000;
  
  static Priority priority1;
  static Priority priority2;
  static Priority priority3;
  static Resource capability1;
  static Resource capability2;
  static Resource capability3;
  static String node1;
  static String node2;
  static String node3;
  static String[] nodes1;
  static String[] nodes2;
  static String[] nodes3;
  static String[] nodes;
  static String rack1;
  static String rack2;
  static String rack3;
  static String[] racks1;
  static String[] racks2;
  static String[] racks3;
  static String[] racks;
  
  private final static int DEFAULT_ITERATION = 2;
  
  @BeforeClass
  public static void setup() throws Exception {
    // start minicluster
    conf = new YarnConfiguration();
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        rolling_interval_sec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, am_expire_ms);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    // setting FifoScheduler as the resource scheduler as currently, container relocation is only
    // implemented for FifoScheduler
    conf.set(YarnConfiguration.RM_SCHEDULER, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".scheduler.fifo.FifoScheduler");
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    
    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    
    priority1 = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    priority3 = Priority.newInstance(3);
    
    capability1 = Resource.newInstance(512, 1);
    capability2 = Resource.newInstance(1024, 1);
    capability3 = Resource.newInstance(1536, 1);
    
    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    
    node1 = nodeReports.get(0).getNodeId().getHost();
    node2 = nodeReports.get(1).getNodeId().getHost();
    node3 = nodeReports.get(2).getNodeId().getHost();
    nodes1 = new String[]{node1};
    nodes2 = new String[]{node2};
    nodes3 = new String[]{node3};
    nodes = new String[]{node1, node2, node3};
    
    rack1 = nodeReports.get(0).getRackName();
    rack2 = nodeReports.get(1).getRackName();
    rack3 = nodeReports.get(2).getRackName();
    racks1 = new String[]{rack1};
    racks2 = new String[]{rack2};
    racks3 = new String[]{rack3};
    racks = new String[]{rack1, rack2, rack3};
  }
  
  @Before
  public void startApp() throws Exception {
    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
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
    RMAppAttempt appAttempt;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
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
    }
    
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    
    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(ClientRMProxy.getAMRMTokenService(conf));
  }
  
  @After
  public void cancelApp() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;
  }
  
  @AfterClass
  public static void tearDown() {
    if (yarnClient != null && yarnClient.getServiceState() == Service.STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == Service.STATE.STARTED) {
      yarnCluster.stop();
    }
  }
  
  @Test(timeout = 60000)
  public void testAMRMClientContainerMove() throws YarnException, IOException {
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
    try {
      // start am rm client and register the application master
      amClient =
          (AMRMClientImpl<AMRMClient.ContainerRequest>) AMRMClient
              .<AMRMClient.ContainerRequest>createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.moveAsk.size());
      assertEquals(0, amClient.pendingMoveAsk.size());
      
      // allocating two regular containers
      AMRMClient.ContainerRequest storedContainer1 =
          new AMRMClient.ContainerRequest(capability1, nodes1, null, priority1);
      AMRMClient.ContainerRequest storedContainer2 =
          new AMRMClient.ContainerRequest(capability2, nodes2, null, priority2);
      
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      assertEquals(6, amClient.ask.size());
      assertEquals(0, amClient.moveAsk.size());
      assertEquals(0, amClient.pendingMoveAsk.size());
      
      List<Container> allocatedContainers = getAllocatedContainers(amClient, DEFAULT_ITERATION);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.moveAsk.size());
      assertEquals(0, amClient.pendingMoveAsk.size());
      
      // relocating a container to another host
      // In local mode, this host is the same as before,
      // but it is not important here
      ContainerId originContainerId = getContainerByCapability(capability1, allocatedContainers)
          .getId();
      NodeId targetNodeId = nodeReports.get(0).getNodeId();
      ContainerMoveRequest moveRequest = ContainerMoveRequest.newInstance(priority1,
          originContainerId, targetNodeId.getHost());
      
      amClient.addContainerMoveRequest(moveRequest);
      assertEquals(0, amClient.ask.size());
      assertEquals(1, amClient.moveAsk.size());
      assertEquals(1, amClient.pendingMoveAsk.size());
      
      amClient.allocate(0.1f);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.moveAsk.size());
      assertEquals(1, amClient.pendingMoveAsk.size());
      
      allocatedContainers = getAllocatedContainers(amClient, DEFAULT_ITERATION);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.moveAsk.size());
      assertEquals(0, amClient.pendingMoveAsk.size());
      
      // select move responses
      List<Container> moveResponses = new ArrayList<Container>();
      for (Container c : allocatedContainers) {
        if (c.getIsMove()) {
          moveResponses.add(c);
        }
      }
      
      // verify that relocation informations are set correctly in the move response
      assertEquals(1, moveResponses.size());
      Container moveResponse = moveResponses.get(0);
      assertEquals(targetNodeId.getHost(), moveResponse.getNodeId().getHost());
      assertEquals(originContainerId, moveResponse.getOriginContainerId());
      assertEquals(priority1, moveResponse.getPriority());
      assertEquals(capability1, moveResponse.getResource());
      assertNotEquals(originContainerId, moveResponse.getId());
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);
    } finally {
      if (amClient != null && amClient.getServiceState() == Service.STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  private List<Container> getAllocatedContainers(
      AMRMClientImpl<AMRMClient.ContainerRequest> amClient, int iterationsLeft)
      throws YarnException, IOException {
    List<Container> allocatedContainers = new ArrayList<Container>();
    while (iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.moveAsk.size());
      
      assertEquals(nodeCount, amClient.getClusterNodeCount());
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      
      if (allocatedContainers.isEmpty()) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    return allocatedContainers;
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  private Container getContainerByCapability(Resource capability, List<Container> containers) {
    for (Container c : containers) {
      if (c.getResource().equals(capability)) {
        return c;
      }
    }
    throw new RuntimeException("Container with capability " + capability + " not found");
  }
}