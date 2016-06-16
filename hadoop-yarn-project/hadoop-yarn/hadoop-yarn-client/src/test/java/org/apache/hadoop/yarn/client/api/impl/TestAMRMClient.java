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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
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
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mortbay.log.Log;

import com.google.common.base.Supplier;

public class TestAMRMClient {
  static Configuration conf = null;
  static MiniYARNCluster yarnCluster = null;
  static YarnClient yarnClient = null;
  static List<NodeReport> nodeReports = null;
  static ApplicationAttemptId attemptId = null;
  static int nodeCount = 3;
  
  static final int rolling_interval_sec = 13;
  static final long am_expire_ms = 4000;

  static Resource capability;
  static Priority priority;
  static Priority priority2;
  static String node;
  static String rack;
  static String[] nodes;
  static String[] racks;
  private final static int DEFAULT_ITERATION = 3;
  
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
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    
    priority = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    capability = Resource.newInstance(1024, 1);

    node = nodeReports.get(0).getNodeId().getHost();
    rack = nodeReports.get(0).getRackName();
    nodes = new String[]{ node };
    racks = new String[]{ rack };
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
          Collections.<String, LocalResource> emptyMap(),
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
    RMAppAttempt appAttempt = null;
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
    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }
  
  @Test (timeout=60000)
  public void testAMRMClientMatchingFit() throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest>createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Resource capability1 = Resource.newInstance(1024, 2);
      Resource capability2 = Resource.newInstance(1024, 1);
      Resource capability3 = Resource.newInstance(1000, 2);
      Resource capability4 = Resource.newInstance(2000, 1);
      Resource capability5 = Resource.newInstance(1000, 3);
      Resource capability6 = Resource.newInstance(2000, 1);
      Resource capability7 = Resource.newInstance(2000, 1);

      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability1, nodes, racks, priority);
      ContainerRequest storedContainer2 = 
          new ContainerRequest(capability2, nodes, racks, priority);
      ContainerRequest storedContainer3 = 
          new ContainerRequest(capability3, nodes, racks, priority);
      ContainerRequest storedContainer4 = 
          new ContainerRequest(capability4, nodes, racks, priority);
      ContainerRequest storedContainer5 = 
          new ContainerRequest(capability5, nodes, racks, priority);
      ContainerRequest storedContainer6 = 
          new ContainerRequest(capability6, nodes, racks, priority);
      ContainerRequest storedContainer7 = 
          new ContainerRequest(capability7, nodes, racks, priority2, false);
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      amClient.addContainerRequest(storedContainer3);
      amClient.addContainerRequest(storedContainer4);
      amClient.addContainerRequest(storedContainer5);
      amClient.addContainerRequest(storedContainer6);
      amClient.addContainerRequest(storedContainer7);
      
      // test matching of containers
      List<? extends Collection<ContainerRequest>> matches;
      ContainerRequest storedRequest;
      // exact match
      Resource testCapability1 = Resource.newInstance(1024,  2);
      matches = amClient.getMatchingRequests(priority, node, testCapability1);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedContainer1, storedRequest);
      amClient.removeContainerRequest(storedContainer1);
      
      // exact matching with order maintained
      Resource testCapability2 = Resource.newInstance(2000, 1);
      matches = amClient.getMatchingRequests(priority, node, testCapability2);
      verifyMatches(matches, 2);
      // must be returned in the order they were made
      int i = 0;
      for(ContainerRequest storedRequest1 : matches.get(0)) {
        if(i++ == 0) {
          assertEquals(storedContainer4, storedRequest1);
        } else {
          assertEquals(storedContainer6, storedRequest1);
        }
      }
      amClient.removeContainerRequest(storedContainer6);
      
      // matching with larger container. all requests returned
      Resource testCapability3 = Resource.newInstance(4000, 4);
      matches = amClient.getMatchingRequests(priority, node, testCapability3);
      assert(matches.size() == 4);
      
      Resource testCapability4 = Resource.newInstance(1024, 2);
      matches = amClient.getMatchingRequests(priority, node, testCapability4);
      assert(matches.size() == 2);
      // verify non-fitting containers are not returned and fitting ones are
      for(Collection<ContainerRequest> testSet : matches) {
        assertEquals(1, testSet.size());
        ContainerRequest testRequest = testSet.iterator().next();
        assertTrue(testRequest != storedContainer4);
        assertTrue(testRequest != storedContainer5);
        assert(testRequest == storedContainer2 || 
                testRequest == storedContainer3);
      }
      
      Resource testCapability5 = Resource.newInstance(512, 4);
      matches = amClient.getMatchingRequests(priority, node, testCapability5);
      assert(matches.size() == 0);
      
      // verify requests without relaxed locality are only returned at specific
      // locations
      Resource testCapability7 = Resource.newInstance(2000, 1);
      matches = amClient.getMatchingRequests(priority2, ResourceRequest.ANY,
          testCapability7);
      assert(matches.size() == 0);
      matches = amClient.getMatchingRequests(priority2, node, testCapability7);
      assert(matches.size() == 1);
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  private void verifyMatches(
                  List<? extends Collection<ContainerRequest>> matches,
                  int matchSize) {
    assertEquals(1, matches.size());
    assertEquals(matches.get(0).size(), matchSize);
  }
  
  @Test (timeout=60000)
  public void testAMRMClientMatchingFitInferredRack() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = new AMRMClientImpl<ContainerRequest>();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Resource capability = Resource.newInstance(1024, 2);

      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, null, priority);
      amClient.addContainerRequest(storedContainer1);

      // verify matching with original node and inferred rack
      List<? extends Collection<ContainerRequest>> matches;
      ContainerRequest storedRequest;
      // exact match node
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedContainer1, storedRequest);
      // inferred match rack
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedContainer1, storedRequest);
      
      // inferred rack match no longer valid after request is removed
      amClient.removeContainerRequest(storedContainer1);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      assertTrue(matches.isEmpty());
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  @Test //(timeout=60000)
  public void testAMRMClientMatchStorage() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      Priority priority1 = Records.newRecord(Priority.class);
      priority1.setPriority(2);
      
      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, racks, priority);
      ContainerRequest storedContainer2 = 
          new ContainerRequest(capability, nodes, racks, priority);
      ContainerRequest storedContainer3 = 
          new ContainerRequest(capability, null, null, priority1);
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      amClient.addContainerRequest(storedContainer3);
      
      // test addition and storage
      int containersRequestedAny = amClient.remoteRequestsTable.get(priority,
          ResourceRequest.ANY, ExecutionType.GUARANTEED, capability)
          .remoteRequest.getNumContainers();
      assertEquals(2, containersRequestedAny);
      containersRequestedAny = amClient.remoteRequestsTable.get(priority1,
          ResourceRequest.ANY, ExecutionType.GUARANTEED, capability)
          .remoteRequest.getNumContainers();
         assertEquals(1, containersRequestedAny);
      List<? extends Collection<ContainerRequest>> matches = 
          amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 2);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 2);
      matches = 
          amClient.getMatchingRequests(priority, ResourceRequest.ANY, capability);
      verifyMatches(matches, 2);
      matches = amClient.getMatchingRequests(priority1, rack, capability);
      assertTrue(matches.isEmpty());
      matches = 
          amClient.getMatchingRequests(priority1, ResourceRequest.ANY, capability);
      verifyMatches(matches, 1);
      
      // test removal
      amClient.removeContainerRequest(storedContainer3);
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 2);
      amClient.removeContainerRequest(storedContainer2);
      matches = amClient.getMatchingRequests(priority, node, capability);
      verifyMatches(matches, 1);
      matches = amClient.getMatchingRequests(priority, rack, capability);
      verifyMatches(matches, 1);
      
      // test matching of containers
      ContainerRequest storedRequest = matches.get(0).iterator().next();
      assertEquals(storedContainer1, storedRequest);
      amClient.removeContainerRequest(storedContainer1);
      matches = 
          amClient.getMatchingRequests(priority, ResourceRequest.ANY, capability);
      assertTrue(matches.isEmpty());
      matches = 
          amClient.getMatchingRequests(priority1, ResourceRequest.ANY, capability);
      assertTrue(matches.isEmpty());
      // 0 requests left. everything got cleaned up
      assertTrue(amClient.remoteRequestsTable.isEmpty());
      
      // go through an exemplary allocation, matching and release cycle
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer3);
      // RM should allocate container within 2 calls to allocate()
      int allocatedContainerCount = 0;
      int iterationsLeft = 3;
      while (allocatedContainerCount < 2
          && iterationsLeft-- > 0) {
        Log.info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft);
        AllocateResponse allocResponse = amClient.allocate(0.1f);
        assertEquals(0, amClient.ask.size());
        assertEquals(0, amClient.release.size());
        
        assertEquals(nodeCount, amClient.getClusterNodeCount());
        allocatedContainerCount += allocResponse.getAllocatedContainers().size();
        for(Container container : allocResponse.getAllocatedContainers()) {
          ContainerRequest expectedRequest = 
              container.getPriority().equals(storedContainer1.getPriority()) ?
                  storedContainer1 : storedContainer3;
          matches = amClient.getMatchingRequests(container.getPriority(), 
                                                 ResourceRequest.ANY, 
                                                 container.getResource());
          // test correct matched container is returned
          verifyMatches(matches, 1);
          ContainerRequest matchedRequest = matches.get(0).iterator().next();
          assertEquals(matchedRequest, expectedRequest);
          amClient.removeContainerRequest(matchedRequest);
          // assign this container, use it and release it
          amClient.releaseAssignedContainer(container.getId());
        }
        if(allocatedContainerCount < containersRequestedAny) {
          // sleep to let NM's heartbeat to RM and trigger allocations
          sleep(100);
        }
      }
      
      assertEquals(2, allocatedContainerCount);
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.release.size());
      assertEquals(0, amClient.ask.size());
      assertEquals(0, allocResponse.getAllocatedContainers().size());
      // 0 requests left. everything got cleaned up
      assertTrue(amClient.remoteRequestsTable.isEmpty());      
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  @Test (timeout=60000)
  public void testAllocationWithBlacklist() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());
      
      ContainerRequest storedContainer1 = 
          new ContainerRequest(capability, nodes, racks, priority);
      amClient.addContainerRequest(storedContainer1);
      assertEquals(3, amClient.ask.size());
      assertEquals(0, amClient.release.size());
      
      List<String> localNodeBlacklist = new ArrayList<String>();
      localNodeBlacklist.add(node);
      
      // put node in black list, so no container assignment
      amClient.updateBlacklist(localNodeBlacklist, null);

      int allocatedContainerCount = getAllocatedContainersNumber(amClient,
        DEFAULT_ITERATION);
      // the only node is in blacklist, so no allocation
      assertEquals(0, allocatedContainerCount);

      // Remove node from blacklist, so get assigned with 2
      amClient.updateBlacklist(null, localNodeBlacklist);
      ContainerRequest storedContainer2 = 
              new ContainerRequest(capability, nodes, racks, priority);
      amClient.addContainerRequest(storedContainer2);
      allocatedContainerCount = getAllocatedContainersNumber(amClient,
          DEFAULT_ITERATION);
      assertEquals(2, allocatedContainerCount);
      
      // Test in case exception in allocate(), blacklist is kept
      assertTrue(amClient.blacklistAdditions.isEmpty());
      assertTrue(amClient.blacklistRemovals.isEmpty());
      
      // create a invalid ContainerRequest - memory value is minus
      ContainerRequest invalidContainerRequest = 
          new ContainerRequest(Resource.newInstance(-1024, 1),
              nodes, racks, priority);
      amClient.addContainerRequest(invalidContainerRequest);
      amClient.updateBlacklist(localNodeBlacklist, null);
      try {
        // allocate() should complain as ContainerRequest is invalid.
        amClient.allocate(0.1f);
        fail("there should be an exception here.");
      } catch (Exception e) {
        assertEquals(1, amClient.blacklistAdditions.size());
      }
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  @Test (timeout=60000)
  public void testAMRMClientWithBlacklist() throws YarnException, IOException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
            .<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      String[] nodes = {"node1", "node2", "node3"};
      
      // Add nodes[0] and nodes[1]
      List<String> nodeList01 = new ArrayList<String>();
      nodeList01.add(nodes[0]);
      nodeList01.add(nodes[1]);
      amClient.updateBlacklist(nodeList01, null);
      assertEquals(2, amClient.blacklistAdditions.size());
      assertEquals(0, amClient.blacklistRemovals.size());
      
      // Add nodes[0] again, verify it is not added duplicated.
      List<String> nodeList02 = new ArrayList<String>();
      nodeList02.add(nodes[0]);
      nodeList02.add(nodes[2]);
      amClient.updateBlacklist(nodeList02, null);
      assertEquals(3, amClient.blacklistAdditions.size());
      assertEquals(0, amClient.blacklistRemovals.size());
      
      // Add nodes[1] and nodes[2] to removal list, 
      // Verify addition list remove these two nodes.
      List<String> nodeList12 = new ArrayList<String>();
      nodeList12.add(nodes[1]);
      nodeList12.add(nodes[2]);
      amClient.updateBlacklist(null, nodeList12);
      assertEquals(1, amClient.blacklistAdditions.size());
      assertEquals(2, amClient.blacklistRemovals.size());
      
      // Add nodes[1] again to addition list, 
      // Verify removal list will remove this node.
      List<String> nodeList1 = new ArrayList<String>();
      nodeList1.add(nodes[1]);
      amClient.updateBlacklist(nodeList1, null);
      assertEquals(2, amClient.blacklistAdditions.size());
      assertEquals(1, amClient.blacklistRemovals.size());
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  private int getAllocatedContainersNumber(
      AMRMClientImpl<ContainerRequest> amClient, int iterationsLeft)
      throws YarnException, IOException {
    int allocatedContainerCount = 0;
    while (iterationsLeft-- > 0) {
      Log.info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft);
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());
        
      assertEquals(nodeCount, amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
        
      if(allocatedContainerCount == 0) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    return allocatedContainerCount;
  }

  @Test (timeout=60000)
  public void testAMRMClient() throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest>createAMRMClient();

      //setting an instance NMTokenCache
      amClient.setNMTokenCache(new NMTokenCache());
      //asserting we are not using the singleton instance cache
      Assert.assertNotSame(NMTokenCache.getSingleton(), 
          amClient.getNMTokenCache());

      amClient.init(conf);
      amClient.start();

      amClient.registerApplicationMaster("Host", 10000, "");

      testAllocation((AMRMClientImpl<ContainerRequest>)amClient);

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  @Test(timeout=30000)
  public void testAskWithNodeLabels() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();

    // add exp=x to ANY
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true, "x"));
    Assert.assertEquals(1, client.ask.size());
    Assert.assertEquals("x", client.ask.iterator().next()
        .getNodeLabelExpression());

    // add exp=x then add exp=a to ANY in same priority, only exp=a should kept
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true, "x"));
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true, "a"));
    Assert.assertEquals(1, client.ask.size());
    Assert.assertEquals("a", client.ask.iterator().next()
        .getNodeLabelExpression());
    
    // add exp=x to ANY, rack and node, only resource request has ANY resource
    // name will be assigned the label expression
    // add exp=x then add exp=a to ANY in same priority, only exp=a should kept
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true,
        "y"));
    Assert.assertEquals(1, client.ask.size());
    for (ResourceRequest req : client.ask) {
      if (ResourceRequest.ANY.equals(req.getResourceName())) {
        Assert.assertEquals("y", req.getNodeLabelExpression());
      } else {
        Assert.assertNull(req.getNodeLabelExpression());
      }
    }
    // set container with nodes and racks with labels
    client.addContainerRequest(new ContainerRequest(
        Resource.newInstance(1024, 1), new String[] { "rack1" },
        new String[] { "node1", "node2" }, Priority.UNDEFINED, true, "y"));
    for (ResourceRequest req : client.ask) {
      if (ResourceRequest.ANY.equals(req.getResourceName())) {
        Assert.assertEquals("y", req.getNodeLabelExpression());
      } else {
        Assert.assertNull(req.getNodeLabelExpression());
      }
    }
  }
  
  private void verifyAddRequestFailed(AMRMClient<ContainerRequest> client,
      ContainerRequest request) {
    try {
      client.addContainerRequest(request);
    } catch (InvalidContainerRequestException e) {
      return;
    }
    Assert.fail();
  }
  
  @Test(timeout=30000)
  public void testAskWithInvalidNodeLabels() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();

    // specified exp with more than one node labels
    verifyAddRequestFailed(client,
        new ContainerRequest(Resource.newInstance(1024, 1), null, null,
            Priority.UNDEFINED, true, "x && y"));
  }

  @Test(timeout=60000)
  public void testAMRMClientWithContainerResourceChange()
      throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.createAMRMClient();
      Assert.assertNotNull(amClient);
      // asserting we are using the singleton instance cache
      Assert.assertSame(
          NMTokenCache.getSingleton(), amClient.getNMTokenCache());
      amClient.init(conf);
      amClient.start();
      assertEquals(STATE.STARTED, amClient.getServiceState());
      // start am nm client
      NMClientImpl nmClient = (NMClientImpl) NMClient.createNMClient();
      Assert.assertNotNull(nmClient);
      // asserting we are using the singleton instance cache
      Assert.assertSame(
          NMTokenCache.getSingleton(), nmClient.getNMTokenCache());
      nmClient.init(conf);
      nmClient.start();
      assertEquals(STATE.STARTED, nmClient.getServiceState());
      // am rm client register the application master with RM
      amClient.registerApplicationMaster("Host", 10000, "");
      // allocate three containers and make sure they are in RUNNING state
      List<Container> containers =
          allocateAndStartContainers(amClient, nmClient, 3);
      // perform container resource increase and decrease tests
      doContainerResourceChange(amClient, containers);
      // unregister and finish up the test
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  private List<Container> allocateAndStartContainers(
      final AMRMClient<ContainerRequest> amClient, final NMClient nmClient,
      int num) throws YarnException, IOException {
    // set up allocation requests
    for (int i = 0; i < num; ++i) {
      amClient.addContainerRequest(
          new ContainerRequest(capability, nodes, racks, priority));
    }
    // send allocation requests
    amClient.allocate(0.1f);
    // sleep to let NM's heartbeat to RM and trigger allocations
    sleep(150);
    // get allocations
    AllocateResponse allocResponse = amClient.allocate(0.1f);
    List<Container> containers = allocResponse.getAllocatedContainers();
    Assert.assertEquals(num, containers.size());
    // build container launch context
    Credentials ts = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    ByteBuffer securityTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    // start a process long enough for increase/decrease action to take effect
    ContainerLaunchContext clc = BuilderUtils.newContainerLaunchContext(
        Collections.<String, LocalResource>emptyMap(),
        new HashMap<String, String>(), Arrays.asList("sleep", "100"),
        new HashMap<String, ByteBuffer>(), securityTokens,
        new HashMap<ApplicationAccessType, String>());
    // start the containers and make sure they are in RUNNING state
    try {
      for (int i = 0; i < num; i++) {
        Container container = containers.get(i);
        nmClient.startContainer(container, clc);
        // NodeManager may still need some time to get the stable
        // container status
        while (true) {
          ContainerStatus status = nmClient.getContainerStatus(
              container.getId(), container.getNodeId());
          if (status.getState() == ContainerState.RUNNING) {
            break;
          }
          sleep(100);
        }
      }
    } catch (YarnException e) {
      throw new AssertionError("Exception is not expected: " + e);
    }
    // sleep to let NM's heartbeat to RM to confirm container launch
    sleep(200);
    return containers;
  }


  private void doContainerResourceChange(
      final AMRMClient<ContainerRequest> amClient, List<Container> containers)
      throws YarnException, IOException {
    Assert.assertEquals(3, containers.size());
    // remember the container IDs
    Container container1 = containers.get(0);
    Container container2 = containers.get(1);
    Container container3 = containers.get(2);
    AMRMClientImpl<ContainerRequest> amClientImpl =
        (AMRMClientImpl<ContainerRequest>) amClient;
    Assert.assertEquals(0, amClientImpl.change.size());
    // verify newer request overwrites older request for the container1
    amClientImpl.requestContainerResourceChange(
        container1, Resource.newInstance(2048, 1));
    amClientImpl.requestContainerResourceChange(
        container1, Resource.newInstance(4096, 1));
    Assert.assertEquals(Resource.newInstance(4096, 1),
        amClientImpl.change.get(container1.getId()).getValue());
    // verify new decrease request cancels old increase request for container1
    amClientImpl.requestContainerResourceChange(
        container1, Resource.newInstance(512, 1));
    Assert.assertEquals(Resource.newInstance(512, 1),
        amClientImpl.change.get(container1.getId()).getValue());
    // request resource increase for container2
    amClientImpl.requestContainerResourceChange(
        container2, Resource.newInstance(2048, 1));
    Assert.assertEquals(Resource.newInstance(2048, 1),
        amClientImpl.change.get(container2.getId()).getValue());
    // verify release request will cancel pending change requests for the same
    // container
    amClientImpl.requestContainerResourceChange(
        container3, Resource.newInstance(2048, 1));
    Assert.assertEquals(3, amClientImpl.pendingChange.size());
    amClientImpl.releaseAssignedContainer(container3.getId());
    Assert.assertEquals(2, amClientImpl.pendingChange.size());
    // as of now: container1 asks to decrease to (512, 1)
    //            container2 asks to increase to (2048, 1)
    // send allocation requests
    AllocateResponse allocResponse = amClient.allocate(0.1f);
    Assert.assertEquals(0, amClientImpl.change.size());
    // we should get decrease confirmation right away
    List<Container> decreasedContainers =
        allocResponse.getDecreasedContainers();
    List<Container> increasedContainers =
        allocResponse.getIncreasedContainers();
    Assert.assertEquals(1, decreasedContainers.size());
    Assert.assertEquals(0, increasedContainers.size());
    // we should get increase allocation after the next NM's heartbeat to RM
    sleep(150);
    // get allocations
    allocResponse = amClient.allocate(0.1f);
    decreasedContainers =
        allocResponse.getDecreasedContainers();
    increasedContainers =
        allocResponse.getIncreasedContainers();
    Assert.assertEquals(1, increasedContainers.size());
    Assert.assertEquals(0, decreasedContainers.size());
  }

  private void testAllocation(final AMRMClientImpl<ContainerRequest> amClient)
      throws YarnException, IOException {
    // setup container request
    
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());
    
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    
    int containersRequestedNode = amClient.remoteRequestsTable.get(priority,
        node, ExecutionType.GUARANTEED, capability).remoteRequest
        .getNumContainers();
    int containersRequestedRack = amClient.remoteRequestsTable.get(priority,
        rack, ExecutionType.GUARANTEED, capability).remoteRequest
        .getNumContainers();
    int containersRequestedAny = amClient.remoteRequestsTable.get(priority,
        ResourceRequest.ANY, ExecutionType.GUARANTEED, capability)
        .remoteRequest.getNumContainers();

    assertEquals(2, containersRequestedNode);
    assertEquals(2, containersRequestedRack);
    assertEquals(2, containersRequestedAny);
    assertEquals(3, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 3;
    Set<ContainerId> releases = new TreeSet<ContainerId>();
    
    amClient.getNMTokenCache().clearCache();
    Assert.assertEquals(0, amClient.getNMTokenCache().numberOfTokensInCache());
    HashMap<String, Token> receivedNMTokens = new HashMap<String, Token>();
    
    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());
      
      assertEquals(nodeCount, amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      for(Container container : allocResponse.getAllocatedContainers()) {
        ContainerId rejectContainerId = container.getId();
        releases.add(rejectContainerId);
        amClient.releaseAssignedContainer(rejectContainerId);
      }
      
      for (NMToken token : allocResponse.getNMTokens()) {
        String nodeID = token.getNodeId().toString();
        if (receivedNMTokens.containsKey(nodeID)) {
          Assert.fail("Received token again for : " + nodeID);          
        }
        receivedNMTokens.put(nodeID, token.getToken());
      }
      
      if(allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    
    // Should receive atleast 1 token
    Assert.assertTrue(receivedNMTokens.size() > 0
        && receivedNMTokens.size() <= nodeCount);
    
    assertEquals(allocatedContainerCount, containersRequestedAny);
    assertEquals(2, amClient.release.size());
    assertEquals(0, amClient.ask.size());
    
    // need to tell the AMRMClient that we dont need these resources anymore
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    assertEquals(3, amClient.ask.size());
    // send 0 container count request for resources that are no longer needed
    ResourceRequest snoopRequest = amClient.ask.iterator().next();
    assertEquals(0, snoopRequest.getNumContainers());
    
    // test RPC exception handling
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority));
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority));
    snoopRequest = amClient.ask.iterator().next();
    assertEquals(2, snoopRequest.getNumContainers());
    
    ApplicationMasterProtocol realRM = amClient.rmClient;
    try {
      ApplicationMasterProtocol mockRM = mock(ApplicationMasterProtocol.class);
      when(mockRM.allocate(any(AllocateRequest.class))).thenAnswer(
          new Answer<AllocateResponse>() {
            public AllocateResponse answer(InvocationOnMock invocation)
                throws Exception {
              amClient.removeContainerRequest(
                             new ContainerRequest(capability, nodes, 
                                                          racks, priority));
              amClient.removeContainerRequest(
                  new ContainerRequest(capability, nodes, racks, priority));
              throw new Exception();
            }
          });
      amClient.rmClient = mockRM;
      amClient.allocate(0.1f);
    }catch (Exception ioe) {}
    finally {
      amClient.rmClient = realRM;
    }

    assertEquals(2, amClient.release.size());
    assertEquals(3, amClient.ask.size());
    snoopRequest = amClient.ask.iterator().next();
    // verify that the remove request made in between makeRequest and allocate 
    // has not been lost
    assertEquals(0, snoopRequest.getNumContainers());
    
    iterationsLeft = 3;
    // do a few iterations to ensure RM is not going send new containers
    while(!releases.isEmpty() || iterationsLeft-- > 0) {
      // inform RM of rejection
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      // RM did not send new containers because AM does not need any
      assertEquals(0, allocResponse.getAllocatedContainers().size());
      if(allocResponse.getCompletedContainersStatuses().size() > 0) {
        for(ContainerStatus cStatus :allocResponse
            .getCompletedContainersStatuses()) {
          if(releases.contains(cStatus.getContainerId())) {
            assertEquals(cStatus.getState(), ContainerState.COMPLETE);
            assertEquals(-100, cStatus.getExitStatus());
            releases.remove(cStatus.getContainerId());
          }
        }
      }
      if(iterationsLeft > 0) {
        // sleep to make sure NM's heartbeat
        sleep(100);
      }
    }
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());
  }

  class CountDownSupplier implements Supplier<Boolean> {
    int counter = 0;
    @Override
    public Boolean get() {
      counter++;
      if (counter >= 3) {
        return true;
      } else {
        return false;
      }
    }
  };

  @Test
  public void testWaitFor() throws InterruptedException {
    AMRMClientImpl<ContainerRequest> amClient = null;
    CountDownSupplier countDownChecker = new CountDownSupplier();

    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<ContainerRequest>) AMRMClient
              .<ContainerRequest> createAMRMClient();
      amClient.init(new YarnConfiguration());
      amClient.start();
      amClient.waitFor(countDownChecker, 1000);
      assertEquals(3, countDownChecker.counter);
    } finally {
      if (amClient != null) {
        amClient.stop();
      }
    }
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test(timeout = 60000)
  public void testAMRMClientOnAMRMTokenRollOver() throws YarnException,
      IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      AMRMTokenSecretManager amrmTokenSecretManager =
          yarnCluster.getResourceManager().getRMContext()
            .getAMRMTokenSecretManager();

      // start am rm client
      amClient = AMRMClient.<ContainerRequest> createAMRMClient();

      amClient.init(conf);
      amClient.start();

      Long startTime = System.currentTimeMillis();
      amClient.registerApplicationMaster("Host", 10000, "");

      org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken_1 =
          getAMRMToken();
      Assert.assertNotNull(amrmToken_1);
      Assert.assertEquals(amrmToken_1.decodeIdentifier().getKeyId(),
        amrmTokenSecretManager.getMasterKey().getMasterKey().getKeyId());

      // Wait for enough time and make sure the roll_over happens
      // At mean time, the old AMRMToken should continue to work
      while (System.currentTimeMillis() - startTime <
          rolling_interval_sec * 1000) {
        amClient.allocate(0.1f);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      amClient.allocate(0.1f);

      org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken_2 =
          getAMRMToken();
      Assert.assertNotNull(amrmToken_2);
      Assert.assertEquals(amrmToken_2.decodeIdentifier().getKeyId(),
        amrmTokenSecretManager.getMasterKey().getMasterKey().getKeyId());

      Assert.assertNotEquals(amrmToken_1, amrmToken_2);

      // can do the allocate call with latest AMRMToken
      AllocateResponse response = amClient.allocate(0.1f);
      
      // Verify latest AMRMToken can be used to send allocation request.
      UserGroupInformation testUser1 =
          UserGroupInformation.createRemoteUser("testUser1");
      
      AMRMTokenIdentifierForTest newVersionTokenIdentifier = 
          new AMRMTokenIdentifierForTest(amrmToken_2.decodeIdentifier(), "message");
      
      Assert.assertEquals("Message is changed after set to newVersionTokenIdentifier",
          "message", newVersionTokenIdentifier.getMessage());
      org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> newVersionToken = 
          new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> (
              newVersionTokenIdentifier.getBytes(), 
              amrmTokenSecretManager.retrievePassword(newVersionTokenIdentifier),
              newVersionTokenIdentifier.getKind(), new Text());
      
      SecurityUtil.setTokenService(newVersionToken, yarnCluster
        .getResourceManager().getApplicationMasterService().getBindAddress());
      testUser1.addToken(newVersionToken);
      
      AllocateRequest request = Records.newRecord(AllocateRequest.class);
      request.setResponseId(response.getResponseId());
      testUser1.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
        @Override
        public ApplicationMasterProtocol run() {
          return (ApplicationMasterProtocol) YarnRPC.create(conf).getProxy(
            ApplicationMasterProtocol.class,
            yarnCluster.getResourceManager().getApplicationMasterService()
                .getBindAddress(), conf);
        }
      }).allocate(request);

      // Make sure previous token has been rolled-over
      // and can not use this rolled-over token to make a allocate all.
      while (true) {
        if (amrmToken_2.decodeIdentifier().getKeyId() != amrmTokenSecretManager
          .getCurrnetMasterKeyData().getMasterKey().getKeyId()) {
          if (amrmTokenSecretManager.getNextMasterKeyData() == null) {
            break;
          } else if (amrmToken_2.decodeIdentifier().getKeyId() !=
              amrmTokenSecretManager.getNextMasterKeyData().getMasterKey()
              .getKeyId()) {
            break;
          }
        }
        amClient.allocate(0.1f);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // DO NOTHING
        }
      }

      try {
        UserGroupInformation testUser2 =
            UserGroupInformation.createRemoteUser("testUser2");
        SecurityUtil.setTokenService(amrmToken_2, yarnCluster
          .getResourceManager().getApplicationMasterService().getBindAddress());
        testUser2.addToken(amrmToken_2);
        testUser2.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) YarnRPC.create(conf).getProxy(
              ApplicationMasterProtocol.class,
              yarnCluster.getResourceManager().getApplicationMasterService()
                .getBindAddress(), conf);
          }
        }).allocate(Records.newRecord(AllocateRequest.class));
        Assert.fail("The old Token should not work");
      } catch (Exception ex) {
        Assert.assertTrue(ex instanceof InvalidToken);
        Assert.assertTrue(ex.getMessage().contains(
          "Invalid AMRMToken from "
              + amrmToken_2.decodeIdentifier().getApplicationAttemptId()));
      }

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
      getAMRMToken() throws IOException {
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    Iterator<org.apache.hadoop.security.token.Token<?>> iter =
        credentials.getAllTokens().iterator();
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> result = null;
    while (iter.hasNext()) {
      org.apache.hadoop.security.token.Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        if (result != null) {
          Assert.fail("credentials has more than one AMRM token."
              + " token1: " + result + " token2: " + token);
        }
        result = (org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>)
            token;
      }
    }
    return result;
  }
}
