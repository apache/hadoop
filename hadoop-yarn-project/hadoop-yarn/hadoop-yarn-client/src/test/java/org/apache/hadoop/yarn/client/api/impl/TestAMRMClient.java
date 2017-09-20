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
import java.util.function.Supplier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
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
import org.apache.hadoop.yarn.api.records.*;
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
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.eclipse.jetty.util.log.Log;


/**
 * Test application master client class to resource manager.
 */
@RunWith(value = Parameterized.class)
public class TestAMRMClient {
  private String schedulerName = null;
  private Configuration conf = null;
  private MiniYARNCluster yarnCluster = null;
  private YarnClient yarnClient = null;
  private List<NodeReport> nodeReports = null;
  private ApplicationAttemptId attemptId = null;
  private int nodeCount = 3;
  
  static final int rolling_interval_sec = 13;
  static final long am_expire_ms = 4000;

  private Resource capability;
  private Priority priority;
  private Priority priority2;
  private String node;
  private String rack;
  private String[] nodes;
  private String[] racks;
  private final static int DEFAULT_ITERATION = 3;

  public TestAMRMClient(String schedulerName) {
    this.schedulerName = schedulerName;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> list = new ArrayList<Object[]>(2);
    list.add(new Object[] {CapacityScheduler.class.getName()});
    list.add(new Object[] {FairScheduler.class.getName()});
    return list;
  }

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();
    createClusterAndStartApplication(conf);
  }

  private void createClusterAndStartApplication(Configuration conf)
      throws Exception {
    // start minicluster
    this.conf = conf;
    conf.set(YarnConfiguration.RM_SCHEDULER, schedulerName);
    conf.setLong(
      YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
      rolling_interval_sec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, am_expire_ms);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    assertTrue("All node managers did not connect to the RM within the "
        + "allotted 5-second timeout",
        yarnCluster.waitForNodeManagersToConnect(5000L));
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    assertEquals("Not all node managers were reported running",
        nodeCount, nodeReports.size());

    priority = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    capability = Resource.newInstance(1024, 1);

    node = nodeReports.get(0).getNodeId().getHost();
    rack = nodeReports.get(0).getRackName();
    nodes = new String[]{ node };
    racks = new String[]{ rack };

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
  public void teardown() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;

    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }

  @Test (timeout = 60000)
  public void testAMRMClientNoMatchingRequests()
      throws IOException, YarnException {
    AMRMClient<ContainerRequest> amClient =  AMRMClient.createAMRMClient();
    amClient.init(conf);
    amClient.start();
    amClient.registerApplicationMaster("Host", 10000, "");

    Resource testCapability1 = Resource.newInstance(1024,  2);
    List<? extends Collection<ContainerRequest>> matches =
        amClient.getMatchingRequests(priority, node, testCapability1);
    assertEquals("Expected no matching requests.", matches.size(), 0);
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

      // Add some CRs with allocReqIds... These will not be returned by
      // the default getMatchingRequests
      ContainerRequest storedContainer11 =
          new ContainerRequest(capability1, nodes, racks, priority, 1);
      ContainerRequest storedContainer33 =
          new ContainerRequest(capability3, nodes, racks, priority, 3);
      ContainerRequest storedContainer43 =
          new ContainerRequest(capability4, nodes, racks, priority, 3);
      amClient.addContainerRequest(storedContainer11);
      amClient.addContainerRequest(storedContainer33);
      amClient.addContainerRequest(storedContainer43);
      
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

      // exact match for allocReqId 1
      Collection<ContainerRequest> reqIdMatches =
          amClient.getMatchingRequests(1);
      assertEquals(1, reqIdMatches.size());
      storedRequest = reqIdMatches.iterator().next();
      assertEquals(storedContainer11, storedRequest);
      amClient.removeContainerRequest(storedContainer11);

      // exact match for allocReqId 3
      reqIdMatches = amClient.getMatchingRequests(3);
      assertEquals(2, reqIdMatches.size());
      Iterator<ContainerRequest> iter = reqIdMatches.iterator();
      storedRequest = iter.next();
      assertEquals(storedContainer43, storedRequest);
      amClient.removeContainerRequest(storedContainer43);
      storedRequest = iter.next();
      assertEquals(storedContainer33, storedRequest);
      amClient.removeContainerRequest(storedContainer33);
      
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

  /**
   * Test fit of both GUARANTEED and OPPORTUNISTIC containers.
   */
  @Test (timeout=60000)
  public void testAMRMClientMatchingFitExecType()
      throws YarnException, IOException {
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
      Resource capability4 = Resource.newInstance(1000, 2);
      Resource capability5 = Resource.newInstance(2000, 2);
      Resource capability6 = Resource.newInstance(2000, 3);
      Resource capability7 = Resource.newInstance(6000, 3);

      // Add 2 GUARANTEED and 7 OPPORTUNISTIC requests.
      ContainerRequest storedGuarContainer1 =
          new ContainerRequest(capability1, nodes, racks, priority);
      ContainerRequest storedGuarContainer2 =
          new ContainerRequest(capability2, nodes, racks, priority);
      ContainerRequest storedOpportContainer1 =
          new ContainerRequest(capability1, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer2 =
          new ContainerRequest(capability2, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer3 =
          new ContainerRequest(capability3, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer4 =
          new ContainerRequest(capability4, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer5 =
          new ContainerRequest(capability5, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer6 =
          new ContainerRequest(capability6, nodes, racks, priority,
              0, true, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      ContainerRequest storedOpportContainer7 =
          new ContainerRequest(capability7, nodes, racks, priority2,
              0, false, null,
              ExecutionTypeRequest.newInstance(ExecutionType.OPPORTUNISTIC));
      amClient.addContainerRequest(storedGuarContainer1);
      amClient.addContainerRequest(storedGuarContainer2);
      amClient.addContainerRequest(storedOpportContainer1);
      amClient.addContainerRequest(storedOpportContainer2);
      amClient.addContainerRequest(storedOpportContainer3);
      amClient.addContainerRequest(storedOpportContainer4);
      amClient.addContainerRequest(storedOpportContainer5);
      amClient.addContainerRequest(storedOpportContainer6);
      amClient.addContainerRequest(storedOpportContainer7);

      // Make sure 3 entries are generated in the ask list for each added
      // container request of a given capability, locality, execution type and
      // priority (one node-local, one rack-local, and one ANY).
      assertEquals(24,
          (((AMRMClientImpl<ContainerRequest>) amClient).ask.size()));

      // test exact matching of GUARANTEED containers
      List<? extends Collection<ContainerRequest>> matches;
      ContainerRequest storedRequest;
      Resource testCapability1 = Resource.newInstance(1024,  2);
      matches = amClient
          .getMatchingRequests(priority, node, ExecutionType.GUARANTEED,
              testCapability1);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedGuarContainer1, storedRequest);
      amClient.removeContainerRequest(storedGuarContainer1);

      // test exact matching of OPPORTUNISTIC containers
      matches = amClient.getMatchingRequests(priority, node,
          ExecutionType.OPPORTUNISTIC, testCapability1);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedOpportContainer1, storedRequest);
      amClient.removeContainerRequest(storedOpportContainer1);

      // exact OPPORTUNISTIC matching with order maintained
      Resource testCapability2 = Resource.newInstance(1000, 2);
      matches = amClient.getMatchingRequests(priority, node,
          ExecutionType.OPPORTUNISTIC, testCapability2);
      verifyMatches(matches, 2);
      // must be returned in the order they were made
      int i = 0;
      for(ContainerRequest storedRequest1 : matches.get(0)) {
        if(i++ == 0) {
          assertEquals(storedOpportContainer3, storedRequest1);
        } else {
          assertEquals(storedOpportContainer4, storedRequest1);
        }
      }
      amClient.removeContainerRequest(storedOpportContainer3);

      // matching with larger container
      Resource testCapability3 = Resource.newInstance(4000, 4);
      matches = amClient.getMatchingRequests(priority, node,
          ExecutionType.OPPORTUNISTIC, testCapability3);
      assert(matches.size() == 4);

      // verify requests without relaxed locality are only returned at specific
      // locations
      Resource testCapability4 = Resource.newInstance(6000, 3);
      matches = amClient.getMatchingRequests(priority2, ResourceRequest.ANY,
          ExecutionType.OPPORTUNISTIC, testCapability4);
      assert(matches.size() == 0);
      matches = amClient.getMatchingRequests(priority2, node,
          ExecutionType.OPPORTUNISTIC, testCapability4);
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
    assertEquals(matchSize, matches.get(0).size());
  }
  
  @Test (timeout=60000)
  public void testAMRMClientMatchingFitInferredRack()
      throws YarnException, IOException {
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

      amClient
          .unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null,
              null);

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

      ProfileCapability profileCapability =
          ProfileCapability.newInstance(capability);
      
      // test addition and storage
      RemoteRequestsTable<ContainerRequest> remoteRequestsTable =
          amClient.getTable(0);
      int containersRequestedAny = remoteRequestsTable.get(priority,
          ResourceRequest.ANY, ExecutionType.GUARANTEED, profileCapability)
          .remoteRequest.getNumContainers();
      assertEquals(2, containersRequestedAny);
      containersRequestedAny = remoteRequestsTable.get(priority1,
          ResourceRequest.ANY, ExecutionType.GUARANTEED, profileCapability)
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
      assertTrue(amClient.getTable(0).isEmpty());
      
      // go through an exemplary allocation, matching and release cycle
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer3);
      // RM should allocate container within 2 calls to allocate()
      int allocatedContainerCount = 0;
      int iterationsLeft = 3;
      while (allocatedContainerCount < 2
          && iterationsLeft-- > 0) {
        Log.getLog().info("Allocated " + allocatedContainerCount + " containers"
            + " with " + iterationsLeft + " iterations left");
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
          // let NM heartbeat to RM and trigger allocations
          triggerSchedulingWithNMHeartBeat();
        }
      }
      
      assertEquals(2, allocatedContainerCount);
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.release.size());
      assertEquals(0, amClient.ask.size());
      assertEquals(0, allocResponse.getAllocatedContainers().size());
      // 0 requests left. everything got cleaned up
      assertTrue(remoteRequestsTable.isEmpty());
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  /**
   * Make sure we get allocations regardless of timing issues.
   */
  private void triggerSchedulingWithNMHeartBeat() {
    // Simulate fair scheduler update thread
    RMContext context = yarnCluster.getResourceManager().getRMContext();
    if (context.getScheduler() instanceof FairScheduler) {
      FairScheduler scheduler = (FairScheduler)context.getScheduler();
      scheduler.update();
    }
    // Trigger NM's heartbeat to RM and trigger allocations
    for (RMNode rmNode : context.getRMNodes().values()) {
      context.getScheduler().handle(new NodeUpdateSchedulerEvent(rmNode));
    }
    if (context.getScheduler() instanceof FairScheduler) {
      FairScheduler scheduler = (FairScheduler)context.getScheduler();
      scheduler.update();
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
      Log.getLog().info("Allocated " + allocatedContainerCount + " containers"
          + " with " + iterationsLeft + " iterations left");
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());
        
      assertEquals(nodeCount, amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
        
      if(allocatedContainerCount == 0) {
        // let NM heartbeat to RM and trigger allocations
        triggerSchedulingWithNMHeartBeat();
      }
    }
    return allocatedContainerCount;
  }

  @Test (timeout=60000)
  public void testAMRMClient() throws YarnException, IOException {
    initAMRMClientAndTest(false);
  }

  @Test (timeout=60000)
  public void testAMRMClientAllocReqId() throws YarnException, IOException {
    initAMRMClientAndTest(true);
  }

  @Test (timeout=60000)
  public void testAMRMClientWithSaslEncryption() throws Exception {
    // we have to create a new instance of MiniYARNCluster to avoid SASL qop
    // mismatches between client and server
    teardown();
    conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, "privacy");
    createClusterAndStartApplication(conf);
    initAMRMClientAndTest(false);
  }

  private void initAMRMClientAndTest(boolean useAllocReqId)
      throws YarnException, IOException {
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

      if (useAllocReqId) {
        testAllocRequestId((AMRMClientImpl<ContainerRequest>)amClient);
      } else {
        testAllocation((AMRMClientImpl<ContainerRequest>) amClient);
      }

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
    assertEquals(1, client.ask.size());
    assertEquals("x", client.ask.iterator().next()
        .getNodeLabelExpression());

    // add exp=x then add exp=a to ANY in same priority, only exp=a should kept
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true, "x"));
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true, "a"));
    assertEquals(1, client.ask.size());
    assertEquals("a", client.ask.iterator().next()
        .getNodeLabelExpression());
    
    // add exp=x to ANY, rack and node, only resource request has ANY resource
    // name will be assigned the label expression
    // add exp=x then add exp=a to ANY in same priority, only exp=a should kept
    client.addContainerRequest(new ContainerRequest(Resource.newInstance(1024,
        1), null, null, Priority.UNDEFINED, true,
        "y"));
    assertEquals(1, client.ask.size());
    for (ResourceRequest req : client.ask) {
      if (ResourceRequest.ANY.equals(req.getResourceName())) {
        assertEquals("y", req.getNodeLabelExpression());
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
        assertEquals("y", req.getNodeLabelExpression());
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
    fail();
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
    // Fair scheduler does not support resource change
    Assume.assumeTrue(schedulerName.equals(CapacityScheduler.class.getName()));
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
    // let NM heartbeat to RM and trigger allocations
    triggerSchedulingWithNMHeartBeat();
    // get allocations
    AllocateResponse allocResponse = amClient.allocate(0.1f);
    List<Container> containers = allocResponse.getAllocatedContainers();
    assertEquals(num, containers.size());

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
          sleep(10);
        }
      }
    } catch (YarnException e) {
      throw new AssertionError("Exception is not expected: " + e);
    }
    // let NM's heartbeat to RM to confirm container launch
    triggerSchedulingWithNMHeartBeat();
    return containers;
  }


  private void doContainerResourceChange(
      final AMRMClient<ContainerRequest> amClient, List<Container> containers)
      throws YarnException, IOException {
    assertEquals(3, containers.size());
    // remember the container IDs
    Container container1 = containers.get(0);
    Container container2 = containers.get(1);
    Container container3 = containers.get(2);
    AMRMClientImpl<ContainerRequest> amClientImpl =
        (AMRMClientImpl<ContainerRequest>) amClient;
    assertEquals(0, amClientImpl.change.size());
    // verify newer request overwrites older request for the container1
    amClientImpl.requestContainerUpdate(container1,
        UpdateContainerRequest.newInstance(container1.getVersion(),
            container1.getId(), ContainerUpdateType.INCREASE_RESOURCE,
            Resource.newInstance(2048, 1), null));
    amClientImpl.requestContainerUpdate(container1,
        UpdateContainerRequest.newInstance(container1.getVersion(),
            container1.getId(), ContainerUpdateType.INCREASE_RESOURCE,
            Resource.newInstance(4096, 1), null));
    assertEquals(Resource.newInstance(4096, 1),
        amClientImpl.change.get(container1.getId()).getValue().getCapability());
    // verify new decrease request cancels old increase request for container1
    amClientImpl.requestContainerUpdate(container1,
        UpdateContainerRequest.newInstance(container1.getVersion(),
            container1.getId(), ContainerUpdateType.DECREASE_RESOURCE,
            Resource.newInstance(512, 1), null));
    assertEquals(Resource.newInstance(512, 1),
        amClientImpl.change.get(container1.getId()).getValue().getCapability());
    // request resource increase for container2
    amClientImpl.requestContainerUpdate(container2,
        UpdateContainerRequest.newInstance(container2.getVersion(),
            container2.getId(), ContainerUpdateType.INCREASE_RESOURCE,
            Resource.newInstance(2048, 1), null));
    assertEquals(Resource.newInstance(2048, 1),
        amClientImpl.change.get(container2.getId()).getValue().getCapability());
    // verify release request will cancel pending change requests for the same
    // container
    amClientImpl.requestContainerUpdate(container3,
        UpdateContainerRequest.newInstance(container3.getVersion(),
            container3.getId(), ContainerUpdateType.INCREASE_RESOURCE,
            Resource.newInstance(2048, 1), null));
    assertEquals(3, amClientImpl.pendingChange.size());
    amClientImpl.releaseAssignedContainer(container3.getId());
    assertEquals(2, amClientImpl.pendingChange.size());
    // as of now: container1 asks to decrease to (512, 1)
    //            container2 asks to increase to (2048, 1)
    // send allocation requests
    AllocateResponse allocResponse = amClient.allocate(0.1f);
    assertEquals(0, amClientImpl.change.size());
    // we should get decrease confirmation right away
    List<UpdatedContainer> updatedContainers =
        allocResponse.getUpdatedContainers();
    assertEquals(1, updatedContainers.size());
    // we should get increase allocation after the next NM's heartbeat to RM
    triggerSchedulingWithNMHeartBeat();
    // get allocations
    allocResponse = amClient.allocate(0.1f);
    updatedContainers =
        allocResponse.getUpdatedContainers();
    assertEquals(1, updatedContainers.size());
  }

  @Test(timeout=60000)
  public void testAMRMClientWithContainerPromotion()
      throws YarnException, IOException {
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient =
        (AMRMClientImpl<AMRMClient.ContainerRequest>) AMRMClient
            .createAMRMClient();
    //asserting we are not using the singleton instance cache
    Assert.assertSame(NMTokenCache.getSingleton(),
        amClient.getNMTokenCache());
    amClient.init(conf);
    amClient.start();

    // start am nm client
    NMClientImpl nmClient = (NMClientImpl) NMClient.createNMClient();
    Assert.assertNotNull(nmClient);
    // asserting we are using the singleton instance cache
    Assert.assertSame(
        NMTokenCache.getSingleton(), nmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();
    assertEquals(STATE.STARTED, nmClient.getServiceState());

    amClient.registerApplicationMaster("Host", 10000, "");
    // setup container request
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // START OPPORTUNISTIC Container, Send allocation request to RM
    amClient.addContainerRequest(
        new AMRMClient.ContainerRequest(capability, null, null, priority2, 0,
            true, null, ExecutionTypeRequest
            .newInstance(ExecutionType.OPPORTUNISTIC, true)));

    ProfileCapability profileCapability =
          ProfileCapability.newInstance(capability);
    int oppContainersRequestedAny =
        amClient.getTable(0).get(priority2, ResourceRequest.ANY,
            ExecutionType.OPPORTUNISTIC, profileCapability).remoteRequest
            .getNumContainers();

    assertEquals(1, oppContainersRequestedAny);
    assertEquals(1, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    Map<ContainerId, Container> allocatedOpportContainers = new HashMap<>();
    int iterationsLeft = 50;

    amClient.getNMTokenCache().clearCache();
    assertEquals(0,
        amClient.getNMTokenCache().numberOfTokensInCache());

    AllocateResponse allocResponse = null;
    while (allocatedContainerCount < oppContainersRequestedAny
        && iterationsLeft-- > 0) {
      allocResponse = amClient.allocate(0.1f);
      // let NM heartbeat to RM and trigger allocations
      //triggerSchedulingWithNMHeartBeat();
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      allocatedContainerCount +=
          allocResponse.getAllocatedContainers().size();
      for (Container container : allocResponse.getAllocatedContainers()) {
        if (container.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
          allocatedOpportContainers.put(container.getId(), container);
        }
      }
      if (allocatedContainerCount < oppContainersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }

    assertEquals(oppContainersRequestedAny, allocatedContainerCount);
    assertEquals(oppContainersRequestedAny, allocatedOpportContainers.size());

    startContainer(allocResponse, nmClient);

    // SEND PROMOTION REQUEST TO RM
    try {
      Container c = allocatedOpportContainers.values().iterator().next();
      amClient.requestContainerUpdate(
          c, UpdateContainerRequest.newInstance(c.getVersion(),
              c.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
              null, ExecutionType.OPPORTUNISTIC));
      fail("Should throw Exception..");
    } catch (IllegalArgumentException e) {
      System.out.println("## " + e.getMessage());
      assertTrue(e.getMessage().contains(
          "target should be GUARANTEED and original should be OPPORTUNISTIC"));
    }

    Container c = allocatedOpportContainers.values().iterator().next();
    amClient.requestContainerUpdate(
        c, UpdateContainerRequest.newInstance(c.getVersion(),
            c.getId(), ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
            null, ExecutionType.GUARANTEED));
    iterationsLeft = 120;
    Map<ContainerId, UpdatedContainer> updatedContainers = new HashMap<>();
    // do a few iterations to ensure RM is not going to send new containers
    while (iterationsLeft-- > 0 && updatedContainers.isEmpty()) {
      // inform RM of rejection
      allocResponse = amClient.allocate(0.1f);
      // RM did not send new containers because AM does not need any
      if (allocResponse.getUpdatedContainers() != null) {
        for (UpdatedContainer updatedContainer : allocResponse
            .getUpdatedContainers()) {
          System.out.println("Got update..");
          updatedContainers.put(updatedContainer.getContainer().getId(),
              updatedContainer);
        }
      }
      if (iterationsLeft > 0) {
        // sleep to make sure NM's heartbeat
        sleep(100);
      }
    }
    assertEquals(1, updatedContainers.size());

    for (ContainerId cId : allocatedOpportContainers.keySet()) {
      Container orig = allocatedOpportContainers.get(cId);
      UpdatedContainer updatedContainer = updatedContainers.get(cId);
      assertNotNull(updatedContainer);
      assertEquals(ExecutionType.GUARANTEED,
          updatedContainer.getContainer().getExecutionType());
      assertEquals(orig.getResource(),
          updatedContainer.getContainer().getResource());
      assertEquals(orig.getNodeId(),
          updatedContainer.getContainer().getNodeId());
      assertEquals(orig.getVersion() + 1,
          updatedContainer.getContainer().getVersion());
    }
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // SEND UPDATE EXECTYPE UPDATE TO NM
    updateContainerExecType(allocResponse, ExecutionType.GUARANTEED, nmClient);

    amClient.ask.clear();
  }

  @Test(timeout=60000)
  public void testAMRMClientWithContainerDemotion()
      throws YarnException, IOException {
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient =
        (AMRMClientImpl<AMRMClient.ContainerRequest>) AMRMClient
            .createAMRMClient();
    //asserting we are not using the singleton instance cache
    Assert.assertSame(NMTokenCache.getSingleton(),
        amClient.getNMTokenCache());
    amClient.init(conf);
    amClient.start();

    NMClientImpl nmClient = (NMClientImpl) NMClient.createNMClient();
    Assert.assertNotNull(nmClient);
    // asserting we are using the singleton instance cache
    Assert.assertSame(
        NMTokenCache.getSingleton(), nmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();
    assertEquals(STATE.STARTED, nmClient.getServiceState());

    amClient.registerApplicationMaster("Host", 10000, "");
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // START OPPORTUNISTIC Container, Send allocation request to RM
    amClient.addContainerRequest(
        new AMRMClient.ContainerRequest(capability, null, null, priority2, 0,
            true, null, ExecutionTypeRequest
            .newInstance(ExecutionType.GUARANTEED, true)));

    ProfileCapability profileCapability =
        ProfileCapability.newInstance(capability);
    int oppContainersRequestedAny =
        amClient.getTable(0).get(priority2, ResourceRequest.ANY,
            ExecutionType.GUARANTEED, profileCapability).remoteRequest
            .getNumContainers();

    assertEquals(1, oppContainersRequestedAny);
    assertEquals(1, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    Map<ContainerId, Container> allocatedGuaranteedContainers = new HashMap<>();
    int iterationsLeft = 50;

    amClient.getNMTokenCache().clearCache();
    assertEquals(0,
        amClient.getNMTokenCache().numberOfTokensInCache());

    AllocateResponse allocResponse = null;
    while (allocatedContainerCount < oppContainersRequestedAny
        && iterationsLeft-- > 0) {
      allocResponse = amClient.allocate(0.1f);
      // let NM heartbeat to RM and trigger allocations
      //triggerSchedulingWithNMHeartBeat();
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      allocatedContainerCount +=
          allocResponse.getAllocatedContainers().size();
      for (Container container : allocResponse.getAllocatedContainers()) {
        if (container.getExecutionType() == ExecutionType.GUARANTEED) {
          allocatedGuaranteedContainers.put(container.getId(), container);
        }
      }
      if (allocatedContainerCount < oppContainersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    assertEquals(oppContainersRequestedAny, allocatedContainerCount);
    assertEquals(oppContainersRequestedAny,
        allocatedGuaranteedContainers.size());
    startContainer(allocResponse, nmClient);

    // SEND DEMOTION REQUEST TO RM
    try {
      Container c = allocatedGuaranteedContainers.values().iterator().next();
      amClient.requestContainerUpdate(
          c, UpdateContainerRequest.newInstance(c.getVersion(),
              c.getId(), ContainerUpdateType.DEMOTE_EXECUTION_TYPE,
              null, ExecutionType.GUARANTEED));
      fail("Should throw Exception..");
    } catch (IllegalArgumentException e) {
      System.out.println("## " + e.getMessage());
      assertTrue(e.getMessage().contains(
          "target should be OPPORTUNISTIC and original should be GUARANTEED"));
    }

    Container c = allocatedGuaranteedContainers.values().iterator().next();
    amClient.requestContainerUpdate(
        c, UpdateContainerRequest.newInstance(c.getVersion(),
            c.getId(), ContainerUpdateType.DEMOTE_EXECUTION_TYPE,
            null, ExecutionType.OPPORTUNISTIC));
    iterationsLeft = 120;
    Map<ContainerId, UpdatedContainer> updatedContainers = new HashMap<>();
    // do a few iterations to ensure RM is not going to send new containers
    while (iterationsLeft-- > 0 && updatedContainers.isEmpty()) {
      // inform RM of rejection
      allocResponse = amClient.allocate(0.1f);
      // RM did not send new containers because AM does not need any
      if (allocResponse.getUpdatedContainers() != null) {
        for (UpdatedContainer updatedContainer : allocResponse
            .getUpdatedContainers()) {
          System.out.println("Got update..");
          updatedContainers.put(updatedContainer.getContainer().getId(),
              updatedContainer);
        }
      }
      if (iterationsLeft > 0) {
        // sleep to make sure NM's heartbeat
        sleep(100);
      }
    }
    assertEquals(1, updatedContainers.size());

    for (ContainerId cId : allocatedGuaranteedContainers.keySet()) {
      Container orig = allocatedGuaranteedContainers.get(cId);
      UpdatedContainer updatedContainer = updatedContainers.get(cId);
      assertNotNull(updatedContainer);
      assertEquals(ExecutionType.OPPORTUNISTIC,
          updatedContainer.getContainer().getExecutionType());
      assertEquals(orig.getResource(),
          updatedContainer.getContainer().getResource());
      assertEquals(orig.getNodeId(),
          updatedContainer.getContainer().getNodeId());
      assertEquals(orig.getVersion() + 1,
          updatedContainer.getContainer().getVersion());
    }
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    updateContainerExecType(allocResponse, ExecutionType.OPPORTUNISTIC,
        nmClient);
    amClient.ask.clear();
  }

  @SuppressWarnings("deprecation")
  private void updateContainerExecType(AllocateResponse allocResponse,
      ExecutionType expectedExecType, NMClientImpl nmClient)
      throws IOException, YarnException {
    for (UpdatedContainer updatedContainer : allocResponse
        .getUpdatedContainers()) {
      Container container = updatedContainer.getContainer();
      nmClient.increaseContainerResource(container);
      // NodeManager may still need some time to get the stable
      // container status
      while (true) {
        ContainerStatus status = nmClient
            .getContainerStatus(container.getId(), container.getNodeId());
        if (status.getExecutionType() == expectedExecType) {
          break;
        }
        sleep(10);
      }
    }
  }

  private void startContainer(AllocateResponse allocResponse,
      NMClientImpl nmClient) throws IOException, YarnException {
    // START THE CONTAINER IN NM
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
    for (Container container : allocResponse.getAllocatedContainers()) {
      nmClient.startContainer(container, clc);
      // NodeManager may still need some time to get the stable
      // container status
      while (true) {
        ContainerStatus status = nmClient
            .getContainerStatus(container.getId(), container.getNodeId());
        if (status.getState() == ContainerState.RUNNING) {
          break;
        }
        sleep(10);
      }
    }
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

    assertNumContainers(amClient, 0, 2, 2, 2, 3, 0);
    int containersRequestedAny = 2;

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 3;
    Set<ContainerId> releases = new TreeSet<ContainerId>();
    
    amClient.getNMTokenCache().clearCache();
    assertEquals(0, amClient.getNMTokenCache().numberOfTokensInCache());
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
          fail("Received token again for : " + nodeID);
        }
        receivedNMTokens.put(nodeID, token.getToken());
      }
      
      if(allocatedContainerCount < containersRequestedAny) {
        // let NM heartbeat to RM and trigger allocations
        triggerSchedulingWithNMHeartBeat();
      }
    }
    
    // Should receive atleast 1 token
    assertTrue(receivedNMTokens.size() > 0
        && receivedNMTokens.size() <= nodeCount);
    
    assertEquals(allocatedContainerCount, containersRequestedAny);
    assertEquals(2, releases.size());
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

    waitForContainerCompletion(3, amClient, releases);
  }

  private void waitForContainerCompletion(int numIterations,
      AMRMClientImpl<ContainerRequest> amClient, Set<ContainerId> releases)
      throws YarnException, IOException {
    // do a few iterations to ensure RM is not going send new containers
    while(!releases.isEmpty() || numIterations-- > 0) {
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
      if(numIterations > 0) {
        // let NM heartbeat to RM and trigger allocations
        triggerSchedulingWithNMHeartBeat();
      }
    }
    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());
  }

  private void testAllocRequestId(
      final AMRMClientImpl<ContainerRequest> amClient) throws YarnException,
      IOException {
    // setup container request

    assertEquals(0, amClient.ask.size());
    assertEquals(0, amClient.release.size());

    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 1));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 1));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 2));
    amClient.addContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 2));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 1));
    amClient.removeContainerRequest(
        new ContainerRequest(capability, nodes, racks, priority, 2));

    assertNumContainers(amClient, 0, 1, 1, 1, 9, 0);
    assertNumContainers(amClient, 1, 1, 1, 1, 9, 0);
    assertNumContainers(amClient, 2, 1, 1, 1, 9, 0);
    int containersRequestedAny = 3;

    // RM should allocate container within 2 calls to allocate()
    List<Container> allocatedContainers = new ArrayList<>();
    int iterationsLeft = 5;
    Set<ContainerId> releases = new TreeSet<ContainerId>();

    while (allocatedContainers.size() < containersRequestedAny
        && iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertEquals(0, amClient.ask.size());
      assertEquals(0, amClient.release.size());

      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      for(Container container : allocResponse.getAllocatedContainers()) {
        ContainerId rejectContainerId = container.getId();
        releases.add(rejectContainerId);
        amClient.releaseAssignedContainer(rejectContainerId);
      }

      if(allocatedContainers.size() < containersRequestedAny) {
        // let NM heartbeat to RM and trigger allocations
        triggerSchedulingWithNMHeartBeat();
      }
    }

    assertEquals(containersRequestedAny, allocatedContainers.size());
    Set<Long> expAllocIds = new HashSet<>(
        Arrays.asList(Long.valueOf(0), Long.valueOf(1), Long.valueOf(2)));
    Set<Long> actAllocIds = new HashSet<>();
    for (Container ac : allocatedContainers) {
      actAllocIds.add(Long.valueOf(ac.getAllocationRequestId()));
    }
    assertEquals(expAllocIds, actAllocIds);
    assertEquals(3, amClient.release.size());
    assertEquals(0, amClient.ask.size());

    waitForContainerCompletion(3, amClient, releases);
  }

  private void assertNumContainers(AMRMClientImpl<ContainerRequest> amClient,
      long allocationReqId, int expNode, int expRack, int expAny,
      int expAsks, int expRelease) {
    RemoteRequestsTable<ContainerRequest> remoteRequestsTable =
        amClient.getTable(allocationReqId);
    ProfileCapability profileCapability =
        ProfileCapability.newInstance(capability);
    int containersRequestedNode = remoteRequestsTable.get(priority,
        node, ExecutionType.GUARANTEED, profileCapability).remoteRequest
        .getNumContainers();
    int containersRequestedRack = remoteRequestsTable.get(priority,
        rack, ExecutionType.GUARANTEED, profileCapability).remoteRequest
        .getNumContainers();
    int containersRequestedAny = remoteRequestsTable.get(priority,
        ResourceRequest.ANY, ExecutionType.GUARANTEED, profileCapability)
        .remoteRequest.getNumContainers();

    assertEquals(expNode, containersRequestedNode);
    assertEquals(expRack, containersRequestedRack);
    assertEquals(expAny, containersRequestedAny);
    assertEquals(expAsks, amClient.ask.size());
    assertEquals(expRelease, amClient.release.size());
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
      assertEquals(amrmToken_1.decodeIdentifier().getKeyId(),
        amrmTokenSecretManager.getMasterKey().getMasterKey().getKeyId());

      // Wait for enough time and make sure the roll_over happens
      // At mean time, the old AMRMToken should continue to work
      while (System.currentTimeMillis() - startTime <
          rolling_interval_sec * 1000) {
        amClient.allocate(0.1f);
        sleep(1000);
      }
      amClient.allocate(0.1f);

      org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken_2 =
          getAMRMToken();
      Assert.assertNotNull(amrmToken_2);
      assertEquals(amrmToken_2.decodeIdentifier().getKeyId(),
        amrmTokenSecretManager.getMasterKey().getMasterKey().getKeyId());

      Assert.assertNotEquals(amrmToken_1, amrmToken_2);

      // can do the allocate call with latest AMRMToken
      AllocateResponse response = amClient.allocate(0.1f);
      
      // Verify latest AMRMToken can be used to send allocation request.
      UserGroupInformation testUser1 =
          UserGroupInformation.createRemoteUser("testUser1");
      
      AMRMTokenIdentifierForTest newVersionTokenIdentifier = 
          new AMRMTokenIdentifierForTest(amrmToken_2.decodeIdentifier(), "message");
      
      assertEquals("Message is changed after set to newVersionTokenIdentifier",
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
        sleep(1000);
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
        fail("The old Token should not work");
      } catch (Exception ex) {
        assertTrue(ex instanceof InvalidToken);
        assertTrue(ex.getMessage().contains(
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
          fail("credentials has more than one AMRM token."
              + " token1: " + result + " token2: " + token);
        }
        result = (org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>)
            token;
      }
    }
    return result;
  }

  @Test(timeout = 60000)
  public void testGetMatchingFitWithProfiles() throws Exception {
    teardown();
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, true);
    createClusterAndStartApplication(conf);
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest>createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");

      ProfileCapability capability1 = ProfileCapability.newInstance("minimum");
      ProfileCapability capability2 = ProfileCapability.newInstance("default");
      ProfileCapability capability3 = ProfileCapability.newInstance("maximum");
      ProfileCapability capability4 = ProfileCapability
          .newInstance("minimum", Resource.newInstance(2048, 1));
      ProfileCapability capability5 = ProfileCapability.newInstance("default");
      ProfileCapability capability6 = ProfileCapability
          .newInstance("default", Resource.newInstance(2048, 1));
      // http has the same capabilities as default
      ProfileCapability capability7 = ProfileCapability.newInstance("http");

      ContainerRequest storedContainer1 =
          new ContainerRequest(capability1, nodes, racks, priority);
      ContainerRequest storedContainer2 =
          new ContainerRequest(capability2, nodes, racks, priority);
      ContainerRequest storedContainer3 =
          new ContainerRequest(capability3, nodes, racks, priority);
      ContainerRequest storedContainer4 =
          new ContainerRequest(capability4, nodes, racks, priority);
      ContainerRequest storedContainer5 =
          new ContainerRequest(capability5, nodes, racks, priority2);
      ContainerRequest storedContainer6 =
          new ContainerRequest(capability6, nodes, racks, priority);
      ContainerRequest storedContainer7 =
          new ContainerRequest(capability7, nodes, racks, priority);


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
      ProfileCapability testCapability1 =
          ProfileCapability.newInstance("minimum");
      matches = amClient
          .getMatchingRequests(priority, node, ExecutionType.GUARANTEED,
              testCapability1);
      verifyMatches(matches, 1);
      storedRequest = matches.get(0).iterator().next();
      assertEquals(storedContainer1, storedRequest);
      amClient.removeContainerRequest(storedContainer1);

      // exact matching with order maintained
      // we should get back 3 matches - default + http because they have the
      // same capability
      ProfileCapability testCapability2 =
          ProfileCapability.newInstance("default");
      matches = amClient
          .getMatchingRequests(priority, node, ExecutionType.GUARANTEED,
              testCapability2);
      verifyMatches(matches, 2);
      // must be returned in the order they were made
      int i = 0;
      for (ContainerRequest storedRequest1 : matches.get(0)) {
        switch(i) {
        case 0:
          assertEquals(storedContainer2, storedRequest1);
          break;
        case 1:
          assertEquals(storedContainer7, storedRequest1);
          break;
        }
        i++;
      }
      amClient.removeContainerRequest(storedContainer5);

      // matching with larger container. all requests returned
      Resource testCapability3 = Resource.newInstance(8192, 8);
      matches = amClient
          .getMatchingRequests(priority, node, testCapability3);
      assertEquals(3, matches.size());

      Resource testCapability4 = Resource.newInstance(2048, 1);
      matches = amClient.getMatchingRequests(priority, node, testCapability4);
      assertEquals(1, matches.size());
    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
}
