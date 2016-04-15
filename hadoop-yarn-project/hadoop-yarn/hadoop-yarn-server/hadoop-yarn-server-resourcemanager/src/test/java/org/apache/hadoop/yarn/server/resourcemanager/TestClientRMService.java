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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestClientRMService {

  private static final Log LOG = LogFactory.getLog(TestClientRMService.class);

  private RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private String appType = "MockApp";

  private static RMDelegationTokenSecretManager dtsm;
  
  private final static String QUEUE_1 = "Q-1";
  private final static String QUEUE_2 = "Q-2";
  private final static String kerberosRule = "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";
  static {
    KerberosName.setRules(kerberosRule);
  }
  
  @BeforeClass
  public static void setupSecretManager() throws IOException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(new NullRMStateStore());
    dtsm = new RMDelegationTokenSecretManager(60000, 60000, 60000, 60000, rmContext);
    dtsm.startThreads();  
  }

  @AfterClass
  public static void teardownSecretManager() {
    if (dtsm != null) {
      dtsm.stopThreads();
    }
  }
  
  @Test
  public void testGetClusterNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
          this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
          this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));

    // Add a healthy node with label = x
    MockNM node = rm.registerNode("host1:1234", 1024);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("x"));
    labelsMgr.replaceLabelsOnNode(map);
    rm.sendNodeStarted(node);
    node.nodeHeartbeat(true);
    
    // Add and lose a node with label = y
    MockNM lostNode = rm.registerNode("host2:1235", 1024);
    rm.sendNodeStarted(lostNode);
    lostNode.nodeHeartbeat(true);
    rm.NMwaitForState(lostNode.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(lostNode);

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc
          .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetClusterNodesRequest request =
        GetClusterNodesRequest.newInstance(EnumSet.of(NodeState.RUNNING));
    List<NodeReport> nodeReports =
        client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(1, nodeReports.size());
    Assert.assertNotSame("Node is expected to be healthy!", NodeState.UNHEALTHY,
        nodeReports.get(0).getNodeState());
    
    // Check node's label = x
    Assert.assertTrue(nodeReports.get(0).getNodeLabels().contains("x"));

    // Now make the node unhealthy.
    node.nodeHeartbeat(false);

    // Call again
    nodeReports = client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals("Unhealthy nodes should not show up by default", 0,
        nodeReports.size());
    
    // Change label of host1 to y
    map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("y"));
    labelsMgr.replaceLabelsOnNode(map);
    
    // Now query for UNHEALTHY nodes
    request = GetClusterNodesRequest.newInstance(EnumSet.of(NodeState.UNHEALTHY));
    nodeReports = client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(1, nodeReports.size());
    Assert.assertEquals("Node is expected to be unhealthy!", NodeState.UNHEALTHY,
        nodeReports.get(0).getNodeState());
    
    Assert.assertTrue(nodeReports.get(0).getNodeLabels().contains("y"));
    
    // Remove labels of host1
    map = new HashMap<NodeId, Set<String>>();
    map.put(node.getNodeId(), ImmutableSet.of("y"));
    labelsMgr.removeLabelsFromNode(map);
    
    // Query all states should return all nodes
    rm.registerNode("host3:1236", 1024);
    request = GetClusterNodesRequest.newInstance(EnumSet.allOf(NodeState.class));
    nodeReports = client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(3, nodeReports.size());
    
    // All host1-3's label should be empty (instead of null)
    for (NodeReport report : nodeReports) {
      Assert.assertTrue(report.getNodeLabels() != null
          && report.getNodeLabels().isEmpty());
    }

    rpc.stopProxy(client, conf);
    rm.close();
  }
  
  @Test
  public void testNonExistingApplicationReport() throws YarnException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null, null);
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetApplicationReportRequest request = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(ApplicationId.newInstance(0, 0));
    try {
      rmService.getApplicationReport(request);
      Assert.fail();
    } catch (ApplicationNotFoundException ex) {
      Assert.assertEquals(ex.getMessage(),
          "Application with id '" + request.getApplicationId()
              + "' doesn't exist in RM.");
    }
  }

   @Test
  public void testGetApplicationReport() throws Exception {
    YarnScheduler yarnScheduler = mock(YarnScheduler.class);
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);

    ApplicationId appId1 = getApplicationId(1);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler,
        null, mockAclsManager, null, null);
    try {
      RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
      GetApplicationReportRequest request = recordFactory
          .newRecordInstance(GetApplicationReportRequest.class);
      request.setApplicationId(appId1);
      GetApplicationReportResponse response = 
          rmService.getApplicationReport(request);
      ApplicationReport report = response.getApplicationReport();
      ApplicationResourceUsageReport usageReport = 
          report.getApplicationResourceUsageReport();
      Assert.assertEquals(10, usageReport.getMemorySeconds());
      Assert.assertEquals(3, usageReport.getVcoreSeconds());
    } finally {
      rmService.close();
    }
  }
  
  @Test
  public void testGetApplicationAttemptReport() throws YarnException,
      IOException {
    ClientRMService rmService = createRMService();
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetApplicationAttemptReportRequest request = recordFactory
        .newRecordInstance(GetApplicationAttemptReportRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    request.setApplicationAttemptId(attemptId);

    try {
      GetApplicationAttemptReportResponse response = rmService
          .getApplicationAttemptReport(request);
      Assert.assertEquals(attemptId, response.getApplicationAttemptReport()
          .getApplicationAttemptId());
    } catch (ApplicationNotFoundException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testGetApplicationResourceUsageReportDummy() throws YarnException,
      IOException {
    ApplicationAttemptId attemptId = getApplicationAttemptId(1);
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });
    ApplicationSubmissionContext asContext = 
        mock(ApplicationSubmissionContext.class);
    YarnConfiguration config = new YarnConfiguration();
    RMAppAttemptImpl rmAppAttemptImpl = new RMAppAttemptImpl(attemptId,
        rmContext, yarnScheduler, null, asContext, config, false, null);
    ApplicationResourceUsageReport report = rmAppAttemptImpl
        .getApplicationResourceUsageReport();
    assertEquals(report, RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT);
  }

  @Test
  public void testGetApplicationAttempts() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetApplicationAttemptsRequest request = recordFactory
        .newRecordInstance(GetApplicationAttemptsRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    request.setApplicationId(ApplicationId.newInstance(123456, 1));

    try {
      GetApplicationAttemptsResponse response = rmService
          .getApplicationAttempts(request);
      Assert.assertEquals(1, response.getApplicationAttemptList().size());
      Assert.assertEquals(attemptId, response.getApplicationAttemptList()
          .get(0).getApplicationAttemptId());

    } catch (ApplicationNotFoundException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testGetContainerReport() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetContainerReportRequest request = recordFactory
        .newRecordInstance(GetContainerReportRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    request.setContainerId(containerId);

    try {
      GetContainerReportResponse response = rmService
          .getContainerReport(request);
      Assert.assertEquals(containerId, response.getContainerReport()
          .getContainerId());
    } catch (ApplicationNotFoundException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testGetContainers() throws YarnException, IOException {
    ClientRMService rmService = createRMService();
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    GetContainersRequest request = recordFactory
        .newRecordInstance(GetContainersRequest.class);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    request.setApplicationAttemptId(attemptId);
    try {
      GetContainersResponse response = rmService.getContainers(request);
      Assert.assertEquals(containerId, response.getContainerList().get(0)
          .getContainerId());
    } catch (ApplicationNotFoundException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  public ClientRMService createRMService() throws IOException {
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    ConcurrentHashMap<ApplicationId, RMApp> apps = getRMApps(rmContext,
        yarnScheduler);
    when(rmContext.getRMApps()).thenReturn(apps);
    when(rmContext.getYarnConfiguration()).thenReturn(new Configuration());
    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null,
        mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(
        mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
            any(QueueACL.class), anyString())).thenReturn(true);
    return new ClientRMService(rmContext, yarnScheduler, appManager,
        mockAclsManager, mockQueueACLsManager, null);
  }

  @Test
  public void testForceKillNonExistingApplication() throws YarnException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null, null);
    ApplicationId applicationId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(), 0);
    KillApplicationRequest request =
        KillApplicationRequest.newInstance(applicationId);
    try {
      rmService.forceKillApplication(request);
      Assert.fail();
    } catch (ApplicationNotFoundException ex) {
      Assert.assertEquals(ex.getMessage(),
          "Trying to kill an absent " +
              "application " + request.getApplicationId());
    }
  }

  @Test
  public void testForceKillApplication() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM();
    rm.init(conf);
    rm.start();

    ClientRMService rmService = rm.getClientRMService();
    GetApplicationsRequest getRequest = GetApplicationsRequest.newInstance(
        EnumSet.of(YarnApplicationState.KILLED));

    RMApp app1 = rm.submitApp(1024);
    RMApp app2 = rm.submitApp(1024, true);

    assertEquals("Incorrect number of apps in the RM", 0,
        rmService.getApplications(getRequest).getApplicationList().size());

    KillApplicationRequest killRequest1 =
        KillApplicationRequest.newInstance(app1.getApplicationId());
    KillApplicationRequest killRequest2 =
        KillApplicationRequest.newInstance(app2.getApplicationId());

    int killAttemptCount = 0;
    for (int i = 0; i < 100; i++) {
      KillApplicationResponse killResponse1 =
          rmService.forceKillApplication(killRequest1);
      killAttemptCount++;
      if (killResponse1.getIsKillCompleted()) {
        break;
      }
      Thread.sleep(10);
    }
    assertTrue("Kill attempt count should be greater than 1 for managed AMs",
        killAttemptCount > 1);
    assertEquals("Incorrect number of apps in the RM", 1,
        rmService.getApplications(getRequest).getApplicationList().size());

    KillApplicationResponse killResponse2 =
        rmService.forceKillApplication(killRequest2);
    assertTrue("Killing UnmanagedAM should falsely acknowledge true",
        killResponse2.getIsKillCompleted());
    for (int i = 0; i < 100; i++) {
      if (2 ==
          rmService.getApplications(getRequest).getApplicationList().size()) {
        break;
      }
      Thread.sleep(10);
    }
    assertEquals("Incorrect number of apps in the RM", 2,
        rmService.getApplications(getRequest).getApplicationList().size());
  }
  
  @Test (expected = ApplicationNotFoundException.class)
  public void testMoveAbsentApplication() throws YarnException {
    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(
        new ConcurrentHashMap<ApplicationId, RMApp>());
    ClientRMService rmService = new ClientRMService(rmContext, null, null,
        null, null, null);
    ApplicationId applicationId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(), 0);
    MoveApplicationAcrossQueuesRequest request =
        MoveApplicationAcrossQueuesRequest.newInstance(applicationId, "newqueue");
    rmService.moveApplicationAcrossQueues(request);
  }

  @Test
  public void testGetQueueInfo() throws Exception {
    YarnScheduler yarnScheduler = mock(YarnScheduler.class);
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), anyString())).thenReturn(true);
    when(mockAclsManager.checkAccess(any(UserGroupInformation.class),
        any(ApplicationAccessType.class), anyString(),
        any(ApplicationId.class))).thenReturn(true);

    ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler,
        null, mockAclsManager, mockQueueACLsManager, null);
    GetQueueInfoRequest request = recordFactory
        .newRecordInstance(GetQueueInfoRequest.class);
    request.setQueueName("testqueue");
    request.setIncludeApplications(true);
    GetQueueInfoResponse queueInfo = rmService.getQueueInfo(request);
    List<ApplicationReport> applications = queueInfo.getQueueInfo()
        .getApplications();
    Assert.assertEquals(2, applications.size());
    request.setQueueName("nonexistentqueue");
    request.setIncludeApplications(true);
    // should not throw exception on nonexistent queue
    queueInfo = rmService.getQueueInfo(request);

    // Case where user does not have application access
    ApplicationACLsManager mockAclsManager1 =
        mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager1 =
        mock(QueueACLsManager.class);
    when(mockQueueACLsManager1.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), anyString())).thenReturn(false);
    when(mockAclsManager1.checkAccess(any(UserGroupInformation.class),
        any(ApplicationAccessType.class), anyString(),
        any(ApplicationId.class))).thenReturn(false);

    ClientRMService rmService1 = new ClientRMService(rmContext, yarnScheduler,
        null, mockAclsManager1, mockQueueACLsManager1, null);
    request.setQueueName("testqueue");
    request.setIncludeApplications(true);
    GetQueueInfoResponse queueInfo1 = rmService1.getQueueInfo(request);
    List<ApplicationReport> applications1 = queueInfo1.getQueueInfo()
        .getApplications();
    Assert.assertEquals(0, applications1.size());
  }

  private static final UserGroupInformation owner =
      UserGroupInformation.createRemoteUser("owner");
  private static final UserGroupInformation other =
      UserGroupInformation.createRemoteUser("other");
  private static final UserGroupInformation tester =
      UserGroupInformation.createRemoteUser("tester");
  private static final String testerPrincipal = "tester@EXAMPLE.COM";
  private static final String ownerPrincipal = "owner@EXAMPLE.COM";
  private static final String otherPrincipal = "other@EXAMPLE.COM";
  private static final UserGroupInformation testerKerb =
      UserGroupInformation.createRemoteUser(testerPrincipal);
  private static final UserGroupInformation ownerKerb =
      UserGroupInformation.createRemoteUser(ownerPrincipal);
  private static final UserGroupInformation otherKerb =
      UserGroupInformation.createRemoteUser(otherPrincipal);
  
  @Test
  public void testTokenRenewalByOwner() throws Exception {
    owner.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenRenewal(owner, owner);
        return null;
      }
    });
  }
  
  @Test
  public void testTokenRenewalWrongUser() throws Exception {
    try {
      owner.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          try {
            checkTokenRenewal(owner, other);
            return null;
          } catch (YarnException ex) {
            Assert.assertTrue(ex.getMessage().contains(owner.getUserName() +
                " tries to renew a token with renewer " +
                other.getUserName()));
            throw ex;
          }
        }
      });
    } catch (Exception e) {
      return;
    }
    Assert.fail("renew should have failed");
  }

  @Test
  public void testTokenRenewalByLoginUser() throws Exception {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenRenewal(owner, owner);
        checkTokenRenewal(owner, other);
        return null;
      }
    });
  }

  private void checkTokenRenewal(UserGroupInformation owner,
      UserGroupInformation renewer) throws IOException, YarnException {
    RMDelegationTokenIdentifier tokenIdentifier =
        new RMDelegationTokenIdentifier(
            new Text(owner.getUserName()), new Text(renewer.getUserName()), null);
    Token<?> token =
        new Token<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
    org.apache.hadoop.yarn.api.records.Token dToken = BuilderUtils.newDelegationToken(
        token.getIdentifier(), token.getKind().toString(),
        token.getPassword(), token.getService().toString());
    RenewDelegationTokenRequest request =
        Records.newRecord(RenewDelegationTokenRequest.class);
    request.setDelegationToken(dToken);

    RMContext rmContext = mock(RMContext.class);
    ClientRMService rmService = new ClientRMService(
        rmContext, null, null, null, null, dtsm);
    rmService.renewDelegationToken(request);
  }

  @Test
  public void testTokenCancellationByOwner() throws Exception {
    // two tests required - one with a kerberos name
    // and with a short name
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(rmService, testerKerb, other);
        return null;
      }
    });
    owner.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(owner, other);
        return null;
      }
    });
  }

  @Test
  public void testTokenCancellationByRenewer() throws Exception {
    // two tests required - one with a kerberos name
    // and with a short name
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(rmService, owner, testerKerb);
        return null;
      }
    });
    other.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        checkTokenCancellation(owner, other);
        return null;
      }
    });
  }

  @Test
  public void testTokenCancellationByWrongUser() {
    // two sets to test -
    // 1. try to cancel tokens of short and kerberos users as a kerberos UGI
    // 2. try to cancel tokens of short and kerberos users as a simple auth UGI

    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    UserGroupInformation[] kerbTestOwners =
        { owner, other, tester, ownerKerb, otherKerb };
    UserGroupInformation[] kerbTestRenewers =
        { owner, other, ownerKerb, otherKerb };
    for (final UserGroupInformation tokOwner : kerbTestOwners) {
      for (final UserGroupInformation tokRenewer : kerbTestRenewers) {
        try {
          testerKerb.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              try {
                checkTokenCancellation(rmService, tokOwner, tokRenewer);
                Assert.fail("We should not reach here; token owner = "
                    + tokOwner.getUserName() + ", renewer = "
                    + tokRenewer.getUserName());
                return null;
              } catch (YarnException e) {
                Assert.assertTrue(e.getMessage().contains(
                  testerKerb.getUserName()
                      + " is not authorized to cancel the token"));
                return null;
              }
            }
          });
        } catch (Exception e) {
          Assert.fail("Unexpected exception; " + e.getMessage());
        }
      }
    }

    UserGroupInformation[] simpleTestOwners =
        { owner, other, ownerKerb, otherKerb, testerKerb };
    UserGroupInformation[] simpleTestRenewers =
        { owner, other, ownerKerb, otherKerb };
    for (final UserGroupInformation tokOwner : simpleTestOwners) {
      for (final UserGroupInformation tokRenewer : simpleTestRenewers) {
        try {
          tester.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              try {
                checkTokenCancellation(tokOwner, tokRenewer);
                Assert.fail("We should not reach here; token owner = "
                    + tokOwner.getUserName() + ", renewer = "
                    + tokRenewer.getUserName());
                return null;
              } catch (YarnException ex) {
                Assert.assertTrue(ex.getMessage().contains(
                  tester.getUserName()
                      + " is not authorized to cancel the token"));
                return null;
              }
            }
          });
        } catch (Exception e) {
          Assert.fail("Unexpected exception; " + e.getMessage());
        }
      }
    }
  }

  private void checkTokenCancellation(UserGroupInformation owner,
      UserGroupInformation renewer) throws IOException, YarnException {
    RMContext rmContext = mock(RMContext.class);
    final ClientRMService rmService =
        new ClientRMService(rmContext, null, null, null, null, dtsm);
    checkTokenCancellation(rmService, owner, renewer);
  }

  private void checkTokenCancellation(ClientRMService rmService,
      UserGroupInformation owner, UserGroupInformation renewer)
      throws IOException, YarnException {
    RMDelegationTokenIdentifier tokenIdentifier =
        new RMDelegationTokenIdentifier(new Text(owner.getUserName()),
          new Text(renewer.getUserName()), null);
    Token<?> token =
        new Token<RMDelegationTokenIdentifier>(tokenIdentifier, dtsm);
    org.apache.hadoop.yarn.api.records.Token dToken =
        BuilderUtils.newDelegationToken(token.getIdentifier(), token.getKind()
          .toString(), token.getPassword(), token.getService().toString());
    CancelDelegationTokenRequest request =
        Records.newRecord(CancelDelegationTokenRequest.class);
    request.setDelegationToken(dToken);
    rmService.cancelDelegationToken(request);
  }

  @Test (timeout = 30000)
  @SuppressWarnings ("rawtypes")
  public void testAppSubmit() throws Exception {
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });
    ApplicationId appId1 = getApplicationId(100);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
            any(QueueACL.class), anyString())).thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);

    // without name and queue

    SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(
        appId1, null, null);
    try {
      rmService.submitApplication(submitRequest1);
    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }
    RMApp app1 = rmContext.getRMApps().get(appId1);
    Assert.assertNotNull("app doesn't exist", app1);
    Assert.assertEquals("app name doesn't match",
        YarnConfiguration.DEFAULT_APPLICATION_NAME, app1.getName());
    Assert.assertEquals("app queue doesn't match",
        YarnConfiguration.DEFAULT_QUEUE_NAME, app1.getQueue());

    // with name and queue
    String name = MockApps.newAppName();
    String queue = MockApps.newQueue();
    ApplicationId appId2 = getApplicationId(101);
    SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(
        appId2, name, queue);
    submitRequest2.getApplicationSubmissionContext().setApplicationType(
        "matchType");
    try {
      rmService.submitApplication(submitRequest2);
    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }
    RMApp app2 = rmContext.getRMApps().get(appId2);
    Assert.assertNotNull("app doesn't exist", app2);
    Assert.assertEquals("app name doesn't match", name, app2.getName());
    Assert.assertEquals("app queue doesn't match", queue, app2.getQueue());

    // duplicate appId
    try {
      rmService.submitApplication(submitRequest2);
    } catch (YarnException e) {
      Assert.fail("Exception is not expected.");
    }

    GetApplicationsRequest getAllAppsRequest =
        GetApplicationsRequest.newInstance(new HashSet<String>());
    GetApplicationsResponse getAllApplicationsResponse =
        rmService.getApplications(getAllAppsRequest);
    Assert.assertEquals(5,
        getAllApplicationsResponse.getApplicationList().size());

    Set<String> appTypes = new HashSet<String>();
    appTypes.add("matchType");

    getAllAppsRequest = GetApplicationsRequest.newInstance(appTypes);
    getAllApplicationsResponse =
        rmService.getApplications(getAllAppsRequest);
    Assert.assertEquals(1,
        getAllApplicationsResponse.getApplicationList().size());
    Assert.assertEquals(appId2,
        getAllApplicationsResponse.getApplicationList()
            .get(0).getApplicationId());
  }

  @Test
  public void testGetApplications() throws IOException, YarnException {
    /**
     * 1. Submit 3 applications alternately in two queues
     * 2. Test each of the filters
     */
    // Basic setup
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), anyString())).thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);

    // Initialize appnames and queues
    String[] queues = {QUEUE_1, QUEUE_2};
    String[] appNames =
        {MockApps.newAppName(), MockApps.newAppName(), MockApps.newAppName()};
    ApplicationId[] appIds =
        {getApplicationId(101), getApplicationId(102), getApplicationId(103)};
    List<String> tags = Arrays.asList("Tag1", "Tag2", "Tag3");
    
    long[] submitTimeMillis = new long[3];
    // Submit applications
    for (int i = 0; i < appIds.length; i++) {
      ApplicationId appId = appIds[i];
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
              ApplicationAccessType.VIEW_APP, null, appId)).thenReturn(true);
      SubmitApplicationRequest submitRequest = mockSubmitAppRequest(
          appId, appNames[i], queues[i % queues.length],
          new HashSet<String>(tags.subList(0, i + 1)));
      rmService.submitApplication(submitRequest);
      submitTimeMillis[i] = System.currentTimeMillis();
    }

    // Test different cases of ClientRMService#getApplications()
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    assertEquals("Incorrect total number of apps", 6,
        rmService.getApplications(request).getApplicationList().size());

    // Check limit
    request.setLimit(1L);
    assertEquals("Failed to limit applications", 1,
        rmService.getApplications(request).getApplicationList().size());
    
    // Check start range
    request = GetApplicationsRequest.newInstance();
    request.setStartRange(submitTimeMillis[0], System.currentTimeMillis());
    
    // 2 applications are submitted after first timeMills
    assertEquals("Incorrect number of matching start range", 
        2, rmService.getApplications(request).getApplicationList().size());
    
    // 1 application is submitted after the second timeMills
    request.setStartRange(submitTimeMillis[1], System.currentTimeMillis());
    assertEquals("Incorrect number of matching start range", 
        1, rmService.getApplications(request).getApplicationList().size());
    
    // no application is submitted after the third timeMills
    request.setStartRange(submitTimeMillis[2], System.currentTimeMillis());
    assertEquals("Incorrect number of matching start range", 
        0, rmService.getApplications(request).getApplicationList().size());

    // Check queue
    request = GetApplicationsRequest.newInstance();
    Set<String> queueSet = new HashSet<String>();
    request.setQueues(queueSet);

    queueSet.add(queues[0]);
    assertEquals("Incorrect number of applications in queue", 2,
        rmService.getApplications(request).getApplicationList().size());
    assertEquals("Incorrect number of applications in queue", 2,
        rmService.getApplications(request, false).getApplicationList().size());

    queueSet.add(queues[1]);
    assertEquals("Incorrect number of applications in queue", 3,
        rmService.getApplications(request).getApplicationList().size());

    // Check user
    request = GetApplicationsRequest.newInstance();
    Set<String> userSet = new HashSet<String>();
    request.setUsers(userSet);

    userSet.add("random-user-name");
    assertEquals("Incorrect number of applications for user", 0,
        rmService.getApplications(request).getApplicationList().size());

    userSet.add(UserGroupInformation.getCurrentUser().getShortUserName());
    assertEquals("Incorrect number of applications for user", 3,
        rmService.getApplications(request).getApplicationList().size());

    // Check tags
    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.ALL, null, null, null, null, null, null,
        null, null);
    Set<String> tagSet = new HashSet<String>();
    request.setApplicationTags(tagSet);
    assertEquals("Incorrect number of matching tags", 6,
        rmService.getApplications(request).getApplicationList().size());

    tagSet = Sets.newHashSet(tags.get(0));
    request.setApplicationTags(tagSet);
    assertEquals("Incorrect number of matching tags", 3,
        rmService.getApplications(request).getApplicationList().size());

    tagSet = Sets.newHashSet(tags.get(1));
    request.setApplicationTags(tagSet);
    assertEquals("Incorrect number of matching tags", 2,
        rmService.getApplications(request).getApplicationList().size());

    tagSet = Sets.newHashSet(tags.get(2));
    request.setApplicationTags(tagSet);
    assertEquals("Incorrect number of matching tags", 1,
        rmService.getApplications(request).getApplicationList().size());

    // Check scope
    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.VIEWABLE);
    assertEquals("Incorrect number of applications for the scope", 6,
        rmService.getApplications(request).getApplicationList().size());

    request = GetApplicationsRequest.newInstance(
        ApplicationsRequestScope.OWN);
    assertEquals("Incorrect number of applications for the scope", 3,
        rmService.getApplications(request).getApplicationList().size());
  }
  
  @Test(timeout=4000)
  public void testConcurrentAppSubmit()
      throws IOException, InterruptedException, BrokenBarrierException,
      YarnException {
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());

    final ApplicationId appId1 = getApplicationId(100);
    final ApplicationId appId2 = getApplicationId(101);
    final SubmitApplicationRequest submitRequest1 = mockSubmitAppRequest(
        appId1, null, null);
    final SubmitApplicationRequest submitRequest2 = mockSubmitAppRequest(
        appId2, null, null);
    
    final CyclicBarrier startBarrier = new CyclicBarrier(2);
    final CyclicBarrier endBarrier = new CyclicBarrier(2);

    @SuppressWarnings("rawtypes")
    EventHandler eventHandler = new EventHandler() {
      @Override
      public void handle(Event rawEvent) {
        if (rawEvent instanceof RMAppEvent) {
          RMAppEvent event = (RMAppEvent) rawEvent;
          if (event.getApplicationId().equals(appId1)) {
            try {
              startBarrier.await();
              endBarrier.await();
            } catch (BrokenBarrierException e) {
              LOG.warn("Broken Barrier", e);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted while awaiting barriers", e);
            }
          }
        }
      }
    };

    when(rmContext.getDispatcher().getEventHandler()).thenReturn(eventHandler);
      
    final ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager, null, null,
            null);

    // submit an app and wait for it to block while in app submission
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          rmService.submitApplication(submitRequest1);
        } catch (YarnException e) {}
      }
    };
    t.start();
    
    // submit another app, so go through while the first app is blocked
    startBarrier.await();
    rmService.submitApplication(submitRequest2);
    endBarrier.await();
    t.join();
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue) {
    return mockSubmitAppRequest(appId, name, queue, null);
  }

  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
      String name, String queue, Set<String> tags) {
    return mockSubmitAppRequest(appId, name, queue, tags, false);
  }

  @SuppressWarnings("deprecation")
  private SubmitApplicationRequest mockSubmitAppRequest(ApplicationId appId,
        String name, String queue, Set<String> tags, boolean unmanaged) {

    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);

    Resource resource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);

    ApplicationSubmissionContext submissionContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    submissionContext.setAMContainerSpec(amContainerSpec);
    submissionContext.setApplicationName(name);
    submissionContext.setQueue(queue);
    submissionContext.setApplicationId(appId);
    submissionContext.setResource(resource);
    submissionContext.setApplicationType(appType);
    submissionContext.setApplicationTags(tags);
    submissionContext.setUnmanagedAM(unmanaged);

    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }

  private void mockRMContext(YarnScheduler yarnScheduler, RMContext rmContext)
      throws IOException {
    Dispatcher dispatcher = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(dispatcher);
    EventHandler eventHandler = mock(EventHandler.class);
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    QueueInfo queInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queInfo.setQueueName("testqueue");
    when(yarnScheduler.getQueueInfo(eq("testqueue"), anyBoolean(), anyBoolean()))
        .thenReturn(queInfo);
    when(yarnScheduler.getQueueInfo(eq("nonexistentqueue"), anyBoolean(), anyBoolean()))
        .thenThrow(new IOException("queue does not exist"));
    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    when(rmContext.getYarnConfiguration()).thenReturn(new YarnConfiguration());
    ConcurrentHashMap<ApplicationId, RMApp> apps = getRMApps(rmContext,
        yarnScheduler);
    when(rmContext.getRMApps()).thenReturn(apps);
    when(yarnScheduler.getAppsInQueue(eq("testqueue"))).thenReturn(
        getSchedulerApps(apps));
     ResourceScheduler rs = mock(ResourceScheduler.class);
     when(rmContext.getScheduler()).thenReturn(rs);
  }

  private ConcurrentHashMap<ApplicationId, RMApp> getRMApps(
      RMContext rmContext, YarnScheduler yarnScheduler) {
    ConcurrentHashMap<ApplicationId, RMApp> apps = 
      new ConcurrentHashMap<ApplicationId, RMApp>();
    ApplicationId applicationId1 = getApplicationId(1);
    ApplicationId applicationId2 = getApplicationId(2);
    ApplicationId applicationId3 = getApplicationId(3);
    YarnConfiguration config = new YarnConfiguration();
    apps.put(applicationId1, getRMApp(rmContext, yarnScheduler, applicationId1,
        config, "testqueue", 10, 3));
    apps.put(applicationId2, getRMApp(rmContext, yarnScheduler, applicationId2,
        config, "a", 20, 2));
    apps.put(applicationId3, getRMApp(rmContext, yarnScheduler, applicationId3,
        config, "testqueue", 40, 5));
    return apps;
  }
  
  private List<ApplicationAttemptId> getSchedulerApps(
      Map<ApplicationId, RMApp> apps) {
    List<ApplicationAttemptId> schedApps = new ArrayList<ApplicationAttemptId>();
    // Return app IDs for the apps in testqueue (as defined in getRMApps)
    schedApps.add(ApplicationAttemptId.newInstance(getApplicationId(1), 0));
    schedApps.add(ApplicationAttemptId.newInstance(getApplicationId(3), 0));
    return schedApps;
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }
  
  private static ApplicationAttemptId getApplicationAttemptId(int id) {
    return ApplicationAttemptId.newInstance(getApplicationId(id), 1);
  }

  private RMAppImpl getRMApp(RMContext rmContext, YarnScheduler yarnScheduler,
      ApplicationId applicationId3, YarnConfiguration config, String queueName,
      final long memorySeconds, final long vcoreSeconds) {
    ApplicationSubmissionContext asContext = mock(ApplicationSubmissionContext.class);
    when(asContext.getMaxAppAttempts()).thenReturn(1);

    RMAppImpl app =
        spy(new RMAppImpl(applicationId3, rmContext, config, null, null,
            queueName, asContext, yarnScheduler, null,
            System.currentTimeMillis(), "YARN", null,
            BuilderUtils.newResourceRequest(
                RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                Resource.newInstance(1024, 1), 1)){
                  @Override
                  public ApplicationReport createAndGetApplicationReport(
                      String clientUserName, boolean allowAccess) {
                    ApplicationReport report = super.createAndGetApplicationReport(
                        clientUserName, allowAccess);
                    ApplicationResourceUsageReport usageReport = 
                        report.getApplicationResourceUsageReport();
                    usageReport.setMemorySeconds(memorySeconds);
                    usageReport.setVcoreSeconds(vcoreSeconds);
                    report.setApplicationResourceUsageReport(usageReport);
                    return report;
                  }
              });

    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    RMAppAttemptImpl rmAppAttemptImpl = spy(new RMAppAttemptImpl(attemptId,
        rmContext, yarnScheduler, null, asContext, config, false, null));
    Container container = Container.newInstance(
        ContainerId.newContainerId(attemptId, 1), null, "", null, null, null);
    RMContainerImpl containerimpl = spy(new RMContainerImpl(container,
        attemptId, null, "", rmContext));
    Map<ApplicationAttemptId, RMAppAttempt> attempts = 
      new HashMap<ApplicationAttemptId, RMAppAttempt>();
    attempts.put(attemptId, rmAppAttemptImpl);
    when(app.getCurrentAppAttempt()).thenReturn(rmAppAttemptImpl);
    when(app.getAppAttempts()).thenReturn(attempts);
    when(rmAppAttemptImpl.getMasterContainer()).thenReturn(container);
    ResourceScheduler rs = mock(ResourceScheduler.class);
    when(rmContext.getScheduler()).thenReturn(rs);
    when(rmContext.getScheduler().getRMContainer(any(ContainerId.class)))
        .thenReturn(containerimpl);
    SchedulerAppReport sAppReport = mock(SchedulerAppReport.class);
    when(
        rmContext.getScheduler().getSchedulerAppInfo(
            any(ApplicationAttemptId.class))).thenReturn(sAppReport);
    List<RMContainer> rmContainers = new ArrayList<RMContainer>();
    rmContainers.add(containerimpl);
    when(
        rmContext.getScheduler().getSchedulerAppInfo(attemptId)
            .getLiveContainers()).thenReturn(rmContainers);
    ContainerStatus cs = mock(ContainerStatus.class);
    when(containerimpl.getFinishedStatus()).thenReturn(cs);
    when(containerimpl.getDiagnosticsInfo()).thenReturn("N/A");
    when(containerimpl.getContainerExitStatus()).thenReturn(0);
    when(containerimpl.getContainerState()).thenReturn(ContainerState.COMPLETE);
    return app;
  }

  private static YarnScheduler mockYarnScheduler() {
    YarnScheduler yarnScheduler = mock(YarnScheduler.class);
    when(yarnScheduler.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    when(yarnScheduler.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(yarnScheduler.getAppsInQueue(QUEUE_1)).thenReturn(
        Arrays.asList(getApplicationAttemptId(101), getApplicationAttemptId(102)));
    when(yarnScheduler.getAppsInQueue(QUEUE_2)).thenReturn(
        Arrays.asList(getApplicationAttemptId(103)));
    ApplicationAttemptId attemptId = getApplicationAttemptId(1);
    when(yarnScheduler.getAppResourceUsageReport(attemptId)).thenReturn(null);
    ResourceCalculator rc = new DefaultResourceCalculator();
    when(yarnScheduler.getResourceCalculator()).thenReturn(rc);
    return yarnScheduler;
  }

  @Test
  public void testReservationAPIs() {
    // initialize
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM nm;
    try {
      nm = rm.registerNode("127.0.0.1:1", 102400, 100);
      // allow plan follower to synchronize
      Thread.sleep(1050);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Create a client.
    ClientRMService clientService = rm.getClientRMService();

    // create a reservation
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        createSimpleReservationRequest(4, arrival, deadline, duration);
    ReservationSubmissionResponse sResponse = null;
    try {
      sResponse = clientService.submitReservation(sRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(sResponse);
    ReservationId reservationID = sResponse.getReservationId();
    Assert.assertNotNull(reservationID);
    LOG.info("Submit reservation response: " + reservationID);

    // Update the reservation
    ReservationDefinition rDef = sRequest.getReservationDefinition();
    ReservationRequest rr =
        rDef.getReservationRequests().getReservationResources().get(0);
    rr.setNumContainers(5);
    arrival = clock.getTime();
    duration = 30000;
    deadline = (long) (arrival + 1.05 * duration);
    rr.setDuration(duration);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    ReservationUpdateRequest uRequest =
        ReservationUpdateRequest.newInstance(rDef, reservationID);
    ReservationUpdateResponse uResponse = null;
    try {
      uResponse = clientService.updateReservation(uRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(sResponse);
    LOG.info("Update reservation response: " + uResponse);

    // Delete the reservation
    ReservationDeleteRequest dRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    ReservationDeleteResponse dResponse = null;
    try {
      dResponse = clientService.deleteReservation(dRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(sResponse);
    LOG.info("Delete reservation response: " + dResponse);

    // clean-up
    rm.stop();
    nm = null;
    rm = null;
  }

  private ReservationSubmissionRequest createSimpleReservationRequest(
      int numContainers, long arrival, long deadline, long duration) {
    // create a request with a single atomic ask
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1),
            numContainers, 1, duration);
    ReservationRequests reqs =
        ReservationRequests.newInstance(Collections.singletonList(r),
            ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(arrival, deadline, reqs,
            "testClientRMService#reservation");
    ReservationSubmissionRequest request =
        ReservationSubmissionRequest.newInstance(rDef,
            ReservationSystemTestUtil.reservationQ);
    return request;
  }
  
  @Test
  public void testGetNodeLabels() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, this.getRMContext()
                .getRMDelegationTokenSecretManager());
      };
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of("x", "y"));

    NodeId node1 = NodeId.newInstance("host1", 1234);
    NodeId node2 = NodeId.newInstance("host2", 1234);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1, ImmutableSet.of("x"));
    map.put(node2, ImmutableSet.of("y"));
    labelsMgr.replaceLabelsOnNode(map);

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc.getProxy(
            ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response =
        client.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList("x", "y")));

    // Get node labels mapping
    GetNodesToLabelsResponse response1 =
        client.getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Map<NodeId, Set<String>> nodeToLabels = response1.getNodeToLabels();
    Assert.assertTrue(nodeToLabels.keySet().containsAll(
        Arrays.asList(node1, node2)));
    Assert.assertTrue(nodeToLabels.get(node1).containsAll(Arrays.asList("x")));
    Assert.assertTrue(nodeToLabels.get(node2).containsAll(Arrays.asList("y")));
    
    rpc.stopProxy(client, conf);
    rm.close();
  }

  @Test
  public void testGetLabelsToNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager, this.getRMContext()
                .getRMDelegationTokenSecretManager());
      };
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of("x", "y", "z"));

    NodeId node1A = NodeId.newInstance("host1", 1234);
    NodeId node1B = NodeId.newInstance("host1", 5678);
    NodeId node2A = NodeId.newInstance("host2", 1234);
    NodeId node3A = NodeId.newInstance("host3", 1234);
    NodeId node3B = NodeId.newInstance("host3", 5678);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1A, ImmutableSet.of("x"));
    map.put(node1B, ImmutableSet.of("z"));
    map.put(node2A, ImmutableSet.of("y"));
    map.put(node3A, ImmutableSet.of("y"));
    map.put(node3B, ImmutableSet.of("z"));
    labelsMgr.replaceLabelsOnNode(map);

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc.getProxy(
            ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response =
        client.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList("x", "y", "z")));

    // Get labels to nodes mapping
    GetLabelsToNodesResponse response1 =
        client.getLabelsToNodes(GetLabelsToNodesRequest.newInstance());
    Map<String, Set<NodeId>> labelsToNodes = response1.getLabelsToNodes();
    Assert.assertTrue(
        labelsToNodes.keySet().containsAll(Arrays.asList("x", "y", "z")));
    Assert.assertTrue(
        labelsToNodes.get("x").containsAll(Arrays.asList(node1A)));
    Assert.assertTrue(
        labelsToNodes.get("y").containsAll(Arrays.asList(node2A, node3A)));
    Assert.assertTrue(
        labelsToNodes.get("z").containsAll(Arrays.asList(node1B, node3B)));

    // Get labels to nodes mapping for specific labels
    Set<String> setlabels =
        new HashSet<String>(Arrays.asList(new String[]{"x", "z"}));
    GetLabelsToNodesResponse response2 =
        client.getLabelsToNodes(GetLabelsToNodesRequest.newInstance(setlabels));
    labelsToNodes = response2.getLabelsToNodes();
    Assert.assertTrue(
        labelsToNodes.keySet().containsAll(Arrays.asList("x", "z")));
    Assert.assertTrue(
        labelsToNodes.get("x").containsAll(Arrays.asList(node1A)));
    Assert.assertTrue(
        labelsToNodes.get("z").containsAll(Arrays.asList(node1B, node3B)));
    Assert.assertEquals(labelsToNodes.get("y"), null);

    rpc.stopProxy(client, conf);
    rm.close();
  }

  private void createExcludeFile(String filename) throws IOException {
    File file = new File(filename);
    if (file.exists()) {
      file.delete();
    }

    FileOutputStream out = new FileOutputStream(file);
    out.write("decommisssionedHost".getBytes());
    out.close();
  }

  @Test
  public void testRMStartWithDecommissionedNode() throws Exception {
    String excludeFile = "excludeFile";
    createExcludeFile(excludeFile);
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        excludeFile);
    MockRM rm = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    rm.start();

    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc
            .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetClusterNodesRequest request =
        GetClusterNodesRequest.newInstance(EnumSet.allOf(NodeState.class));
    List<NodeReport> nodeReports = client.getClusterNodes(request).getNodeReports();
    Assert.assertEquals(1, nodeReports.size());

    rm.stop();
    rpc.stopProxy(client, conf);
    new File(excludeFile).delete();
  }
}
