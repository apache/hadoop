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
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
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
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
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
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;

import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestClientRMService {

  private static final Log LOG = LogFactory.getLog(TestClientRMService.class);

  private RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private String appType = "MockApp";
  
  private final static String QUEUE_1 = "Q-1";
  private final static String QUEUE_2 = "Q-2";

  @Test
  public void testGetDecommissioningClusterNodes() throws Exception {
    MockRM rm = new MockRM() {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager,
            this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    rm.start();

    int nodeMemory = 1024;
    MockNM nm1 = rm.registerNode("host1:1234", nodeMemory);
    rm.sendNodeStarted(nm1);
    nm1.nodeHeartbeat(true);
    rm.waitForState(nm1.getNodeId(), NodeState.RUNNING);
    Integer decommissioningTimeout = 600;
    rm.sendNodeGracefulDecommission(nm1, decommissioningTimeout);
    rm.waitForState(nm1.getNodeId(), NodeState.DECOMMISSIONING);

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc
            .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    List<NodeReport> nodeReports = client.getClusterNodes(
        GetClusterNodesRequest.newInstance(
            EnumSet.of(NodeState.DECOMMISSIONING)))
        .getNodeReports();
    Assert.assertEquals(1, nodeReports.size());
    NodeReport nr = nodeReports.iterator().next();
    Assert.assertEquals(decommissioningTimeout, nr.getDecommissioningTimeout());
    Assert.assertNull(nr.getNodeUpdateType());

    rpc.stopProxy(client, conf);
    rm.close();
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
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));

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
    rm.waitForState(lostNode.getNodeId(), NodeState.RUNNING);
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
    Assert.assertNull(nodeReports.get(0).getDecommissioningTimeout());
    Assert.assertNull(nodeReports.get(0).getNodeUpdateType());

    // Now make the node unhealthy.
    node.nodeHeartbeat(false);
    rm.waitForState(node.getNodeId(), NodeState.UNHEALTHY);

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
    Assert.assertNull(nodeReports.get(0).getDecommissioningTimeout());
    Assert.assertNull(nodeReports.get(0).getNodeUpdateType());
    
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
      Assert.assertNull(report.getDecommissioningTimeout());
      Assert.assertNull(report.getNodeUpdateType());
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
              + "' doesn't exist in RM. Please check that the "
              + "job submission was successful.");
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
      Assert.assertEquals("<Not set>", report.getAmNodeLabelExpression());
      Assert.assertEquals("<Not set>", report.getAppNodeLabelExpression());

      // if application has am node label set to blank
      ApplicationId appId2 = getApplicationId(2);
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
          ApplicationAccessType.VIEW_APP, null, appId2)).thenReturn(true);
      request.setApplicationId(appId2);
      response = rmService.getApplicationReport(request);
      report = response.getApplicationReport();

      Assert.assertEquals(NodeLabel.DEFAULT_NODE_LABEL_PARTITION,
          report.getAmNodeLabelExpression());
      Assert.assertEquals(NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET,
          report.getAppNodeLabelExpression());

      // if application has am node label set to blank
      ApplicationId appId3 = getApplicationId(3);
      when(mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
          ApplicationAccessType.VIEW_APP, null, appId3)).thenReturn(true);

      request.setApplicationId(appId3);
      response = rmService.getApplicationReport(request);
      report = response.getApplicationReport();

      Assert.assertEquals("high-mem", report.getAmNodeLabelExpression());
      Assert.assertEquals("high-mem", report.getAppNodeLabelExpression());

      // if application id is null
      GetApplicationReportRequest invalidRequest = recordFactory
          .newRecordInstance(GetApplicationReportRequest.class);
      invalidRequest.setApplicationId(null);
      try {
        rmService.getApplicationReport(invalidRequest);
      } catch (YarnException e) {
        // rmService should return a ApplicationNotFoundException
        // when a null application id is provided
        Assert.assertTrue(e instanceof ApplicationNotFoundException);
      }
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
        rmContext, yarnScheduler, null, asContext, config, null, null);
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

  public ClientRMService createRMService() throws IOException, YarnException {
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
            any(QueueACL.class), any(RMApp.class), any(String.class),
            any())).thenReturn(true);
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
    conf.setBoolean(MockRM.ENABLE_WEBAPP, true);
    MockRM rm = new MockRM(conf);
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
    String diagnostic = "message1";
    killRequest1.setDiagnostics(diagnostic);
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
    assertTrue("Diagnostic message is incorrect",
        app1.getDiagnostics().toString().contains(diagnostic));

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
        MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
            "newqueue");
    rmService.moveApplicationAcrossQueues(request);
  }

  @Test
  public void testMoveApplicationSubmitTargetQueue() throws Exception {
    // move the application as the owner
    ApplicationId applicationId = getApplicationId(1);
    UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
    QueueACLsManager queueACLsManager = getQueueAclManager("allowed_queue",
        QueueACL.SUBMIT_APPLICATIONS, aclUGI);
    ApplicationACLsManager appAclsManager = getAppAclManager();

    ClientRMService rmService = createClientRMServiceForMoveApplicationRequest(
        applicationId, aclUGI.getShortUserName(), appAclsManager,
        queueACLsManager);

    // move as the owner queue in the acl
    MoveApplicationAcrossQueuesRequest moveAppRequest =
        MoveApplicationAcrossQueuesRequest.
        newInstance(applicationId, "allowed_queue");
    rmService.moveApplicationAcrossQueues(moveAppRequest);

    // move as the owner queue not in the acl
    moveAppRequest = MoveApplicationAcrossQueuesRequest.newInstance(
        applicationId, "not_allowed");

    try {
      rmService.moveApplicationAcrossQueues(moveAppRequest);
      Assert.fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      Assert.assertTrue("AccessControlException is expected",
          rex.getCause() instanceof AccessControlException);
    }

    // ACL is owned by "moveuser", move is performed as a different user
    aclUGI = UserGroupInformation.createUserForTesting("moveuser",
        new String[]{});
    queueACLsManager = getQueueAclManager("move_queue",
        QueueACL.SUBMIT_APPLICATIONS, aclUGI);
    appAclsManager = getAppAclManager();
    ClientRMService rmService2 =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueACLsManager);

    // access to the queue not OK: user not allowed in this queue
    MoveApplicationAcrossQueuesRequest moveAppRequest2 =
        MoveApplicationAcrossQueuesRequest.
            newInstance(applicationId, "move_queue");
    try {
      rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      Assert.fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      Assert.assertTrue("AccessControlException is expected",
          rex.getCause() instanceof AccessControlException);
    }

    // execute the move as the acl owner
    // access to the queue OK: user allowed in this queue
    aclUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        return rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      }
    });
  }

  @Test
  public void testMoveApplicationAdminTargetQueue() throws Exception {
    ApplicationId applicationId = getApplicationId(1);
    UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
    QueueACLsManager queueAclsManager = getQueueAclManager("allowed_queue",
        QueueACL.ADMINISTER_QUEUE, aclUGI);
    ApplicationACLsManager appAclsManager = getAppAclManager();
    ClientRMService rmService =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueAclsManager);

    // user is admin move to queue in acl
    MoveApplicationAcrossQueuesRequest moveAppRequest =
        MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
            "allowed_queue");
    rmService.moveApplicationAcrossQueues(moveAppRequest);

    // user is admin move to queue not in acl
    moveAppRequest = MoveApplicationAcrossQueuesRequest.newInstance(
        applicationId, "not_allowed");

    try {
      rmService.moveApplicationAcrossQueues(moveAppRequest);
      Assert.fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      Assert.assertTrue("AccessControlException is expected",
          rex.getCause() instanceof AccessControlException);
    }

    // ACL is owned by "moveuser", move is performed as a different user
    aclUGI = UserGroupInformation.createUserForTesting("moveuser",
        new String[]{});
    queueAclsManager = getQueueAclManager("move_queue",
        QueueACL.ADMINISTER_QUEUE, aclUGI);
    appAclsManager = getAppAclManager();
    ClientRMService rmService2 =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueAclsManager);

    // no access to this queue
    MoveApplicationAcrossQueuesRequest moveAppRequest2 =
        MoveApplicationAcrossQueuesRequest.
            newInstance(applicationId, "move_queue");

    try {
      rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      Assert.fail("The request should fail with an AccessControlException");
    } catch (YarnException rex) {
      Assert.assertTrue("AccessControlException is expected",
          rex.getCause() instanceof AccessControlException);
    }

    // execute the move as the acl owner
    // access to the queue OK: user allowed in this queue
    aclUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        return rmService2.moveApplicationAcrossQueues(moveAppRequest2);
      }
    });
  }

  @Test (expected = YarnException.class)
  public void testNonExistingQueue() throws Exception {
    ApplicationId applicationId = getApplicationId(1);
    UserGroupInformation aclUGI = UserGroupInformation.getCurrentUser();
    QueueACLsManager queueAclsManager = getQueueAclManager();
    ApplicationACLsManager appAclsManager = getAppAclManager();
    ClientRMService rmService =
        createClientRMServiceForMoveApplicationRequest(applicationId,
            aclUGI.getShortUserName(), appAclsManager, queueAclsManager);

    MoveApplicationAcrossQueuesRequest moveAppRequest =
        MoveApplicationAcrossQueuesRequest.newInstance(applicationId,
            "unknown_queue");
    rmService.moveApplicationAcrossQueues(moveAppRequest);
  }

  /**
   * Create an instance of ClientRMService for testing
   * moveApplicationAcrossQueues requests.
   * @param applicationId the application
   * @return ClientRMService
   */
  private ClientRMService createClientRMServiceForMoveApplicationRequest(
      ApplicationId applicationId, String appOwner,
      ApplicationACLsManager appAclsManager, QueueACLsManager queueAclsManager)
      throws IOException {
    RMApp app = mock(RMApp.class);
    when(app.getUser()).thenReturn(appOwner);
    when(app.getState()).thenReturn(RMAppState.RUNNING);
    ConcurrentHashMap<ApplicationId, RMApp> apps = new ConcurrentHashMap<>();
    apps.put(applicationId, app);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getRMApps()).thenReturn(apps);

    RMAppManager rmAppManager = mock(RMAppManager.class);
    return new ClientRMService(rmContext, null, rmAppManager, appAclsManager,
        queueAclsManager, null);
  }

  /**
   * Plain application acl manager that always returns true.
   * @return ApplicationACLsManager
   */
  private ApplicationACLsManager getAppAclManager() {
    ApplicationACLsManager aclsManager = mock(ApplicationACLsManager.class);
    when(aclsManager.checkAccess(
        any(UserGroupInformation.class),
        any(ApplicationAccessType.class),
        any(String.class),
        any(ApplicationId.class))).thenReturn(true);
    return aclsManager;
  }

  /**
   * Generate the Queue acl.
   * @param allowedQueue the queue to allow the move to
   * @param queueACL the acl to check: submit app or queue admin
   * @param aclUser the user to check
   * @return QueueACLsManager
   */
  private QueueACLsManager getQueueAclManager(String allowedQueue,
      QueueACL queueACL, UserGroupInformation aclUser) throws IOException {
    // ACL that checks the queue is allowed
    QueueACLsManager queueACLsManager = mock(QueueACLsManager.class);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyListOf(String.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) {
              final UserGroupInformation user =
                  (UserGroupInformation) invocationOnMock.getArguments()[0];
              final QueueACL acl =
                  (QueueACL) invocationOnMock.getArguments()[1];
              return (queueACL.equals(acl) &&
                  aclUser.getShortUserName().equals(user.getShortUserName()));
            }
        });

    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyListOf(String.class),
        any(String.class))).thenAnswer(new Answer<Boolean>() {
          @Override
          public Boolean answer(InvocationOnMock invocationOnMock) {
            final UserGroupInformation user =
                (UserGroupInformation) invocationOnMock.getArguments()[0];
            final QueueACL acl = (QueueACL) invocationOnMock.getArguments()[1];
            final String queue = (String) invocationOnMock.getArguments()[5];
            return (allowedQueue.equals(queue) && queueACL.equals(acl) &&
                aclUser.getShortUserName().equals(user.getShortUserName()));
          }
        });
    return queueACLsManager;
  }

  /**
   * QueueACLsManager that always returns false when a target queue is passed
   * in and true for other checks to simulate a missing queue.
   * @return QueueACLsManager
   */
  private QueueACLsManager getQueueAclManager() {
    QueueACLsManager queueACLsManager = mock(QueueACLsManager.class);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyListOf(String.class),
        any(String.class))).thenReturn(false);
    when(queueACLsManager.checkAccess(
        any(UserGroupInformation.class),
        any(QueueACL.class),
        any(RMApp.class),
        any(String.class),
        anyListOf(String.class))).thenReturn(true);
    return queueACLsManager;
  }

  @Test
  public void testGetQueueInfo() throws Exception {
    YarnScheduler yarnScheduler = mock(YarnScheduler.class);
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);
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
    Map<String, QueueConfigurations> queueConfigsByPartition =
        queueInfo.getQueueInfo().getQueueConfigurations();
    Assert.assertEquals(1, queueConfigsByPartition.size());
    Assert.assertTrue(queueConfigsByPartition.containsKey("*"));
    QueueConfigurations queueConfigs = queueConfigsByPartition.get("*");
    Assert.assertEquals(0.5f, queueConfigs.getCapacity(), 0.0001f);
    Assert.assertEquals(0.1f, queueConfigs.getAbsoluteCapacity(), 0.0001f);
    Assert.assertEquals(1.0f, queueConfigs.getMaxCapacity(), 0.0001f);
    Assert.assertEquals(1.0f, queueConfigs.getAbsoluteMaxCapacity(), 0.0001f);
    Assert.assertEquals(0.2f, queueConfigs.getMaxAMPercentage(), 0.0001f);

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
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(false);
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
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    ApplicationId appId1 = getApplicationId(100);

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    when(
        mockAclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
            ApplicationAccessType.VIEW_APP, null, appId1)).thenReturn(true);

    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);
    rmService.init(new Configuration());

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

    // Test query with uppercase appType also works
    appTypes = new HashSet<String>();
    appTypes.add("MATCHTYPE");
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
  public void testGetApplications() throws Exception {
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
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
    .getRMTimelineCollectorManager();

    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler,
        null, mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler()).thenReturn(
        new EventHandler<Event>() {
          public void handle(Event event) {}
        });

    ApplicationACLsManager mockAclsManager = mock(ApplicationACLsManager.class);
    QueueACLsManager mockQueueACLsManager = mock(QueueACLsManager.class);
    when(mockQueueACLsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        any()))
        .thenReturn(true);
    ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager,
            mockAclsManager, mockQueueACLsManager, null);
    rmService.init(new Configuration());

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
      // make sure each app is submitted at a different time
      Thread.sleep(1);
      rmService.submitApplication(submitRequest);
      submitTimeMillis[i] = rmService.getApplicationReport(
          GetApplicationReportRequest.newInstance(appId))
          .getApplicationReport().getStartTime();
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
    request.setStartRange(submitTimeMillis[0] + 1, System.currentTimeMillis());
    
    // 2 applications are submitted after first timeMills
    assertEquals("Incorrect number of matching start range", 
        2, rmService.getApplications(request).getApplicationList().size());
    
    // 1 application is submitted after the second timeMills
    request.setStartRange(submitTimeMillis[1] + 1, System.currentTimeMillis());
    assertEquals("Incorrect number of matching start range", 
        1, rmService.getApplications(request).getApplicationList().size());
    
    // no application is submitted after the third timeMills
    request.setStartRange(submitTimeMillis[2] + 1, System.currentTimeMillis());
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
        rmService.getApplications(request).getApplicationList().size());

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

    rmService.setDisplayPerUserApps(true);
    userSet.clear();
    assertEquals("Incorrect number of applications for user", 6,
        rmService.getApplications(request).getApplicationList().size());
    rmService.setDisplayPerUserApps(false);

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

    EventHandler<Event> eventHandler = new EventHandler<Event>() {
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
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    final ClientRMService rmService =
        new ClientRMService(rmContext, yarnScheduler, appManager, null, null,
            null);
    rmService.init(new Configuration());

    // submit an app and wait for it to block while in app submission
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          rmService.submitApplication(submitRequest1);
        } catch (YarnException | IOException e) {}
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
    submissionContext.setPriority(Priority.newInstance(0));

    SubmitApplicationRequest submitRequest =
        recordFactory.newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(submissionContext);
    return submitRequest;
  }

  private void mockRMContext(YarnScheduler yarnScheduler, RMContext rmContext)
      throws IOException {
    Dispatcher dispatcher = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(dispatcher);
    @SuppressWarnings("unchecked")
    EventHandler<Event> eventHandler = mock(EventHandler.class);
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);

    QueueInfo queInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queInfo.setQueueName("testqueue");
    QueueConfigurations queueConfigs =
        recordFactory.newRecordInstance(QueueConfigurations.class);
    queueConfigs.setCapacity(0.5f);
    queueConfigs.setAbsoluteCapacity(0.1f);
    queueConfigs.setMaxCapacity(1.0f);
    queueConfigs.setAbsoluteMaxCapacity(1.0f);
    queueConfigs.setMaxAMPercentage(0.2f);
    Map<String, QueueConfigurations> queueConfigsByPartition =
        new HashMap<>();
    queueConfigsByPartition.put("*", queueConfigs);
    queInfo.setQueueConfigurations(queueConfigsByPartition);

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
        config, "testqueue", 10, 3,null,null));
    apps.put(applicationId2, getRMApp(rmContext, yarnScheduler, applicationId2,
        config, "a", 20, 2,null,""));
    apps.put(applicationId3, getRMApp(rmContext, yarnScheduler, applicationId3,
        config, "testqueue", 40, 5,"high-mem","high-mem"));
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
      final long memorySeconds, final long vcoreSeconds,
      String appNodeLabelExpression, String amNodeLabelExpression) {
    ApplicationSubmissionContext asContext = mock(ApplicationSubmissionContext.class);
    when(asContext.getMaxAppAttempts()).thenReturn(1);
    when(asContext.getNodeLabelExpression()).thenReturn(appNodeLabelExpression);
    when(asContext.getPriority()).thenReturn(Priority.newInstance(0));
    RMAppImpl app =
        spy(new RMAppImpl(applicationId3, rmContext, config, null, null,
            queueName, asContext, yarnScheduler, null,
            System.currentTimeMillis(), "YARN", null,
            Collections.singletonList(BuilderUtils.newResourceRequest(
                RMAppAttemptImpl.AM_CONTAINER_PRIORITY, ResourceRequest.ANY,
                Resource.newInstance(1024, 1), 1))){
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
    app.getAMResourceRequests().get(0)
        .setNodeLabelExpression(amNodeLabelExpression);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456, 1), 1);
    RMAppAttemptImpl rmAppAttemptImpl = spy(new RMAppAttemptImpl(attemptId,
        rmContext, yarnScheduler, null, asContext, config, null, app));
    Container container = Container.newInstance(
        ContainerId.newContainerId(attemptId, 1), null, "", null, null, null);
    RMContainerImpl containerimpl = spy(new RMContainerImpl(container,
        SchedulerRequestKey.extractFrom(container), attemptId, null, "",
        rmContext));
    Map<ApplicationAttemptId, RMAppAttempt> attempts = 
      new HashMap<ApplicationAttemptId, RMAppAttempt>();
    attempts.put(attemptId, rmAppAttemptImpl);
    when(app.getCurrentAppAttempt()).thenReturn(rmAppAttemptImpl);
    when(app.getAppAttempts()).thenReturn(attempts);
    when(app.getApplicationPriority()).thenReturn(Priority.newInstance(0));
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
    when(containerimpl.completed()).thenReturn(false);
    when(containerimpl.getDiagnosticsInfo()).thenReturn("N/A");
    when(containerimpl.getContainerExitStatus()).thenReturn(0);
    when(containerimpl.getContainerState()).thenReturn(ContainerState.COMPLETE);
    return app;
  }

  private static YarnScheduler mockYarnScheduler() throws YarnException {
    YarnScheduler yarnScheduler = mock(YarnScheduler.class);
    when(yarnScheduler.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
    when(yarnScheduler.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(yarnScheduler.getMaximumResourceCapability(any(String.class)))
        .thenReturn(Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
    when(yarnScheduler.getAppsInQueue(QUEUE_1)).thenReturn(
        Arrays.asList(getApplicationAttemptId(101), getApplicationAttemptId(102)));
    when(yarnScheduler.getAppsInQueue(QUEUE_2)).thenReturn(
        Arrays.asList(getApplicationAttemptId(103)));
    ApplicationAttemptId attemptId = getApplicationAttemptId(1);
    when(yarnScheduler.getAppResourceUsageReport(attemptId)).thenReturn(null);

    ResourceCalculator rs = mock(ResourceCalculator.class);
    when(yarnScheduler.getResourceCalculator()).thenReturn(rs);

    when(yarnScheduler.checkAndGetApplicationPriority(any(Priority.class),
        any(UserGroupInformation.class), anyString(), any(ApplicationId.class)))
            .thenReturn(Priority.newInstance(0));
    return yarnScheduler;
  }

  private ResourceManager setupResourceManager() {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    MockRM rm = new MockRM(conf);
    rm.start();
    try {
      rm.registerNode("127.0.0.1:1", 102400, 100);
      // allow plan follower to synchronize
      Thread.sleep(1050);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    return rm;
  }

  private ReservationSubmissionRequest submitReservationTestHelper(
      ClientRMService clientService, long arrival, long deadline,
      long duration) {
    ReservationSubmissionResponse sResponse = null;
    GetNewReservationRequest newReservationRequest =
        GetNewReservationRequest.newInstance();
    ReservationId reservationID = null;
    try {
      reservationID = clientService.getNewReservation(newReservationRequest)
          .getReservationId();
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    ReservationSubmissionRequest sRequest =
        ReservationSystemTestUtil.createSimpleReservationRequest(reservationID,
            4, arrival, deadline, duration);
    try {
      sResponse = clientService.submitReservation(sRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(sResponse);
    Assert.assertNotNull(reservationID);
    System.out.println("Submit reservation response: " + reservationID);
    return sRequest;
  }

  @Test
  public void testCreateReservation() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // Submit the reservation again with the same request and make sure it
    // passes.
    try {
      clientService.submitReservation(sRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Submit the reservation with the same reservation id but different
    // reservation definition, and ensure YarnException is thrown.
    arrival = clock.getTime();
    ReservationDefinition rDef = sRequest.getReservationDefinition();
    rDef.setArrival(arrival + duration);
    sRequest.setReservationDefinition(rDef);
    try {
      clientService.submitReservation(sRequest);
      Assert.fail("Reservation submission should fail if a duplicate "
          + "reservation id is used, but the reservation definition has been "
          + "updated.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YarnException);
    }

    rm.stop();
  }

  @Test
  public void testUpdateReservation() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationDefinition rDef = sRequest.getReservationDefinition();
    ReservationRequest rr =
        rDef.getReservationRequests().getReservationResources().get(0);
    ReservationId reservationID = sRequest.getReservationId();
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
    Assert.assertNotNull(uResponse);
    System.out.println("Update reservation response: " + uResponse);

    rm.stop();
  }

  @Test
  public void testListReservationsByReservationId() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationId reservationID = sRequest.getReservationId();
    ReservationListResponse response = null;
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
        -1, false);
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(1, response.getReservationAllocationState().size());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getResourceAllocationRequests().size(), 0);

    rm.stop();
  }

  @Test
  public void testListReservationsByTimeInterval() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by a point in time within the reservation
    // range.
    arrival = clock.getTime();
    ReservationId reservationID = sRequest.getReservationId();
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", arrival + duration / 2,
        arrival + duration / 2, true);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(1, response.getReservationAllocationState().size());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    // List reservations, search by time within reservation interval.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", 1, Long.MAX_VALUE, true);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(1, response.getReservationAllocationState().size());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), reservationID.getId());
    // Verify that the full resource allocations exist.
    Assert.assertTrue(response.getReservationAllocationState().get(0)
        .getResourceAllocationRequests().size() > 0);

    // Verify that the full RDL is returned.
    ReservationRequests reservationRequests =
        response.getReservationAllocationState().get(0)
            .getReservationDefinition().getReservationRequests();
    Assert.assertEquals("R_ALL",
        reservationRequests.getInterpreter().toString());
    Assert.assertTrue(reservationRequests.getReservationResources().get(0)
        .getDuration() == duration);

    rm.stop();
  }

  @Test
  public void testListReservationsByInvalidTimeInterval() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by invalid end time == -1.
    ReservationListRequest request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 1, -1, true);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(1, response.getReservationAllocationState().size());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), sRequest.getReservationId().getId());

    // List reservations, search by invalid end time < -1.
    request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 1, -10, true);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(1, response.getReservationAllocationState().size());
    Assert.assertEquals(response.getReservationAllocationState().get(0)
        .getReservationId().getId(), sRequest.getReservationId().getId());

    rm.stop();
  }

  @Test
  public void testListReservationsByTimeIntervalContainingNoReservations() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    // List reservations, search by very large start time.
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", Long.MAX_VALUE, -1, false);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getReservationAllocationState().size(), 0);

    duration = 30000;
    deadline = sRequest.getReservationDefinition().getDeadline();

    // List reservations, search by start time after the reservation
    // end time.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", deadline + duration,
        deadline + 2 * duration, false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getReservationAllocationState().size(), 0);

    arrival = clock.getTime();
    // List reservations, search by end time before the reservation start
    // time.
    request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, "", 0, arrival - duration,
        false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getReservationAllocationState().size(), 0);

    // List reservations, search by very small end time.
    request = ReservationListRequest
        .newInstance(ReservationSystemTestUtil.reservationQ, "", 0, 1, false);

    response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Ensure all reservations are filtered out.
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getReservationAllocationState().size(), 0);

    rm.stop();
  }

  @Test
  public void testReservationDelete() {
    ResourceManager rm = setupResourceManager();
    ClientRMService clientService = rm.getClientRMService();
    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);
    ReservationSubmissionRequest sRequest =
        submitReservationTestHelper(clientService, arrival, deadline, duration);

    ReservationId reservationID = sRequest.getReservationId();
    // Delete the reservation
    ReservationDeleteRequest dRequest =
        ReservationDeleteRequest.newInstance(reservationID);
    ReservationDeleteResponse dResponse = null;
    try {
      dResponse = clientService.deleteReservation(dRequest);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(dResponse);
    System.out.println("Delete reservation response: " + dResponse);

    // List reservations, search by non-existent reservationID
    ReservationListRequest request = ReservationListRequest.newInstance(
        ReservationSystemTestUtil.reservationQ, reservationID.toString(), -1,
        -1, false);

    ReservationListResponse response = null;
    try {
      response = clientService.listReservations(request);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(response);
    Assert.assertEquals(0, response.getReservationAllocationState().size());

    rm.stop();
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
    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y");
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY));

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
    ApplicationClientProtocol client = (ApplicationClientProtocol) rpc
        .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabelList().containsAll(
        Arrays.asList(labelX, labelY)));

    // Get node labels mapping
    GetNodesToLabelsResponse response1 = client
        .getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Map<NodeId, Set<String>> nodeToLabels = response1.getNodeToLabels();
    Assert.assertTrue(nodeToLabels.keySet().containsAll(
        Arrays.asList(node1, node2)));
    Assert.assertTrue(nodeToLabels.get(node1)
        .containsAll(Arrays.asList(labelX.getName())));
    Assert.assertTrue(nodeToLabels.get(node2)
        .containsAll(Arrays.asList(labelY.getName())));
    // Below label "x" is not present in the response as exclusivity is true
    Assert.assertFalse(nodeToLabels.get(node1).containsAll(
        Arrays.asList(NodeLabel.newInstance("x"))));

    rpc.stopProxy(client, conf);
    rm.stop();
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

    NodeLabel labelX = NodeLabel.newInstance("x", false);
    NodeLabel labelY = NodeLabel.newInstance("y", false);
    NodeLabel labelZ = NodeLabel.newInstance("z", false);
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabels(ImmutableSet.of(labelX, labelY, labelZ));

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
    ApplicationClientProtocol client = (ApplicationClientProtocol) rpc
        .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabelList().containsAll(
        Arrays.asList(labelX, labelY, labelZ)));

    // Get labels to nodes mapping
    GetLabelsToNodesResponse response1 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance());
    Map<String, Set<NodeId>> labelsToNodes = response1.getLabelsToNodes();
    Assert.assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX.getName(), labelY.getName(), labelZ.getName())));
    Assert.assertTrue(labelsToNodes.get(labelX.getName()).containsAll(
        Arrays.asList(node1A)));
    Assert.assertTrue(labelsToNodes.get(labelY.getName()).containsAll(
        Arrays.asList(node2A, node3A)));
    Assert.assertTrue(labelsToNodes.get(labelZ.getName()).containsAll(
        Arrays.asList(node1B, node3B)));

    // Get labels to nodes mapping for specific labels
    Set<String> setlabels = new HashSet<String>(Arrays.asList(new String[]{"x",
        "z"}));
    GetLabelsToNodesResponse response2 = client
        .getLabelsToNodes(GetLabelsToNodesRequest.newInstance(setlabels));
    labelsToNodes = response2.getLabelsToNodes();
    Assert.assertTrue(labelsToNodes.keySet().containsAll(
        Arrays.asList(labelX.getName(), labelZ.getName())));
    Assert.assertTrue(labelsToNodes.get(labelX.getName()).containsAll(
        Arrays.asList(node1A)));
    Assert.assertTrue(labelsToNodes.get(labelZ.getName()).containsAll(
        Arrays.asList(node1B, node3B)));
    Assert.assertEquals(labelsToNodes.get(labelY.getName()), null);

    rpc.stopProxy(client, conf);
    rm.close();
  }

  @Test(timeout = 120000)
  public void testUpdatePriorityAndKillAppWithZeroClusterResource()
      throws Exception {
    int maxPriority = 10;
    int appPriority = 5;
    YarnConfiguration conf = new YarnConfiguration();
    Assume.assumeFalse("FairScheduler does not support Application Priorities",
        conf.get(YarnConfiguration.RM_SCHEDULER)
            .equals(FairScheduler.class.getName()));
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        maxPriority);
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    RMApp app1 = rm.submitApp(1024, Priority.newInstance(appPriority));
    ClientRMService rmService = rm.getClientRMService();
    testApplicationPriorityUpdation(rmService, app1, appPriority, appPriority);
    rm.killApp(app1.getApplicationId());
    rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);
    rm.stop();
  }

  @Test(timeout = 120000)
  public void testUpdateApplicationPriorityRequest() throws Exception {
    int maxPriority = 10;
    int appPriority = 5;
    YarnConfiguration conf = new YarnConfiguration();
    Assume.assumeFalse("FairScheduler does not support Application Priorities",
        conf.get(YarnConfiguration.RM_SCHEDULER)
            .equals(FairScheduler.class.getName()));
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY,
        maxPriority);
    MockRM rm = new MockRM(conf);
    rm.init(conf);
    rm.start();
    rm.registerNode("host1:1234", 1024);
    // Start app1 with appPriority 5
    RMApp app1 = rm.submitApp(1024, Priority.newInstance(appPriority));

    Assert.assertEquals("Incorrect priority has been set to application",
        appPriority, app1.getApplicationPriority().getPriority());

    appPriority = 11;
    ClientRMService rmService = rm.getClientRMService();
    testApplicationPriorityUpdation(rmService, app1, appPriority, maxPriority);

    appPriority = 9;
    testApplicationPriorityUpdation(rmService, app1, appPriority, appPriority);

    rm.killApp(app1.getApplicationId());
    rm.waitForState(app1.getApplicationId(), RMAppState.KILLED);


    // Update priority request for invalid application id.
    ApplicationId invalidAppId = ApplicationId.newInstance(123456789L, 3);
    UpdateApplicationPriorityRequest updateRequest =
        UpdateApplicationPriorityRequest.newInstance(invalidAppId,
            Priority.newInstance(appPriority));
    try {
      rmService.updateApplicationPriority(updateRequest);
      Assert.fail("ApplicationNotFoundException should be thrown "
          + "for invalid application id");
    } catch (ApplicationNotFoundException e) {
      // Expected
    }

    updateRequest =
        UpdateApplicationPriorityRequest.newInstance(app1.getApplicationId(),
            Priority.newInstance(11));
    Assert.assertEquals("Incorrect priority has been set to application",
        appPriority, rmService.updateApplicationPriority(updateRequest)
            .getApplicationPriority().getPriority());

    rm.stop();
  }

  private void testApplicationPriorityUpdation(ClientRMService rmService,
      RMApp app1, int tobeUpdatedPriority, int expected) throws YarnException,
      IOException {
    UpdateApplicationPriorityRequest updateRequest =
        UpdateApplicationPriorityRequest.newInstance(app1.getApplicationId(),
            Priority.newInstance(tobeUpdatedPriority));

    UpdateApplicationPriorityResponse updateApplicationPriority =
        rmService.updateApplicationPriority(updateRequest);

    Assert.assertEquals("Incorrect priority has been set to application",
        expected, app1.getApplicationSubmissionContext().getPriority()
            .getPriority());
    Assert.assertEquals("Incorrect priority has been returned", expected,
        updateApplicationPriority.getApplicationPriority().getPriority());
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

  @Test
  public void testGetResourceTypesInfoWhenResourceProfileDisabled()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler,
            this.rmAppManager, this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      }
    };
    rm.start();

    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    ApplicationClientProtocol client =
        (ApplicationClientProtocol) rpc
            .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Make call
    GetAllResourceTypeInfoRequest request =
        GetAllResourceTypeInfoRequest.newInstance();
    GetAllResourceTypeInfoResponse response = client.getResourceTypeInfo(request);

    Assert.assertEquals(2, response.getResourceTypeInfo().size());

    // Check memory
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getName(),
        response.getResourceTypeInfo().get(0).getName());
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        response.getResourceTypeInfo().get(0).getDefaultUnit());

    // Check vcores
    Assert.assertEquals(ResourceInformation.VCORES.getName(),
        response.getResourceTypeInfo().get(1).getName());
    Assert.assertEquals(ResourceInformation.VCORES.getUnits(),
        response.getResourceTypeInfo().get(1).getDefaultUnit());

    rm.stop();
    rpc.stopProxy(client, conf);
  }

  @Test
  public void testGetApplicationsWithPerUserApps()
      throws IOException, YarnException {
    /*
     * Submit 3 applications alternately in two queues
     */
    // Basic setup
    YarnScheduler yarnScheduler = mockYarnScheduler();
    RMContext rmContext = mock(RMContext.class);
    mockRMContext(yarnScheduler, rmContext);
    RMStateStore stateStore = mock(RMStateStore.class);
    when(rmContext.getStateStore()).thenReturn(stateStore);
    doReturn(mock(RMTimelineCollectorManager.class)).when(rmContext)
        .getRMTimelineCollectorManager();

    RMAppManager appManager = new RMAppManager(rmContext, yarnScheduler, null,
        mock(ApplicationACLsManager.class), new Configuration());
    when(rmContext.getDispatcher().getEventHandler())
        .thenReturn(new EventHandler<Event>() {
          public void handle(Event event) {
          }
        });

    // Simulate Queue ACL manager which returns false always
    QueueACLsManager queueAclsManager = mock(QueueACLsManager.class);
    when(queueAclsManager.checkAccess(any(UserGroupInformation.class),
        any(QueueACL.class), any(RMApp.class), any(String.class),
        anyListOf(String.class))).thenReturn(false);

    // Simulate app ACL manager which returns false always
    ApplicationACLsManager appAclsManager = mock(ApplicationACLsManager.class);
    when(appAclsManager.checkAccess(eq(UserGroupInformation.getCurrentUser()),
        any(ApplicationAccessType.class), any(String.class),
        any(ApplicationId.class))).thenReturn(false);
    ClientRMService rmService = new ClientRMService(rmContext, yarnScheduler,
        appManager, appAclsManager, queueAclsManager, null);
    rmService.init(new Configuration());

    // Initialize appnames and queues
    String[] queues = {QUEUE_1, QUEUE_2};
    String[] appNames = {MockApps.newAppName(), MockApps.newAppName(),
        MockApps.newAppName()};
    ApplicationId[] appIds = {getApplicationId(101), getApplicationId(102),
        getApplicationId(103)};
    List<String> tags = Arrays.asList("Tag1", "Tag2", "Tag3");

    long[] submitTimeMillis = new long[3];
    // Submit applications
    for (int i = 0; i < appIds.length; i++) {
      ApplicationId appId = appIds[i];
      SubmitApplicationRequest submitRequest = mockSubmitAppRequest(appId,
          appNames[i], queues[i % queues.length],
          new HashSet<String>(tags.subList(0, i + 1)));
      rmService.submitApplication(submitRequest);
      submitTimeMillis[i] = System.currentTimeMillis();
    }

    // Test different cases of ClientRMService#getApplications()
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    assertEquals("Incorrect total number of apps", 6,
        rmService.getApplications(request).getApplicationList().size());

    rmService.setDisplayPerUserApps(true);
    assertEquals("Incorrect number of applications for user", 0,
        rmService.getApplications(request).getApplicationList().size());
    rmService.setDisplayPerUserApps(false);
  }
}
