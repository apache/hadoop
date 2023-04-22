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

package org.apache.hadoop.yarn.server.router.clientrm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code FederationInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request interceptor chains.
 */
public class TestFederationClientInterceptor extends BaseRouterClientRMTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationClientInterceptor.class);

  private TestableFederationClientInterceptor interceptor;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreUtil;
  private List<SubClusterId> subClusters;

  private String user = "test-user";

  private final static int NUM_SUBCLUSTER = 4;

  private final static int APP_PRIORITY_ZERO = 0;

  private final static long DEFAULT_DURATION = 10 * 60 * 1000;

  @Override
  public void setUp() throws IOException {
    super.setUpConfig();
    interceptor = new TestableFederationClientInterceptor();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(this.getConf());
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, getConf());
    stateStoreUtil = new FederationStateStoreTestUtil(stateStore);

    interceptor.setConf(this.getConf());
    interceptor.init(user);
    RouterDelegationTokenSecretManager tokenSecretManager =
        interceptor.createRouterRMDelegationTokenSecretManager(this.getConf());

    tokenSecretManager.startThreads();
    interceptor.setTokenSecretManager(tokenSecretManager);

    subClusters = new ArrayList<>();

    try {
      for (int i = 0; i < NUM_SUBCLUSTER; i++) {
        SubClusterId sc = SubClusterId.newInstance(Integer.toString(i));
        stateStoreUtil.registerSubCluster(sc);
        subClusters.add(sc);
      }
    } catch (YarnException e) {
      LOG.error(e.getMessage());
      Assert.fail();
    }

    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    String mockPassThroughInterceptorClass =
        PassThroughClientRequestInterceptor.class.getName();

    // Create a request interceptor pipeline for testing. The last one in the
    // chain is the federation interceptor that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
        + "," + TestableFederationClientInterceptor.class.getName());

    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());

    // Disable StateStoreFacade cache
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
    conf.setInt("yarn.scheduler.maximum-allocation-mb", 100 * 1024);
    conf.setInt("yarn.scheduler.maximum-allocation-vcores", 100);

    conf.setBoolean("hadoop.security.authentication", true);
    return conf;
  }

  /**
   * This test validates the correctness of GetNewApplication. The return
   * ApplicationId has to belong to one of the SubCluster in the cluster.
   */
  @Test
  public void testGetNewApplication() throws YarnException, IOException {
    LOG.info("Test FederationClientInterceptor: Get New Application.");

    GetNewApplicationRequest request = GetNewApplicationRequest.newInstance();
    GetNewApplicationResponse response = interceptor.getNewApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getApplicationId());
    Assert.assertEquals(response.getApplicationId().getClusterTimestamp(),
        ResourceManager.getClusterTimeStamp());
  }

  /**
   * This test validates the correctness of SubmitApplication. The application
   * has to be submitted to one of the SubCluster in the cluster.
   */
  @Test
  public void testSubmitApplication()
      throws YarnException, IOException {
    LOG.info("Test FederationClientInterceptor: Submit Application.");

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    SubClusterId scIdResult = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult);
    Assert.assertTrue(subClusters.contains(scIdResult));
  }

  private SubmitApplicationRequest mockSubmitApplicationRequest(
      ApplicationId appId) {
    ContainerLaunchContext amContainerSpec = mock(ContainerLaunchContext.class);
    ApplicationSubmissionContext context = ApplicationSubmissionContext.newInstance(
        appId, MockApps.newAppName(), "default",
        Priority.newInstance(APP_PRIORITY_ZERO), amContainerSpec, false, false, -1,
        Resources.createResource(YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
        "MockApp");
    SubmitApplicationRequest request = SubmitApplicationRequest.newInstance(context);
    return request;
  }

  /**
   * This test validates the correctness of SubmitApplication in case of
   * multiple submission. The first retry has to be submitted to the same
   * SubCluster of the first attempt.
   */
  @Test
  public void testSubmitApplicationMultipleSubmission()
      throws YarnException, IOException, InterruptedException {
    LOG.info(
        "Test FederationClientInterceptor: Submit Application - Multiple");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // First attempt
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    SubClusterId scIdResult = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult);

    // First retry
    response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    SubClusterId scIdResult2 = stateStoreUtil.queryApplicationHomeSC(appId);
    Assert.assertNotNull(scIdResult2);
    Assert.assertEquals(scIdResult, scIdResult);
  }

  /**
   * This test validates the correctness of SubmitApplication in case of empty
   * request.
   */
  @Test
  public void testSubmitApplicationEmptyRequest()
      throws Exception {
    LOG.info("Test FederationClientInterceptor: Submit Application - Empty.");

    // null request1
    LambdaTestUtils.intercept(YarnException.class,
        "Missing submitApplication request or applicationSubmissionContext information.",
        () -> interceptor.submitApplication(null));

    // null request2
    LambdaTestUtils.intercept(YarnException.class,
        "Missing submitApplication request or applicationSubmissionContext information.",
        () -> interceptor.submitApplication(SubmitApplicationRequest.newInstance(null)));

    // null request3
    ApplicationSubmissionContext context = ApplicationSubmissionContext
        .newInstance(null, "", "", null, null, false, false, -1, null, null);
    SubmitApplicationRequest request =
        SubmitApplicationRequest.newInstance(context);
    LambdaTestUtils.intercept(YarnException.class,
        "Missing submitApplication request or applicationSubmissionContext information.",
        () -> interceptor.submitApplication(request));
  }

  /**
   * This test validates the correctness of ForceKillApplication in case the
   * application exists in the cluster.
   */
  @Test
  public void testForceKillApplication()
      throws YarnException, IOException, InterruptedException {
    LOG.info("Test FederationClientInterceptor: Force Kill Application.");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application we are going to kill later
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    KillApplicationRequest requestKill = KillApplicationRequest.newInstance(appId);
    KillApplicationResponse responseKill = interceptor.forceKillApplication(requestKill);
    Assert.assertNotNull(responseKill);
  }

  /**
   * This test validates the correctness of ForceKillApplication in case of
   * application does not exist in StateStore.
   */
  @Test
  public void testForceKillApplicationNotExists() throws Exception {
    LOG.info("Test FederationClientInterceptor: Force Kill Application - Not Exists");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    KillApplicationRequest requestKill =
        KillApplicationRequest.newInstance(appId);

    LambdaTestUtils.intercept(YarnException.class,
        "Application " + appId + " does not exist in FederationStateStore.",
        () -> interceptor.forceKillApplication(requestKill));
  }

  /**
   * This test validates the correctness of ForceKillApplication in case of
   * empty request.
   */
  @Test
  public void testForceKillApplicationEmptyRequest()
      throws Exception {
    LOG.info("Test FederationClientInterceptor: Force Kill Application - Empty.");

    // null request1
    LambdaTestUtils.intercept(YarnException.class,
        "Missing forceKillApplication request or ApplicationId.",
        () -> interceptor.forceKillApplication(null));

    // null request2
    KillApplicationRequest killRequest = KillApplicationRequest.newInstance(null);
    LambdaTestUtils.intercept(YarnException.class,
        "Missing forceKillApplication request or ApplicationId.",
        () -> interceptor.forceKillApplication(killRequest));
  }

  /**
   * This test validates the correctness of GetApplicationReport in case the
   * application exists in the cluster.
   */
  @Test
  public void testGetApplicationReport()
      throws YarnException, IOException, InterruptedException {
    LOG.info("Test FederationClientInterceptor: Get Application Report");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application we want the report later
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    GetApplicationReportRequest requestGet =
        GetApplicationReportRequest.newInstance(appId);

    GetApplicationReportResponse responseGet =
        interceptor.getApplicationReport(requestGet);

    Assert.assertNotNull(responseGet);
  }

  /**
   * This test validates the correctness of GetApplicationReport in case the
   * application does not exist in StateStore.
   */
  @Test
  public void testGetApplicationNotExists()
      throws Exception {
    LOG.info("Test ApplicationClientProtocol: Get Application Report - Not Exists.");

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    GetApplicationReportRequest requestGet = GetApplicationReportRequest.newInstance(appId);
    LambdaTestUtils.intercept(YarnException.class,
        "Application " + appId + " does not exist in FederationStateStore.",
        () -> interceptor.getApplicationReport(requestGet));
  }

  /**
   * This test validates the correctness of GetApplicationReport in case of
   * empty request.
   */
  @Test
  public void testGetApplicationEmptyRequest()
      throws Exception {
    LOG.info("Test FederationClientInterceptor: Get Application Report - Empty.");

    // null request1
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationReport request or applicationId information.",
        () -> interceptor.getApplicationReport(null));

    // null request2
    GetApplicationReportRequest reportRequest = GetApplicationReportRequest.newInstance(null);
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationReport request or applicationId information.",
        () -> interceptor.getApplicationReport(reportRequest));
  }

  /**
   * This test validates the correctness of GetApplicationAttemptReport in case the
   * application exists in the cluster.
   */
  @Test
  public void testGetApplicationAttemptReport()
          throws YarnException, IOException, InterruptedException {
    LOG.info("Test FederationClientInterceptor: Get ApplicationAttempt Report.");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application we want the applicationAttempt report later
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    // Call GetApplicationAttempts Get ApplicationAttemptId
    GetApplicationAttemptsRequest attemptsRequest =
         GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
         interceptor.getApplicationAttempts(attemptsRequest);

    // Wait for app to start
    while(attemptsResponse.getApplicationAttemptList().size() == 0) {
      attemptsResponse =
          interceptor.getApplicationAttempts(attemptsRequest);
    }

    Assert.assertNotNull(attemptsResponse);

    GetApplicationAttemptReportRequest requestGet =
         GetApplicationAttemptReportRequest.newInstance(
         attemptsResponse.getApplicationAttemptList().get(0).getApplicationAttemptId());

    GetApplicationAttemptReportResponse responseGet =
         interceptor.getApplicationAttemptReport(requestGet);

    Assert.assertNotNull(responseGet);
  }

  /**
   * This test validates the correctness of GetApplicationAttemptReport in case the
   * application does not exist in StateStore.
   */
  @Test
  public void testGetApplicationAttemptNotExists() throws Exception {
    LOG.info("Test FederationClientInterceptor: Get ApplicationAttempt Report - Not Exists.");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptID =
        ApplicationAttemptId.newInstance(appId, 1);
    GetApplicationAttemptReportRequest requestGet =
        GetApplicationAttemptReportRequest.newInstance(appAttemptID);

    LambdaTestUtils.intercept(YarnException.class, "ApplicationAttempt " +
        appAttemptID + " belongs to Application " +
        appId + " does not exist in FederationStateStore.",
        () -> interceptor.getApplicationAttemptReport(requestGet));
  }

  /**
   * This test validates the correctness of GetApplicationAttemptReport in case of
   * empty request.
   */
  @Test
  public void testGetApplicationAttemptEmptyRequest()
      throws Exception {
    LOG.info("Test FederationClientInterceptor: Get ApplicationAttempt Report - Empty.");

    // null request1
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationAttemptReport request or applicationId " +
        "or applicationAttemptId information.",
        () -> interceptor.getApplicationAttemptReport(null));

    // null request2
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationAttemptReport request or applicationId " +
        "or applicationAttemptId information.",
        () -> interceptor.getApplicationAttemptReport(
        GetApplicationAttemptReportRequest.newInstance(null)));

    // null request3
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getApplicationAttemptReport request or applicationId " +
        "or applicationAttemptId information.",
        () -> interceptor.getApplicationAttemptReport(
        GetApplicationAttemptReportRequest.newInstance(
        ApplicationAttemptId.newInstance(null, 1))));
  }


  @Test
  public void testGetClusterMetricsRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Cluster Metrics request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getClusterMetrics request.",
        () -> interceptor.getClusterMetrics(null));

    // normal request.
    GetClusterMetricsResponse response =
        interceptor.getClusterMetrics(GetClusterMetricsRequest.newInstance());
    Assert.assertEquals(subClusters.size(),
        response.getClusterMetrics().getNumNodeManagers());

    // Clear Membership
    Map<SubClusterId, SubClusterInfo> membership = new HashMap<>();
    membership.putAll(stateStore.getMembership());
    stateStore.getMembership().clear();

    ClientMethod remoteMethod = new ClientMethod("getClusterMetrics",
        new Class[] {GetClusterMetricsRequest.class},
        new Object[] {GetClusterMetricsRequest.newInstance()});
    Collection<GetClusterMetricsResponse> clusterMetrics = interceptor.invokeConcurrent(
        remoteMethod, GetClusterMetricsResponse.class);
    Assert.assertTrue(clusterMetrics.isEmpty());

    // Restore membership
    stateStore.setMembership(membership);
  }

  /**
   * This test validates the correctness of GetApplicationsResponse in case the
   * application exists in the cluster.
   */
  @Test
  public void testGetApplicationsResponse()
      throws YarnException, IOException, InterruptedException {
    LOG.info("Test FederationClientInterceptor: Get Applications Response.");
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);

    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    Set<String> appTypes = Collections.singleton("MockApp");
    GetApplicationsRequest requestGet = GetApplicationsRequest.newInstance(appTypes);
    GetApplicationsResponse responseGet = interceptor.getApplications(requestGet);

    Assert.assertNotNull(responseGet);
  }

  /**
   * This test validates the correctness of GetApplicationsResponse in case of
   * empty request.
   */
  @Test
  public void testGetApplicationsNullRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor: Get Applications request.");
    LambdaTestUtils.intercept(YarnException.class, "Missing getApplications request.",
        () -> interceptor.getApplications(null));
  }

  /**
   * This test validates the correctness of GetApplicationsResponse in case applications
   * with given type does not exist.
   */
  @Test
  public void testGetApplicationsApplicationTypeNotExists() throws Exception{
    LOG.info("Test FederationClientInterceptor: Application with type does not exist.");

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);

    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    Set<String> appTypes = Collections.singleton("SPARK");

    GetApplicationsRequest requestGet = GetApplicationsRequest.newInstance(appTypes);
    GetApplicationsResponse responseGet = interceptor.getApplications(requestGet);

    Assert.assertNotNull(responseGet);
    Assert.assertTrue(responseGet.getApplicationList().isEmpty());
  }

  /**
   * This test validates the correctness of GetApplicationsResponse in case applications
   * with given YarnApplicationState does not exist.
   */
  @Test
  public void testGetApplicationsApplicationStateNotExists() throws Exception {
    LOG.info("Test FederationClientInterceptor: Application with state does not exist.");

    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);

    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(
        YarnApplicationState.class);
    applicationStates.add(YarnApplicationState.KILLED);

    GetApplicationsRequest requestGet =
        GetApplicationsRequest.newInstance(applicationStates);

    GetApplicationsResponse responseGet = interceptor.getApplications(requestGet);

    Assert.assertNotNull(responseGet);
    Assert.assertTrue(responseGet.getApplicationList().isEmpty());
  }

  @Test
  public void testGetClusterNodesRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Cluster Nodes request.");
    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getClusterNodes request.",
        () -> interceptor.getClusterNodes(null));
    // normal request.
    GetClusterNodesResponse response =
        interceptor.getClusterNodes(GetClusterNodesRequest.newInstance());
    Assert.assertEquals(subClusters.size(), response.getNodeReports().size());
  }

  @Test
  public void testGetNodeToLabelsRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor :  Get Node To Labels request.");
    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getNodesToLabels request.",
        () -> interceptor.getNodeToLabels(null));
    // normal request.
    GetNodesToLabelsResponse response =
        interceptor.getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Assert.assertEquals(0, response.getNodeToLabels().size());
  }

  @Test
  public void testGetLabelsToNodesRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor :  Get Labels To Node request.");
    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getLabelsToNodes request.",
        () -> interceptor.getLabelsToNodes(null));
    // normal request.
    GetLabelsToNodesResponse response =
        interceptor.getLabelsToNodes(GetLabelsToNodesRequest.newInstance());
    Assert.assertEquals(0, response.getLabelsToNodes().size());
  }

  @Test
  public void testClusterNodeLabelsRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor :  Get Cluster NodeLabels request.");
    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getClusterNodeLabels request.",
        () -> interceptor.getClusterNodeLabels(null));
    // normal request.
    GetClusterNodeLabelsResponse response =
        interceptor.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertEquals(0, response.getNodeLabelList().size());
  }

  @Test
  public void testGetQueueUserAcls() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get QueueUserAcls request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getQueueUserAcls request.",
        () -> interceptor.getQueueUserAcls(null));

    // normal request
    GetQueueUserAclsInfoResponse response = interceptor.getQueueUserAcls(
        GetQueueUserAclsInfoRequest.newInstance());

    Assert.assertNotNull(response);

    List<QueueACL> submitAndAdministerAcl = new ArrayList<>();
    submitAndAdministerAcl.add(QueueACL.SUBMIT_APPLICATIONS);
    submitAndAdministerAcl.add(QueueACL.ADMINISTER_QUEUE);

    QueueUserACLInfo exceptRootQueueACLInfo = QueueUserACLInfo.newInstance("root",
        submitAndAdministerAcl);

    QueueUserACLInfo queueRootQueueACLInfo = response.getUserAclsInfoList().stream().
        filter(acl->acl.getQueueName().equals("root")).
        collect(Collectors.toList()).get(0);

    Assert.assertEquals(exceptRootQueueACLInfo, queueRootQueueACLInfo);
  }

  @Test
  public void testListReservations() throws Exception {
    LOG.info("Test FederationClientInterceptor :  Get ListReservations request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing listReservations request.",
        () -> interceptor.listReservations(null));

    // normal request
    ReservationId reservationId = ReservationId.newInstance(1653487680L, 1L);
    ReservationListResponse response = interceptor.listReservations(
        ReservationListRequest.newInstance("root.decided", reservationId.toString()));
    Assert.assertNotNull(response);
    Assert.assertEquals(0, response.getReservationAllocationState().size());
  }

  @Test
  public void testGetContainersRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Containers request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getContainers request " +
        "or ApplicationAttemptId.", () -> interceptor.getContainers(null));

    // normal request
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
        GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
        interceptor.getApplicationAttempts(attemptsRequest);

    // Wait for app to start
    while(attemptsResponse.getApplicationAttemptList().size() == 0) {
      attemptsResponse =
          interceptor.getApplicationAttempts(attemptsRequest);
    }

    Assert.assertNotNull(attemptsResponse);

    // Call GetContainers
    GetContainersRequest containersRequest =
        GetContainersRequest.newInstance(
        attemptsResponse.getApplicationAttemptList().get(0).getApplicationAttemptId());
    GetContainersResponse containersResponse =
        interceptor.getContainers(containersRequest);

    Assert.assertNotNull(containersResponse);
  }

  @Test
  public void testGetContainerReportRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Container Report request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getContainerReport request " +
        "or containerId", () -> interceptor.getContainerReport(null));

    // normal request
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
         GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
         interceptor.getApplicationAttempts(attemptsRequest);

    // Wait for app to start
    while(attemptsResponse.getApplicationAttemptList().size() == 0) {
      attemptsResponse =
          interceptor.getApplicationAttempts(attemptsRequest);
    }
    Assert.assertNotNull(attemptsResponse);

    ApplicationAttemptId attemptId = attemptsResponse.getApplicationAttemptList().
        get(0).getApplicationAttemptId();
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);

    // Call ContainerReport, RM does not allocate Container, Here is null
    GetContainerReportRequest containerReportRequest =
         GetContainerReportRequest.newInstance(containerId);
    GetContainerReportResponse containerReportResponse =
         interceptor.getContainerReport(containerReportRequest);

    Assert.assertEquals(containerReportResponse, null);
  }

  @Test
  public void getApplicationAttempts() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Application Attempts request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getApplicationAttempts " +
            "request or application id.", () -> interceptor.getApplicationAttempts(null));

    // normal request
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
         GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
         interceptor.getApplicationAttempts(attemptsRequest);

    Assert.assertNotNull(attemptsResponse);
  }

  @Test
  public void testGetResourceTypeInfoRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor :  Get Resource TypeInfo request.");
    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getResourceTypeInfo request.",
        () -> interceptor.getResourceTypeInfo(null));
    // normal request.
    GetAllResourceTypeInfoResponse response =
        interceptor.getResourceTypeInfo(GetAllResourceTypeInfoRequest.newInstance());
    Assert.assertEquals(2, response.getResourceTypeInfo().size());
  }

  @Test
  public void testFailApplicationAttempt() throws Exception {
    LOG.info("Test FederationClientInterceptor : Fail Application Attempt request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing failApplicationAttempt request " +
        "or applicationId or applicationAttemptId information.",
        () -> interceptor.failApplicationAttempt(null));

    // normal request
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    SubClusterId subClusterId = interceptor.getApplicationHomeSubCluster(appId);
    Assert.assertNotNull(subClusterId);

    MockRM mockRM = interceptor.getMockRMs().get(subClusterId);
    mockRM.waitForState(appId, RMAppState.ACCEPTED);
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);
    mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.SCHEDULED);

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
        GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
        interceptor.getApplicationAttempts(attemptsRequest);
    Assert.assertNotNull(attemptsResponse);

    ApplicationAttemptId attemptId = attemptsResponse.getApplicationAttemptList().
        get(0).getApplicationAttemptId();

    FailApplicationAttemptRequest requestFailAppAttempt =
        FailApplicationAttemptRequest.newInstance(attemptId);
    FailApplicationAttemptResponse responseFailAppAttempt =
        interceptor.failApplicationAttempt(requestFailAppAttempt);

    Assert.assertNotNull(responseFailAppAttempt);
  }

  @Test
  public void testUpdateApplicationPriority() throws Exception {
    LOG.info("Test FederationClientInterceptor : Update Application Priority request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing updateApplicationPriority request " +
        "or applicationId or applicationPriority information.",
        () -> interceptor.updateApplicationPriority(null));

    // normal request
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    SubClusterId subClusterId = interceptor.getApplicationHomeSubCluster(appId);
    Assert.assertNotNull(subClusterId);

    MockRM mockRM = interceptor.getMockRMs().get(subClusterId);
    mockRM.waitForState(appId, RMAppState.ACCEPTED);
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);
    mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.SCHEDULED);

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
        GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
        interceptor.getApplicationAttempts(attemptsRequest);
    Assert.assertNotNull(attemptsResponse);

    Priority priority = Priority.newInstance(20);
    UpdateApplicationPriorityRequest requestUpdateAppPriority =
        UpdateApplicationPriorityRequest.newInstance(appId, priority);
    UpdateApplicationPriorityResponse responseAppPriority =
        interceptor.updateApplicationPriority(requestUpdateAppPriority);

    Assert.assertNotNull(responseAppPriority);
    Assert.assertEquals(20,
        responseAppPriority.getApplicationPriority().getPriority());
  }

  @Test
  public void testUpdateApplicationTimeouts() throws Exception {
    LOG.info("Test FederationClientInterceptor : Update Application Timeouts request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing updateApplicationTimeouts request " +
        "or applicationId or applicationTimeouts information.",
        () -> interceptor.updateApplicationTimeouts(null));

    // normal request
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    SubClusterId subClusterId = interceptor.getApplicationHomeSubCluster(appId);
    Assert.assertNotNull(subClusterId);

    MockRM mockRM = interceptor.getMockRMs().get(subClusterId);
    mockRM.waitForState(appId, RMAppState.ACCEPTED);
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);
    mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.SCHEDULED);

    // Call GetApplicationAttempts
    GetApplicationAttemptsRequest attemptsRequest =
        GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse attemptsResponse =
        interceptor.getApplicationAttempts(attemptsRequest);
    Assert.assertNotNull(attemptsResponse);

    String appTimeout =
        Times.formatISO8601(System.currentTimeMillis() + 5 * 1000);
    Map<ApplicationTimeoutType, String> applicationTimeouts = new HashMap<>();
    applicationTimeouts.put(ApplicationTimeoutType.LIFETIME, appTimeout);

    UpdateApplicationTimeoutsRequest timeoutsRequest =
        UpdateApplicationTimeoutsRequest.newInstance(appId, applicationTimeouts);
    UpdateApplicationTimeoutsResponse timeoutsResponse =
        interceptor.updateApplicationTimeouts(timeoutsRequest);

    String responseTimeOut =
        timeoutsResponse.getApplicationTimeouts().get(ApplicationTimeoutType.LIFETIME);
    Assert.assertNotNull(timeoutsResponse);
    Assert.assertEquals(appTimeout, responseTimeOut);
  }

  @Test
  public void testSignalContainer() throws Exception {
    LOG.info("Test FederationClientInterceptor : Signal Container request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing signalToContainer request " +
        "or containerId or command information.", () -> interceptor.signalToContainer(null));

    // normal request
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);
    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    SubClusterId subClusterId = interceptor.getApplicationHomeSubCluster(appId);
    Assert.assertNotNull(subClusterId);

    MockRM mockRM = interceptor.getMockRMs().get(subClusterId);
    mockRM.waitForState(appId, RMAppState.ACCEPTED);
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);
    mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
        RMAppAttemptState.SCHEDULED);
    MockNM nm = interceptor.getMockNMs().get(subClusterId);
    nm.nodeHeartbeat(true);
    MockRM.waitForState(rmApp.getCurrentAppAttempt(), RMAppAttemptState.ALLOCATED);
    mockRM.sendAMLaunched(rmApp.getCurrentAppAttempt().getAppAttemptId());

    ContainerId containerId = rmApp.getCurrentAppAttempt().getMasterContainer().getId();

    SignalContainerRequest signalContainerRequest =
        SignalContainerRequest.newInstance(containerId, SignalContainerCommand.GRACEFUL_SHUTDOWN);
    SignalContainerResponse signalContainerResponse =
        interceptor.signalToContainer(signalContainerRequest);

    Assert.assertNotNull(signalContainerResponse);
  }

  @Test
  public void testMoveApplicationAcrossQueues() throws Exception {
    LOG.info("Test FederationClientInterceptor : MoveApplication AcrossQueues request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing moveApplicationAcrossQueues request " +
        "or applicationId or target queue.", () -> interceptor.moveApplicationAcrossQueues(null));

    // normal request
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    SubmitApplicationRequest request = mockSubmitApplicationRequest(appId);

    // Submit the application
    SubmitApplicationResponse response = interceptor.submitApplication(request);

    Assert.assertNotNull(response);
    Assert.assertNotNull(stateStoreUtil.queryApplicationHomeSC(appId));

    SubClusterId subClusterId = interceptor.getApplicationHomeSubCluster(appId);
    Assert.assertNotNull(subClusterId);

    MockRM mockRM = interceptor.getMockRMs().get(subClusterId);
    mockRM.waitForState(appId, RMAppState.ACCEPTED);
    RMApp rmApp = mockRM.getRMContext().getRMApps().get(appId);
    mockRM.waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
          RMAppAttemptState.SCHEDULED);
    MockNM nm = interceptor.getMockNMs().get(subClusterId);
    nm.nodeHeartbeat(true);
    MockRM.waitForState(rmApp.getCurrentAppAttempt(), RMAppAttemptState.ALLOCATED);
    mockRM.sendAMLaunched(rmApp.getCurrentAppAttempt().getAppAttemptId());

    MoveApplicationAcrossQueuesRequest acrossQueuesRequest =
        MoveApplicationAcrossQueuesRequest.newInstance(appId, "root.target");
    MoveApplicationAcrossQueuesResponse acrossQueuesResponse =
        interceptor.moveApplicationAcrossQueues(acrossQueuesRequest);

    Assert.assertNotNull(acrossQueuesResponse);
  }


  @Test
  public void testGetQueueInfo() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Queue Info request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getQueueInfo request or queueName.",
        () -> interceptor.getQueueInfo(null));

    // normal request
    GetQueueInfoResponse response = interceptor.getQueueInfo(
        GetQueueInfoRequest.newInstance("root", true, true, true));

    Assert.assertNotNull(response);

    QueueInfo queueInfo = response.getQueueInfo();
    Assert.assertNotNull(queueInfo);
    Assert.assertEquals(queueInfo.getQueueName(),  "root");
    Assert.assertEquals(queueInfo.getCapacity(), 4.0, 0);
    Assert.assertEquals(queueInfo.getCurrentCapacity(), 0.0, 0);
    Assert.assertEquals(queueInfo.getChildQueues().size(), 12, 0);
    Assert.assertEquals(queueInfo.getAccessibleNodeLabels().size(), 1);
  }

  @Test
  public void testGetResourceProfiles() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Resource Profiles request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getResourceProfiles request.",
        () -> interceptor.getResourceProfiles(null));

    // normal request
    GetAllResourceProfilesRequest request = GetAllResourceProfilesRequest.newInstance();
    GetAllResourceProfilesResponse response = interceptor.getResourceProfiles(request);

    Assert.assertNotNull(response);
    Map<String, Resource> resProfiles = response.getResourceProfiles();

    Resource maxResProfiles = resProfiles.get("maximum");
    Assert.assertEquals(32768, maxResProfiles.getMemorySize());
    Assert.assertEquals(16, maxResProfiles.getVirtualCores());

    Resource defaultResProfiles = resProfiles.get("default");
    Assert.assertEquals(8192, defaultResProfiles.getMemorySize());
    Assert.assertEquals(8, defaultResProfiles.getVirtualCores());

    Resource minimumResProfiles = resProfiles.get("minimum");
    Assert.assertEquals(4096, minimumResProfiles.getMemorySize());
    Assert.assertEquals(4, minimumResProfiles.getVirtualCores());
  }

  @Test
  public void testGetResourceProfile() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get Resource Profile request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getResourceProfile request or profileName.",
        () -> interceptor.getResourceProfile(null));

    // normal request
    GetResourceProfileRequest request = GetResourceProfileRequest.newInstance("maximum");
    GetResourceProfileResponse response = interceptor.getResourceProfile(request);

    Assert.assertNotNull(response);
    Assert.assertEquals(32768, response.getResource().getMemorySize());
    Assert.assertEquals(16, response.getResource().getVirtualCores());

    GetResourceProfileRequest request2 = GetResourceProfileRequest.newInstance("default");
    GetResourceProfileResponse response2 = interceptor.getResourceProfile(request2);

    Assert.assertNotNull(response2);
    Assert.assertEquals(8192, response2.getResource().getMemorySize());
    Assert.assertEquals(8, response2.getResource().getVirtualCores());

    GetResourceProfileRequest request3 = GetResourceProfileRequest.newInstance("minimum");
    GetResourceProfileResponse response3 = interceptor.getResourceProfile(request3);

    Assert.assertNotNull(response3);
    Assert.assertEquals(4096, response3.getResource().getMemorySize());
    Assert.assertEquals(4, response3.getResource().getVirtualCores());
  }

  @Test
  public void testGetAttributesToNodes() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get AttributesToNodes request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getAttributesToNodes request " +
        "or nodeAttributes.", () -> interceptor.getAttributesToNodes(null));

    // normal request
    GetAttributesToNodesResponse response =
        interceptor.getAttributesToNodes(GetAttributesToNodesRequest.newInstance());

    Assert.assertNotNull(response);
    Map<NodeAttributeKey, List<NodeToAttributeValue>> attrs = response.getAttributesToNodes();
    Assert.assertNotNull(attrs);
    Assert.assertEquals(4, attrs.size());

    NodeAttribute gpu = NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
        NodeAttributeType.STRING, "nvidia");
    NodeToAttributeValue attributeValue1 =
        NodeToAttributeValue.newInstance("0-host1", gpu.getAttributeValue());
    NodeAttributeKey gpuKey = gpu.getAttributeKey();
    Assert.assertTrue(attrs.get(gpuKey).contains(attributeValue1));
  }

  @Test
  public void testClusterNodeAttributes() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get ClusterNodeAttributes request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class, "Missing getClusterNodeAttributes request.",
        () -> interceptor.getClusterNodeAttributes(null));

    // normal request
    GetClusterNodeAttributesResponse response =
        interceptor.getClusterNodeAttributes(GetClusterNodeAttributesRequest.newInstance());

    Assert.assertNotNull(response);
    Set<NodeAttributeInfo> nodeAttributeInfos = response.getNodeAttributes();
    Assert.assertNotNull(nodeAttributeInfos);
    Assert.assertEquals(4, nodeAttributeInfos.size());

    NodeAttributeInfo nodeAttributeInfo1 =
        NodeAttributeInfo.newInstance(NodeAttributeKey.newInstance("GPU"),
        NodeAttributeType.STRING);
    Assert.assertTrue(nodeAttributeInfos.contains(nodeAttributeInfo1));

    NodeAttributeInfo nodeAttributeInfo2 =
        NodeAttributeInfo.newInstance(NodeAttributeKey.newInstance("OS"),
        NodeAttributeType.STRING);
    Assert.assertTrue(nodeAttributeInfos.contains(nodeAttributeInfo2));
  }

  @Test
  public void testNodesToAttributes() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get NodesToAttributes request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getNodesToAttributes request or hostNames.",
        () -> interceptor.getNodesToAttributes(null));

    // normal request
    Set<String> hostNames = Collections.singleton("0-host1");
    GetNodesToAttributesResponse response =
        interceptor.getNodesToAttributes(GetNodesToAttributesRequest.newInstance(hostNames));
    Assert.assertNotNull(response);

    Map<String, Set<NodeAttribute>> nodeAttributeMap = response.getNodeToAttributes();
    Assert.assertNotNull(nodeAttributeMap);
    Assert.assertEquals(1, nodeAttributeMap.size());

    NodeAttribute gpu = NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
        NodeAttributeType.STRING, "nvida");
    Assert.assertTrue(nodeAttributeMap.get("0-host1").contains(gpu));
  }

  @Test
  public void testGetNewReservation() throws Exception {
    LOG.info("Test FederationClientInterceptor : Get NewReservation request.");

    // null request
    LambdaTestUtils.intercept(YarnException.class,
        "Missing getNewReservation request.", () -> interceptor.getNewReservation(null));

    // normal request
    GetNewReservationRequest request = GetNewReservationRequest.newInstance();
    GetNewReservationResponse response = interceptor.getNewReservation(request);
    Assert.assertNotNull(response);

    ReservationId reservationId = response.getReservationId();
    Assert.assertNotNull(reservationId);
    Assert.assertTrue(reservationId.toString().contains("reservation"));
    Assert.assertEquals(reservationId.getClusterTimestamp(), ResourceManager.getClusterTimeStamp());
  }

  @Test
  public void testSubmitReservation() throws Exception {
    LOG.info("Test FederationClientInterceptor : SubmitReservation request.");

    // get new reservationId
    GetNewReservationRequest request = GetNewReservationRequest.newInstance();
    GetNewReservationResponse response = interceptor.getNewReservation(request);
    Assert.assertNotNull(response);

    // Submit Reservation
    ReservationId reservationId = response.getReservationId();
    ReservationDefinition rDefinition = createReservationDefinition(1024, 1);
    ReservationSubmissionRequest rSubmissionRequest = ReservationSubmissionRequest.newInstance(
        rDefinition, "decided", reservationId);

    ReservationSubmissionResponse submissionResponse =
        interceptor.submitReservation(rSubmissionRequest);
    Assert.assertNotNull(submissionResponse);

    SubClusterId subClusterId = stateStoreUtil.queryReservationHomeSC(reservationId);
    Assert.assertNotNull(subClusterId);
    Assert.assertTrue(subClusters.contains(subClusterId));
  }

  @Test
  public void testSubmitReservationEmptyRequest() throws Exception {
    LOG.info("Test FederationClientInterceptor : SubmitReservation request empty.");

    String errorMsg =
        "Missing submitReservation request or reservationId or reservation definition or queue.";

    // null request1
    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> interceptor.submitReservation(null));

    // null request2
    ReservationSubmissionRequest request2 =
        ReservationSubmissionRequest.newInstance(null, null, null);
    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> interceptor.submitReservation(request2));

    // null request3
    ReservationSubmissionRequest request3 =
        ReservationSubmissionRequest.newInstance(null, "q1", null);
    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> interceptor.submitReservation(request3));

    // null request4
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    ReservationSubmissionRequest request4 =
        ReservationSubmissionRequest.newInstance(null, null,  reservationId);
    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> interceptor.submitReservation(request4));

    // null request5
    long arrival = Time.now();
    long deadline = arrival + (int)(DEFAULT_DURATION * 1.1);

    ReservationRequest rRequest = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 1, 1, DEFAULT_DURATION);
    ReservationRequest[] rRequests = new ReservationRequest[] {rRequest};
    ReservationDefinition rDefinition = createReservationDefinition(arrival, deadline, rRequests,
        ReservationRequestInterpreter.R_ALL, "u1");
    ReservationSubmissionRequest request5 =
        ReservationSubmissionRequest.newInstance(rDefinition, null,  reservationId);
    LambdaTestUtils.intercept(YarnException.class, errorMsg,
        () -> interceptor.submitReservation(request5));
  }

  @Test
  public void testSubmitReservationMultipleSubmission() throws Exception {
    LOG.info("Test FederationClientInterceptor: Submit Reservation - Multiple");

    // get new reservationId
    GetNewReservationRequest request = GetNewReservationRequest.newInstance();
    GetNewReservationResponse response = interceptor.getNewReservation(request);
    Assert.assertNotNull(response);

    // First Submit Reservation
    ReservationId reservationId = response.getReservationId();
    ReservationDefinition rDefinition = createReservationDefinition(1024, 1);
    ReservationSubmissionRequest rSubmissionRequest = ReservationSubmissionRequest.newInstance(
        rDefinition, "decided", reservationId);
    ReservationSubmissionResponse submissionResponse =
        interceptor.submitReservation(rSubmissionRequest);
    Assert.assertNotNull(submissionResponse);

    SubClusterId subClusterId1 = stateStoreUtil.queryReservationHomeSC(reservationId);
    Assert.assertNotNull(subClusterId1);
    Assert.assertTrue(subClusters.contains(subClusterId1));

    // First Retry, repeat the submission
    ReservationSubmissionResponse submissionResponse1 =
        interceptor.submitReservation(rSubmissionRequest);
    Assert.assertNotNull(submissionResponse1);

    // Expect reserved clusters to be consistent
    SubClusterId subClusterId2 = stateStoreUtil.queryReservationHomeSC(reservationId);
    Assert.assertNotNull(subClusterId2);
    Assert.assertEquals(subClusterId1, subClusterId2);
  }

  @Test
  public void testUpdateReservation() throws Exception {
    LOG.info("Test FederationClientInterceptor : UpdateReservation request.");

    // get new reservationId
    GetNewReservationRequest request = GetNewReservationRequest.newInstance();
    GetNewReservationResponse response = interceptor.getNewReservation(request);
    Assert.assertNotNull(response);

    // allow plan follower to synchronize, manually trigger an assignment
    Map<SubClusterId, MockRM> mockRMs = interceptor.getMockRMs();
    for (MockRM mockRM : mockRMs.values()) {
      ReservationSystem reservationSystem = mockRM.getReservationSystem();
      reservationSystem.synchronizePlan("root.decided", true);
    }

    // Submit Reservation
    ReservationId reservationId = response.getReservationId();
    ReservationDefinition rDefinition = createReservationDefinition(1024, 1);
    ReservationSubmissionRequest rSubmissionRequest = ReservationSubmissionRequest.newInstance(
        rDefinition, "decided", reservationId);

    ReservationSubmissionResponse submissionResponse =
        interceptor.submitReservation(rSubmissionRequest);
    Assert.assertNotNull(submissionResponse);

    // Update Reservation
    ReservationDefinition rDefinition2 = createReservationDefinition(2048, 1);
    ReservationUpdateRequest updateRequest =
        ReservationUpdateRequest.newInstance(rDefinition2, reservationId);
    ReservationUpdateResponse updateResponse =
        interceptor.updateReservation(updateRequest);
    Assert.assertNotNull(updateResponse);

    SubClusterId subClusterId = stateStoreUtil.queryReservationHomeSC(reservationId);
    Assert.assertNotNull(subClusterId);
  }

  @Test
  public void testDeleteReservation() throws Exception {
    LOG.info("Test FederationClientInterceptor : DeleteReservation request.");

    // get new reservationId
    GetNewReservationRequest request = GetNewReservationRequest.newInstance();
    GetNewReservationResponse response = interceptor.getNewReservation(request);
    Assert.assertNotNull(response);

    // allow plan follower to synchronize, manually trigger an assignment
    Map<SubClusterId, MockRM> mockRMs = interceptor.getMockRMs();
    for (MockRM mockRM : mockRMs.values()) {
      ReservationSystem reservationSystem = mockRM.getReservationSystem();
      reservationSystem.synchronizePlan("root.decided", true);
    }

    // Submit Reservation
    ReservationId reservationId = response.getReservationId();
    ReservationDefinition rDefinition = createReservationDefinition(1024, 1);
    ReservationSubmissionRequest rSubmissionRequest = ReservationSubmissionRequest.newInstance(
        rDefinition, "decided", reservationId);

    ReservationSubmissionResponse submissionResponse =
        interceptor.submitReservation(rSubmissionRequest);
    Assert.assertNotNull(submissionResponse);

    // Delete Reservation
    ReservationDeleteRequest deleteRequest = ReservationDeleteRequest.newInstance(reservationId);
    ReservationDeleteResponse deleteResponse = interceptor.deleteReservation(deleteRequest);
    Assert.assertNotNull(deleteResponse);

    LambdaTestUtils.intercept(YarnException.class,
        "Reservation " + reservationId + " does not exist",
        () -> stateStoreUtil.queryReservationHomeSC(reservationId));
  }


  private ReservationDefinition createReservationDefinition(int memory, int core) {
    // get reservationId
    long arrival = Time.now();
    long deadline = arrival + (int)(DEFAULT_DURATION * 1.1);

    ReservationRequest rRequest = ReservationRequest.newInstance(
        Resource.newInstance(memory, core), 1, 1, DEFAULT_DURATION);
    ReservationRequest[] rRequests = new ReservationRequest[] {rRequest};

    ReservationDefinition rDefinition = createReservationDefinition(arrival, deadline, rRequests,
        ReservationRequestInterpreter.R_ALL, "u1");

    return rDefinition;
  }

  /**
   * This method is used to create a ReservationDefinition.
   *
   * @param arrival Job arrival time
   * @param deadline Job deadline
   * @param reservationRequests reservationRequest Array
   * @param rType Enumeration of various types of
   *              dependencies among multiple ReservationRequest
   * @param username username
   * @return ReservationDefinition
   */
  private ReservationDefinition createReservationDefinition(long arrival,
      long deadline, ReservationRequest[] reservationRequests,
      ReservationRequestInterpreter rType, String username) {
    ReservationRequests requests = ReservationRequests
        .newInstance(Arrays.asList(reservationRequests), rType);
    return ReservationDefinition.newInstance(arrival, deadline,
        requests, username, "0", Priority.UNDEFINED);
  }

  @Test
  public void testGetNumMinThreads() {
    // If we don't configure YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE,
    // we expect to get 5 threads
    int minThreads = interceptor.getNumMinThreads(this.getConf());
    Assert.assertEquals(5, minThreads);

    // If we configure YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE,
    // we expect to get 3 threads
    this.getConf().unset(YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE);
    this.getConf().setInt(YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MINIMUM_POOL_SIZE, 3);
    int minThreads2 = interceptor.getNumMinThreads(this.getConf());
    Assert.assertEquals(3, minThreads2);
  }

  @Test
  public void testGetNumMaxThreads() {
    // If we don't configure YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE,
    // we expect to get 5 threads
    int minThreads = interceptor.getNumMaxThreads(this.getConf());
    Assert.assertEquals(5, minThreads);

    // If we configure YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE,
    // we expect to get 8 threads
    this.getConf().unset(YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE);
    this.getConf().setInt(YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_MAXIMUM_POOL_SIZE, 8);
    int minThreads2 = interceptor.getNumMaxThreads(this.getConf());
    Assert.assertEquals(8, minThreads2);
  }

  @Test
  public void testGetDelegationToken() throws IOException, YarnException {

    // We design such a unit test to check
    // that the execution of the GetDelegationToken method is as expected.
    //
    // 1. Apply for a DelegationToken for renewer1,
    // the Router returns the DelegationToken of the user, and the KIND of the token is
    // RM_DELEGATION_TOKEN
    //
    // 2. We maintain the compatibility with RMDelegationTokenIdentifier,
    // we can serialize the token into RMDelegationTokenIdentifier.
    //
    // 3. We can get the issueDate, and compare the data in the StateStore,
    // the data should be consistent.

    // Step1. We apply for DelegationToken for renewer1
    // Both response & delegationToken cannot be empty
    GetDelegationTokenRequest request = mock(GetDelegationTokenRequest.class);
    when(request.getRenewer()).thenReturn("renewer1");
    GetDelegationTokenResponse response = interceptor.getDelegationToken(request);
    Assert.assertNotNull(response);
    Token delegationToken = response.getRMDelegationToken();
    Assert.assertNotNull(delegationToken);
    Assert.assertEquals("RM_DELEGATION_TOKEN", delegationToken.getKind());

    // Step2. Serialize the returned Token as RMDelegationTokenIdentifier.
    org.apache.hadoop.security.token.Token<RMDelegationTokenIdentifier> token =
        ConverterUtils.convertFromYarn(delegationToken, (Text) null);
    RMDelegationTokenIdentifier rMDelegationTokenIdentifier = token.decodeIdentifier();
    Assert.assertNotNull(rMDelegationTokenIdentifier);

    // Step3. Verify the returned data of the token.
    String renewer = rMDelegationTokenIdentifier.getRenewer().toString();
    long issueDate = rMDelegationTokenIdentifier.getIssueDate();
    long maxDate = rMDelegationTokenIdentifier.getMaxDate();
    Assert.assertEquals("renewer1", renewer);

    long tokenMaxLifetime = this.getConf().getLong(
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    Assert.assertEquals(issueDate + tokenMaxLifetime, maxDate);

    RouterRMDTSecretManagerState managerState = stateStore.getRouterRMSecretManagerState();
    Assert.assertNotNull(managerState);

    Map<RMDelegationTokenIdentifier, RouterStoreToken> delegationTokenState =
        managerState.getTokenState();
    Assert.assertNotNull(delegationTokenState);
    Assert.assertTrue(delegationTokenState.containsKey(rMDelegationTokenIdentifier));

    long tokenRenewInterval = this.getConf().getLong(
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    RouterStoreToken resultRouterStoreToken = delegationTokenState.get(rMDelegationTokenIdentifier);
    Assert.assertNotNull(resultRouterStoreToken);
    long renewDate = resultRouterStoreToken.getRenewDate();
    Assert.assertEquals(issueDate + tokenRenewInterval, renewDate);
  }

  @Test
  public void testRenewDelegationToken() throws IOException, YarnException {

    // We design such a unit test to check
    // that the execution of the GetDelegationToken method is as expected
    // 1. Call GetDelegationToken to apply for delegationToken.
    // 2. Call renewDelegationToken to refresh delegationToken.
    // By looking at the code of AbstractDelegationTokenSecretManager#renewToken,
    // we know that renewTime is calculated as Math.min(id.getMaxDate(), now + tokenRenewInterval)
    // so renewTime will be less than or equal to maxDate.
    // 3. We will compare whether the expirationTime returned to the
    // client is consistent with the renewDate in the stateStore.

    // Step1. Call GetDelegationToken to apply for delegationToken.
    GetDelegationTokenRequest request = mock(GetDelegationTokenRequest.class);
    when(request.getRenewer()).thenReturn("renewer2");
    GetDelegationTokenResponse response = interceptor.getDelegationToken(request);
    Assert.assertNotNull(response);
    Token delegationToken = response.getRMDelegationToken();

    org.apache.hadoop.security.token.Token<RMDelegationTokenIdentifier> token =
        ConverterUtils.convertFromYarn(delegationToken, (Text) null);
    RMDelegationTokenIdentifier rMDelegationTokenIdentifier = token.decodeIdentifier();
    String renewer = rMDelegationTokenIdentifier.getRenewer().toString();
    long maxDate = rMDelegationTokenIdentifier.getMaxDate();
    Assert.assertEquals("renewer2", renewer);

    // Step2. Call renewDelegationToken to refresh delegationToken.
    RenewDelegationTokenRequest renewRequest = Records.newRecord(RenewDelegationTokenRequest.class);
    renewRequest.setDelegationToken(delegationToken);
    RenewDelegationTokenResponse renewResponse = interceptor.renewDelegationToken(renewRequest);
    Assert.assertNotNull(renewResponse);

    long expDate = renewResponse.getNextExpirationTime();
    Assert.assertTrue(expDate <= maxDate);

    // Step3. Compare whether the expirationTime returned to
    // the client is consistent with the renewDate in the stateStore
    RouterRMDTSecretManagerState managerState = stateStore.getRouterRMSecretManagerState();
    Map<RMDelegationTokenIdentifier, RouterStoreToken> delegationTokenState =
        managerState.getTokenState();
    Assert.assertNotNull(delegationTokenState);
    Assert.assertTrue(delegationTokenState.containsKey(rMDelegationTokenIdentifier));
    RouterStoreToken resultRouterStoreToken = delegationTokenState.get(rMDelegationTokenIdentifier);
    Assert.assertNotNull(resultRouterStoreToken);
    long renewDate = resultRouterStoreToken.getRenewDate();
    Assert.assertEquals(expDate, renewDate);
  }

  @Test
  public void testCancelDelegationToken() throws IOException, YarnException {

    // We design such a unit test to check
    // that the execution of the CancelDelegationToken method is as expected
    // 1. Call GetDelegationToken to apply for delegationToken.
    // 2. Call CancelDelegationToken to cancel delegationToken.
    // 3. Query the data in the StateStore and confirm that the Delegation has been deleted.

    // Step1. Call GetDelegationToken to apply for delegationToken.
    GetDelegationTokenRequest request = mock(GetDelegationTokenRequest.class);
    when(request.getRenewer()).thenReturn("renewer3");
    GetDelegationTokenResponse response = interceptor.getDelegationToken(request);
    Assert.assertNotNull(response);
    Token delegationToken = response.getRMDelegationToken();

    // Step2. Call CancelDelegationToken to cancel delegationToken.
    CancelDelegationTokenRequest cancelTokenRequest =
        CancelDelegationTokenRequest.newInstance(delegationToken);
    CancelDelegationTokenResponse cancelTokenResponse =
        interceptor.cancelDelegationToken(cancelTokenRequest);
    Assert.assertNotNull(cancelTokenResponse);

    // Step3. Query the data in the StateStore and confirm that the Delegation has been deleted.
    // At this point, the size of delegationTokenState should be 0.
    RouterRMDTSecretManagerState managerState = stateStore.getRouterRMSecretManagerState();
    Map<RMDelegationTokenIdentifier, RouterStoreToken> delegationTokenState =
        managerState.getTokenState();
    Assert.assertNotNull(delegationTokenState);
    Assert.assertEquals(0, delegationTokenState.size());
  }
}
