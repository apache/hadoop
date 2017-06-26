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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class for all the RouterClientRMService test cases. It provides utility
 * methods that can be used by the concrete test case classes.
 *
 */
public abstract class BaseRouterClientRMTest {

  /**
   * The RouterClientRMService instance that will be used by all the test cases.
   */
  private MockRouterClientRMService clientrmService;
  /**
   * Thread pool used for asynchronous operations.
   */
  private static ExecutorService threadpool = Executors.newCachedThreadPool();
  private Configuration conf;
  private AsyncDispatcher dispatcher;

  public final static int TEST_MAX_CACHE_SIZE = 10;

  protected MockRouterClientRMService getRouterClientRMService() {
    Assert.assertNotNull(this.clientrmService);
    return this.clientrmService;
  }

  protected YarnConfiguration createConfiguration() {
    YarnConfiguration config = new YarnConfiguration();
    String mockPassThroughInterceptorClass =
        PassThroughClientRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain will call the mock resource manager. The others in the chain will
    // simply forward it to the next one in the chain
    config.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + mockPassThroughInterceptorClass + ","
            + MockClientRequestInterceptor.class.getName());

    config.setInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
        TEST_MAX_CACHE_SIZE);
    return config;
  }

  @Before
  public void setUp() {
    this.conf = createConfiguration();
    this.dispatcher = new AsyncDispatcher();
    this.dispatcher.init(conf);
    this.dispatcher.start();
    this.clientrmService = createAndStartRouterClientRMService();
  }

  public void setUpConfig() {
    this.conf = createConfiguration();
  }

  protected Configuration getConf() {
    return this.conf;
  }

  @After
  public void tearDown() {
    if (clientrmService != null) {
      clientrmService.stop();
      clientrmService = null;
    }
    if (this.dispatcher != null) {
      this.dispatcher.stop();
    }
  }

  protected ExecutorService getThreadPool() {
    return threadpool;
  }

  protected MockRouterClientRMService createAndStartRouterClientRMService() {
    MockRouterClientRMService svc = new MockRouterClientRMService();
    svc.init(conf);
    svc.start();
    return svc;
  }

  protected static class MockRouterClientRMService
      extends RouterClientRMService {
    public MockRouterClientRMService() {
      super();
    }
  }

  protected GetNewApplicationResponse getNewApplication(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetNewApplicationResponse>() {
          @Override
          public GetNewApplicationResponse run() throws Exception {
            GetNewApplicationRequest req =
                GetNewApplicationRequest.newInstance();
            GetNewApplicationResponse response =
                getRouterClientRMService().getNewApplication(req);
            return response;
          }
        });
  }

  protected SubmitApplicationResponse submitApplication(
      final ApplicationId appId, String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<SubmitApplicationResponse>() {
          @Override
          public SubmitApplicationResponse run() throws Exception {
            ApplicationSubmissionContext context =
                ApplicationSubmissionContext.newInstance(appId, "", "", null,
                    null, false, false, -1, null, null);
            SubmitApplicationRequest req =
                SubmitApplicationRequest.newInstance(context);
            SubmitApplicationResponse response =
                getRouterClientRMService().submitApplication(req);
            return response;
          }
        });
  }

  protected KillApplicationResponse forceKillApplication(
      final ApplicationId appId, String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<KillApplicationResponse>() {
          @Override
          public KillApplicationResponse run() throws Exception {
            KillApplicationRequest req =
                KillApplicationRequest.newInstance(appId);
            KillApplicationResponse response =
                getRouterClientRMService().forceKillApplication(req);
            return response;
          }
        });
  }

  protected GetClusterMetricsResponse getClusterMetrics(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetClusterMetricsResponse>() {
          @Override
          public GetClusterMetricsResponse run() throws Exception {
            GetClusterMetricsRequest req =
                GetClusterMetricsRequest.newInstance();
            GetClusterMetricsResponse response =
                getRouterClientRMService().getClusterMetrics(req);
            return response;
          }
        });
  }

  protected GetClusterNodesResponse getClusterNodes(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetClusterNodesResponse>() {
          @Override
          public GetClusterNodesResponse run() throws Exception {
            GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
            GetClusterNodesResponse response =
                getRouterClientRMService().getClusterNodes(req);
            return response;
          }
        });
  }

  protected GetQueueInfoResponse getQueueInfo(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetQueueInfoResponse>() {
          @Override
          public GetQueueInfoResponse run() throws Exception {
            GetQueueInfoRequest req =
                GetQueueInfoRequest.newInstance("default", false, false, false);
            GetQueueInfoResponse response =
                getRouterClientRMService().getQueueInfo(req);
            return response;
          }
        });
  }

  protected GetQueueUserAclsInfoResponse getQueueUserAcls(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetQueueUserAclsInfoResponse>() {
          @Override
          public GetQueueUserAclsInfoResponse run() throws Exception {
            GetQueueUserAclsInfoRequest req =
                GetQueueUserAclsInfoRequest.newInstance();
            GetQueueUserAclsInfoResponse response =
                getRouterClientRMService().getQueueUserAcls(req);
            return response;
          }
        });
  }

  protected MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      String user, final ApplicationId appId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<MoveApplicationAcrossQueuesResponse>() {
          @Override
          public MoveApplicationAcrossQueuesResponse run() throws Exception {

            MoveApplicationAcrossQueuesRequest req =
                MoveApplicationAcrossQueuesRequest.newInstance(appId,
                    "newQueue");
            MoveApplicationAcrossQueuesResponse response =
                getRouterClientRMService().moveApplicationAcrossQueues(req);
            return response;
          }
        });
  }

  public GetNewReservationResponse getNewReservation(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetNewReservationResponse>() {
          @Override
          public GetNewReservationResponse run() throws Exception {
            GetNewReservationResponse response = getRouterClientRMService()
                .getNewReservation(GetNewReservationRequest.newInstance());
            return response;
          }
        });
  }

  protected ReservationSubmissionResponse submitReservation(String user,
      final ReservationId reservationId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ReservationSubmissionResponse>() {
          @Override
          public ReservationSubmissionResponse run() throws Exception {
            Clock clock = new UTCClock();
            long arrival = clock.getTime();
            long duration = 60000;
            long deadline = (long) (arrival + 1.05 * duration);

            ReservationSubmissionRequest req = createSimpleReservationRequest(1,
                arrival, deadline, duration, reservationId);
            ReservationSubmissionResponse response =
                getRouterClientRMService().submitReservation(req);
            return response;
          }
        });
  }

  protected ReservationUpdateResponse updateReservation(String user,
      final ReservationId reservationId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ReservationUpdateResponse>() {
          @Override
          public ReservationUpdateResponse run() throws Exception {
            Clock clock = new UTCClock();
            long arrival = clock.getTime();
            long duration = 60000;
            long deadline = (long) (arrival + 1.05 * duration);
            ReservationDefinition rDef =
                createSimpleReservationRequest(1, arrival, deadline, duration,
                    reservationId).getReservationDefinition();

            ReservationUpdateRequest req =
                ReservationUpdateRequest.newInstance(rDef, reservationId);
            ReservationUpdateResponse response =
                getRouterClientRMService().updateReservation(req);
            return response;
          }
        });
  }

  protected ReservationDeleteResponse deleteReservation(String user,
      final ReservationId reservationId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ReservationDeleteResponse>() {
          @Override
          public ReservationDeleteResponse run() throws Exception {
            ReservationDeleteRequest req =
                ReservationDeleteRequest.newInstance(reservationId);
            ReservationDeleteResponse response =
                getRouterClientRMService().deleteReservation(req);
            return response;
          }
        });
  }

  protected GetNodesToLabelsResponse getNodeToLabels(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetNodesToLabelsResponse>() {
          @Override
          public GetNodesToLabelsResponse run() throws Exception {
            GetNodesToLabelsRequest req = GetNodesToLabelsRequest.newInstance();
            GetNodesToLabelsResponse response =
                getRouterClientRMService().getNodeToLabels(req);
            return response;
          }
        });
  }

  protected GetLabelsToNodesResponse getLabelsToNodes(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetLabelsToNodesResponse>() {
          @Override
          public GetLabelsToNodesResponse run() throws Exception {
            GetLabelsToNodesRequest req = GetLabelsToNodesRequest.newInstance();
            GetLabelsToNodesResponse response =
                getRouterClientRMService().getLabelsToNodes(req);
            return response;
          }
        });
  }

  protected GetClusterNodeLabelsResponse getClusterNodeLabels(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetClusterNodeLabelsResponse>() {
          @Override
          public GetClusterNodeLabelsResponse run() throws Exception {
            GetClusterNodeLabelsRequest req =
                GetClusterNodeLabelsRequest.newInstance();
            GetClusterNodeLabelsResponse response =
                getRouterClientRMService().getClusterNodeLabels(req);
            return response;
          }
        });
  }

  protected GetApplicationReportResponse getApplicationReport(String user,
      final ApplicationId appId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetApplicationReportResponse>() {
          @Override
          public GetApplicationReportResponse run() throws Exception {
            GetApplicationReportRequest req =
                GetApplicationReportRequest.newInstance(appId);
            GetApplicationReportResponse response =
                getRouterClientRMService().getApplicationReport(req);
            return response;
          }
        });
  }

  protected GetApplicationsResponse getApplications(String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetApplicationsResponse>() {
          @Override
          public GetApplicationsResponse run() throws Exception {
            GetApplicationsRequest req = GetApplicationsRequest.newInstance();
            GetApplicationsResponse response =
                getRouterClientRMService().getApplications(req);
            return response;
          }
        });
  }

  protected GetApplicationAttemptReportResponse getApplicationAttemptReport(
      String user, final ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<GetApplicationAttemptReportResponse>() {
          @Override
          public GetApplicationAttemptReportResponse run() throws Exception {
            GetApplicationAttemptReportRequest req =
                GetApplicationAttemptReportRequest.newInstance(appAttemptId);
            GetApplicationAttemptReportResponse response =
                getRouterClientRMService().getApplicationAttemptReport(req);
            return response;
          }
        });
  }

  protected GetApplicationAttemptsResponse getApplicationAttempts(String user,
      final ApplicationId applicationId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetApplicationAttemptsResponse>() {
          @Override
          public GetApplicationAttemptsResponse run() throws Exception {
            GetApplicationAttemptsRequest req =
                GetApplicationAttemptsRequest.newInstance(applicationId);
            GetApplicationAttemptsResponse response =
                getRouterClientRMService().getApplicationAttempts(req);
            return response;
          }
        });
  }

  protected GetContainerReportResponse getContainerReport(String user,
      final ContainerId containerId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetContainerReportResponse>() {
          @Override
          public GetContainerReportResponse run() throws Exception {
            GetContainerReportRequest req =
                GetContainerReportRequest.newInstance(containerId);
            GetContainerReportResponse response =
                getRouterClientRMService().getContainerReport(req);
            return response;
          }
        });
  }

  protected GetContainersResponse getContainers(String user,
      final ApplicationAttemptId appAttemptId)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetContainersResponse>() {
          @Override
          public GetContainersResponse run() throws Exception {
            GetContainersRequest req =
                GetContainersRequest.newInstance(appAttemptId);
            GetContainersResponse response =
                getRouterClientRMService().getContainers(req);
            return response;
          }
        });
  }

  protected GetDelegationTokenResponse getDelegationToken(final String user)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<GetDelegationTokenResponse>() {
          @Override
          public GetDelegationTokenResponse run() throws Exception {
            GetDelegationTokenRequest req =
                GetDelegationTokenRequest.newInstance(user);
            GetDelegationTokenResponse response =
                getRouterClientRMService().getDelegationToken(req);
            return response;
          }
        });
  }

  protected RenewDelegationTokenResponse renewDelegationToken(String user,
      final Token token)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RenewDelegationTokenResponse>() {
          @Override
          public RenewDelegationTokenResponse run() throws Exception {
            RenewDelegationTokenRequest req =
                RenewDelegationTokenRequest.newInstance(token);
            RenewDelegationTokenResponse response =
                getRouterClientRMService().renewDelegationToken(req);
            return response;
          }
        });
  }

  protected CancelDelegationTokenResponse cancelDelegationToken(String user,
      final Token token)
      throws YarnException, IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<CancelDelegationTokenResponse>() {
          @Override
          public CancelDelegationTokenResponse run() throws Exception {
            CancelDelegationTokenRequest req =
                CancelDelegationTokenRequest.newInstance(token);
            CancelDelegationTokenResponse response =
                getRouterClientRMService().cancelDelegationToken(req);
            return response;
          }
        });
  }

  private ReservationSubmissionRequest createSimpleReservationRequest(
      int numContainers, long arrival, long deadline, long duration,
      ReservationId reservationId) {
    // create a request with a single atomic ask
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), numContainers, 1, duration);
    ReservationRequests reqs = ReservationRequests.newInstance(
        Collections.singletonList(r), ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef = ReservationDefinition.newInstance(arrival,
        deadline, reqs, "testRouterClientRMService#reservation");
    ReservationSubmissionRequest request = ReservationSubmissionRequest
        .newInstance(rDef, "dedicated", reservationId);
    return request;
  }

}
