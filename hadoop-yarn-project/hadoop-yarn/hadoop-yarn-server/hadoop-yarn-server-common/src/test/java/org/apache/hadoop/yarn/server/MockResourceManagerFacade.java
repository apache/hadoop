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

package org.apache.hadoop.yarn.server;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.utils.AMRMClientUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Mock Resource Manager facade implementation that exposes all the methods
 * implemented by the YARN RM. The behavior and the values returned by this mock
 * implementation is expected by the Router/AMRMProxy unit test cases. So please
 * change the implementation with care.
 */
public class MockResourceManagerFacade implements ApplicationClientProtocol,
    ApplicationMasterProtocol, ResourceManagerAdministrationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockResourceManagerFacade.class);

  private HashSet<ApplicationId> applicationMap = new HashSet<>();
  private HashSet<ApplicationId> keepContainerOnUams = new HashSet<>();
  private HashMap<ApplicationAttemptId, List<ContainerId>>
      applicationContainerIdMap = new HashMap<>();
  private AtomicInteger containerIndex = new AtomicInteger(0);
  private Configuration conf;
  private int subClusterId;
  final private AtomicInteger applicationCounter = new AtomicInteger(0);

  // True if the Mock RM is running, false otherwise.
  // This property allows us to write tests for specific scenario as YARN RM
  // down e.g. network issue, failover.
  private boolean isRunning;

  private boolean shouldReRegisterNext = false;

  // For unit test synchronization
  private static Object syncObj = new Object();

  public static Object getSyncObj() {
    return syncObj;
  }

  public MockResourceManagerFacade(Configuration conf,
      int startContainerIndex) {
    this(conf, startContainerIndex, 0, true);
  }

  public MockResourceManagerFacade(Configuration conf, int startContainerIndex,
      int subClusterId, boolean isRunning) {
    this.conf = conf;
    this.containerIndex.set(startContainerIndex);
    this.subClusterId = subClusterId;
    this.isRunning = isRunning;
  }

  public void setShouldReRegisterNext() {
    shouldReRegisterNext = true;
  }

  public void setRunningMode(boolean mode) {
    this.isRunning = mode;
  }

  private static ApplicationAttemptId getAppIdentifier() throws IOException {
    AMRMTokenIdentifier result = null;
    UserGroupInformation remoteUgi = UserGroupInformation.getCurrentUser();
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }
    return result != null ? result.getApplicationAttemptId()
        : ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0);
  }

  private void validateRunning() throws ConnectException {
    if (!isRunning) {
      throw new ConnectException("RM is stopped");
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request)
      throws YarnException, IOException {

    validateRunning();
    ApplicationAttemptId attemptId = getAppIdentifier();
    LOG.info("Registering application attempt: " + attemptId);

    shouldReRegisterNext = false;

    List<Container> containersFromPreviousAttempt = null;

    synchronized (applicationContainerIdMap) {
      if (applicationContainerIdMap.containsKey(attemptId)) {
        if (keepContainerOnUams.contains(attemptId.getApplicationId())) {
          // For UAM with the keepContainersFromPreviousAttempt flag, return all
          // running containers
          containersFromPreviousAttempt = new ArrayList<>();
          for (ContainerId containerId : applicationContainerIdMap
              .get(attemptId)) {
            containersFromPreviousAttempt.add(Container.newInstance(containerId,
                null, null, null, null, null));
          }
        } else {
          throw new InvalidApplicationMasterRequestException(
              AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE);
        }
      } else {
        // Keep track of the containers that are returned to this application
        applicationContainerIdMap.put(attemptId, new ArrayList<ContainerId>());
      }
    }

    // Make sure we wait for certain test cases last in the method
    synchronized (syncObj) {
      syncObj.notifyAll();
      // We reuse the port number to indicate whether the unit test want us to
      // wait here
      if (request.getRpcPort() > 1000) {
        LOG.info("Register call in RM start waiting");
        try {
          syncObj.wait();
          LOG.info("Register call in RM wait finished");
        } catch (InterruptedException e) {
          LOG.info("Register call in RM wait interrupted", e);
        }
      }
    }

    return RegisterApplicationMasterResponse.newInstance(null, null, null, null,
        containersFromPreviousAttempt, request.getHost(), null);
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request)
      throws YarnException, IOException {

    validateRunning();

    ApplicationAttemptId attemptId = getAppIdentifier();
    LOG.info("Finishing application attempt: " + attemptId);

    if (shouldReRegisterNext) {
      String message = "AM is not registered, should re-register.";
      LOG.warn(message);
      throw new ApplicationMasterNotRegisteredException(message);
    }

    synchronized (applicationContainerIdMap) {
      // Remove the containers that were being tracked for this application
      Assert.assertTrue("The application id is NOT registered: " + attemptId,
          applicationContainerIdMap.containsKey(attemptId));
      applicationContainerIdMap.remove(attemptId);
    }

    return FinishApplicationMasterResponse.newInstance(
        request.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED
            ? true : false);
  }

  protected ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(12345, id);
  }

  protected ApplicationAttemptId getApplicationAttemptId(int id) {
    return ApplicationAttemptId.newInstance(getApplicationId(id), 1);
  }

  @SuppressWarnings("deprecation")
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    validateRunning();

    if (request.getAskList() != null && request.getAskList().size() > 0
        && request.getReleaseList() != null
        && request.getReleaseList().size() > 0) {
      Assert.fail("The mock RM implementation does not support receiving "
          + "askList and releaseList in the same heartbeat");
    }

    ApplicationAttemptId attemptId = getAppIdentifier();
    LOG.info("Allocate from application attempt: " + attemptId);

    if (shouldReRegisterNext) {
      String message = "AM is not registered, should re-register.";
      LOG.warn(message);
      throw new ApplicationMasterNotRegisteredException(message);
    }

    ArrayList<Container> containerList = new ArrayList<Container>();
    if (request.getAskList() != null) {
      for (ResourceRequest rr : request.getAskList()) {
        for (int i = 0; i < rr.getNumContainers(); i++) {
          ContainerId containerId = ContainerId.newInstance(
              getApplicationAttemptId(1), containerIndex.incrementAndGet());
          Container container = Records.newRecord(Container.class);
          container.setId(containerId);
          container.setPriority(rr.getPriority());

          // We don't use the node for running containers in the test cases. So
          // it is OK to hard code it to some dummy value
          NodeId nodeId =
              NodeId.newInstance(!Strings.isNullOrEmpty(rr.getResourceName())
                  ? rr.getResourceName() : "dummy", 1000);
          container.setNodeId(nodeId);
          container.setResource(rr.getCapability());
          containerList.add(container);

          synchronized (applicationContainerIdMap) {
            // Keep track of the containers returned to this application. We
            // will need it in future
            Assert.assertTrue(
                "The application id is Not registered before allocate(): "
                    + attemptId,
                applicationContainerIdMap.containsKey(attemptId));
            List<ContainerId> ids = applicationContainerIdMap.get(attemptId);
            ids.add(containerId);
          }
        }
      }
    }

    List<ContainerStatus> completedList = new ArrayList<>();
    if (request.getReleaseList() != null
        && request.getReleaseList().size() > 0) {
      LOG.info("Releasing containers: " + request.getReleaseList().size());
      synchronized (applicationContainerIdMap) {
        Assert
            .assertTrue(
                "The application id is not registered before allocate(): "
                    + attemptId,
                applicationContainerIdMap.containsKey(attemptId));
        List<ContainerId> ids = applicationContainerIdMap.get(attemptId);

        for (ContainerId id : request.getReleaseList()) {
          boolean found = false;
          for (ContainerId c : ids) {
            if (c.equals(id)) {
              found = true;
              break;
            }
          }

          Assert.assertTrue("ContainerId " + id
              + " being released is not valid for application: "
              + conf.get("AMRMTOKEN"), found);

          ids.remove(id);
          completedList.add(
              ContainerStatus.newInstance(id, ContainerState.COMPLETE, "", 0));
        }
      }
    }

    LOG.info("Allocating containers: " + containerList.size()
        + " for application attempt: " + conf.get("AMRMTOKEN"));

    // Always issue a new AMRMToken as if RM rolled master key
    Token newAMRMToken = Token.newInstance(new byte[0], "", new byte[0], "");

    return AllocateResponse.newInstance(0, completedList, containerList,
        new ArrayList<NodeReport>(), null, AMCommand.AM_RESYNC, 1, null,
        new ArrayList<NMToken>(), newAMRMToken,
        new ArrayList<UpdatedContainer>());
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {

    validateRunning();

    GetApplicationReportResponse response =
        Records.newRecord(GetApplicationReportResponse.class);
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
    report.setApplicationId(request.getApplicationId());
    report.setCurrentApplicationAttemptId(
        ApplicationAttemptId.newInstance(request.getApplicationId(), 1));
    report.setAMRMToken(Token.newInstance(new byte[0], "", new byte[0], ""));
    response.setApplicationReport(report);
    return response;
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {

    validateRunning();

    GetApplicationAttemptReportResponse response =
        Records.newRecord(GetApplicationAttemptReportResponse.class);
    ApplicationAttemptReport report =
        Records.newRecord(ApplicationAttemptReport.class);
    report.setApplicationAttemptId(request.getApplicationAttemptId());
    report.setYarnApplicationAttemptState(YarnApplicationAttemptState.LAUNCHED);
    response.setApplicationAttemptReport(report);
    return response;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {

    validateRunning();

    return GetNewApplicationResponse.newInstance(ApplicationId.newInstance(
        subClusterId, applicationCounter.incrementAndGet()), null, null);
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {

    validateRunning();

    ApplicationId appId = null;
    if (request.getApplicationSubmissionContext() != null) {
      appId = request.getApplicationSubmissionContext().getApplicationId();
    }
    LOG.info("Application submitted: " + appId);
    applicationMap.add(appId);

    if (request.getApplicationSubmissionContext().getUnmanagedAM()
        || request.getApplicationSubmissionContext()
            .getKeepContainersAcrossApplicationAttempts()) {
      keepContainerOnUams.add(appId);
    }
    return SubmitApplicationResponse.newInstance();
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {

    validateRunning();

    ApplicationId appId = null;
    if (request.getApplicationId() != null) {
      appId = request.getApplicationId();
      if (!applicationMap.remove(appId)) {
        throw new ApplicationNotFoundException(
            "Trying to kill an absent application: " + appId);
      }
      keepContainerOnUams.remove(appId);
    }
    LOG.info("Force killing application: " + appId);
    return KillApplicationResponse.newInstance(true);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {

    validateRunning();

    return GetClusterMetricsResponse.newInstance(null);
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {

    validateRunning();

    return GetApplicationsResponse.newInstance(null);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {

    validateRunning();

    return GetClusterNodesResponse.newInstance(null);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {

    validateRunning();

    return GetQueueInfoResponse.newInstance(null);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {

    validateRunning();

    return GetQueueUserAclsInfoResponse.newInstance(null);
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {

    validateRunning();

    return GetDelegationTokenResponse.newInstance(null);
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {

    validateRunning();

    return RenewDelegationTokenResponse.newInstance(0);
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {

    validateRunning();

    return CancelDelegationTokenResponse.newInstance();
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {

    validateRunning();

    return MoveApplicationAcrossQueuesResponse.newInstance();
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {

    validateRunning();

    return GetApplicationAttemptsResponse.newInstance(null);
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {

    validateRunning();

    return GetContainerReportResponse.newInstance(null);
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {

    validateRunning();

    ApplicationAttemptId attemptId = request.getApplicationAttemptId();
    List<ContainerReport> containers = new ArrayList<>();
    synchronized (applicationContainerIdMap) {
      // Return the list of running containers that were being tracked for this
      // application
      Assert.assertTrue("The application id is NOT registered: " + attemptId,
          applicationContainerIdMap.containsKey(attemptId));
      List<ContainerId> ids = applicationContainerIdMap.get(attemptId);
      for (ContainerId c : ids) {
        containers.add(ContainerReport.newInstance(c, null, null, null, 0, 0,
            null, null, 0, null, null));
      }
    }
    return GetContainersResponse.newInstance(containers);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {

    validateRunning();

    return ReservationSubmissionResponse.newInstance();
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {

    validateRunning();

    return ReservationListResponse
        .newInstance(new ArrayList<ReservationAllocationState>());
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {

    validateRunning();

    return ReservationUpdateResponse.newInstance();
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {

    validateRunning();

    return ReservationDeleteResponse.newInstance();
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {

    validateRunning();

    return GetNodesToLabelsResponse
        .newInstance(new HashMap<NodeId, Set<String>>());
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {

    validateRunning();

    return GetClusterNodeLabelsResponse.newInstance(new ArrayList<NodeLabel>());
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {

    validateRunning();

    return GetLabelsToNodesResponse.newInstance(null);
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {

    validateRunning();

    return GetNewReservationResponse
        .newInstance(ReservationId.newInstance(0, 0));
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {

    validateRunning();

    return FailApplicationAttemptResponse.newInstance();
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {

    validateRunning();

    return UpdateApplicationPriorityResponse.newInstance(null);
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {

    validateRunning();

    return new SignalContainerResponsePBImpl();
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {

    validateRunning();

    return UpdateApplicationTimeoutsResponse.newInstance();
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {

    validateRunning();

    return RefreshQueuesResponse.newInstance();
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {

    validateRunning();

    return RefreshNodesResponse.newInstance();
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {

    validateRunning();

    return RefreshSuperUserGroupsConfigurationResponse.newInstance();
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {

    validateRunning();

    return RefreshUserToGroupsMappingsResponse.newInstance();
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {

    validateRunning();

    return RefreshAdminAclsResponse.newInstance();
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {

    validateRunning();

    return RefreshServiceAclsResponse.newInstance();
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {

    validateRunning();

    return UpdateNodeResourceResponse.newInstance();
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {

    validateRunning();

    return RefreshNodesResourcesResponse.newInstance();
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {

    validateRunning();

    return AddToClusterNodeLabelsResponse.newInstance();
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {

    validateRunning();

    return RemoveFromClusterNodeLabelsResponse.newInstance();
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {

    validateRunning();

    return ReplaceLabelsOnNodeResponse.newInstance();
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {

    validateRunning();

    return CheckForDecommissioningNodesResponse.newInstance(null);
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {

    validateRunning();

    return RefreshClusterMaxPriorityResponse.newInstance();
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {

    validateRunning();

    return new String[0];
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    return null;
  }
}
