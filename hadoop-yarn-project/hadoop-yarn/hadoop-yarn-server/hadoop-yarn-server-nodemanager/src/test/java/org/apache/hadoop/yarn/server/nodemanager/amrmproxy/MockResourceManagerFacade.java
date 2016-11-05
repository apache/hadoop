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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.base.Strings;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.eclipse.jetty.util.log.Log;

/**
 * Mock Resource Manager facade implementation that exposes all the methods
 * implemented by the YARN RM. The behavior and the values returned by this mock
 * implementation is expected by the unit test cases. So please change the
 * implementation with care.
 */
public class MockResourceManagerFacade implements
    ApplicationMasterProtocol, ApplicationClientProtocol {

  private HashMap<String, List<ContainerId>> applicationContainerIdMap =
      new HashMap<String, List<ContainerId>>();
  private HashMap<ContainerId, Container> allocatedContainerMap =
      new HashMap<ContainerId, Container>();
  private AtomicInteger containerIndex = new AtomicInteger(0);
  private Configuration conf;

  public MockResourceManagerFacade(Configuration conf,
      int startContainerIndex) {
    this.conf = conf;
    this.containerIndex.set(startContainerIndex);
  }

  private static String getAppIdentifier() throws IOException {
    AMRMTokenIdentifier result = null;
    UserGroupInformation remoteUgi = UserGroupInformation.getCurrentUser();
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }
    return result != null ? result.getApplicationAttemptId().toString()
        : "";
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    String amrmToken = getAppIdentifier();
    Log.getLog().info("Registering application attempt: " + amrmToken);

    synchronized (applicationContainerIdMap) {
      Assert.assertFalse("The application id is already registered: "
          + amrmToken, applicationContainerIdMap.containsKey(amrmToken));
      // Keep track of the containers that are returned to this application
      applicationContainerIdMap.put(amrmToken,
          new ArrayList<ContainerId>());
    }

    return RegisterApplicationMasterResponse.newInstance(null, null, null,
        null, null, request.getHost(), null);
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    String amrmToken = getAppIdentifier();
    Log.getLog().info("Finishing application attempt: " + amrmToken);

    synchronized (applicationContainerIdMap) {
      // Remove the containers that were being tracked for this application
      Assert.assertTrue("The application id is NOT registered: "
          + amrmToken, applicationContainerIdMap.containsKey(amrmToken));
      List<ContainerId> ids = applicationContainerIdMap.remove(amrmToken);
      for (ContainerId c : ids) {
        allocatedContainerMap.remove(c);
      }
    }

    return FinishApplicationMasterResponse
        .newInstance(request.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED ? true
            : false);
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
    if (request.getAskList() != null && request.getAskList().size() > 0
        && request.getReleaseList() != null
        && request.getReleaseList().size() > 0) {
      Assert.fail("The mock RM implementation does not support receiving "
          + "askList and releaseList in the same heartbeat");
    }

    String amrmToken = getAppIdentifier();

    ArrayList<Container> containerList = new ArrayList<Container>();
    if (request.getAskList() != null) {
      for (ResourceRequest rr : request.getAskList()) {
        for (int i = 0; i < rr.getNumContainers(); i++) {
          ContainerId containerId =
              ContainerId.newInstance(getApplicationAttemptId(1),
                  containerIndex.incrementAndGet());
          Container container = Records.newRecord(Container.class);
          container.setId(containerId);
          container.setPriority(rr.getPriority());

          // We don't use the node for running containers in the test cases. So
          // it is OK to hard code it to some dummy value
          NodeId nodeId =
              NodeId.newInstance(
                  !Strings.isNullOrEmpty(rr.getResourceName()) ? rr
                      .getResourceName() : "dummy", 1000);
          container.setNodeId(nodeId);
          container.setResource(rr.getCapability());
          containerList.add(container);

          synchronized (applicationContainerIdMap) {
            // Keep track of the containers returned to this application. We
            // will need it in future
            Assert.assertTrue(
                "The application id is Not registered before allocate(): "
                    + amrmToken,
                applicationContainerIdMap.containsKey(amrmToken));
            List<ContainerId> ids =
                applicationContainerIdMap.get(amrmToken);
            ids.add(containerId);
            this.allocatedContainerMap.put(containerId, container);
          }
        }
      }
    }

    if (request.getReleaseList() != null
        && request.getReleaseList().size() > 0) {
      Log.getLog().info("Releasing containers: "
          + request.getReleaseList().size());
      synchronized (applicationContainerIdMap) {
        Assert.assertTrue(
            "The application id is not registered before allocate(): "
                + amrmToken,
            applicationContainerIdMap.containsKey(amrmToken));
        List<ContainerId> ids = applicationContainerIdMap.get(amrmToken);

        for (ContainerId id : request.getReleaseList()) {
          boolean found = false;
          for (ContainerId c : ids) {
            if (c.equals(id)) {
              found = true;
              break;
            }
          }

          Assert.assertTrue(
              "ContainerId " + id
                  + " being released is not valid for application: "
                  + conf.get("AMRMTOKEN"), found);

          ids.remove(id);

          // Return the released container back to the AM with new fake Ids. The
          // test case does not care about the IDs. The IDs are faked because
          // otherwise the LRM will throw duplication identifier exception. This
          // returning of fake containers is ONLY done for testing purpose - for
          // the test code to get confirmation that the sub-cluster resource
          // managers received the release request
          ContainerId fakeContainerId =
              ContainerId.newInstance(getApplicationAttemptId(1),
                  containerIndex.incrementAndGet());
          Container fakeContainer = allocatedContainerMap.get(id);
          fakeContainer.setId(fakeContainerId);
          containerList.add(fakeContainer);
        }
      }
    }

    Log.getLog().info("Allocating containers: " + containerList.size()
        + " for application attempt: " + conf.get("AMRMTOKEN"));
    return AllocateResponse.newInstance(0,
        new ArrayList<ContainerStatus>(), containerList,
        new ArrayList<NodeReport>(), null, AMCommand.AM_RESYNC, 1, null,
        new ArrayList<NMToken>(),
        new ArrayList<UpdatedContainer>());
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException,
      IOException {

    GetApplicationReportResponse response =
        Records.newRecord(GetApplicationReportResponse.class);
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
    report.setApplicationId(request.getApplicationId());
    report.setCurrentApplicationAttemptId(ApplicationAttemptId
        .newInstance(request.getApplicationId(), 1));
    response.setApplicationReport(report);
    return response;
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    GetApplicationAttemptReportResponse response =
        Records.newRecord(GetApplicationAttemptReportResponse.class);
    ApplicationAttemptReport report =
        Records.newRecord(ApplicationAttemptReport.class);
    report.setApplicationAttemptId(request.getApplicationAttemptId());
    report
        .setYarnApplicationAttemptState(YarnApplicationAttemptState.LAUNCHED);
    response.setApplicationAttemptReport(report);
    return response;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(
      GetClusterNodesRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationListResponse listReservations(
          ReservationListRequest request) throws YarnException,
          IOException {
      throw new NotImplementedException();
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException,
      IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request) throws YarnException,
      IOException {
    return null;
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws IOException {
return null;
}

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }
}
