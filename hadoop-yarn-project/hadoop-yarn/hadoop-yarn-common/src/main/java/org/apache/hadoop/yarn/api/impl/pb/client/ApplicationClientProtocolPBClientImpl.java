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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CancelDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FailApplicationAttemptRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.FailApplicationAttemptResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationAttemptsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAttributesToNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAttributesToNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterMetricsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeAttributesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeAttributesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetClusterNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerReportResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLabelsToNodesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLabelsToNodesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewReservationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNewReservationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToAttributesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToAttributesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetNodesToLabelsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetQueueUserAclsInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.KillApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.MoveApplicationAcrossQueuesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RenewDelegationTokenResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationDeleteResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationListRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationListResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationUpdateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationPriorityRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationPriorityResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationTimeoutsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationTimeoutsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceProfilesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceProfilesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FailApplicationAttemptRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationTimeoutsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

@Private
public class ApplicationClientProtocolPBClientImpl implements ApplicationClientProtocol,
    Closeable {

  private ApplicationClientProtocolPB proxy;

  public ApplicationClientProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ApplicationClientProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy = RPC.getProxy(ApplicationClientProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    FailApplicationAttemptRequestProto requestProto =
        ((FailApplicationAttemptRequestPBImpl) request).getProto();
    try {
      return new FailApplicationAttemptResponsePBImpl(proxy.failApplicationAttempt(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    KillApplicationRequestProto requestProto =
        ((KillApplicationRequestPBImpl) request).getProto();
    try {
      return new KillApplicationResponsePBImpl(proxy.forceKillApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException,
      IOException {
    GetApplicationReportRequestProto requestProto =
        ((GetApplicationReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationReportResponsePBImpl(proxy.getApplicationReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException,
      IOException {
    GetClusterMetricsRequestProto requestProto =
        ((GetClusterMetricsRequestPBImpl) request).getProto();
    try {
      return new GetClusterMetricsResponsePBImpl(proxy.getClusterMetrics(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException,
      IOException {
    GetNewApplicationRequestProto requestProto =
        ((GetNewApplicationRequestPBImpl) request).getProto();
    try {
      return new GetNewApplicationResponsePBImpl(proxy.getNewApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException,
      IOException {
    SubmitApplicationRequestProto requestProto =
        ((SubmitApplicationRequestPBImpl) request).getProto();
    try {
      return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException,
      IOException {
    GetApplicationsRequestProto requestProto =
        ((GetApplicationsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationsResponsePBImpl(proxy.getApplications(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodesResponse
      getClusterNodes(GetClusterNodesRequest request)
          throws YarnException, IOException {
    GetClusterNodesRequestProto requestProto =
        ((GetClusterNodesRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodesResponsePBImpl(proxy.getClusterNodes(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    GetQueueInfoRequestProto requestProto =
        ((GetQueueInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueInfoResponsePBImpl(proxy.getQueueInfo(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException,
      IOException {
    GetQueueUserAclsInfoRequestProto requestProto =
        ((GetQueueUserAclsInfoRequestPBImpl) request).getProto();
    try {
      return new GetQueueUserAclsInfoResponsePBImpl(proxy.getQueueUserAcls(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException,
      IOException {
    GetDelegationTokenRequestProto requestProto =
        ((GetDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException,
      IOException {
    RenewDelegationTokenRequestProto requestProto = 
        ((RenewDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException,
      IOException {
    CancelDelegationTokenRequestProto requestProto =
        ((CancelDelegationTokenRequestPBImpl) request).getProto();
    try {
      return new CancelDelegationTokenResponsePBImpl(
          proxy.cancelDelegationToken(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
  
  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException,
      IOException {
    MoveApplicationAcrossQueuesRequestProto requestProto =
        ((MoveApplicationAcrossQueuesRequestPBImpl) request).getProto();
    try {
      return new MoveApplicationAcrossQueuesResponsePBImpl(
          proxy.moveApplicationAcrossQueues(null, requestProto));

    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
  
  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    GetApplicationAttemptReportRequestProto requestProto =
        ((GetApplicationAttemptReportRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptReportResponsePBImpl(
        proxy.getApplicationAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    GetApplicationAttemptsRequestProto requestProto =
        ((GetApplicationAttemptsRequestPBImpl) request).getProto();
    try {
      return new GetApplicationAttemptsResponsePBImpl(
        proxy.getApplicationAttempts(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    GetContainerReportRequestProto requestProto =
        ((GetContainerReportRequestPBImpl) request).getProto();
    try {
      return new GetContainerReportResponsePBImpl(proxy.getContainerReport(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    GetContainersRequestProto requestProto =
        ((GetContainersRequestPBImpl) request).getProto();
    try {
      return new GetContainersResponsePBImpl(proxy.getContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request)
    throws YarnException, IOException {
    YarnServiceProtos.GetNewReservationRequestProto requestProto =
        ((GetNewReservationRequestPBImpl) request).getProto();
    try {
      return new GetNewReservationResponsePBImpl(proxy.getNewReservation(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest request)
      throws YarnException, IOException {
    ReservationSubmissionRequestProto requestProto =
        ((ReservationSubmissionRequestPBImpl) request).getProto();
    try {
      return new ReservationSubmissionResponsePBImpl(proxy.submitReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationUpdateResponse updateReservation(ReservationUpdateRequest request)
      throws YarnException, IOException {
    ReservationUpdateRequestProto requestProto =
        ((ReservationUpdateRequestPBImpl) request).getProto();
    try {
      return new ReservationUpdateResponsePBImpl(proxy.updateReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationDeleteResponse deleteReservation(ReservationDeleteRequest request)
      throws YarnException, IOException {
    ReservationDeleteRequestProto requestProto =
        ((ReservationDeleteRequestPBImpl) request).getProto();
    try {
      return new ReservationDeleteResponsePBImpl(proxy.deleteReservation(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReservationListResponse listReservations(ReservationListRequest
                     request) throws YarnException, IOException {
    YarnServiceProtos.ReservationListRequestProto requestProto =
        ((ReservationListRequestPBImpl) request).getProto();
    try {
      return new ReservationListResponsePBImpl(proxy.listReservations(null,
      requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.GetNodesToLabelsRequestProto
        requestProto =
        ((GetNodesToLabelsRequestPBImpl) request).getProto();
    try {
      return new GetNodesToLabelsResponsePBImpl(proxy.getNodeToLabels(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.GetLabelsToNodesRequestProto requestProto =
        ((GetLabelsToNodesRequestPBImpl) request).getProto();
    try {
      return new GetLabelsToNodesResponsePBImpl(proxy.getLabelsToNodes(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
      GetClusterNodeLabelsRequestProto
        requestProto =
        ((GetClusterNodeLabelsRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodeLabelsResponsePBImpl(proxy.getClusterNodeLabels(
          null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request) throws YarnException, IOException {
    UpdateApplicationPriorityRequestProto requestProto =
        ((UpdateApplicationPriorityRequestPBImpl) request).getProto();
    try {
      return new UpdateApplicationPriorityResponsePBImpl(
          proxy.updateApplicationPriority(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    YarnServiceProtos.SignalContainerRequestProto requestProto =
        ((SignalContainerRequestPBImpl) request).getProto();
    try {
      return new SignalContainerResponsePBImpl(
          proxy.signalToContainer(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    UpdateApplicationTimeoutsRequestProto requestProto =
        ((UpdateApplicationTimeoutsRequestPBImpl) request).getProto();
    try {
      return new UpdateApplicationTimeoutsResponsePBImpl(
          proxy.updateApplicationTimeouts(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    YarnServiceProtos.GetAllResourceProfilesRequestProto requestProto =
        ((GetAllResourceProfilesRequestPBImpl) request).getProto();
    try {
      return new GetAllResourceProfilesResponsePBImpl(
          proxy.getResourceProfiles(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    YarnServiceProtos.GetResourceProfileRequestProto requestProto =
        ((GetResourceProfileRequestPBImpl) request).getProto();
    try {
      return new GetResourceProfileResponsePBImpl(
          proxy.getResourceProfile(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    YarnServiceProtos.GetAllResourceTypeInfoRequestProto requestProto =
        ((GetAllResourceTypeInfoRequestPBImpl) request).getProto();
    try {
      return new GetAllResourceTypeInfoResponsePBImpl(
          proxy.getResourceTypeInfo(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetAttributesToNodesResponse getAttributesToNodes(
      GetAttributesToNodesRequest request) throws YarnException, IOException {
    YarnServiceProtos.GetAttributesToNodesRequestProto requestProto =
        ((GetAttributesToNodesRequestPBImpl) request).getProto();
    try {
      return new GetAttributesToNodesResponsePBImpl(
          proxy.getAttributesToNodes(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetClusterNodeAttributesResponse getClusterNodeAttributes(
      GetClusterNodeAttributesRequest request)
      throws YarnException, IOException {
    YarnServiceProtos.GetClusterNodeAttributesRequestProto requestProto =
        ((GetClusterNodeAttributesRequestPBImpl) request).getProto();
    try {
      return new GetClusterNodeAttributesResponsePBImpl(
          proxy.getClusterNodeAttributes(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetNodesToAttributesResponse getNodesToAttributes(
      GetNodesToAttributesRequest request) throws YarnException, IOException {
    YarnServiceProtos.GetNodesToAttributesRequestProto requestProto =
        ((GetNodesToAttributesRequestPBImpl) request).getProto();
    try {
      return new GetNodesToAttributesResponsePBImpl(
          proxy.getNodesToAttributes(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
