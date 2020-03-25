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

package org.apache.hadoop.yarn.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
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
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationPriorityRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationPriorityResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationTimeoutsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UpdateApplicationTimeoutsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SubmitApplicationResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceProfilesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceProfilesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetAllResourceTypeInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetResourceProfileResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FailApplicationAttemptRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FailApplicationAttemptResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationAttemptsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerReportResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLabelsToNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewReservationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewReservationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.KillApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.MoveApplicationAcrossQueuesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationDeleteResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationSubmissionResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationUpdateResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReservationListResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationTimeoutsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationTimeoutsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceProfilesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceTypeInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceTypeInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceProfilesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeAttributesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAttributesToNodesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToAttributesResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

@Private
public class ApplicationClientProtocolPBServiceImpl implements ApplicationClientProtocolPB {

  private ApplicationClientProtocol real;
  
  public ApplicationClientProtocolPBServiceImpl(ApplicationClientProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public FailApplicationAttemptResponseProto failApplicationAttempt(RpcController arg0,
          FailApplicationAttemptRequestProto proto) throws ServiceException {
    FailApplicationAttemptRequestPBImpl request = new FailApplicationAttemptRequestPBImpl(proto);
    try {
      FailApplicationAttemptResponse response = real.failApplicationAttempt(request);
      return ((FailApplicationAttemptResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public KillApplicationResponseProto forceKillApplication(RpcController arg0,
      KillApplicationRequestProto proto) throws ServiceException {
    KillApplicationRequestPBImpl request = new KillApplicationRequestPBImpl(proto);
    try {
      KillApplicationResponse response = real.forceKillApplication(request);
      return ((KillApplicationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetApplicationReportResponseProto getApplicationReport(
      RpcController arg0, GetApplicationReportRequestProto proto)
      throws ServiceException {
    GetApplicationReportRequestPBImpl request = new GetApplicationReportRequestPBImpl(proto);
    try {
      GetApplicationReportResponse response = real.getApplicationReport(request);
      return ((GetApplicationReportResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterMetricsResponseProto getClusterMetrics(RpcController arg0,
      GetClusterMetricsRequestProto proto) throws ServiceException {
    GetClusterMetricsRequestPBImpl request = new GetClusterMetricsRequestPBImpl(proto);
    try {
      GetClusterMetricsResponse response = real.getClusterMetrics(request);
      return ((GetClusterMetricsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNewApplicationResponseProto getNewApplication(
      RpcController arg0, GetNewApplicationRequestProto proto)
      throws ServiceException {
    GetNewApplicationRequestPBImpl request = new GetNewApplicationRequestPBImpl(proto);
    try {
      GetNewApplicationResponse response = real.getNewApplication(request);
      return ((GetNewApplicationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SubmitApplicationResponseProto submitApplication(RpcController arg0,
      SubmitApplicationRequestProto proto) throws ServiceException {
    SubmitApplicationRequestPBImpl request = new SubmitApplicationRequestPBImpl(proto);
    try {
      SubmitApplicationResponse response = real.submitApplication(request);
      return ((SubmitApplicationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetApplicationsResponseProto getApplications(
      RpcController controller, GetApplicationsRequestProto proto)
      throws ServiceException {
    GetApplicationsRequestPBImpl request =
      new GetApplicationsRequestPBImpl(proto);
    try {
      GetApplicationsResponse response = real.getApplications(request);
      return ((GetApplicationsResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterNodesResponseProto getClusterNodes(RpcController controller,
      GetClusterNodesRequestProto proto) throws ServiceException {
    GetClusterNodesRequestPBImpl request =
      new GetClusterNodesRequestPBImpl(proto);
    try {
      GetClusterNodesResponse response = real.getClusterNodes(request);
      return ((GetClusterNodesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetQueueInfoResponseProto getQueueInfo(RpcController controller,
      GetQueueInfoRequestProto proto) throws ServiceException {
    GetQueueInfoRequestPBImpl request =
      new GetQueueInfoRequestPBImpl(proto);
    try {
      GetQueueInfoResponse response = real.getQueueInfo(request);
      return ((GetQueueInfoResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetQueueUserAclsInfoResponseProto getQueueUserAcls(
      RpcController controller, GetQueueUserAclsInfoRequestProto proto)
      throws ServiceException {
    GetQueueUserAclsInfoRequestPBImpl request =
      new GetQueueUserAclsInfoRequestPBImpl(proto);
    try {
      GetQueueUserAclsInfoResponse response = real.getQueueUserAcls(request);
      return ((GetQueueUserAclsInfoResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, GetDelegationTokenRequestProto proto)
      throws ServiceException {
    GetDelegationTokenRequestPBImpl request =
        new GetDelegationTokenRequestPBImpl(proto);
      try {
        GetDelegationTokenResponse response = real.getDelegationToken(request);
        return ((GetDelegationTokenResponsePBImpl)response).getProto();
      } catch (YarnException e) {
        throw new ServiceException(e);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
  }

  @Override
  public RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, RenewDelegationTokenRequestProto proto)
      throws ServiceException {
    RenewDelegationTokenRequestPBImpl request =
        new RenewDelegationTokenRequestPBImpl(proto);
      try {
        RenewDelegationTokenResponse response = real.renewDelegationToken(request);
        return ((RenewDelegationTokenResponsePBImpl)response).getProto();
      } catch (YarnException e) {
        throw new ServiceException(e);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
  }

  @Override
  public CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, CancelDelegationTokenRequestProto proto)
      throws ServiceException {
    CancelDelegationTokenRequestPBImpl request =
        new CancelDelegationTokenRequestPBImpl(proto);
      try {
        CancelDelegationTokenResponse response = real.cancelDelegationToken(request);
        return ((CancelDelegationTokenResponsePBImpl)response).getProto();
      } catch (YarnException e) {
        throw new ServiceException(e);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
  }
  
  @Override
  public MoveApplicationAcrossQueuesResponseProto moveApplicationAcrossQueues(
      RpcController controller, MoveApplicationAcrossQueuesRequestProto proto)
      throws ServiceException {
    MoveApplicationAcrossQueuesRequestPBImpl request =
        new MoveApplicationAcrossQueuesRequestPBImpl(proto);
    try {
      MoveApplicationAcrossQueuesResponse response = real.moveApplicationAcrossQueues(request);
      return ((MoveApplicationAcrossQueuesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public GetApplicationAttemptReportResponseProto getApplicationAttemptReport(
      RpcController controller, GetApplicationAttemptReportRequestProto proto)
      throws ServiceException {
    GetApplicationAttemptReportRequestPBImpl request =
        new GetApplicationAttemptReportRequestPBImpl(proto);
    try {
      GetApplicationAttemptReportResponse response =
          real.getApplicationAttemptReport(request);
      return ((GetApplicationAttemptReportResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetApplicationAttemptsResponseProto getApplicationAttempts(
      RpcController controller, GetApplicationAttemptsRequestProto proto)
      throws ServiceException {
    GetApplicationAttemptsRequestPBImpl request =
        new GetApplicationAttemptsRequestPBImpl(proto);
    try {
      GetApplicationAttemptsResponse response =
          real.getApplicationAttempts(request);
      return ((GetApplicationAttemptsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainerReportResponseProto getContainerReport(
      RpcController controller, GetContainerReportRequestProto proto)
      throws ServiceException {
    GetContainerReportRequestPBImpl request =
        new GetContainerReportRequestPBImpl(proto);
    try {
      GetContainerReportResponse response = real.getContainerReport(request);
      return ((GetContainerReportResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainersResponseProto getContainers(RpcController controller,
      GetContainersRequestProto proto) throws ServiceException {
    GetContainersRequestPBImpl request = new GetContainersRequestPBImpl(proto);
    try {
      GetContainersResponse response = real.getContainers(request);
      return ((GetContainersResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNewReservationResponseProto getNewReservation(
      RpcController arg0, GetNewReservationRequestProto proto) throws
      ServiceException {
    GetNewReservationRequestPBImpl request =
        new GetNewReservationRequestPBImpl(proto);
    try {
      GetNewReservationResponse response = real.getNewReservation(request);
      return ((GetNewReservationResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReservationSubmissionResponseProto submitReservation(RpcController controller,
      ReservationSubmissionRequestProto requestProto) throws ServiceException {
    ReservationSubmissionRequestPBImpl request =
        new ReservationSubmissionRequestPBImpl(requestProto);
    try {
      ReservationSubmissionResponse response = real.submitReservation(request);
      return ((ReservationSubmissionResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReservationUpdateResponseProto updateReservation(RpcController controller,
      ReservationUpdateRequestProto requestProto) throws ServiceException {
    ReservationUpdateRequestPBImpl request =
        new ReservationUpdateRequestPBImpl(requestProto);
    try {
      ReservationUpdateResponse response = real.updateReservation(request);
      return ((ReservationUpdateResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReservationDeleteResponseProto deleteReservation(RpcController controller,
      ReservationDeleteRequestProto requestProto) throws ServiceException {
    ReservationDeleteRequestPBImpl request =
        new ReservationDeleteRequestPBImpl(requestProto);
    try {
      ReservationDeleteResponse response = real.deleteReservation(request);
      return ((ReservationDeleteResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReservationListResponseProto listReservations(RpcController controller,
            ReservationListRequestProto requestProto) throws ServiceException {
    ReservationListRequestPBImpl request =
            new ReservationListRequestPBImpl(requestProto);
    try {
      ReservationListResponse response = real.listReservations(request);
      return ((ReservationListResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNodesToLabelsResponseProto getNodeToLabels(
      RpcController controller, GetNodesToLabelsRequestProto proto)
      throws ServiceException {
    GetNodesToLabelsRequestPBImpl request =
        new GetNodesToLabelsRequestPBImpl(proto);
    try {
      GetNodesToLabelsResponse response = real.getNodeToLabels(request);
      return ((GetNodesToLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLabelsToNodesResponseProto getLabelsToNodes(
      RpcController controller, GetLabelsToNodesRequestProto proto)
      throws ServiceException {
    GetLabelsToNodesRequestPBImpl request =
        new GetLabelsToNodesRequestPBImpl(proto);
    try {
      GetLabelsToNodesResponse response = real.getLabelsToNodes(request);
      return ((GetLabelsToNodesResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetClusterNodeLabelsResponseProto getClusterNodeLabels(
      RpcController controller, GetClusterNodeLabelsRequestProto proto)
      throws ServiceException {
    GetClusterNodeLabelsRequestPBImpl request =
        new GetClusterNodeLabelsRequestPBImpl(proto);
    try {
      GetClusterNodeLabelsResponse response =
          real.getClusterNodeLabels(request);
      return ((GetClusterNodeLabelsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdateApplicationPriorityResponseProto updateApplicationPriority(
      RpcController controller, UpdateApplicationPriorityRequestProto proto)
      throws ServiceException {
    UpdateApplicationPriorityRequestPBImpl request =
        new UpdateApplicationPriorityRequestPBImpl(proto);
    try {
      UpdateApplicationPriorityResponse response =
          real.updateApplicationPriority(request);
      return ((UpdateApplicationPriorityResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SignalContainerResponseProto signalToContainer(
      RpcController controller,
      YarnServiceProtos.SignalContainerRequestProto proto) throws ServiceException {
    SignalContainerRequestPBImpl request = new SignalContainerRequestPBImpl(proto);
    try {
      SignalContainerResponse response = real.signalToContainer(request);
      return ((SignalContainerResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdateApplicationTimeoutsResponseProto updateApplicationTimeouts(
      RpcController controller, UpdateApplicationTimeoutsRequestProto proto)
      throws ServiceException {
    UpdateApplicationTimeoutsRequestPBImpl request =
        new UpdateApplicationTimeoutsRequestPBImpl(proto);
    try {
      UpdateApplicationTimeoutsResponse response =
          real.updateApplicationTimeouts(request);
      return ((UpdateApplicationTimeoutsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetAllResourceProfilesResponseProto getResourceProfiles(
      RpcController controller, GetAllResourceProfilesRequestProto proto)
      throws ServiceException {
    GetAllResourceProfilesRequestPBImpl req =
        new GetAllResourceProfilesRequestPBImpl(proto);
    try {
      GetAllResourceProfilesResponse resp = real.getResourceProfiles(req);
      return ((GetAllResourceProfilesResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetResourceProfileResponseProto getResourceProfile(
      RpcController controller, GetResourceProfileRequestProto proto)
      throws ServiceException {
    GetResourceProfileRequestPBImpl req =
        new GetResourceProfileRequestPBImpl(proto);
    try {
      GetResourceProfileResponse resp = real.getResourceProfile(req);
      return ((GetResourceProfileResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetAllResourceTypeInfoResponseProto getResourceTypeInfo(
      RpcController controller, GetAllResourceTypeInfoRequestProto proto)
      throws ServiceException {
    GetAllResourceTypeInfoRequestPBImpl req = new GetAllResourceTypeInfoRequestPBImpl(
        proto);
    try {
      GetAllResourceTypeInfoResponse resp = real.getResourceTypeInfo(req);
      return ((GetAllResourceTypeInfoResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetClusterNodeAttributesResponseProto getClusterNodeAttributes(
      RpcController controller,
      YarnServiceProtos.GetClusterNodeAttributesRequestProto proto)
      throws ServiceException {
    GetClusterNodeAttributesRequest req =
        new GetClusterNodeAttributesRequestPBImpl(proto);
    try {
      GetClusterNodeAttributesResponse resp =
          real.getClusterNodeAttributes(req);
      return ((GetClusterNodeAttributesResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetAttributesToNodesResponseProto getAttributesToNodes(
      RpcController controller,
      YarnServiceProtos.GetAttributesToNodesRequestProto proto)
      throws ServiceException {
    GetAttributesToNodesRequestPBImpl req =
        new GetAttributesToNodesRequestPBImpl(proto);
    try {
      GetAttributesToNodesResponse resp = real.getAttributesToNodes(req);
      return ((GetAttributesToNodesResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetNodesToAttributesResponseProto getNodesToAttributes(
      RpcController controller,
      YarnServiceProtos.GetNodesToAttributesRequestProto proto)
      throws ServiceException {
    GetNodesToAttributesRequestPBImpl req =
        new GetNodesToAttributesRequestPBImpl(proto);
    try {
      GetNodesToAttributesResponse resp = real.getNodesToAttributes(req);
      return ((GetNodesToAttributesResponsePBImpl) resp).getProto();
    } catch (YarnException ye) {
      throw new ServiceException(ye);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }
}
