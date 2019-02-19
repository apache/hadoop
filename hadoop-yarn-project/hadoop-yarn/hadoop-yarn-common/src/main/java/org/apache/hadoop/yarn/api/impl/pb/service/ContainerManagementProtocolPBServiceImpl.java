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
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.CommitResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ContainerUpdateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ContainerUpdateResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLocalizationStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetLocalizationStatusesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesResponsePBImpl;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReInitializeContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReInitializeContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ResourceLocalizationRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ResourceLocalizationResponsePBImpl;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RestartContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RollbackResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersResponsePBImpl;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReInitializeContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReInitializeContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ResourceLocalizationRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ResourceLocalizationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RestartContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RollbackResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.CommitResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerUpdateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerUpdateResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Private
public class ContainerManagementProtocolPBServiceImpl implements ContainerManagementProtocolPB  {

  private ContainerManagementProtocol real;
  
  public ContainerManagementProtocolPBServiceImpl(ContainerManagementProtocol impl) {
    this.real = impl;
  }

  @Override
  public StartContainersResponseProto startContainers(RpcController arg0,
      StartContainersRequestProto proto) throws ServiceException {
    StartContainersRequestPBImpl request = new StartContainersRequestPBImpl(proto);
    try {
      StartContainersResponse response = real.startContainers(request);
      return ((StartContainersResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StopContainersResponseProto stopContainers(RpcController arg0,
      StopContainersRequestProto proto) throws ServiceException {
    StopContainersRequestPBImpl request = new StopContainersRequestPBImpl(proto);
    try {
      StopContainersResponse response = real.stopContainers(request);
      return ((StopContainersResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContainerStatusesResponseProto getContainerStatuses(
      RpcController arg0, GetContainerStatusesRequestProto proto)
      throws ServiceException {
    GetContainerStatusesRequestPBImpl request = new GetContainerStatusesRequestPBImpl(proto);
    try {
      GetContainerStatusesResponse response = real.getContainerStatuses(request);
      return ((GetContainerStatusesResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IncreaseContainersResourceResponseProto increaseContainersResource(
      RpcController controller, IncreaseContainersResourceRequestProto proto)
      throws ServiceException {
    IncreaseContainersResourceRequestPBImpl request =
        new IncreaseContainersResourceRequestPBImpl(proto);
    try {
      ContainerUpdateResponse resp = real.updateContainer(ContainerUpdateRequest
          .newInstance(request.getContainersToIncrease()));
      IncreaseContainersResourceResponse response =
          IncreaseContainersResourceResponse
              .newInstance(resp.getSuccessfullyUpdatedContainers(),
                  resp.getFailedRequests());
      return ((IncreaseContainersResourceResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ContainerUpdateResponseProto updateContainer(
      RpcController controller, ContainerUpdateRequestProto proto)
      throws ServiceException {
    ContainerUpdateRequestPBImpl request =
        new ContainerUpdateRequestPBImpl(proto);
    try {
      ContainerUpdateResponse response = real.updateContainer(request);
      return ((ContainerUpdateResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SignalContainerResponseProto signalToContainer(RpcController arg0,
      SignalContainerRequestProto proto) throws ServiceException {
    final SignalContainerRequestPBImpl request =
        new SignalContainerRequestPBImpl(proto);
    try {
      final SignalContainerResponse response = real.signalToContainer(request);
      return ((SignalContainerResponsePBImpl)response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ResourceLocalizationResponseProto localize(RpcController controller,
      ResourceLocalizationRequestProto proto) throws ServiceException {
    ResourceLocalizationRequestPBImpl request =
        new ResourceLocalizationRequestPBImpl(proto);
    try {
      ResourceLocalizationResponse response = real.localize(request);
      return ((ResourceLocalizationResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReInitializeContainerResponseProto reInitializeContainer(
      RpcController controller, ReInitializeContainerRequestProto proto)
      throws ServiceException {
    ReInitializeContainerRequestPBImpl request =
        new ReInitializeContainerRequestPBImpl(proto);
    try {
      ReInitializeContainerResponse response =
          real.reInitializeContainer(request);
      return ((ReInitializeContainerResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RestartContainerResponseProto restartContainer(
      RpcController controller, ContainerIdProto containerId)
      throws ServiceException {
    ContainerId request = ProtoUtils.convertFromProtoFormat(containerId);
    try {
      RestartContainerResponse response = real.restartContainer(request);
      return ((RestartContainerResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RollbackResponseProto rollbackLastReInitialization(
      RpcController controller, ContainerIdProto containerId) throws
      ServiceException {
    ContainerId request = ProtoUtils.convertFromProtoFormat(containerId);
    try {
      RollbackResponse response = real.rollbackLastReInitialization(request);
      return ((RollbackResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CommitResponseProto commitLastReInitialization(
      RpcController controller, ContainerIdProto containerId) throws
      ServiceException {
    ContainerId request = ProtoUtils.convertFromProtoFormat(containerId);
    try {
      CommitResponse response = real.commitLastReInitialization(request);
      return ((CommitResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLocalizationStatusesResponseProto getLocalizationStatuses(
      RpcController controller, GetLocalizationStatusesRequestProto request)
      throws ServiceException {
    GetLocalizationStatusesRequestPBImpl lclReq =
        new GetLocalizationStatusesRequestPBImpl(request);
    try {
      GetLocalizationStatusesResponse response = real.getLocalizationStatuses(
          lclReq);
      return ((GetLocalizationStatusesResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}
