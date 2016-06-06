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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncreaseContainersResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersResponsePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncreaseContainersResourceRequestProto;

import com.google.protobuf.ServiceException;

@Private
public class ContainerManagementProtocolPBClientImpl implements ContainerManagementProtocol,
    Closeable {

  // Not a documented config. Only used for tests
  static final String NM_COMMAND_TIMEOUT = YarnConfiguration.YARN_PREFIX
      + "rpc.nm-command-timeout";

  /**
   * Maximum of 1 minute timeout for a Node to react to the command
   */
  static final int DEFAULT_COMMAND_TIMEOUT = 60000;

  private ContainerManagementProtocolPB proxy;

  public ContainerManagementProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ContainerManagementProtocolPB.class,
      ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    int expireIntvl = conf.getInt(NM_COMMAND_TIMEOUT, DEFAULT_COMMAND_TIMEOUT);
    proxy =
        (ContainerManagementProtocolPB) RPC.getProxy(ContainerManagementProtocolPB.class,
          clientVersion, addr, ugi, conf,
          NetUtils.getDefaultSocketFactory(conf), expireIntvl);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public StartContainersResponse
      startContainers(StartContainersRequest requests) throws YarnException,
          IOException {
    StartContainersRequestProto requestProto =
        ((StartContainersRequestPBImpl) requests).getProto();
    try {
      return new StartContainersResponsePBImpl(proxy.startContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public StopContainersResponse stopContainers(StopContainersRequest requests)
      throws YarnException, IOException {
    StopContainersRequestProto requestProto =
        ((StopContainersRequestPBImpl) requests).getProto();
    try {
      return new StopContainersResponsePBImpl(proxy.stopContainers(null,
        requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {
    GetContainerStatusesRequestProto requestProto =
        ((GetContainerStatusesRequestPBImpl) request).getProto();
    try {
      return new GetContainerStatusesResponsePBImpl(proxy.getContainerStatuses(
        null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest request) throws YarnException,
      IOException {
    IncreaseContainersResourceRequestProto requestProto =
        ((IncreaseContainersResourceRequestPBImpl)request).getProto();
    try {
      return new IncreaseContainersResourceResponsePBImpl(
          proxy.increaseContainersResource(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    SignalContainerRequestProto requestProto =
        ((SignalContainerRequestPBImpl) request).getProto();
    try {
      return new SignalContainerResponsePBImpl(
          proxy.signalToContainer(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
