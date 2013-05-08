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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.ContainerManagerPB;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainerResponsePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StopContainerRequestProto;

import com.google.protobuf.ServiceException;

public class ContainerManagerPBClientImpl implements ContainerManager,
    Closeable {

  // Not a documented config. Only used for tests
  static final String NM_COMMAND_TIMEOUT = YarnConfiguration.YARN_PREFIX
      + "rpc.nm-command-timeout";

  /**
   * Maximum of 1 minute timeout for a Node to react to the command
   */
  static final int DEFAULT_COMMAND_TIMEOUT = 60000;

  private ContainerManagerPB proxy;

  public ContainerManagerPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ContainerManagerPB.class,
      ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    int expireIntvl = conf.getInt(NM_COMMAND_TIMEOUT, DEFAULT_COMMAND_TIMEOUT);
    proxy =
        (ContainerManagerPB) RPC.getProxy(ContainerManagerPB.class,
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
  public GetContainerStatusResponse getContainerStatus(
      GetContainerStatusRequest request) throws YarnRemoteException,
      IOException {
    GetContainerStatusRequestProto requestProto =
        ((GetContainerStatusRequestPBImpl) request).getProto();
    try {
      return new GetContainerStatusResponsePBImpl(proxy.getContainerStatus(
        null, requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public StartContainerResponse startContainer(StartContainerRequest request)
      throws YarnRemoteException, IOException {
    StartContainerRequestProto requestProto =
        ((StartContainerRequestPBImpl) request).getProto();
    try {
      return new StartContainerResponsePBImpl(proxy.startContainer(null,
        requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }

  @Override
  public StopContainerResponse stopContainer(StopContainerRequest request)
      throws YarnRemoteException, IOException {
    StopContainerRequestProto requestProto =
        ((StopContainerRequestPBImpl) request).getProto();
    try {
      return new StopContainerResponsePBImpl(proxy.stopContainer(null,
        requestProto));
    } catch (ServiceException e) {
      throw RPCUtil.unwrapAndThrowException(e);
    }
  }
}
