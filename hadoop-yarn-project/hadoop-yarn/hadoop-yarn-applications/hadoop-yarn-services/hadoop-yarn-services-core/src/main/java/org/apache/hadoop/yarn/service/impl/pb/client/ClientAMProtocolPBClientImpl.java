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

package org.apache.hadoop.yarn.service.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.service.ClientAMProtocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.service.impl.pb.service.ClientAMProtocolPB;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;

public class ClientAMProtocolPBClientImpl
    implements ClientAMProtocol, Closeable {

  private ClientAMProtocolPB proxy;

  public ClientAMProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ClientAMProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = RPC.getProxy(ClientAMProtocolPB.class, clientVersion, addr, conf);

  }

  @Override public FlexComponentsResponseProto flexComponents(
      FlexComponentsRequestProto request) throws IOException, YarnException {
    try {
      return proxy.flexComponents(null, request);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }
    return null;
  }

  @Override
  public GetStatusResponseProto getStatus(GetStatusRequestProto request)
      throws IOException, YarnException {
    try {
      return proxy.getStatus(null, request);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }
    return null;
  }

  @Override
  public StopResponseProto stop(StopRequestProto requestProto)
      throws IOException, YarnException {
    try {
      return proxy.stop(null, requestProto);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }
    return null;
  }

  @Override public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public UpgradeServiceResponseProto upgrade(
      UpgradeServiceRequestProto request) throws IOException, YarnException {
    try {
      return proxy.upgradeService(null, request);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }
    return null;
  }

  @Override
  public RestartServiceResponseProto restart(RestartServiceRequestProto request)
      throws IOException, YarnException {
    try {
      return proxy.restartService(null, request);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }
    return null;
  }
}
