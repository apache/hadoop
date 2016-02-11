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

package org.apache.hadoop.yarn.server.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;


import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistSchedAllocateResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.DistSchedRegisterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .FinishApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .FinishApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb
    .RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class DistributedSchedulerProtocolPBClientImpl implements
    DistributedSchedulerProtocol, Closeable {

  private DistributedSchedulerProtocolPB proxy;

  public DistributedSchedulerProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DistributedSchedulerProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = RPC.getProxy(DistributedSchedulerProtocolPB.class, clientVersion,
        addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public DistSchedRegisterResponse
  registerApplicationMasterForDistributedScheduling
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    YarnServiceProtos.RegisterApplicationMasterRequestProto requestProto =
        ((RegisterApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new DistSchedRegisterResponsePBImpl(
          proxy.registerApplicationMasterForDistributedScheduling(
              null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public DistSchedAllocateResponse allocateForDistributedScheduling
      (AllocateRequest request) throws YarnException, IOException {
    YarnServiceProtos.AllocateRequestProto requestProto =
        ((AllocateRequestPBImpl) request).getProto();
    try {
      return new DistSchedAllocateResponsePBImpl(
          proxy.allocateForDistributedScheduling(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    YarnServiceProtos.RegisterApplicationMasterRequestProto requestProto =
        ((RegisterApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new RegisterApplicationMasterResponsePBImpl(
          proxy.registerApplicationMaster(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    YarnServiceProtos.FinishApplicationMasterRequestProto requestProto =
        ((FinishApplicationMasterRequestPBImpl) request).getProto();
    try {
      return new FinishApplicationMasterResponsePBImpl(
          proxy.finishApplicationMaster(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    YarnServiceProtos.AllocateRequestProto requestProto =
        ((AllocateRequestPBImpl) request).getProto();
    try {
      return new AllocateResponsePBImpl(proxy.allocate(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
