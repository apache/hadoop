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
package org.apache.hadoop.ha.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link HAServiceProtocol} interfaces to the RPC server implementing
 * {@link HAServiceProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HAServiceProtocolClientSideTranslatorPB implements
    HAServiceProtocol, Closeable, ProtocolTranslator {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final static MonitorHealthRequestProto MONITOR_HEALTH_REQ = 
      MonitorHealthRequestProto.newBuilder().build();
  private final static TransitionToActiveRequestProto TRANSITION_TO_ACTIVE_REQ = 
      TransitionToActiveRequestProto.newBuilder().build();
  private final static TransitionToStandbyRequestProto TRANSITION_TO_STANDBY_REQ = 
      TransitionToStandbyRequestProto.newBuilder().build();
  private final static GetServiceStatusRequestProto GET_SERVICE_STATUS_REQ = 
      GetServiceStatusRequestProto.newBuilder().build();
  
  private final HAServiceProtocolPB rpcProxy;

  public HAServiceProtocolClientSideTranslatorPB(InetSocketAddress addr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    rpcProxy = RPC.getProxy(HAServiceProtocolPB.class,
        RPC.getProtocolVersion(HAServiceProtocolPB.class), addr, conf);
  }
  
  public HAServiceProtocolClientSideTranslatorPB(
      InetSocketAddress addr, Configuration conf,
      SocketFactory socketFactory, int timeout) throws IOException {
    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    rpcProxy = RPC.getProxy(HAServiceProtocolPB.class,
        RPC.getProtocolVersion(HAServiceProtocolPB.class), addr,
        UserGroupInformation.getCurrentUser(), conf, socketFactory, timeout);
  }

  @Override
  public void monitorHealth() throws IOException {
    try {
      rpcProxy.monitorHealth(NULL_CONTROLLER, MONITOR_HEALTH_REQ);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void transitionToActive() throws IOException {
    try {
      rpcProxy.transitionToActive(NULL_CONTROLLER, TRANSITION_TO_ACTIVE_REQ);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void transitionToStandby() throws IOException {
    try {
      rpcProxy.transitionToStandby(NULL_CONTROLLER, TRANSITION_TO_STANDBY_REQ);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public HAServiceStatus getServiceStatus() throws IOException {
    GetServiceStatusResponseProto status;
    try {
      status = rpcProxy.getServiceStatus(NULL_CONTROLLER,
          GET_SERVICE_STATUS_REQ);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    
    HAServiceStatus ret = new HAServiceStatus(
        convert(status.getState()));
    if (status.getReadyToBecomeActive()) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive(status.getNotReadyReason());
    }
    return ret;
  }
  
  private HAServiceState convert(HAServiceStateProto state) {
    switch(state) {
    case ACTIVE:
      return HAServiceState.ACTIVE;
    case STANDBY:
      return HAServiceState.STANDBY;
    case INITIALIZING:
    default:
      return HAServiceState.INITIALIZING;
    }
  }
  
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
