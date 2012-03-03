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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStateRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.ReadyToBecomeActiveRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

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
    HAServiceProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final static MonitorHealthRequestProto MONITOR_HEALTH_REQ = 
      MonitorHealthRequestProto.newBuilder().build();
  private final static TransitionToActiveRequestProto TRANSITION_TO_ACTIVE_REQ = 
      TransitionToActiveRequestProto.newBuilder().build();
  private final static TransitionToStandbyRequestProto TRANSITION_TO_STANDBY_REQ = 
      TransitionToStandbyRequestProto.newBuilder().build();
  private final static GetServiceStateRequestProto GET_SERVICE_STATE_REQ = 
      GetServiceStateRequestProto.newBuilder().build();
  private final static ReadyToBecomeActiveRequestProto ACTIVE_READY_REQ = 
      ReadyToBecomeActiveRequestProto.newBuilder().build();
  
  private final HAServiceProtocolPB rpcProxy;

  public HAServiceProtocolClientSideTranslatorPB(InetSocketAddress addr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    rpcProxy = RPC.getProxy(HAServiceProtocolPB.class,
        RPC.getProtocolVersion(HAServiceProtocolPB.class), addr, conf);
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
  public HAServiceState getServiceState() throws IOException {
    HAServiceStateProto state;
    try {
      state = rpcProxy.getServiceState(NULL_CONTROLLER,
          GET_SERVICE_STATE_REQ).getState();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
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
  public boolean readyToBecomeActive() throws IOException {
    try {
      return rpcProxy.readyToBecomeActive(NULL_CONTROLLER, ACTIVE_READY_REQ)
          .getReadyToBecomeActive();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}
