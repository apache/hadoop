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

package org.apache.hadoop.yarn.server.api.impl.pb.service;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ResourceTrackerPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ResourceTrackerPBServiceImpl implements ResourceTrackerPB {

  private ResourceTracker real;
  
  public ResourceTrackerPBServiceImpl(ResourceTracker impl) {
    this.real = impl;
  }
  
  @Override
  public RegisterNodeManagerResponseProto registerNodeManager(
      RpcController controller, RegisterNodeManagerRequestProto proto)
      throws ServiceException {
    RegisterNodeManagerRequestPBImpl request = new RegisterNodeManagerRequestPBImpl(proto);
    try {
      RegisterNodeManagerResponse response = real.registerNodeManager(request);
      return ((RegisterNodeManagerResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public NodeHeartbeatResponseProto nodeHeartbeat(RpcController controller,
      NodeHeartbeatRequestProto proto) throws ServiceException {
    NodeHeartbeatRequestPBImpl request = new NodeHeartbeatRequestPBImpl(proto);
    try {
      NodeHeartbeatResponse response = real.nodeHeartbeat(request);
      return ((NodeHeartbeatResponsePBImpl)response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UnRegisterNodeManagerResponseProto unRegisterNodeManager(
      RpcController controller, UnRegisterNodeManagerRequestProto proto)
      throws ServiceException {
    UnRegisterNodeManagerRequestPBImpl request =
        new UnRegisterNodeManagerRequestPBImpl(proto);
    try {
      UnRegisterNodeManagerResponse response = real
          .unRegisterNodeManager(request);
      return ((UnRegisterNodeManagerResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}
