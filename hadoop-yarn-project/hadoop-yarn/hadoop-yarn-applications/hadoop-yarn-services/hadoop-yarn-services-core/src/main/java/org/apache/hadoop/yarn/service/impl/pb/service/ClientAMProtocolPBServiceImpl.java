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

package org.apache.hadoop.yarn.service.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;
import org.apache.hadoop.yarn.service.ClientAMProtocol;

import java.io.IOException;

public class ClientAMProtocolPBServiceImpl implements ClientAMProtocolPB {

  private ClientAMProtocol real;

  public ClientAMProtocolPBServiceImpl(ClientAMProtocol impl) {
    this.real = impl;
  }

  @Override
  public FlexComponentsResponseProto flexComponents(RpcController controller,
      FlexComponentsRequestProto request) throws ServiceException {
    try {
      return real.flexComponents(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override public GetStatusResponseProto getStatus(RpcController controller,
      GetStatusRequestProto request) throws ServiceException {
    try {
      return real.getStatus(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public org.apache.hadoop.yarn.proto.ClientAMProtocol.StopResponseProto stop(
      RpcController controller,
      org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto request)
      throws ServiceException {
    try {
      return real.stop(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpgradeServiceResponseProto upgradeService(RpcController controller,
      UpgradeServiceRequestProto request) throws ServiceException {
    try {
      return real.upgrade(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RestartServiceResponseProto restartService(RpcController controller,
      RestartServiceRequestProto request) throws ServiceException {
    try {
      return real.restart(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CompInstancesUpgradeResponseProto upgrade(RpcController controller,
      CompInstancesUpgradeRequestProto request) throws ServiceException {
    try {
      return real.upgrade(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCompInstancesResponseProto getCompInstances(
      RpcController controller, GetCompInstancesRequestProto request)
      throws ServiceException {
    try {
      return real.getCompInstances(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CancelUpgradeResponseProto cancelUpgrade(
      RpcController controller, CancelUpgradeRequestProto request)
      throws ServiceException {
    try {
      return real.cancelUpgrade(request);
    } catch (IOException | YarnException e) {
      throw new ServiceException(e);
    }
  }
}
