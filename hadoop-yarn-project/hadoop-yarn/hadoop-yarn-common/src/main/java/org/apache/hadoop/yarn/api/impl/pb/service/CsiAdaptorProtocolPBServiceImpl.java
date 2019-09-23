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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.api.CsiAdaptorPB;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetPluginInfoRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetPluginInfoResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NodePublishVolumeRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NodePublishVolumeResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NodeUnpublishVolumeRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.NodeUnpublishVolumeResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ValidateVolumeCapabilitiesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ValidateVolumeCapabilitiesResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;

import java.io.IOException;

/**
 * CSI adaptor server side implementation, this is hosted on a node manager.
 */
public class CsiAdaptorProtocolPBServiceImpl implements CsiAdaptorPB {

  private final CsiAdaptorProtocol real;
  public CsiAdaptorProtocolPBServiceImpl(CsiAdaptorProtocol impl) {
    this.real = impl;
  }

  @Override
  public CsiAdaptorProtos.GetPluginInfoResponse getPluginInfo(
      RpcController controller, CsiAdaptorProtos.GetPluginInfoRequest request)
      throws ServiceException {
    try {
      GetPluginInfoRequest req =
          new GetPluginInfoRequestPBImpl(request);
      GetPluginInfoResponse response = real.getPluginInfo(req);
      return ((GetPluginInfoResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CsiAdaptorProtos.ValidateVolumeCapabilitiesResponse
      validateVolumeCapacity(RpcController controller,
      CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest request)
      throws ServiceException {
    try {
      ValidateVolumeCapabilitiesRequestPBImpl req =
          new ValidateVolumeCapabilitiesRequestPBImpl(request);
      ValidateVolumeCapabilitiesResponse response =
          real.validateVolumeCapacity(req);
      return ((ValidateVolumeCapabilitiesResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CsiAdaptorProtos.NodePublishVolumeResponse nodePublishVolume(
      RpcController controller,
      CsiAdaptorProtos.NodePublishVolumeRequest request)
      throws ServiceException {
    try {
      NodePublishVolumeRequestPBImpl req =
          new NodePublishVolumeRequestPBImpl(request);
      NodePublishVolumeResponse response = real.nodePublishVolume(req);
      return ((NodePublishVolumeResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CsiAdaptorProtos.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      RpcController controller,
      CsiAdaptorProtos.NodeUnpublishVolumeRequest request)
      throws ServiceException {
    try {
      NodeUnpublishVolumeRequestPBImpl req =
          new NodeUnpublishVolumeRequestPBImpl(request);
      NodeUnpublishVolumeResponse response = real.nodeUnpublishVolume(req);
      return ((NodeUnpublishVolumeResponsePBImpl) response).getProto();
    } catch (YarnException | IOException e) {
      throw new ServiceException(e);
    }
  }
}
