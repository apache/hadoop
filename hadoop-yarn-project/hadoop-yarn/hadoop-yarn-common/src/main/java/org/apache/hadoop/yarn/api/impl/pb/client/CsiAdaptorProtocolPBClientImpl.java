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

import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.CsiAdaptorPB;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.NodeUnpublishVolumeResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
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
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * CSI adaptor client implementation.
 */
public class CsiAdaptorProtocolPBClientImpl
    implements CsiAdaptorProtocol, Closeable {

  private final CsiAdaptorPB proxy;

  public CsiAdaptorProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, CsiAdaptorPB.class, ProtobufRpcEngine2.class);
    this.proxy = RPC.getProxy(CsiAdaptorPB.class, clientVersion, addr, conf);
  }

  @Override
  public GetPluginInfoResponse getPluginInfo(
      GetPluginInfoRequest request) throws YarnException, IOException {
    CsiAdaptorProtos.GetPluginInfoRequest requestProto =
        ((GetPluginInfoRequestPBImpl) request).getProto();
    try {
      return new GetPluginInfoResponsePBImpl(
          proxy.getPluginInfo(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ValidateVolumeCapabilitiesResponse validateVolumeCapacity(
      ValidateVolumeCapabilitiesRequest request)
      throws YarnException, IOException {
    CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest requestProto =
        ((ValidateVolumeCapabilitiesRequestPBImpl) request).getProto();
    try {
      return new ValidateVolumeCapabilitiesResponsePBImpl(
          proxy.validateVolumeCapacity(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public NodePublishVolumeResponse nodePublishVolume(
      NodePublishVolumeRequest request) throws IOException, YarnException {
    CsiAdaptorProtos.NodePublishVolumeRequest requestProto =
        ((NodePublishVolumeRequestPBImpl) request).getProto();
    try {
      return new NodePublishVolumeResponsePBImpl(
          proxy.nodePublishVolume(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public NodeUnpublishVolumeResponse nodeUnpublishVolume(
      NodeUnpublishVolumeRequest request) throws YarnException, IOException {
    CsiAdaptorProtos.NodeUnpublishVolumeRequest requestProto =
        ((NodeUnpublishVolumeRequestPBImpl) request).getProto();
    try {
      return new NodeUnpublishVolumeResponsePBImpl(
          proxy.nodeUnpublishVolume(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    if(this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }
}
