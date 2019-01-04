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

package org.apache.hadoop.yarn.csi.client;

import csi.v0.Csi;
import csi.v0.Csi.GetPluginInfoRequest;
import csi.v0.Csi.GetPluginInfoResponse;
import org.apache.hadoop.yarn.csi.utils.GrpcHelper;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * A CSI client implementation that communicates with a CSI driver via
 * unix domain socket. It leverages gRPC blocking stubs to synchronize
 * the call with CSI driver. CSI spec is designed as a set of synchronized
 * APIs, in order to make the call idempotent for failure recovery,
 * so the client does the same.
 */
public class CsiClientImpl implements CsiClient {

  private final SocketAddress address;

  public CsiClientImpl(String address) {
    this.address = GrpcHelper.getSocketAddress(address);
  }

  @Override
  public GetPluginInfoResponse getPluginInfo() throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(address).build()) {
      GetPluginInfoRequest request = GetPluginInfoRequest.getDefaultInstance();
      return client.createIdentityBlockingStub().getPluginInfo(request);
    }
  }

  @Override
  public Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(
      Csi.ValidateVolumeCapabilitiesRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(address).build()) {
      return client.createControllerBlockingStub()
          .validateVolumeCapabilities(request);
    }
  }

  @Override
  public Csi.NodePublishVolumeResponse nodePublishVolume(
      Csi.NodePublishVolumeRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(address).build()) {
      return client.createNodeBlockingStub()
          .nodePublishVolume(request);
    }
  }

  @Override
  public Csi.NodeUnpublishVolumeResponse nodeUnpublishVolume(
      Csi.NodeUnpublishVolumeRequest request) throws IOException {
    try (CsiGrpcClient client = CsiGrpcClient.newBuilder()
        .setDomainSocketAddress(address).build()) {
      return client.createNodeBlockingStub()
          .nodeUnpublishVolume(request);
    }
  }
}
