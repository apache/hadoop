/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.csi;

import org.apache.hadoop.ozone.util.OzoneVersionInfo;

import com.google.protobuf.BoolValue;
import csi.v1.Csi.GetPluginCapabilitiesResponse;
import csi.v1.Csi.GetPluginInfoResponse;
import csi.v1.Csi.PluginCapability;
import csi.v1.Csi.PluginCapability.Service;
import static csi.v1.Csi.PluginCapability.Service.Type.CONTROLLER_SERVICE;
import csi.v1.Csi.ProbeResponse;
import csi.v1.IdentityGrpc.IdentityImplBase;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the CSI identity service.
 */
public class IdentitiyService extends IdentityImplBase {

  @Override
  public void getPluginInfo(csi.v1.Csi.GetPluginInfoRequest request,
      StreamObserver<csi.v1.Csi.GetPluginInfoResponse> responseObserver) {
    GetPluginInfoResponse response = GetPluginInfoResponse.newBuilder()
        .setName("org.apache.hadoop.ozone")
        .setVendorVersion(OzoneVersionInfo.OZONE_VERSION_INFO.getVersion())
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getPluginCapabilities(
      csi.v1.Csi.GetPluginCapabilitiesRequest request,
      StreamObserver<GetPluginCapabilitiesResponse> responseObserver) {
    GetPluginCapabilitiesResponse response =
        GetPluginCapabilitiesResponse.newBuilder()
            .addCapabilities(PluginCapability.newBuilder().setService(
                Service.newBuilder().setType(CONTROLLER_SERVICE)))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();

  }

  @Override
  public void probe(csi.v1.Csi.ProbeRequest request,
      StreamObserver<csi.v1.Csi.ProbeResponse> responseObserver) {
    ProbeResponse response = ProbeResponse.newBuilder()
        .setReady(BoolValue.of(true))
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();

  }
}
