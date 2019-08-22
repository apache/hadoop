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

import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneClient;

import csi.v1.ControllerGrpc.ControllerImplBase;
import csi.v1.Csi.CapacityRange;
import csi.v1.Csi.ControllerGetCapabilitiesRequest;
import csi.v1.Csi.ControllerGetCapabilitiesResponse;
import csi.v1.Csi.ControllerServiceCapability;
import csi.v1.Csi.ControllerServiceCapability.RPC;
import csi.v1.Csi.ControllerServiceCapability.RPC.Type;
import csi.v1.Csi.CreateVolumeRequest;
import csi.v1.Csi.CreateVolumeResponse;
import csi.v1.Csi.DeleteVolumeRequest;
import csi.v1.Csi.DeleteVolumeResponse;
import csi.v1.Csi.Volume;
import io.grpc.stub.StreamObserver;

/**
 * CSI controller service.
 * <p>
 * This service usually runs only once and responsible for the creation of
 * the volume.
 */
public class ControllerService extends ControllerImplBase {

  private final String volumeOwner;

  private long defaultVolumeSize;

  private OzoneClient ozoneClient;

  public ControllerService(OzoneClient ozoneClient, long volumeSize,
      String volumeOwner) {
    this.volumeOwner = volumeOwner;
    this.defaultVolumeSize = volumeSize;
    this.ozoneClient = ozoneClient;
  }

  @Override
  public void createVolume(CreateVolumeRequest request,
      StreamObserver<CreateVolumeResponse> responseObserver) {
    try {
      ozoneClient.getObjectStore()
          .createS3Bucket(volumeOwner, request.getName());

      long size = findSize(request.getCapacityRange());

      CreateVolumeResponse response = CreateVolumeResponse.newBuilder()
          .setVolume(Volume.newBuilder()
              .setVolumeId(request.getName())
              .setCapacityBytes(size))
          .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(e);
    }
  }

  private long findSize(CapacityRange capacityRange) {
    if (capacityRange.getRequiredBytes() != 0) {
      return capacityRange.getRequiredBytes();
    } else {
      if (capacityRange.getLimitBytes() != 0) {
        return Math.min(defaultVolumeSize, capacityRange.getLimitBytes());
      } else {
        //~1 gig
        return defaultVolumeSize;
      }
    }
  }

  @Override
  public void deleteVolume(DeleteVolumeRequest request,
      StreamObserver<DeleteVolumeResponse> responseObserver) {
    try {
      ozoneClient.getObjectStore().deleteS3Bucket(request.getVolumeId());

      DeleteVolumeResponse response = DeleteVolumeResponse.newBuilder()
          .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void controllerGetCapabilities(
      ControllerGetCapabilitiesRequest request,
      StreamObserver<ControllerGetCapabilitiesResponse> responseObserver) {
    ControllerGetCapabilitiesResponse response =
        ControllerGetCapabilitiesResponse.newBuilder()
            .addCapabilities(
                ControllerServiceCapability.newBuilder().setRpc(
                    RPC.newBuilder().setType(Type.CREATE_DELETE_VOLUME)))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
