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

package org.apache.hadoop.yarn.csi.translator;

import csi.v0.Csi;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeType;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.ArrayList;
import java.util.List;

/**
 * Proto message translator for ValidateVolumeCapabilitiesRequest.
 * @param <A> ValidateVolumeCapabilitiesRequest
 * @param <B> Csi.ValidateVolumeCapabilitiesRequest
 */
public class ValidateVolumeCapabilitiesRequestProtoTranslator<A, B>
    implements ProtoTranslator<ValidateVolumeCapabilitiesRequest,
            Csi.ValidateVolumeCapabilitiesRequest> {

  @Override
  public Csi.ValidateVolumeCapabilitiesRequest convertTo(
      ValidateVolumeCapabilitiesRequest request) throws YarnException {
    Csi.ValidateVolumeCapabilitiesRequest.Builder buidler =
        Csi.ValidateVolumeCapabilitiesRequest.newBuilder();
    buidler.setVolumeId(request.getVolumeId());
    if (request.getVolumeCapabilities() != null
        && request.getVolumeCapabilities().size() > 0) {
      buidler.putAllVolumeAttributes(request.getVolumeAttributes());
    }
    for (VolumeCapability cap :
        request.getVolumeCapabilities()) {
      Csi.VolumeCapability.AccessMode accessMode =
          Csi.VolumeCapability.AccessMode.newBuilder()
              .setModeValue(cap.getAccessMode().ordinal())
              .build();
      Csi.VolumeCapability.MountVolume mountVolume =
          Csi.VolumeCapability.MountVolume.newBuilder()
              .addAllMountFlags(cap.getMountFlags())
              .build();
      Csi.VolumeCapability capability =
          Csi.VolumeCapability.newBuilder()
              .setAccessMode(accessMode)
              .setMount(mountVolume)
              .build();
      buidler.addVolumeCapabilities(capability);
    }
    return buidler.build();
  }

  @Override
  public ValidateVolumeCapabilitiesRequest convertFrom(
      Csi.ValidateVolumeCapabilitiesRequest request) throws YarnException {
    ValidateVolumeCapabilitiesRequest result = ValidateVolumeCapabilitiesRequest
        .newInstance(request.getVolumeId(), request.getVolumeAttributesMap());
    for (Csi.VolumeCapability csiCap :
        request.getVolumeCapabilitiesList()) {
      ValidateVolumeCapabilitiesRequest.AccessMode mode =
          ValidateVolumeCapabilitiesRequest.AccessMode
              .valueOf(csiCap.getAccessMode().getMode().name());
      if (!csiCap.hasMount()) {
        throw new YarnException("Invalid request,"
            + " mount is not found in the request.");
      }
      List<String> mountFlags = new ArrayList<>();
      for (int i=0; i<csiCap.getMount().getMountFlagsCount(); i++) {
        mountFlags.add(csiCap.getMount().getMountFlags(i));
      }
      VolumeCapability capability = new VolumeCapability(mode,
          VolumeType.FILE_SYSTEM, mountFlags);
      result.addVolumeCapability(capability);
    }
    return result;
  }
}
