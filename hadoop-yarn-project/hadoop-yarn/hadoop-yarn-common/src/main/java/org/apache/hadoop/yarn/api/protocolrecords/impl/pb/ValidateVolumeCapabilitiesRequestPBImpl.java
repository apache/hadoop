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
package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PB wrapper for CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest.
 */
public class ValidateVolumeCapabilitiesRequestPBImpl extends
    ValidateVolumeCapabilitiesRequest {

  private CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest.Builder builder;

  public ValidateVolumeCapabilitiesRequestPBImpl(
      CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest proto) {
    this.builder = proto.toBuilder();
  }

  public ValidateVolumeCapabilitiesRequestPBImpl() {
    this.builder = CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest
       .newBuilder();
  }

  @Override
  public String getVolumeId() {
    Preconditions.checkNotNull(builder);
    return builder.getVolumeId();
  }

  @Override
  public void setVolumeAttributes(Map<String, String> attributes) {
    Preconditions.checkNotNull(builder);
    builder.addAllVolumeAttributes(ProtoUtils.convertToProtoFormat(attributes));
  }

  @Override
  public void setVolumeId(String volumeId) {
    Preconditions.checkNotNull(builder);
    builder.setVolumeId(volumeId);
  }

  @Override
  public void addVolumeCapability(VolumeCapability volumeCapability) {
    Preconditions.checkNotNull(builder);
    CsiAdaptorProtos.VolumeCapability vc =
        CsiAdaptorProtos.VolumeCapability.newBuilder()
            .setAccessMode(CsiAdaptorProtos.VolumeCapability.AccessMode
                .valueOf(volumeCapability.getAccessMode().ordinal()))
            .setVolumeType(CsiAdaptorProtos.VolumeCapability.VolumeType
                .valueOf(volumeCapability.getVolumeType().ordinal()))
            .addAllMountFlags(volumeCapability.getMountFlags())
            .build();
    builder.addVolumeCapabilities(vc);
  }

  @Override
  public List<VolumeCapability> getVolumeCapabilities() {
    Preconditions.checkNotNull(builder);
    List<VolumeCapability> caps = new ArrayList<>(
        builder.getVolumeCapabilitiesCount());
    builder.getVolumeCapabilitiesList().forEach(capability -> {
      VolumeCapability vc = new VolumeCapability(
          AccessMode.valueOf(capability.getAccessMode().name()),
          VolumeType.valueOf(capability.getVolumeType().name()),
          capability.getMountFlagsList());
      caps.add(vc);
    });
    return caps;
  }

  @Override
  public Map<String, String> getVolumeAttributes() {
    Preconditions.checkNotNull(builder);
    return ProtoUtils.convertStringStringMapProtoListToMap(
        builder.getVolumeAttributesList());
  }

  public CsiAdaptorProtos.ValidateVolumeCapabilitiesRequest getProto() {
    Preconditions.checkNotNull(builder);
    return builder.build();
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
}
