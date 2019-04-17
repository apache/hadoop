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
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.NodePublishVolumeRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.CsiAdaptorProtos;
import org.apache.hadoop.yarn.proto.YarnProtos;

import java.util.Map;

/**
 * Request to publish volume on node manager.
 */
public class NodePublishVolumeRequestPBImpl extends
    NodePublishVolumeRequest {

  private CsiAdaptorProtos.NodePublishVolumeRequest.Builder builder;

  public NodePublishVolumeRequestPBImpl() {
    this.builder = CsiAdaptorProtos.NodePublishVolumeRequest.newBuilder();
  }

  public NodePublishVolumeRequestPBImpl(
      CsiAdaptorProtos.NodePublishVolumeRequest request) {
    this.builder = request.toBuilder();
  }

  public CsiAdaptorProtos.NodePublishVolumeRequest getProto() {
    Preconditions.checkNotNull(builder);
    return builder.build();
  }

  @Override
  public void setVolumeId(String volumeId) {
    Preconditions.checkNotNull(builder);
    builder.setVolumeId(volumeId);
  }

  @Override
  public String getVolumeId() {
    Preconditions.checkNotNull(builder);
    return builder.getVolumeId();
  }

  @Override
  public void setReadonly(boolean readonly) {
    Preconditions.checkNotNull(builder);
    builder.setReadonly(readonly);
  }

  @Override
  public boolean getReadOnly() {
    Preconditions.checkNotNull(builder);
    return builder.getReadonly();
  }

  @Override
  public void setSecrets(Map<String, String> secrets) {
    if (secrets != null) {
      Preconditions.checkNotNull(builder);
      for(Map.Entry<String, String> entry : secrets.entrySet()) {
        YarnProtos.StringStringMapProto mapEntry =
            YarnProtos.StringStringMapProto.newBuilder()
                .setKey(entry.getKey())
                .setValue(entry.getValue())
                .build();
        builder.addSecrets(mapEntry);
      }
    }
  }

  @Override
  public Map<String, String> getSecrets() {
    Preconditions.checkNotNull(builder);
    return builder.getSecretsCount() > 0 ?
        ProtoUtils.convertStringStringMapProtoListToMap(
            builder.getSecretsList()) : ImmutableMap.of();
  }

  @Override
  public String getTargetPath() {
    Preconditions.checkNotNull(builder);
    return builder.getTargetPath();
  }

  @Override
  public void setStagingPath(String stagingPath) {
    Preconditions.checkNotNull(builder);
    builder.setStagingTargetPath(stagingPath);
  }

  @Override
  public String getStagingPath() {
    Preconditions.checkNotNull(builder);
    return builder.getStagingTargetPath();
  }

  @Override
  public void setPublishContext(Map<String, String> publishContext) {
    if (publishContext != null) {
      Preconditions.checkNotNull(builder);
      for(Map.Entry<String, String> entry : publishContext.entrySet()) {
        YarnProtos.StringStringMapProto mapEntry =
            YarnProtos.StringStringMapProto.newBuilder()
                .setKey(entry.getKey())
                .setValue(entry.getValue())
                .build();
        builder.addPublishContext(mapEntry);
      }
    }
  }

  @Override
  public Map<String, String> getPublishContext() {
    Preconditions.checkNotNull(builder);
    return builder.getPublishContextCount() > 0 ?
        ProtoUtils.convertStringStringMapProtoListToMap(
            builder.getPublishContextList()) : ImmutableMap.of();
  }

  @Override
  public void setTargetPath(String targetPath) {
    if (targetPath != null) {
      Preconditions.checkNotNull(builder);
      builder.setTargetPath(targetPath);
    }
  }

  @Override
  public void setVolumeCapability(
      VolumeCapability capability) {
    if (capability != null) {
      CsiAdaptorProtos.VolumeCapability vc =
          CsiAdaptorProtos.VolumeCapability.newBuilder()
              .setAccessMode(CsiAdaptorProtos.VolumeCapability
                  .AccessMode.valueOf(
                      capability.getAccessMode().ordinal()))
              .setVolumeType(CsiAdaptorProtos.VolumeCapability
                  .VolumeType.valueOf(capability.getVolumeType().ordinal()))
              .addAllMountFlags(capability.getMountFlags())
              .build();
      builder.setVolumeCapability(vc);
    }
  }

  @Override
  public VolumeCapability getVolumeCapability() {
    CsiAdaptorProtos.VolumeCapability cap0 = builder.getVolumeCapability();
    if (builder.hasVolumeCapability()) {
      return new VolumeCapability(
          ValidateVolumeCapabilitiesRequest.AccessMode
              .valueOf(cap0.getAccessMode().name()),
          ValidateVolumeCapabilitiesRequest.VolumeType
              .valueOf(cap0.getVolumeType().name()),
          cap0.getMountFlagsList());
    }
    return null;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
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
