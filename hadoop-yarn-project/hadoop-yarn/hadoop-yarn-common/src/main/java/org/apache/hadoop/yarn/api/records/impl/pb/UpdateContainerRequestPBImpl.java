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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

/**
 * Implementation of <code>UpdateContainerRequest</code>.
 */
public class UpdateContainerRequestPBImpl extends UpdateContainerRequest {
  private YarnServiceProtos.UpdateContainerRequestProto proto =
      YarnServiceProtos.UpdateContainerRequestProto.getDefaultInstance();
  private YarnServiceProtos.UpdateContainerRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private ContainerId existingContainerId = null;
  private Resource targetCapability = null;

  public UpdateContainerRequestPBImpl() {
    builder = YarnServiceProtos.UpdateContainerRequestProto.newBuilder();
  }

  public UpdateContainerRequestPBImpl(YarnServiceProtos
      .UpdateContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServiceProtos.UpdateContainerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int getContainerVersion() {
    YarnServiceProtos.UpdateContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasContainerVersion()) {
      return 0;
    }
    return p.getContainerVersion();
  }

  @Override
  public void setContainerVersion(int containerVersion) {
    maybeInitBuilder();
    builder.setContainerVersion(containerVersion);
  }

  @Override
  public ContainerId getContainerId() {
    YarnServiceProtos.UpdateContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.existingContainerId != null) {
      return this.existingContainerId;
    }
    if (p.hasContainerId()) {
      this.existingContainerId =
          ProtoUtils.convertFromProtoFormat(p.getContainerId());
    }
    return this.existingContainerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.existingContainerId = containerId;
  }

  @Override
  public Resource getCapability() {
    YarnServiceProtos.UpdateContainerRequestProtoOrBuilder p = viaProto ? proto
        : builder;
    if (this.targetCapability != null) {
      return this.targetCapability;
    }
    if (p.hasCapability()) {
      this.targetCapability =
          ProtoUtils.convertFromProtoFormat(p.getCapability());
    }
    return this.targetCapability;
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
    }
    this.targetCapability = capability;
  }

  @Override
  public ExecutionType getExecutionType() {
    YarnServiceProtos.UpdateContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasExecutionType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public void setExecutionType(ExecutionType execType) {
    maybeInitBuilder();
    if (execType == null) {
      builder.clearExecutionType();
      return;
    }
    builder.setExecutionType(ProtoUtils.convertToProtoFormat(execType));
  }

  @Override
  public ContainerUpdateType getContainerUpdateType() {
    YarnServiceProtos.UpdateContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasUpdateType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getUpdateType());
  }

  @Override
  public void setContainerUpdateType(ContainerUpdateType updateType) {
    maybeInitBuilder();
    if (updateType == null) {
      builder.clearUpdateType();
      return;
    }
    builder.setUpdateType(ProtoUtils.convertToProtoFormat(updateType));
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServiceProtos.UpdateContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.existingContainerId != null) {
      builder.setContainerId(
          ProtoUtils.convertToProtoFormat(this.existingContainerId));
    }
    if (this.targetCapability != null) {
      builder.setCapability(
          ProtoUtils.convertToProtoFormat(this.targetCapability));
    }
  }


}
