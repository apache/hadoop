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

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReInitializeContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReInitializeContainerRequestProtoOrBuilder;

// CHECKSTYLE:OFF
public class ReInitializeContainerRequestPBImpl extends ReInitializeContainerRequest {
  private ReInitializeContainerRequestProto proto =
      ReInitializeContainerRequestProto.getDefaultInstance();
  private ReInitializeContainerRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private ContainerId containerId;
  private ContainerLaunchContext containerLaunchContext;

  public ReInitializeContainerRequestPBImpl() {
    builder = ReInitializeContainerRequestProto.newBuilder();
  }

  public ReInitializeContainerRequestPBImpl(ReInitializeContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReInitializeContainerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      builder.setContainerId(ProtoUtils.convertToProtoFormat(this.containerId));
    }
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReInitializeContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ContainerId getContainerId() {
    ReInitializeContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = ProtoUtils.convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public ContainerLaunchContext getContainerLaunchContext() {
    ReInitializeContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.containerLaunchContext != null) {
      return this.containerLaunchContext;
    }
    if (!p.hasContainerLaunchContext()) {
      return null;
    }
    this.containerLaunchContext =
        convertFromProtoFormat(p.getContainerLaunchContext());
    return this.containerLaunchContext;
  }

  @Override
  public void setContainerLaunchContext(
      ContainerLaunchContext containerLaunchContext) {
    maybeInitBuilder();
    if (containerLaunchContext == null)
      builder.clearContainerLaunchContext();
    this.containerLaunchContext = containerLaunchContext;
  }

  @Override
  public boolean getAutoCommit() {
    ReInitializeContainerRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAutoCommit()) {
      return false;
    }
    return (p.getAutoCommit());
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    maybeInitBuilder();
    builder.setAutoCommit(autoCommit);
  }

  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

}
