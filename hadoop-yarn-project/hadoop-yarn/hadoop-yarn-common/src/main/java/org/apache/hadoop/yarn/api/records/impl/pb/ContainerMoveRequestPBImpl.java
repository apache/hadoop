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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerMoveRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerMoveRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerMoveRequestProtoOrBuilder;

@Private
@Unstable
public class ContainerMoveRequestPBImpl extends ContainerMoveRequest {
  
  ContainerMoveRequestProto proto = ContainerMoveRequestProto.getDefaultInstance();
  ContainerMoveRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Priority priority = null;
  private ContainerId originContainerId = null;
  
  public ContainerMoveRequestPBImpl() {
    builder = ContainerMoveRequestProto.newBuilder();
  }
  
  public ContainerMoveRequestPBImpl(ContainerMoveRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerMoveRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void mergeLocalToBuilder() {
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.originContainerId != null) {
      builder.setOriginContainerId(convertToProtoFormat(this.originContainerId));
    }
  }
  
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerMoveRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public Priority getPriority() {
    ContainerMoveRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }
  
  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }
  
  @Override
  public ContainerId getOriginContainerId() {
    ContainerMoveRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.originContainerId != null) {
      return this.originContainerId;
    }
    if (!p.hasOriginContainerId()) {
      return null;
    }
    this.originContainerId = convertFromProtoFormat(p.getOriginContainerId());
    return this.originContainerId;
  }
  
  @Override
  public void setOriginContainerId(ContainerId originContainerId) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearOriginContainerId();
    this.originContainerId = originContainerId;
  }
  
  @Override
  public String getTargetHost() {
    ContainerMoveRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTargetHost()) {
      return null;
    }
    return (p.getTargetHost());
  }
  
  @Override
  public void setTargetHost(String targetHost) {
    maybeInitBuilder();
    if (targetHost == null)
      builder.clearTargetHost();
    builder.setTargetHost(targetHost);
  }
  
  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }
  
  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl) t).getProto();
  }
  
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }
  
  @Override
  public String toString() {
    return "{Priority: " + getPriority()
        + ", OriginContainerId: " + getOriginContainerId()
        + ", TargetHost: " + getTargetHost() + "}";
  }
}
