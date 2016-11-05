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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

public class NMContainerStatusPBImpl extends NMContainerStatus {

  NMContainerStatusProto proto = NMContainerStatusProto
    .getDefaultInstance();
  NMContainerStatusProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId = null;
  private Resource resource = null;
  private Priority priority = null;

  public NMContainerStatusPBImpl() {
    builder = NMContainerStatusProto.newBuilder();
  }

  public NMContainerStatusPBImpl(NMContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NMContainerStatusProto getProto() {

    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return this.getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(getContainerId()).append(", ")
        .append("CreateTime: ").append(getCreationTime()).append(", ")
        .append("Version: ").append(getVersion()).append(", ")
        .append("State: ").append(getContainerState()).append(", ")
        .append("Capability: ").append(getAllocatedResource()).append(", ")
        .append("Diagnostics: ").append(getDiagnostics()).append(", ")
        .append("ExitStatus: ").append(getContainerExitStatus()).append(", ")
        .append("NodeLabelExpression: ").append(getNodeLabelExpression())
        .append("Priority: ").append(getPriority())
        .append("]");
    return sb.toString();
  }

  @Override
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public String getDiagnostics() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return (p.getDiagnostics());
  }

  @Override
  public ContainerState getContainerState() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null)
      builder.clearContainerId();
    this.containerId = containerId;
  }

  @Override
  public void setDiagnostics(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnosticsInfo);
  }

  @Override
  public void setContainerState(ContainerState containerState) {
    maybeInitBuilder();
    if (containerState == null) {
      builder.clearContainerState();
      return;
    }
    builder.setContainerState(convertToProtoFormat(containerState));
  }

  @Override
  public int getContainerExitStatus() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  @Override
  public int getVersion() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVersion();
  }

  @Override
  public void setVersion(int version) {
    maybeInitBuilder();
    builder.setVersion(version);
  }

  @Override
  public Priority getPriority() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
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
  public long getCreationTime() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCreationTime();
  }

  @Override
  public void setCreationTime(long creationTime) {
    maybeInitBuilder();
    builder.setCreationTime(creationTime);
  }
  
  @Override
  public String getNodeLabelExpression() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNodeLabelExpression()) {
      return p.getNodeLabelExpression();
    }
    return CommonNodeLabelsManager.NO_LABEL;
  }

  @Override
  public void setNodeLabelExpression(String nodeLabelExpression) {
    maybeInitBuilder();
    if (nodeLabelExpression == null) {
      builder.clearNodeLabelExpression();
      return;
    }
    builder.setNodeLabelExpression(nodeLabelExpression);
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }

    if (this.resource != null
        && !((ResourcePBImpl) this.resource).getProto().equals(
          builder.getResource())) {
      builder.setResource(convertToProtoFormat(this.resource));
    }

    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
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
      builder = NMContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl) t).getProto();
  }

  private ContainerStateProto
      convertToProtoFormat(ContainerState containerState) {
    return ProtoUtils.convertToProtoFormat(containerState);
  }

  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }
}
