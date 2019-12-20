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

import com.google.gson.Gson;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

import com.google.protobuf.TextFormat;

import java.util.List;
import java.util.Map;

public class ContainerReportPBImpl extends ContainerReport {

  ContainerReportProto proto = ContainerReportProto.getDefaultInstance();
  ContainerReportProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId = null;
  private Resource resource = null;
  private NodeId nodeId = null;
  private Priority priority = null;

  public ContainerReportPBImpl() {
    builder = ContainerReportProto.newBuilder();
  }

  public ContainerReportPBImpl(ContainerReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public NodeId getAssignedNode() {
    if (this.nodeId != null) {
      return this.nodeId;
    }
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public String getDiagnosticsInfo() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsInfo()) {
      return null;
    }
    return (p.getDiagnosticsInfo());
  }

  @Override
  public ContainerState getContainerState() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  @Override
  public long getFinishTime() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public String getLogUrl() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLogUrl()) {
      return null;
    }
    return (p.getLogUrl());
  }

  @Override
  public Priority getPriority() {
    if (this.priority != null) {
      return this.priority;
    }
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public long getCreationTime() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCreationTime();
  }

  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  @Override
  public void setAssignedNode(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null)
      builder.clearContainerId();
    this.containerId = containerId;
  }

  @Override
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnosticsInfo();
      return;
    }
    builder.setDiagnosticsInfo(diagnosticsInfo);
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
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  @Override
  public String getExposedPorts() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExposedPorts();
  }

  @Override
  public void setExposedPorts(Map<String, List<Map<String, String>>> ports) {
    maybeInitBuilder();
    if (ports == null) {
      builder.clearExposedPorts();
      return;
    }
    Gson gson = new Gson();
    String strPorts = gson.toJson(ports);
    builder.setExposedPorts(strPorts);
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public void setLogUrl(String logUrl) {
    maybeInitBuilder();
    if (logUrl == null) {
      builder.clearLogUrl();
      return;
    }
    builder.setLogUrl(logUrl);
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearPriority();
    }
    this.priority = priority;
  }

  @Override
  public void setCreationTime(long creationTime) {
    maybeInitBuilder();
    builder.setCreationTime(creationTime);
  }

  public ContainerReportProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.nodeId != null
        && !((NodeIdPBImpl) nodeId).getProto().equals(builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.priority != null
        && !((PriorityPBImpl) this.priority).getProto().equals(
          builder.getPriority())) {
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
      builder = ContainerReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority p) {
    return ((PriorityPBImpl) p).getProto();
  }

  private ContainerStateProto
      convertToProtoFormat(ContainerState containerState) {
    return ProtoUtils.convertToProtoFormat(containerState);
  }

  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

  @Override
  public String getNodeHttpAddress() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeHttpAddress()) {
      return null;
    }
    return (p.getNodeHttpAddress());
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    maybeInitBuilder();
    if (nodeHttpAddress == null) {
      builder.clearNodeHttpAddress();
      return;
    }
    builder.setNodeHttpAddress(nodeHttpAddress);
  }

  @Override
  public ExecutionType getExecutionType() {
    ContainerReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasExecutionType()) {
      return ExecutionType.GUARANTEED;  // default value
    }
    return ProtoUtils.convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public void setExecutionType(ExecutionType executionType) {
    maybeInitBuilder();
    if (executionType == null) {
      builder.clearExecutionType();
      return;
    }
    builder.setExecutionType(ProtoUtils.convertToProtoFormat(executionType));
  }
}
