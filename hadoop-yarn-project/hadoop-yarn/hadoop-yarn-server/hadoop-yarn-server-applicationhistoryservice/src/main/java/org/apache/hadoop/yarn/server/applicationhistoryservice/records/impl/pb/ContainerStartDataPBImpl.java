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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

public class ContainerStartDataPBImpl extends ContainerStartData {

  ContainerStartDataProto proto = ContainerStartDataProto.getDefaultInstance();
  ContainerStartDataProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId;
  private Resource resource;
  private NodeId nodeId;
  private Priority priority;

  public ContainerStartDataPBImpl() {
    builder = ContainerStartDataProto.newBuilder();
  }

  public ContainerStartDataPBImpl(ContainerStartDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
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
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAllocatedResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getAllocatedResource());
    return this.resource;
  }

  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearAllocatedResource();
    }
    this.resource = resource;
  }

  @Override
  public NodeId getAssignedNode() {
    if (this.nodeId != null) {
      return this.nodeId;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAssignedNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getAssignedNodeId());
    return this.nodeId;
  }

  @Override
  public void setAssignedNode(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearAssignedNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public Priority getPriority() {
    if (this.priority != null) {
      return this.priority;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
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
  public long getStartTime() {
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  public ContainerStartDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
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
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) this.containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.resource != null) {
      builder.setAllocatedResource(convertToProtoFormat(this.resource));
    }
    if (this.nodeId != null
        && !((NodeIdPBImpl) this.nodeId).getProto().equals(
          builder.getAssignedNodeId())) {
      builder.setAssignedNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.priority != null
        && !((PriorityPBImpl) this.priority).getProto().equals(
          builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
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
      builder = ContainerStartDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdProto convertToProtoFormat(ContainerId containerId) {
    return ((ContainerIdPBImpl) containerId).getProto();
  }

  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  private ResourceProto convertToProtoFormat(Resource resource) {
    return ProtoUtils.convertToProtoFormat(resource);
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl) nodeId).getProto();
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto nodeId) {
    return new NodeIdPBImpl(nodeId);
  }

  private PriorityProto convertToProtoFormat(Priority priority) {
    return ((PriorityPBImpl) priority).getProto();
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto priority) {
    return new PriorityPBImpl(priority);
  }

}
