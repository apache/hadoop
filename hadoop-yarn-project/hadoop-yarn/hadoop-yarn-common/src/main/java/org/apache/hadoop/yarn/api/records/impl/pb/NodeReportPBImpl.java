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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class NodeReportPBImpl extends NodeReport {

  private NodeReportProto proto = NodeReportProto.getDefaultInstance();
  private NodeReportProto.Builder builder = null;
  private boolean viaProto = false;
  private NodeId nodeId;
  private Resource used;
  private Resource capability;
  Set<String> labels;

  public NodeReportPBImpl() {
    builder = NodeReportProto.newBuilder();
  }
  
  public NodeReportPBImpl(NodeReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public Resource getCapability() {
    if (this.capability != null) {
      return this.capability;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }

  @Override
  public String getHealthReport() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHealthReport();
  }
  
  @Override
  public void setHealthReport(String healthReport) {
    maybeInitBuilder();
    if (healthReport == null) {
      builder.clearHealthReport();
      return;
    }
    builder.setHealthReport(healthReport);
  }
  
  @Override
  public long getLastHealthReportTime() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHealthReportTime();
  }
  
  @Override
  public void setLastHealthReportTime(long lastHealthReportTime) {
    maybeInitBuilder();
    builder.setLastHealthReportTime(lastHealthReportTime);
  }
  
  @Override
  public String getHttpAddress() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasHttpAddress()) ? p.getHttpAddress() : null;
  }

  @Override
  public int getNumContainers() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumContainers()) ? p.getNumContainers() : 0;
  }

  @Override
  public String getRackName() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRackName()) ? p.getRackName() : null;
  }

  @Override
  public Resource getUsed() {
    if (this.used != null) {
      return this.used;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUsed()) {
      return null;
    }
    this.used = convertFromProtoFormat(p.getUsed());
    return this.used;
  }

  @Override
  public NodeId getNodeId() {
    if (this.nodeId != null) {
      return this.nodeId;
    }
    
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }
  
  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
  }
  
  @Override
  public NodeState getNodeState() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeState()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getNodeState());
  }
  
  @Override
  public void setNodeState(NodeState nodeState) {
    maybeInitBuilder();
    if (nodeState == null) {
      builder.clearNodeState();
      return;
    }
    builder.setNodeState(ProtoUtils.convertToProtoFormat(nodeState));
  }

  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null)
      builder.clearCapability();
    this.capability = capability;
  }

  @Override
  public void setHttpAddress(String httpAddress) {
    maybeInitBuilder();
    if (httpAddress == null) {
      builder.clearHttpAddress();
      return;
    }
    builder.setHttpAddress(httpAddress);
  }

  @Override
  public void setNumContainers(int numContainers) {
    maybeInitBuilder();
    if (numContainers == 0) {
      builder.clearNumContainers();
      return;
    }
    builder.setNumContainers(numContainers);
  }

  @Override
  public void setRackName(String rackName) {
    maybeInitBuilder();
    if (rackName == null) {
      builder.clearRackName();
      return;
    }
    builder.setRackName(rackName);
  }

  @Override
  public void setUsed(Resource used) {
    maybeInitBuilder();
    if (used == null)
      builder.clearUsed();
    this.used = used;
  }

  public NodeReportProto getProto() {
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
    if (this.nodeId != null
        && !((NodeIdPBImpl) this.nodeId).getProto().equals(
            builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.used != null
        && !((ResourcePBImpl) this.used).getProto().equals(
            builder.getUsed())) {
      builder.setUsed(convertToProtoFormat(this.used));
    }
    if (this.capability != null
        && !((ResourcePBImpl) this.capability).getProto().equals(
            builder.getCapability())) {
      builder.setCapability(convertToProtoFormat(this.capability));
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      builder.addAllNodeLabels(this.labels);
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
      builder = NodeReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  
  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }
  
  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl) nodeId).getProto();
  }
  
  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource r) {
    return ((ResourcePBImpl) r).getProto();
  }

  @Override
  public Set<String> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public void setNodeLabels(Set<String> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
  }
    
  private void initNodeLabels() {
    if (this.labels != null) {
      return;
    }
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    this.labels = new HashSet<String>();
    this.labels.addAll(p.getNodeLabelsList());
  }
}
