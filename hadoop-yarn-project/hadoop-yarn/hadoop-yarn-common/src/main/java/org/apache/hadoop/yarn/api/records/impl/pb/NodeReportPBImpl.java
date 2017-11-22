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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeAttributeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProto;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class NodeReportPBImpl extends NodeReport {

  private NodeReportProto proto = NodeReportProto.getDefaultInstance();
  private NodeReportProto.Builder builder = null;
  private boolean viaProto = false;
  private NodeId nodeId;
  private Resource guaranteedResourceUsed;
  private Resource opportunisticResourceUsed;
  private Resource capability;
  private ResourceUtilization containersUtilization = null;
  private ResourceUtilization nodeUtilization = null;
  Set<String> labels;
  private Set<NodeAttribute> nodeAttributes;

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
  public int getNumGuaranteedContainers() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumGuaranteedContainers()) ?
        p.getNumGuaranteedContainers() : 0;
  }

  @Override
  public int getNumOpportunisticContainers() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNumOpportunisticContainers()) ?
        p.getNumOpportunisticContainers() : 0;
  }

  @Override
  public String getRackName() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRackName()) ? p.getRackName() : null;
  }

  @Deprecated
  @Override
  public Resource getUsed() {
    return getGuaranteedResourceUsed();
  }

  @Override
  public Resource getOpportunisticResourceUsed() {
    if (this.opportunisticResourceUsed != null) {
      return this.opportunisticResourceUsed;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasOpportunisticResourceUsed()) {
      return null;
    }
    this.opportunisticResourceUsed =
        convertFromProtoFormat(p.getOpportunisticResourceUsed());
    return this.opportunisticResourceUsed;
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
    if (nodeId == null) {
      builder.clearNodeId();
    }
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
    if (capability == null) {
      builder.clearCapability();
    }
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
  public void setNumGuaranteedContainers(int numContainers) {
    maybeInitBuilder();
    if (numContainers == 0) {
      builder.clearNumGuaranteedContainers();
      return;
    }
    builder.setNumGuaranteedContainers(numContainers);
  }

  @Override
  public void setNumOpportunisticContainers(int numContainers) {
    maybeInitBuilder();
    if (numContainers == 0) {
      builder.clearNumOpportunisticContainers();
      return;
    }
    builder.setNumOpportunisticContainers(numContainers);

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

  @Deprecated
  @Override
  public void setUsed(Resource used) {
    setGuaranteedResourceUsed(used);
  }

  @Override
  public Resource getGuaranteedResourceUsed() {
    if (this.guaranteedResourceUsed != null) {
      return this.guaranteedResourceUsed;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasGuaranteedResourceUsed()) {
      return null;
    }
    this.guaranteedResourceUsed =
        convertFromProtoFormat(p.getGuaranteedResourceUsed());
    return this.guaranteedResourceUsed;
  }

  @Override
  public void setGuaranteedResourceUsed(Resource guaranteed) {
    maybeInitBuilder();
    if (guaranteedResourceUsed == null) {
      builder.clearGuaranteedResourceUsed();
    }
    this.guaranteedResourceUsed = guaranteed;
  }

  @Override
  public void setOpportunisticResourceUsed(Resource opportunisticUsed) {
    maybeInitBuilder();
    if (opportunisticUsed == null) {
      builder.clearOpportunisticResourceUsed();
    }
    this.opportunisticResourceUsed = opportunisticUsed;
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

  private void mergeLocalToBuilder() {
    if (this.nodeId != null
        && !((NodeIdPBImpl) this.nodeId).getProto().equals(
            builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.guaranteedResourceUsed != null) {
      builder.setGuaranteedResourceUsed(
          convertToProtoFormat(this.guaranteedResourceUsed));
    }
    if (this.opportunisticResourceUsed != null) {
      builder.setOpportunisticResourceUsed(
          convertToProtoFormat(this.opportunisticResourceUsed));
    }
    if (this.capability != null) {
      builder.setCapability(convertToProtoFormat(this.capability));
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      builder.addAllNodeLabels(this.labels);
    }
    if (this.nodeAttributes != null) {
      builder.clearNodeAttributes();
      List<NodeAttributeProto> attrList = new ArrayList<>();
      for (NodeAttribute attr : this.nodeAttributes) {
        attrList.add(convertToProtoFormat(attr));
      }
      builder.addAllNodeAttributes(attrList);
    }
    if (this.nodeUtilization != null
        && !((ResourceUtilizationPBImpl) this.nodeUtilization).getProto()
            .equals(builder.getNodeUtilization())) {
      builder.setNodeUtilization(convertToProtoFormat(this.nodeUtilization));
    }
    if (this.containersUtilization != null
        && !((ResourceUtilizationPBImpl) this.containersUtilization).getProto()
            .equals(builder.getContainersUtilization())) {
      builder
          .setContainersUtilization(convertToProtoFormat(
              this.containersUtilization));
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

  private NodeAttributeProto convertToProtoFormat(NodeAttribute nodeAttr) {
    return ((NodeAttributePBImpl) nodeAttr).getProto();
  }

  private NodeAttributePBImpl convertFromProtoFormat(
      NodeAttributeProto nodeAttr) {
    return new NodeAttributePBImpl(nodeAttr);
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource r) {
    return ProtoUtils.convertToProtoFormat(r);
  }

  private ResourceUtilizationPBImpl convertFromProtoFormat(
      ResourceUtilizationProto p) {
    return new ResourceUtilizationPBImpl(p);
  }

  private ResourceUtilizationProto convertToProtoFormat(ResourceUtilization r) {
    return ((ResourceUtilizationPBImpl) r).getProto();
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

  @Override
  public ResourceUtilization getAggregatedContainersUtilization() {
    if (this.containersUtilization != null) {
      return this.containersUtilization;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainersUtilization()) {
      return null;
    }
    this.containersUtilization = convertFromProtoFormat(p
        .getContainersUtilization());
    return this.containersUtilization;
  }

  @Override
  public void setAggregatedContainersUtilization(
      ResourceUtilization containersResourceUtilization) {
    maybeInitBuilder();
    if (containersResourceUtilization == null) {
      builder.clearContainersUtilization();
    }
    this.containersUtilization = containersResourceUtilization;
  }

  @Override
  public ResourceUtilization getNodeUtilization() {
    if (this.nodeUtilization != null) {
      return this.nodeUtilization;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeUtilization()) {
      return null;
    }
    this.nodeUtilization = convertFromProtoFormat(p.getNodeUtilization());
    return this.nodeUtilization;
  }

  @Override
  public void setNodeUtilization(ResourceUtilization nodeResourceUtilization) {
    maybeInitBuilder();
    if (nodeResourceUtilization == null) {
      builder.clearNodeUtilization();
    }
    this.nodeUtilization = nodeResourceUtilization;
  }

  @Override
  public Integer getDecommissioningTimeout() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasDecommissioningTimeout())
        ? p.getDecommissioningTimeout() : null;
  }

  @Override
  public void setDecommissioningTimeout(Integer decommissioningTimeout) {
    maybeInitBuilder();
    if (decommissioningTimeout == null || decommissioningTimeout < 0) {
      builder.clearDecommissioningTimeout();
      return;
    }
    builder.setDecommissioningTimeout(decommissioningTimeout);
  }

  @Override
  public NodeUpdateType getNodeUpdateType() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNodeUpdateType()) ?
        ProtoUtils.convertFromProtoFormat(p.getNodeUpdateType()) : null;
  }

  @Override
  public void setNodeUpdateType(NodeUpdateType nodeUpdateType) {
    maybeInitBuilder();
    if (nodeUpdateType == null) {
      builder.clearNodeUpdateType();
      return;
    }
    builder.setNodeUpdateType(ProtoUtils.convertToProtoFormat(nodeUpdateType));
  }

  @Override
  public void setNodeAttributes(Set<NodeAttribute> nodeAttrs) {
    maybeInitBuilder();
    builder.clearNodeAttributes();
    this.nodeAttributes = nodeAttrs;
  }

  @Override
  public Set<NodeAttribute> getNodeAttributes() {
    if (nodeAttributes != null) {
      return nodeAttributes;
    }
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    this.nodeAttributes = new HashSet<>();
    for (NodeAttributeProto nattrProto : p.getNodeAttributesList()) {
      nodeAttributes.add(convertFromProtoFormat(nattrProto));
    }
    return nodeAttributes;
  }
}
