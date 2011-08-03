package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

public class NodeReportPBImpl extends ProtoBase<NodeReportProto>
    implements NodeReport {

  private NodeReportProto proto = NodeReportProto.getDefaultInstance();
  private NodeReportProto.Builder builder = null;
  private boolean viaProto = false;
  private NodeId nodeId;
  private Resource used;
  private Resource capability;
  private NodeHealthStatus nodeHealthStatus;
  
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
  public NodeHealthStatus getNodeHealthStatus() {
    if (this.nodeHealthStatus != null) {
      return this.nodeHealthStatus;
    }

    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeHealthStatus()) {
      return null;
    }
    this.nodeHealthStatus = convertFromProtoFormat(p.getNodeHealthStatus());
    return this.nodeHealthStatus;
  }

  @Override
  public String getNodeAddress() {
    NodeReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasNodeAddress()) ? p.getNodeAddress() : null;
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
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null)
      builder.clearCapability();
    this.capability = capability;
  }

  @Override
  public void setNodeHealthStatus(NodeHealthStatus healthStatus) {
    maybeInitBuilder();
    if (healthStatus == null)
      builder.clearNodeHealthStatus();
    this.nodeHealthStatus = healthStatus;
  }

  @Override
  public void setNodeAddress(String nodeAddress) {
    maybeInitBuilder();
    if (nodeAddress == null) {
      builder.clearNodeAddress();
      return;
    }
    builder.setNodeAddress(nodeAddress);
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

  @Override
  public NodeReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
    if (this.nodeHealthStatus != null
        && !((NodeHealthStatusPBImpl) this.nodeHealthStatus).getProto().equals(
            builder.getNodeHealthStatus())) {
      builder.setNodeHealthStatus(convertToProtoFormat(this.nodeHealthStatus));
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

  private NodeHealthStatusPBImpl convertFromProtoFormat(NodeHealthStatusProto p) {
    return new NodeHealthStatusPBImpl(p);
  }

  private NodeHealthStatusProto convertToProtoFormat(NodeHealthStatus r) {
    return ((NodeHealthStatusPBImpl) r).getProto();
  }
}
