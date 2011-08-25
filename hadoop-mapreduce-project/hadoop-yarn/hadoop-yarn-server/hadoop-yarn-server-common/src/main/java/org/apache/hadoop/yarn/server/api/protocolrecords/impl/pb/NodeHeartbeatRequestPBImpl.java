package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;


    
public class NodeHeartbeatRequestPBImpl extends ProtoBase<NodeHeartbeatRequestProto> implements NodeHeartbeatRequest {
  NodeHeartbeatRequestProto proto = NodeHeartbeatRequestProto.getDefaultInstance();
  NodeHeartbeatRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeStatus nodeStatus = null;
  
  
  public NodeHeartbeatRequestPBImpl() {
    builder = NodeHeartbeatRequestProto.newBuilder();
  }

  public NodeHeartbeatRequestPBImpl(NodeHeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeHeartbeatRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeStatus != null) {
      builder.setNodeStatus(convertToProtoFormat(this.nodeStatus));
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
      builder = NodeHeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public NodeStatus getNodeStatus() {
    NodeHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeStatus != null) {
      return this.nodeStatus;
    }
    if (!p.hasNodeStatus()) {
      return null;
    }
    this.nodeStatus = convertFromProtoFormat(p.getNodeStatus());
    return this.nodeStatus;
  }

  @Override
  public void setNodeStatus(NodeStatus nodeStatus) {
    maybeInitBuilder();
    if (nodeStatus == null) 
      builder.clearNodeStatus();
    this.nodeStatus = nodeStatus;
  }

  private NodeStatusPBImpl convertFromProtoFormat(NodeStatusProto p) {
    return new NodeStatusPBImpl(p);
  }

  private NodeStatusProto convertToProtoFormat(NodeStatus t) {
    return ((NodeStatusPBImpl)t).getProto();
  }



}  
