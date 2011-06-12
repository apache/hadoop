package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProtoOrBuilder;


    
public class NodeIdPBImpl extends ProtoBase<NodeIdProto> implements NodeId {
  NodeIdProto proto = NodeIdProto.getDefaultInstance();
  NodeIdProto.Builder builder = null;
  boolean viaProto = false;
  
  public NodeIdPBImpl() {
    builder = NodeIdProto.newBuilder();
  }

  public NodeIdPBImpl(NodeIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public NodeIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getId() {
    NodeIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }



}  
