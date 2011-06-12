package org.apache.hadoop.yarn.server.api.records.impl.pb;


import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.RegistrationResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.RegistrationResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;


    
public class RegistrationResponsePBImpl extends ProtoBase<RegistrationResponseProto> implements RegistrationResponse {
  RegistrationResponseProto proto = RegistrationResponseProto.getDefaultInstance();
  RegistrationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private NodeId nodeId = null;
  private ByteBuffer secretKey = null;
  
  
  
  public RegistrationResponsePBImpl() {
    builder = RegistrationResponseProto.newBuilder();
  }

  public RegistrationResponsePBImpl(RegistrationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegistrationResponseProto getProto() {
    
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.secretKey != null) {
      builder.setSecretKey(convertToProtoFormat(this.secretKey));
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
      builder = RegistrationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public NodeId getNodeId() {
    RegistrationResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
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
  public ByteBuffer getSecretKey() {
    RegistrationResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.secretKey != null) {
      return this.secretKey;
    }
    if (!p.hasSecretKey()) {
      return null;
    }
    this.secretKey = convertFromProtoFormat(p.getSecretKey());
    return this.secretKey;
  }

  @Override
  public void setSecretKey(ByteBuffer secretKey) {
    maybeInitBuilder();
    if (secretKey == null) 
      builder.clearSecretKey();
    this.secretKey = secretKey;
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }

}  
