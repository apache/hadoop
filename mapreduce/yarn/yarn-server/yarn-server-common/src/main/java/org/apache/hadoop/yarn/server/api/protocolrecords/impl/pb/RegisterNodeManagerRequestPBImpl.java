package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;


    
public class RegisterNodeManagerRequestPBImpl extends ProtoBase<RegisterNodeManagerRequestProto> implements RegisterNodeManagerRequest {
  RegisterNodeManagerRequestProto proto = RegisterNodeManagerRequestProto.getDefaultInstance();
  RegisterNodeManagerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Resource resource = null;
  private NodeId nodeId = null;
  
  public RegisterNodeManagerRequestPBImpl() {
    builder = RegisterNodeManagerRequestProto.newBuilder();
  }

  public RegisterNodeManagerRequestPBImpl(RegisterNodeManagerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterNodeManagerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
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
      builder = RegisterNodeManagerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Resource getResource() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.resource = resource;
  }

  @Override
  public NodeId getNodeId() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public int getHttpPort() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpPort()) {
      return 0;
    }
    return (p.getHttpPort());
  }

  @Override
  public void setHttpPort(int httpPort) {
    maybeInitBuilder();
    builder.setHttpPort(httpPort);
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }



}  
