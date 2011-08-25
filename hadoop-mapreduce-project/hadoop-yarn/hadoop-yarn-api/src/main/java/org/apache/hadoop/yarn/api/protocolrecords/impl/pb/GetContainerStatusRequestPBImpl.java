package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusRequestProtoOrBuilder;


    
public class GetContainerStatusRequestPBImpl extends ProtoBase<GetContainerStatusRequestProto> implements GetContainerStatusRequest {
  GetContainerStatusRequestProto proto = GetContainerStatusRequestProto.getDefaultInstance();
  GetContainerStatusRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  
  
  public GetContainerStatusRequestPBImpl() {
    builder = GetContainerStatusRequestProto.newBuilder();
  }

  public GetContainerStatusRequestPBImpl(GetContainerStatusRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetContainerStatusRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
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
      builder = GetContainerStatusRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerId getContainerId() {
    GetContainerStatusRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }



}  
