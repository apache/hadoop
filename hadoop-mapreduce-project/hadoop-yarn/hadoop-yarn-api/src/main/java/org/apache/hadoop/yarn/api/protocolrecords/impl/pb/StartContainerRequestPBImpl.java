package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProtoOrBuilder;


    
public class StartContainerRequestPBImpl extends ProtoBase<StartContainerRequestProto> implements StartContainerRequest {
  StartContainerRequestProto proto = StartContainerRequestProto.getDefaultInstance();
  StartContainerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerLaunchContext containerLaunchContext = null;
  
  
  public StartContainerRequestPBImpl() {
    builder = StartContainerRequestProto.newBuilder();
  }

  public StartContainerRequestPBImpl(StartContainerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public StartContainerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
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
      builder = StartContainerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerLaunchContext getContainerLaunchContext() {
    StartContainerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerLaunchContext != null) {
      return this.containerLaunchContext;
    }
    if (!p.hasContainerLaunchContext()) {
      return null;
    }
    this.containerLaunchContext = convertFromProtoFormat(p.getContainerLaunchContext());
    return this.containerLaunchContext;
  }

  @Override
  public void setContainerLaunchContext(ContainerLaunchContext containerLaunchContext) {
    maybeInitBuilder();
    if (containerLaunchContext == null) 
      builder.clearContainerLaunchContext();
    this.containerLaunchContext = containerLaunchContext;
  }

  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }



}  
