package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.util.ProtoUtils;


    
public class ContainerStatusPBImpl extends ProtoBase<ContainerStatusProto> implements ContainerStatus {
  ContainerStatusProto proto = ContainerStatusProto.getDefaultInstance();
  ContainerStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  
  
  public ContainerStatusPBImpl() {
    builder = ContainerStatusProto.newBuilder();
  }

  public ContainerStatusPBImpl(ContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerStatusProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (containerId != null) {
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
      builder = ContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ContainerState getState() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(ContainerState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }
  @Override
  public ContainerId getContainerId() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId =  convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) 
      builder.clearContainerId();
    this.containerId = containerId;
  }
  @Override
  public String getExitStatus() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getExitStatus());
  }

  @Override
  public void setExitStatus(String exitStatus) {
    maybeInitBuilder();
    builder.setExitStatus(exitStatus);
  }

  @Override
  public String getDiagnostics() {
    ContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getDiagnostics());    
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }

  private ContainerStateProto convertToProtoFormat(ContainerState e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ContainerState convertFromProtoFormat(ContainerStateProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }



}  
