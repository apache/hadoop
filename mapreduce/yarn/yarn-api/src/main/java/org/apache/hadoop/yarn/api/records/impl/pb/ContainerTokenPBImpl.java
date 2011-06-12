package org.apache.hadoop.yarn.api.records.impl.pb;


import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTokenProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerTokenProtoOrBuilder;


    
public class ContainerTokenPBImpl extends ProtoBase<ContainerTokenProto> implements ContainerToken {
  ContainerTokenProto proto = ContainerTokenProto.getDefaultInstance();
  ContainerTokenProto.Builder builder = null;
  boolean viaProto = false;
  
  private ByteBuffer identifier;
  private ByteBuffer password;
  
  
  public ContainerTokenPBImpl() {
    builder = ContainerTokenProto.newBuilder();
  }

  public ContainerTokenPBImpl(ContainerTokenProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerTokenProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.identifier != null) {
      builder.setIdentifier(convertToProtoFormat(this.identifier));
    }
    if (this.password != null) {
      builder.setPassword(convertToProtoFormat(this.password));
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
      builder = ContainerTokenProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ByteBuffer getIdentifier() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.identifier != null) {
      return this.identifier;
    }
    if (!p.hasIdentifier()) {
      return null;
    }
    this.identifier = convertFromProtoFormat(p.getIdentifier());
    return this.identifier;
  }

  @Override
  public void setIdentifier(ByteBuffer identifier) {
    maybeInitBuilder();
    if (identifier == null) 
      builder.clearIdentifier();
    this.identifier = identifier;
  }
  @Override
  public ByteBuffer getPassword() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.password != null) {
      return this.password;
    }
    if (!p.hasPassword()) {
      return null;
    }
    this.password =  convertFromProtoFormat(p.getPassword());
    return this.password;
  }

  @Override
  public void setPassword(ByteBuffer password) {
    maybeInitBuilder();
    if (password == null) 
      builder.clearPassword();
    this.password = password;
  }
  @Override
  public String getKind() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasKind()) {
      return null;
    }
    return (p.getKind());
  }

  @Override
  public void setKind(String kind) {
    maybeInitBuilder();
    if (kind == null) {
      builder.clearKind();
      return;
    }
    builder.setKind((kind));
  }
  @Override
  public String getService() {
    ContainerTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasService()) {
      return null;
    }
    return (p.getService());
  }

  @Override
  public void setService(String service) {
    maybeInitBuilder();
    if (service == null) {
      builder.clearService();
      return;
    }
    builder.setService((service));
  }

}  
