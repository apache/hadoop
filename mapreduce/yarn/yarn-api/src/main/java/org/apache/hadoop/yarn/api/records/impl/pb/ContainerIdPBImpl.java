package org.apache.hadoop.yarn.api.records.impl.pb;


import java.text.NumberFormat;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProtoOrBuilder;


    
public class ContainerIdPBImpl extends ProtoBase<ContainerIdProto> implements ContainerId {
  ContainerIdProto proto = ContainerIdProto.getDefaultInstance();
  ContainerIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private ApplicationAttemptId appAttemptId = null;
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(4);
  }
  
  protected static final NumberFormat counterFormat = NumberFormat.getInstance();
  static {
    counterFormat.setGroupingUsed(false);
    counterFormat.setMinimumIntegerDigits(6);
  }
  
  
  public ContainerIdPBImpl() {
    builder = ContainerIdProto.newBuilder();
  }

  public ContainerIdPBImpl(ContainerIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null && !((ApplicationIdPBImpl)applicationId).getProto().equals(builder.getAppId())) {
      builder.setAppId(convertToProtoFormat(this.applicationId));
    }
    if (this.appAttemptId != null && !((ApplicationAttemptIdPBImpl)appAttemptId).getProto().equals(builder.getAppAttemptId())) {
      builder.setAppAttemptId(convertToProtoFormat(this.appAttemptId));
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
      builder = ContainerIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public ApplicationId getAppId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasAppId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getAppId());
    return this.applicationId;
  }

  @Override
  public ApplicationAttemptId getAppAttemptId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appAttemptId != null) {
      return this.appAttemptId;
    }
    if (!p.hasAppAttemptId()) {
      return null;
    }
    this.appAttemptId = convertFromProtoFormat(p.getAppAttemptId());
    return this.appAttemptId;
  }

  @Override
  public void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) 
      builder.clearAppId();
    this.applicationId = appId;
  }

  @Override
  public void setAppAttemptId(ApplicationAttemptId atId) {
    maybeInitBuilder();
    if (atId == null) 
      builder.clearAppAttemptId();
    this.appAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public int compareTo(ContainerId other) {
    if (this.getAppId().compareTo(other.getAppId()) == 0) {
      return this.getId() - other.getId();
    } else {
      return this.getAppId().compareTo(other.getAppId());
    }
    
  }
  
  @Override
  public String toString() {
    String id = (this.getAppId() != null) ? this.getAppId().getClusterTimestamp() + "_" +
        idFormat.format(this.getAppId().getId()): "none";
    return "containerid_" + id + "_" + counterFormat.format(getId());
  }
}  
