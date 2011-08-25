package org.apache.hadoop.yarn.api.records.impl.pb;


import java.text.NumberFormat;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

public class ApplicationAttemptIdPBImpl extends ProtoBase<ApplicationAttemptIdProto> implements ApplicationAttemptId {
  ApplicationAttemptIdProto proto = ApplicationAttemptIdProto.getDefaultInstance();
  ApplicationAttemptIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
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
  
  
  public ApplicationAttemptIdPBImpl() {
    builder = ApplicationAttemptIdProto.newBuilder();
  }

  public ApplicationAttemptIdPBImpl(ApplicationAttemptIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ApplicationAttemptIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.applicationId != null && !((ApplicationIdPBImpl)applicationId).getProto().equals(builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationAttemptIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getAttemptId() {
    ApplicationAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAttemptId());
  }

  @Override
  public synchronized void setAttemptId(int attemptId) {
    maybeInitBuilder();
    builder.setAttemptId((attemptId));
  }
  @Override
  public synchronized ApplicationId getApplicationId() {
    ApplicationAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public synchronized void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) 
      builder.clearApplicationId();
    this.applicationId = appId;
  }

  private synchronized ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private synchronized ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  @Override
  public synchronized int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public synchronized boolean equals(Object other) {
    if (other == null) return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public synchronized int compareTo(ApplicationAttemptId other) {
    int compareAppIds = this.getApplicationId().compareTo(
        other.getApplicationId());
    if (compareAppIds == 0) {
      return this.getAttemptId() - other.getAttemptId();
    } else {
      return compareAppIds;
    }
    
  }
  
  @Override
  public synchronized String toString() {
    String id = (this.getApplicationId() != null) ? this.getApplicationId().getClusterTimestamp() + "_" +
        idFormat.format(this.getApplicationId().getId()): "none";
    return "appattempt_" + id + "_" + counterFormat.format(getAttemptId());
  }
}  
