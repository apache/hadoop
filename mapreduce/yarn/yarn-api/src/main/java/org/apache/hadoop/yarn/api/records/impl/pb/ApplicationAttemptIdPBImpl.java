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
  
  public ApplicationAttemptIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null && !((ApplicationIdPBImpl)applicationId).getProto().equals(builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
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
      builder = ApplicationAttemptIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getAttemptId() {
    ApplicationAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAttemptId());
  }

  @Override
  public void setAttemptId(int attemptId) {
    maybeInitBuilder();
    builder.setAttemptId((attemptId));
  }
  @Override
  public ApplicationId getApplicationId() {
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
  public void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) 
      builder.clearApplicationId();
    this.applicationId = appId;
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
  public int compareTo(ApplicationAttemptId other) {
    int compareAppIds = this.getApplicationId().compareTo(
        other.getApplicationId());
    if (compareAppIds == 0) {
      return this.getAttemptId() - other.getAttemptId();
    } else {
      return compareAppIds;
    }
    
  }
  
  @Override
  public String toString() {
    String id = (this.getApplicationId() != null) ? this.getApplicationId().getClusterTimestamp() + "_" +
        idFormat.format(this.getApplicationId().getId()): "none";
    return "appattempt_" + id + "_" + counterFormat.format(getAttemptId());
  }
}  
