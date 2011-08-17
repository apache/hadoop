package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProtoOrBuilder;


    
public class ApplicationStatusPBImpl extends ProtoBase<ApplicationStatusProto> implements ApplicationStatus {
  ApplicationStatusProto proto = ApplicationStatusProto.getDefaultInstance();
  ApplicationStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId applicationAttemptId = null;  
  
  
  public ApplicationStatusPBImpl() {
    builder = ApplicationStatusProto.newBuilder();
  }

  public ApplicationStatusPBImpl(ApplicationStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationStatusProto getProto() {
  
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationAttemptId != null && !((ApplicationAttemptIdPBImpl)this.applicationAttemptId).getProto().equals(builder.getApplicationAttemptId())) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
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
      builder = ApplicationStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getResponseId() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptId = convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null) 
      builder.clearApplicationAttemptId();
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public float getProgress() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }



}  
