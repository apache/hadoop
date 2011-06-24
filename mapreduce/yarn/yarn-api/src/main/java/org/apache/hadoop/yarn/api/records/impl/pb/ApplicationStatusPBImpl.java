package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStatusProtoOrBuilder;


    
public class ApplicationStatusPBImpl extends ProtoBase<ApplicationStatusProto> implements ApplicationStatus {
  ApplicationStatusProto proto = ApplicationStatusProto.getDefaultInstance();
  ApplicationStatusProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  
  
  
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
    if (this.applicationId != null && !((ApplicationIdPBImpl)this.applicationId).getProto().equals(builder.getApplicationId())) {
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
  public ApplicationId getApplicationId() {
    ApplicationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasApplicationId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getApplicationId());
    
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
    }
    this.applicationId = applicationId;
    
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

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }



}  
