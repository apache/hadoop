package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationMasterRequestProtoOrBuilder;


    
public class GetApplicationMasterRequestPBImpl extends ProtoBase<GetApplicationMasterRequestProto> implements GetApplicationMasterRequest {
  GetApplicationMasterRequestProto proto = GetApplicationMasterRequestProto.getDefaultInstance();
  GetApplicationMasterRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  
  
  public GetApplicationMasterRequestPBImpl() {
    builder = GetApplicationMasterRequestProto.newBuilder();
  }

  public GetApplicationMasterRequestPBImpl(GetApplicationMasterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetApplicationMasterRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (applicationId != null) {
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
      builder = GetApplicationMasterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationId getApplicationId() {
    GetApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) 
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }



}  
