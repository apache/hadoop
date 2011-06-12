package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationMasterPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationMasterProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationMasterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetApplicationMasterResponseProtoOrBuilder;


    
public class GetApplicationMasterResponsePBImpl extends ProtoBase<GetApplicationMasterResponseProto> implements GetApplicationMasterResponse {
  GetApplicationMasterResponseProto proto = GetApplicationMasterResponseProto.getDefaultInstance();
  GetApplicationMasterResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationMaster applicationMaster = null;
  
  
  public GetApplicationMasterResponsePBImpl() {
    builder = GetApplicationMasterResponseProto.newBuilder();
  }

  public GetApplicationMasterResponsePBImpl(GetApplicationMasterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetApplicationMasterResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationMaster != null) {
      builder.setApplicationMaster(convertToProtoFormat(this.applicationMaster));
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
      builder = GetApplicationMasterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationMaster getApplicationMaster() {
    GetApplicationMasterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationMaster != null) {
      return this.applicationMaster;
    }
    if (!p.hasApplicationMaster()) {
      return null;
    }
    this.applicationMaster = convertFromProtoFormat(p.getApplicationMaster());
    return this.applicationMaster;
  }

  @Override
  public void setApplicationMaster(ApplicationMaster applicationMaster) {
    maybeInitBuilder();
    if (applicationMaster == null) 
      builder.clearApplicationMaster();
    this.applicationMaster = applicationMaster;
  }

  private ApplicationMasterPBImpl convertFromProtoFormat(ApplicationMasterProto p) {
    return new ApplicationMasterPBImpl(p);
  }

  private ApplicationMasterProto convertToProtoFormat(ApplicationMaster t) {
    return ((ApplicationMasterPBImpl)t).getProto();
  }



}  
