package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder;


    
public class RegisterApplicationMasterRequestPBImpl extends ProtoBase<RegisterApplicationMasterRequestProto> implements RegisterApplicationMasterRequest {
  RegisterApplicationMasterRequestProto proto = RegisterApplicationMasterRequestProto.getDefaultInstance();
  RegisterApplicationMasterRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId applicationAttemptId = null;
  
  
  public RegisterApplicationMasterRequestPBImpl() {
    builder = RegisterApplicationMasterRequestProto.newBuilder();
  }

  public RegisterApplicationMasterRequestPBImpl(RegisterApplicationMasterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterApplicationMasterRequestProto getProto() {
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
      builder = RegisterApplicationMasterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setApplicationAttemptId(ApplicationAttemptId applicationMaster) {
    maybeInitBuilder();
    if (applicationMaster == null) 
      builder.clearApplicationAttemptId();
    this.applicationAttemptId = applicationMaster;
  }

  @Override
  public String getHost() {
    RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHost();
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    builder.setHost(host);
  }

  @Override
  public int getRpcPort() {
    RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public void setRpcPort(int port) {
    maybeInitBuilder();
    builder.setRpcPort(port);
  }

  @Override
  public String getTrackingUrl() {
    RegisterApplicationMasterRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    builder.setTrackingUrl(url);
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

}  
