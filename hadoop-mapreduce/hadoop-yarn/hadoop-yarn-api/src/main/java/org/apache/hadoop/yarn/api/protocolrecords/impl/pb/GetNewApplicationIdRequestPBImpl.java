package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNewApplicationIdRequestProto;


    
public class GetNewApplicationIdRequestPBImpl extends ProtoBase<GetNewApplicationIdRequestProto> implements GetNewApplicationIdRequest {
  GetNewApplicationIdRequestProto proto = GetNewApplicationIdRequestProto.getDefaultInstance();
  GetNewApplicationIdRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public GetNewApplicationIdRequestPBImpl() {
    builder = GetNewApplicationIdRequestProto.newBuilder();
  }

  public GetNewApplicationIdRequestPBImpl(GetNewApplicationIdRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetNewApplicationIdRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetNewApplicationIdRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
