package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SubmitApplicationResponseProto;


    
public class SubmitApplicationResponsePBImpl extends ProtoBase<SubmitApplicationResponseProto> implements SubmitApplicationResponse {
  SubmitApplicationResponseProto proto = SubmitApplicationResponseProto.getDefaultInstance();
  SubmitApplicationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public SubmitApplicationResponsePBImpl() {
    builder = SubmitApplicationResponseProto.newBuilder();
  }

  public SubmitApplicationResponsePBImpl(SubmitApplicationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public SubmitApplicationResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubmitApplicationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
