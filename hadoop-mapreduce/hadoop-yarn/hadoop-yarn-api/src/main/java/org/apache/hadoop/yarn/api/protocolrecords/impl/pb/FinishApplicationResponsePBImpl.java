package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationResponseProto;


    
public class FinishApplicationResponsePBImpl extends ProtoBase<FinishApplicationResponseProto> implements FinishApplicationResponse {
  FinishApplicationResponseProto proto = FinishApplicationResponseProto.getDefaultInstance();
  FinishApplicationResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FinishApplicationResponsePBImpl() {
    builder = FinishApplicationResponseProto.newBuilder();
  }

  public FinishApplicationResponsePBImpl(FinishApplicationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FinishApplicationResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FinishApplicationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
