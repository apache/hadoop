package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.FinishApplicationMasterResponseProto;


    
public class FinishApplicationMasterResponsePBImpl extends ProtoBase<FinishApplicationMasterResponseProto> implements FinishApplicationMasterResponse {
  FinishApplicationMasterResponseProto proto = FinishApplicationMasterResponseProto.getDefaultInstance();
  FinishApplicationMasterResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FinishApplicationMasterResponsePBImpl() {
    builder = FinishApplicationMasterResponseProto.newBuilder();
  }

  public FinishApplicationMasterResponsePBImpl(FinishApplicationMasterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FinishApplicationMasterResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FinishApplicationMasterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
