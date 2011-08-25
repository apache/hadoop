package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class KillTaskResponsePBImpl extends ProtoBase<KillTaskResponseProto> implements KillTaskResponse {
  KillTaskResponseProto proto = KillTaskResponseProto.getDefaultInstance();
  KillTaskResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public KillTaskResponsePBImpl() {
    builder = KillTaskResponseProto.newBuilder();
  }

  public KillTaskResponsePBImpl(KillTaskResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillTaskResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
