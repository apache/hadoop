package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class KillJobResponsePBImpl extends ProtoBase<KillJobResponseProto> implements KillJobResponse {
  KillJobResponseProto proto = KillJobResponseProto.getDefaultInstance();
  KillJobResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public KillJobResponsePBImpl() {
    builder = KillJobResponseProto.newBuilder();
  }

  public KillJobResponsePBImpl(KillJobResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillJobResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillJobResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
