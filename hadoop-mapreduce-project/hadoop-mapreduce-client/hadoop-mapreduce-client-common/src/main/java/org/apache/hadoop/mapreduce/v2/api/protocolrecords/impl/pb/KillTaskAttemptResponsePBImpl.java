package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class KillTaskAttemptResponsePBImpl extends ProtoBase<KillTaskAttemptResponseProto> implements KillTaskAttemptResponse {
  KillTaskAttemptResponseProto proto = KillTaskAttemptResponseProto.getDefaultInstance();
  KillTaskAttemptResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public KillTaskAttemptResponsePBImpl() {
    builder = KillTaskAttemptResponseProto.newBuilder();
  }

  public KillTaskAttemptResponsePBImpl(KillTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
