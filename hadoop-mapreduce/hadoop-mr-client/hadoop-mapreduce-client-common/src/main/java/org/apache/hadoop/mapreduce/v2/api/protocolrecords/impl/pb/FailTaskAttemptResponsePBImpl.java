package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class FailTaskAttemptResponsePBImpl extends ProtoBase<FailTaskAttemptResponseProto> implements FailTaskAttemptResponse {
  FailTaskAttemptResponseProto proto = FailTaskAttemptResponseProto.getDefaultInstance();
  FailTaskAttemptResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FailTaskAttemptResponsePBImpl() {
    builder = FailTaskAttemptResponseProto.newBuilder();
  }

  public FailTaskAttemptResponsePBImpl(FailTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FailTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FailTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

}  
