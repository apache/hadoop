package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.CountersPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetCountersResponsePBImpl extends ProtoBase<GetCountersResponseProto> implements GetCountersResponse {
  GetCountersResponseProto proto = GetCountersResponseProto.getDefaultInstance();
  GetCountersResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private Counters counters = null;
  
  
  public GetCountersResponsePBImpl() {
    builder = GetCountersResponseProto.newBuilder();
  }

  public GetCountersResponsePBImpl(GetCountersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetCountersResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
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
      builder = GetCountersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Counters getCounters() {
    GetCountersResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.counters != null) {
      return this.counters;
    }
    if (!p.hasCounters()) {
      return null;
    }
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) 
      builder.clearCounters();
    this.counters = counters;
  }

  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }



}  
