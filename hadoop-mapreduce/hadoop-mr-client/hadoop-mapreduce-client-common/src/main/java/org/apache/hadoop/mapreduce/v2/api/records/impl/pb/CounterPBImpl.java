package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class CounterPBImpl extends ProtoBase<CounterProto> implements Counter {
  CounterProto proto = CounterProto.getDefaultInstance();
  CounterProto.Builder builder = null;
  boolean viaProto = false;
  
  public CounterPBImpl() {
    builder = CounterProto.newBuilder();
  }

  public CounterPBImpl(CounterProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public CounterProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CounterProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName((name));
  }
  @Override
  public long getValue() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getValue());
  }

  @Override
  public void setValue(long value) {
    maybeInitBuilder();
    builder.setValue((value));
  }
  @Override
  public String getDisplayName() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDisplayName()) {
      return null;
    }
    return (p.getDisplayName());
  }

  @Override
  public void setDisplayName(String displayName) {
    maybeInitBuilder();
    if (displayName == null) {
      builder.clearDisplayName();
      return;
    }
    builder.setDisplayName((displayName));
  }



}  
