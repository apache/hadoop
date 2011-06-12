package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProtoOrBuilder;


    
public class PriorityPBImpl extends ProtoBase<PriorityProto> implements Priority {
  PriorityProto proto = PriorityProto.getDefaultInstance();
  PriorityProto.Builder builder = null;
  boolean viaProto = false;
  
  public PriorityPBImpl() {
    builder = PriorityProto.newBuilder();
  }

  public PriorityPBImpl(PriorityProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public PriorityProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = PriorityProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getPriority() {
    PriorityProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPriority());
  }

  @Override
  public void setPriority(int priority) {
    maybeInitBuilder();
    builder.setPriority((priority));
  }

  
  @Override
  public int compareTo(Priority other) {
    return this.getPriority() - other.getPriority();
  }


}  
