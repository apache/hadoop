package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerResponseProto;


    
public class StartContainerResponsePBImpl extends ProtoBase<StartContainerResponseProto> implements StartContainerResponse {
  StartContainerResponseProto proto = StartContainerResponseProto.getDefaultInstance();
  StartContainerResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public StartContainerResponsePBImpl() {
    builder = StartContainerResponseProto.newBuilder();
  }

  public StartContainerResponsePBImpl(StartContainerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public StartContainerResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
