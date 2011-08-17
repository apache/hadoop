package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsRequestProto;


    
public class GetClusterMetricsRequestPBImpl extends ProtoBase<GetClusterMetricsRequestProto> implements GetClusterMetricsRequest {
  GetClusterMetricsRequestProto proto = GetClusterMetricsRequestProto.getDefaultInstance();
  GetClusterMetricsRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public GetClusterMetricsRequestPBImpl() {
    builder = GetClusterMetricsRequestProto.newBuilder();
  }

  public GetClusterMetricsRequestPBImpl(GetClusterMetricsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetClusterMetricsRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterMetricsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
