package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.impl.pb.YarnClusterMetricsPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnClusterMetricsProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterMetricsResponseProtoOrBuilder;


    
public class GetClusterMetricsResponsePBImpl extends ProtoBase<GetClusterMetricsResponseProto> implements GetClusterMetricsResponse {
  GetClusterMetricsResponseProto proto = GetClusterMetricsResponseProto.getDefaultInstance();
  GetClusterMetricsResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private YarnClusterMetrics yarnClusterMetrics = null;
  
  
  public GetClusterMetricsResponsePBImpl() {
    builder = GetClusterMetricsResponseProto.newBuilder();
  }

  public GetClusterMetricsResponsePBImpl(GetClusterMetricsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetClusterMetricsResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.yarnClusterMetrics != null) {
      builder.setClusterMetrics(convertToProtoFormat(this.yarnClusterMetrics));
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
      builder = GetClusterMetricsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public YarnClusterMetrics getClusterMetrics() {
    GetClusterMetricsResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.yarnClusterMetrics != null) {
      return this.yarnClusterMetrics;
    }
    if (!p.hasClusterMetrics()) {
      return null;
    }
    this.yarnClusterMetrics = convertFromProtoFormat(p.getClusterMetrics());
    return this.yarnClusterMetrics;
  }

  @Override
  public void setClusterMetrics(YarnClusterMetrics clusterMetrics) {
    maybeInitBuilder();
    if (clusterMetrics == null) 
      builder.clearClusterMetrics();
    this.yarnClusterMetrics = clusterMetrics;
  }

  private YarnClusterMetricsPBImpl convertFromProtoFormat(YarnClusterMetricsProto p) {
    return new YarnClusterMetricsPBImpl(p);
  }

  private YarnClusterMetricsProto convertToProtoFormat(YarnClusterMetrics t) {
    return ((YarnClusterMetricsPBImpl)t).getProto();
  }



}  
