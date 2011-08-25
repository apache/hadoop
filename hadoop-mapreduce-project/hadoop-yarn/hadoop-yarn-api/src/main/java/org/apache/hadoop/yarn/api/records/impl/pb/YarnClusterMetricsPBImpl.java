package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnClusterMetricsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnClusterMetricsProtoOrBuilder;


    
public class YarnClusterMetricsPBImpl extends ProtoBase<YarnClusterMetricsProto> implements YarnClusterMetrics {
  YarnClusterMetricsProto proto = YarnClusterMetricsProto.getDefaultInstance();
  YarnClusterMetricsProto.Builder builder = null;
  boolean viaProto = false;
  
  public YarnClusterMetricsPBImpl() {
    builder = YarnClusterMetricsProto.newBuilder();
  }

  public YarnClusterMetricsPBImpl(YarnClusterMetricsProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public YarnClusterMetricsProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnClusterMetricsProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getNumNodeManagers() {
    YarnClusterMetricsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumNodeManagers());
  }

  @Override
  public void setNumNodeManagers(int numNodeManagers) {
    maybeInitBuilder();
    builder.setNumNodeManagers((numNodeManagers));
  }



}  
