package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeHealthStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeHealthStatusProtoOrBuilder;

public class NodeHealthStatusPBImpl extends ProtoBase<NodeHealthStatusProto>
    implements NodeHealthStatus {

  private NodeHealthStatusProto.Builder builder;
  private boolean viaProto = false;
  private NodeHealthStatusProto proto = NodeHealthStatusProto
      .getDefaultInstance();

  public NodeHealthStatusPBImpl() {
    this.builder = NodeHealthStatusProto.newBuilder();
  }

  public NodeHealthStatusPBImpl(NodeHealthStatusProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public NodeHealthStatusProto getProto() {
    mergeLocalToProto();
    this.proto = this.viaProto ? this.proto : this.builder.build();
    this.viaProto = true;
    return this.proto;
  }

  private void mergeLocalToProto() {
    if (this.viaProto)
      maybeInitBuilder();
    this.proto = this.builder.build();

    this.viaProto = true;
  }

  private void maybeInitBuilder() {
    if (this.viaProto || this.builder == null) {
      this.builder = NodeHealthStatusProto.newBuilder(this.proto);
    }
    this.viaProto = false;
  }

  @Override
  public boolean getIsNodeHealthy() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getIsNodeHealthy();
  }

  @Override
  public void setIsNodeHealthy(boolean isNodeHealthy) {
    maybeInitBuilder();
    this.builder.setIsNodeHealthy(isNodeHealthy);
  }

  @Override
  public String getHealthReport() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    if (!p.hasHealthReport()) {
      return null;
    }
    return (p.getHealthReport());
  }

  @Override
  public void setHealthReport(String healthReport) {
    maybeInitBuilder();
    if (healthReport == null) {
      this.builder.clearHealthReport();
      return;
    }
    this.builder.setHealthReport((healthReport));
  }

  @Override
  public long getLastHealthReportTime() {
    NodeHealthStatusProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return (p.getLastHealthReportTime());
  }

  @Override
  public void setLastHealthReportTime(long lastHealthReport) {
    maybeInitBuilder();
    this.builder.setLastHealthReportTime((lastHealthReport));
  }

}
