package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder;

public class GetQueueInfoRequestPBImpl extends
    ProtoBase<GetQueueInfoRequestProto> implements GetQueueInfoRequest {

  GetQueueInfoRequestProto proto = 
    GetQueueInfoRequestProto.getDefaultInstance();
  GetQueueInfoRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueInfoRequestPBImpl() {
    builder = GetQueueInfoRequestProto.newBuilder();
  }
  
  public GetQueueInfoRequestPBImpl(GetQueueInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public boolean getIncludeApplications() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasIncludeApplications()) ? p.getIncludeApplications() : false;
  }

  @Override
  public boolean getIncludeChildQueues() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasIncludeChildQueues()) ? p.getIncludeChildQueues() : false;
  }

  @Override
  public String getQueueName() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public boolean getRecursive() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRecursive()) ? p.getRecursive() : false;
  }

  @Override
  public void setIncludeApplications(boolean includeApplications) {
    maybeInitBuilder();
    builder.setIncludeApplications(includeApplications);
  }

  @Override
  public void setIncludeChildQueues(boolean includeChildQueues) {
    maybeInitBuilder();
    builder.setIncludeChildQueues(includeChildQueues);
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName((queueName));
  }

  @Override
  public void setRecursive(boolean recursive) {
    maybeInitBuilder();
    builder.setRecursive(recursive);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetQueueInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public GetQueueInfoRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

}
