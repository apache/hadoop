package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.impl.pb.QueueInfoPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoResponseProtoOrBuilder;

public class GetQueueInfoResponsePBImpl extends ProtoBase<GetQueueInfoResponseProto> 
implements GetQueueInfoResponse {

  QueueInfo queueInfo;
  
  GetQueueInfoResponseProto proto = 
    GetQueueInfoResponseProto.getDefaultInstance();
  GetQueueInfoResponseProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueInfoResponsePBImpl() {
    builder = GetQueueInfoResponseProto.newBuilder();
  }
  
  public GetQueueInfoResponsePBImpl(GetQueueInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public GetQueueInfoResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public QueueInfo getQueueInfo() {
    if (this.queueInfo != null) {
      return this.queueInfo;
    }

    GetQueueInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueueInfo()) {
      return null;
    }
    this.queueInfo = convertFromProtoFormat(p.getQueueInfo());
    return this.queueInfo;
  }

  @Override
  public void setQueueInfo(QueueInfo queueInfo) {
    maybeInitBuilder();
    if(queueInfo == null) {
      builder.clearQueueInfo();
    }
    this.queueInfo = queueInfo;
  }

  private void mergeLocalToBuilder() {
    if (this.queueInfo != null) {
      builder.setQueueInfo(convertToProtoFormat(this.queueInfo));
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
      builder = GetQueueInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private QueueInfo convertFromProtoFormat(QueueInfoProto queueInfo) {
    return new QueueInfoPBImpl(queueInfo);
  }

  private QueueInfoProto convertToProtoFormat(QueueInfo queueInfo) {
    return ((QueueInfoPBImpl)queueInfo).getProto();
  }

}
