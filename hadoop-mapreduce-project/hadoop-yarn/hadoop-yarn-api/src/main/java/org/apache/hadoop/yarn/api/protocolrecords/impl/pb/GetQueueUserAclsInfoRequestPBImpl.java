package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueUserAclsInfoRequestProto;

public class GetQueueUserAclsInfoRequestPBImpl extends
    ProtoBase<GetQueueUserAclsInfoRequestProto> implements
    GetQueueUserAclsInfoRequest {

  GetQueueUserAclsInfoRequestProto proto = 
    GetQueueUserAclsInfoRequestProto.getDefaultInstance();
  GetQueueUserAclsInfoRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueUserAclsInfoRequestPBImpl() {
    builder = GetQueueUserAclsInfoRequestProto.newBuilder();
  }

  public GetQueueUserAclsInfoRequestPBImpl(GetQueueUserAclsInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetQueueUserAclsInfoRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

}
