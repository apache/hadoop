package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllApplicationsRequestProto;

public class GetAllApplicationsRequestPBImpl extends
    ProtoBase<GetAllApplicationsRequestProto> implements GetAllApplicationsRequest {
  GetAllApplicationsRequestProto proto = GetAllApplicationsRequestProto.getDefaultInstance();
  GetAllApplicationsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetAllApplicationsRequestPBImpl() {
    builder = GetAllApplicationsRequestProto.newBuilder();
  }

  public GetAllApplicationsRequestPBImpl(GetAllApplicationsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetAllApplicationsRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

}
