package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodesRequestProto;

public class GetClusterNodesRequestPBImpl extends
    ProtoBase<GetClusterNodesRequestProto> implements GetClusterNodesRequest {

  GetClusterNodesRequestProto proto = GetClusterNodesRequestProto.getDefaultInstance();
  GetClusterNodesRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetClusterNodesRequestPBImpl() {
    builder = GetClusterNodesRequestProto.newBuilder();
  }

  public GetClusterNodesRequestPBImpl(GetClusterNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetClusterNodesRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

}
