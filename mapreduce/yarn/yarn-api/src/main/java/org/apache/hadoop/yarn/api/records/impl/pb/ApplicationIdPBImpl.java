package org.apache.hadoop.yarn.api.records.impl.pb;


import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProtoOrBuilder;


    
public class ApplicationIdPBImpl extends ProtoBase<ApplicationIdProto> implements ApplicationId {
  ApplicationIdProto proto = ApplicationIdProto.getDefaultInstance();
  ApplicationIdProto.Builder builder = null;
  boolean viaProto = false;
  
  public ApplicationIdPBImpl() {
    builder = ApplicationIdProto.newBuilder();
  }

  public ApplicationIdPBImpl(ApplicationIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ApplicationIdProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public int getId() {
    ApplicationIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public long getClusterTimestamp() {
    ApplicationIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getClusterTimestamp());
  }

  @Override
  public void setClusterTimestamp(long clusterTimestamp) {
    maybeInitBuilder();
    builder.setClusterTimestamp((clusterTimestamp));
  }

  @Override
  public int compareTo(ApplicationId other) {
    if (this.getId() - other.getId() == 0) {
      return this.getClusterTimestamp() > other.getClusterTimestamp() ? 1 : 
        this.getClusterTimestamp() < other.getClusterTimestamp() ? -1 : 0;
    } else {
      return this.getId() - other.getId();
    }
  }
}  
