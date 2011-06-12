package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class KillJobRequestPBImpl extends ProtoBase<KillJobRequestProto> implements KillJobRequest {
  KillJobRequestProto proto = KillJobRequestProto.getDefaultInstance();
  KillJobRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private JobId jobId = null;
  
  
  public KillJobRequestPBImpl() {
    builder = KillJobRequestProto.newBuilder();
  }

  public KillJobRequestPBImpl(KillJobRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public KillJobRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
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
      builder = KillJobRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    KillJobRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    this.jobId = convertFromProtoFormat(p.getJobId());
    return this.jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null) 
      builder.clearJobId();
    this.jobId = jobId;
  }

  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }



}  
