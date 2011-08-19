package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetJobReportResponsePBImpl extends ProtoBase<GetJobReportResponseProto> implements GetJobReportResponse {
  GetJobReportResponseProto proto = GetJobReportResponseProto.getDefaultInstance();
  GetJobReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private JobReport jobReport = null;
  
  
  public GetJobReportResponsePBImpl() {
    builder = GetJobReportResponseProto.newBuilder();
  }

  public GetJobReportResponsePBImpl(GetJobReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetJobReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.jobReport != null) {
      builder.setJobReport(convertToProtoFormat(this.jobReport));
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
      builder = GetJobReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobReport getJobReport() {
    GetJobReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobReport != null) {
      return this.jobReport;
    }
    if (!p.hasJobReport()) {
      return null;
    }
    this.jobReport = convertFromProtoFormat(p.getJobReport());
    return this.jobReport;
  }

  @Override
  public void setJobReport(JobReport jobReport) {
    maybeInitBuilder();
    if (jobReport == null) 
      builder.clearJobReport();
    this.jobReport = jobReport;
  }

  private JobReportPBImpl convertFromProtoFormat(JobReportProto p) {
    return new JobReportPBImpl(p);
  }

  private JobReportProto convertToProtoFormat(JobReport t) {
    return ((JobReportPBImpl)t).getProto();
  }



}  
