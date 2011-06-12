package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetTaskAttemptReportResponsePBImpl extends ProtoBase<GetTaskAttemptReportResponseProto> implements GetTaskAttemptReportResponse {
  GetTaskAttemptReportResponseProto proto = GetTaskAttemptReportResponseProto.getDefaultInstance();
  GetTaskAttemptReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskAttemptReport taskAttemptReport = null;
  
  
  public GetTaskAttemptReportResponsePBImpl() {
    builder = GetTaskAttemptReportResponseProto.newBuilder();
  }

  public GetTaskAttemptReportResponsePBImpl(GetTaskAttemptReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskAttemptReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskAttemptReport != null) {
      builder.setTaskAttemptReport(convertToProtoFormat(this.taskAttemptReport));
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
      builder = GetTaskAttemptReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptReport getTaskAttemptReport() {
    GetTaskAttemptReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptReport != null) {
      return this.taskAttemptReport;
    }
    if (!p.hasTaskAttemptReport()) {
      return null;
    }
    this.taskAttemptReport =  convertFromProtoFormat(p.getTaskAttemptReport());
    return this.taskAttemptReport;
  }

  @Override
  public void setTaskAttemptReport(TaskAttemptReport taskAttemptReport) {
    maybeInitBuilder();
    if (taskAttemptReport == null) 
      builder.clearTaskAttemptReport();
    this.taskAttemptReport = taskAttemptReport;
  }

  private TaskAttemptReportPBImpl convertFromProtoFormat(TaskAttemptReportProto p) {
    return new TaskAttemptReportPBImpl(p);
  }

  private TaskAttemptReportProto convertToProtoFormat(TaskAttemptReport t) {
    return ((TaskAttemptReportPBImpl)t).getProto();
  }



}  
