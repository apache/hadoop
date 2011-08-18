package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetTaskReportResponsePBImpl extends ProtoBase<GetTaskReportResponseProto> implements GetTaskReportResponse {
  GetTaskReportResponseProto proto = GetTaskReportResponseProto.getDefaultInstance();
  GetTaskReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskReport taskReport = null;
  
  
  public GetTaskReportResponsePBImpl() {
    builder = GetTaskReportResponseProto.newBuilder();
  }

  public GetTaskReportResponsePBImpl(GetTaskReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskReport != null) {
      builder.setTaskReport(convertToProtoFormat(this.taskReport));
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
      builder = GetTaskReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskReport getTaskReport() {
    GetTaskReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskReport != null) {
      return this.taskReport;
    }
    if (!p.hasTaskReport()) {
      return null;
    }
    this.taskReport =  convertFromProtoFormat(p.getTaskReport());
    return this.taskReport;
  }

  @Override
  public void setTaskReport(TaskReport taskReport) {
    maybeInitBuilder();
    if (taskReport == null) 
      builder.clearTaskReport();
    this.taskReport = taskReport;
  }

  private TaskReportPBImpl convertFromProtoFormat(TaskReportProto p) {
    return new TaskReportPBImpl(p);
  }

  private TaskReportProto convertToProtoFormat(TaskReport t) {
    return ((TaskReportPBImpl)t).getProto();
  }



}  
