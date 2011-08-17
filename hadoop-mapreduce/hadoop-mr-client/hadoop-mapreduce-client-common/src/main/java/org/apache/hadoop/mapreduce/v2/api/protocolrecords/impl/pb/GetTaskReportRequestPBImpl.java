package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class GetTaskReportRequestPBImpl extends ProtoBase<GetTaskReportRequestProto> implements GetTaskReportRequest {
  GetTaskReportRequestProto proto = GetTaskReportRequestProto.getDefaultInstance();
  GetTaskReportRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  
  
  public GetTaskReportRequestPBImpl() {
    builder = GetTaskReportRequestProto.newBuilder();
  }

  public GetTaskReportRequestPBImpl(GetTaskReportRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetTaskReportRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
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
      builder = GetTaskReportRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskId getTaskId() {
    GetTaskReportRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    this.taskId =  convertFromProtoFormat(p.getTaskId());
    return this.taskId;
  }

  @Override
  public void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null) 
      builder.clearTaskId();
    this.taskId = taskId;
  }

  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }



}  
