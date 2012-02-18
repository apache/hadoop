/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;

public class TaskIdPBImpl extends TaskId {
  TaskIdProto proto = TaskIdProto.getDefaultInstance();
  TaskIdProto.Builder builder = null;
  boolean viaProto = false;

  private JobId jobId = null;  

  public TaskIdPBImpl() {
    builder = TaskIdProto.newBuilder(proto);
  }

  public TaskIdPBImpl(TaskIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized TaskIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.jobId != null
        && !((JobIdPBImpl) this.jobId).getProto().equals(builder.getJobId())) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  @Override
  public synchronized JobId getJobId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    jobId = convertFromProtoFormat(p.getJobId());
    return jobId;
  }

  @Override
  public synchronized void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null)
      builder.clearJobId();
    this.jobId = jobId;
  }

  @Override
  public synchronized TaskType getTaskType() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskType()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskType());
  }

  @Override
  public synchronized void setTaskType(TaskType taskType) {
    maybeInitBuilder();
    if (taskType == null) {
      builder.clearTaskType();
      return;
    }
    builder.setTaskType(convertToProtoFormat(taskType));
  }

  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

  private TaskTypeProto convertToProtoFormat(TaskType e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private TaskType convertFromProtoFormat(TaskTypeProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }
}