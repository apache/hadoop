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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
    
public class TaskAttemptIdPBImpl extends TaskAttemptId {
  TaskAttemptIdProto proto = TaskAttemptIdProto.getDefaultInstance();
  TaskAttemptIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  
  
  
  public TaskAttemptIdPBImpl() {
    builder = TaskAttemptIdProto.newBuilder();
  }

  public TaskAttemptIdPBImpl(TaskAttemptIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized TaskAttemptIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.taskId != null
        && !((TaskIdPBImpl) this.taskId).getProto().equals(builder.getTaskId())) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
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
      builder = TaskAttemptIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getId() {
    TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public synchronized TaskId getTaskId() {
    TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    taskId = convertFromProtoFormat(p.getTaskId());
    return taskId;
  }

  @Override
  public synchronized void setTaskId(TaskId taskId) {
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