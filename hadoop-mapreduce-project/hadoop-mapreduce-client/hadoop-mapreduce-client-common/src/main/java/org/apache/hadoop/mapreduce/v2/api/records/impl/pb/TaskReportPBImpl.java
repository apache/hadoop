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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;


    
public class TaskReportPBImpl extends ProtoBase<TaskReportProto> implements TaskReport {
  TaskReportProto proto = TaskReportProto.getDefaultInstance();
  TaskReportProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  private Counters counters = null;
  private List<TaskAttemptId> runningAttempts = null;
  private TaskAttemptId successfulAttemptId = null;
  private List<String> diagnostics = null;
  private String status;
  
  
  public TaskReportPBImpl() {
    builder = TaskReportProto.newBuilder();
  }

  public TaskReportPBImpl(TaskReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public TaskReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
    if (this.runningAttempts != null) {
      addRunningAttemptsToProto();
    }
    if (this.successfulAttemptId != null) {
      builder.setSuccessfulAttempt(convertToProtoFormat(this.successfulAttemptId));
    }
    if (this.diagnostics != null) {
      addDiagnosticsToProto();
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
      builder = TaskReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Counters getCounters() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.counters != null) {
      return this.counters;
    }
    if (!p.hasCounters()) {
      return null;
    }
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) 
      builder.clearCounters();
    this.counters = counters;
  }
  @Override
  public long getStartTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  
  @Override
  public long getFinishTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }
  
  @Override
  public TaskId getTaskId() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    this.taskId = convertFromProtoFormat(p.getTaskId());
    return this.taskId;
  }

  @Override
  public void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null) 
      builder.clearTaskId();
    this.taskId = taskId;
  }
  @Override
  public float getProgress() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }

  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public TaskState getTaskState() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskState()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskState());
  }

  @Override
  public void setTaskState(TaskState taskState) {
    maybeInitBuilder();
    if (taskState == null) {
      builder.clearTaskState();
      return;
    }
    builder.setTaskState(convertToProtoFormat(taskState));
  }
  @Override
  public List<TaskAttemptId> getRunningAttemptsList() {
    initRunningAttempts();
    return this.runningAttempts;
  }
  @Override
  public TaskAttemptId getRunningAttempt(int index) {
    initRunningAttempts();
    return this.runningAttempts.get(index);
  }
  @Override
  public int getRunningAttemptsCount() {
    initRunningAttempts();
    return this.runningAttempts.size();
  }
  
  private void initRunningAttempts() {
    if (this.runningAttempts != null) {
      return;
    }
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskAttemptIdProto> list = p.getRunningAttemptsList();
    this.runningAttempts = new ArrayList<TaskAttemptId>();

    for (TaskAttemptIdProto c : list) {
      this.runningAttempts.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllRunningAttempts(final List<TaskAttemptId> runningAttempts) {
    if (runningAttempts == null)
      return;
    initRunningAttempts();
    this.runningAttempts.addAll(runningAttempts);
  }
  
  private void addRunningAttemptsToProto() {
    maybeInitBuilder();
    builder.clearRunningAttempts();
    if (runningAttempts == null)
      return;
    Iterable<TaskAttemptIdProto> iterable = new Iterable<TaskAttemptIdProto>() {
      @Override
      public Iterator<TaskAttemptIdProto> iterator() {
        return new Iterator<TaskAttemptIdProto>() {

          Iterator<TaskAttemptId> iter = runningAttempts.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskAttemptIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllRunningAttempts(iterable);
  }
  @Override
  public void addRunningAttempt(TaskAttemptId runningAttempts) {
    initRunningAttempts();
    this.runningAttempts.add(runningAttempts);
  }
  @Override
  public void removeRunningAttempt(int index) {
    initRunningAttempts();
    this.runningAttempts.remove(index);
  }
  @Override
  public void clearRunningAttempts() {
    initRunningAttempts();
    this.runningAttempts.clear();
  }
  @Override
  public TaskAttemptId getSuccessfulAttempt() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.successfulAttemptId != null) {
      return this.successfulAttemptId;
    }
    if (!p.hasSuccessfulAttempt()) {
      return null;
    }
    this.successfulAttemptId = convertFromProtoFormat(p.getSuccessfulAttempt());
    return this.successfulAttemptId;
  }

  @Override
  public void setSuccessfulAttempt(TaskAttemptId successfulAttempt) {
    maybeInitBuilder();
    if (successfulAttempt == null) 
      builder.clearSuccessfulAttempt();
    this.successfulAttemptId = successfulAttempt;
  }
  @Override
  public List<String> getDiagnosticsList() {
    initDiagnostics();
    return this.diagnostics;
  }
  @Override
  public String getDiagnostics(int index) {
    initDiagnostics();
    return this.diagnostics.get(index);
  }
  @Override
  public int getDiagnosticsCount() {
    initDiagnostics();
    return this.diagnostics.size();
  }
  
  private void initDiagnostics() {
    if (this.diagnostics != null) {
      return;
    }
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getDiagnosticsList();
    this.diagnostics = new ArrayList<String>();

    for (String c : list) {
      this.diagnostics.add(c);
    }
  }
  
  @Override
  public void addAllDiagnostics(final List<String> diagnostics) {
    if (diagnostics == null)
      return;
    initDiagnostics();
    this.diagnostics.addAll(diagnostics);
  }
  
  private void addDiagnosticsToProto() {
    maybeInitBuilder();
    builder.clearDiagnostics();
    if (diagnostics == null) 
      return;
    builder.addAllDiagnostics(diagnostics);
  }
  @Override
  public void addDiagnostics(String diagnostics) {
    initDiagnostics();
    this.diagnostics.add(diagnostics);
  }
  @Override
  public void removeDiagnostics(int index) {
    initDiagnostics();
    this.diagnostics.remove(index);
  }
  @Override
  public void clearDiagnostics() {
    initDiagnostics();
    this.diagnostics.clear();
  }

  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }

  private TaskStateProto convertToProtoFormat(TaskState e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private TaskState convertFromProtoFormat(TaskStateProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }



}  
