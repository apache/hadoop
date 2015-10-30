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

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.PhaseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;


    
public class TaskAttemptReportPBImpl extends ProtoBase<TaskAttemptReportProto> implements TaskAttemptReport {
  TaskAttemptReportProto proto = TaskAttemptReportProto.getDefaultInstance();
  TaskAttemptReportProto.Builder builder = null;
  boolean viaProto = false;

  private TaskAttemptId taskAttemptId = null;
  private Counters counters = null;
  private org.apache.hadoop.mapreduce.Counters rawCounters = null;
  private ContainerId containerId = null;

  public TaskAttemptReportPBImpl() {
    builder = TaskAttemptReportProto.newBuilder();
  }

  public TaskAttemptReportPBImpl(TaskAttemptReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public TaskAttemptReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
    convertRawCountersToCounters();
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
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
      builder = TaskAttemptReportProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public Counters getCounters() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    convertRawCountersToCounters();
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
    if (counters == null) {
      builder.clearCounters();
    }
    this.counters = counters;
    this.rawCounters = null;
  }

  @Override
  public org.apache.hadoop.mapreduce.Counters
        getRawCounters() {
    return this.rawCounters;
  }

  @Override
  public void setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters) {
    setCounters(null);
    this.rawCounters = rCounters;
  }

  private void convertRawCountersToCounters() {
    if (this.counters == null && this.rawCounters != null) {
      this.counters = TypeConverter.toYarn(rawCounters);
      this.rawCounters = null;
    }
  }

  @Override
  public long getStartTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  @Override
  public long getFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }
  
  @Override
  public long getShuffleFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getShuffleFinishTime());
  }

  @Override
  public void setShuffleFinishTime(long time) {
    maybeInitBuilder();
    builder.setShuffleFinishTime(time);
  }

  @Override
  public long getSortFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSortFinishTime());
  }

  @Override
  public void setSortFinishTime(long time) {
    maybeInitBuilder();
    builder.setSortFinishTime(time);
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    maybeInitBuilder();
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    this.taskAttemptId = taskAttemptId;
  }
  @Override
  public TaskAttemptState getTaskAttemptState() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskAttemptState());
  }

  @Override
  public void setTaskAttemptState(TaskAttemptState taskAttemptState) {
    maybeInitBuilder();
    if (taskAttemptState == null) {
      builder.clearTaskAttemptState();
      return;
    }
    builder.setTaskAttemptState(convertToProtoFormat(taskAttemptState));
  }
  @Override
  public float getProgress() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }
  @Override
  public String getDiagnosticInfo() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticInfo()) {
      return null;
    }
    return (p.getDiagnosticInfo());
  }

  @Override
  public void setDiagnosticInfo(String diagnosticInfo) {
    maybeInitBuilder();
    if (diagnosticInfo == null) {
      builder.clearDiagnosticInfo();
      return;
    }
    builder.setDiagnosticInfo((diagnosticInfo));
  }
  @Override
  public String getStateString() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStateString()) {
      return null;
    }
    return (p.getStateString());
  }

  @Override
  public void setStateString(String stateString) {
    maybeInitBuilder();
    if (stateString == null) {
      builder.clearStateString();
      return;
    }
    builder.setStateString((stateString));
  }
  @Override
  public Phase getPhase() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPhase()) {
      return null;
    }
    return convertFromProtoFormat(p.getPhase());
  }

  @Override
  public void setPhase(Phase phase) {
    maybeInitBuilder();
    if (phase == null) {
      builder.clearPhase();
      return;
    }
    builder.setPhase(convertToProtoFormat(phase));
  }
  
  @Override
  public String getNodeManagerHost() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeManagerHost()) {
      return null;
    }
    return p.getNodeManagerHost();
  }
  
  @Override
  public void setNodeManagerHost(String nmHost) {
    maybeInitBuilder();
    if (nmHost == null) {
      builder.clearNodeManagerHost();
      return;
    }
    builder.setNodeManagerHost(nmHost);
  }
  
  @Override
  public int getNodeManagerPort() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNodeManagerPort());
  }
  
  @Override
  public void setNodeManagerPort(int nmPort) {
    maybeInitBuilder();
    builder.setNodeManagerPort(nmPort);
  }
  
  @Override
  public int getNodeManagerHttpPort() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNodeManagerHttpPort());
  }
  
  @Override
  public void setNodeManagerHttpPort(int nmHttpPort) {
    maybeInitBuilder();
    builder.setNodeManagerHttpPort(nmHttpPort);
  }
  
  @Override
  public ContainerId getContainerId() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (containerId != null) {
      return containerId;
    } // Else via proto
    if (!p.hasContainerId()) {
      return null;
    }
    containerId = convertFromProtoFormat(p.getContainerId());
    return containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

  private TaskAttemptStateProto convertToProtoFormat(TaskAttemptState e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private TaskAttemptState convertFromProtoFormat(TaskAttemptStateProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

  private PhaseProto convertToProtoFormat(Phase e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private Phase convertFromProtoFormat(PhaseProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }
}  
