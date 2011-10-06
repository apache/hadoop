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
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class JobReportPBImpl extends ProtoBase<JobReportProto> implements JobReport {
  JobReportProto proto = JobReportProto.getDefaultInstance();
  JobReportProto.Builder builder = null;
  boolean viaProto = false;
  
  private JobId jobId = null;
  
  
  public JobReportPBImpl() {
    builder = JobReportProto.newBuilder();
  }

  public JobReportPBImpl(JobReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public JobReportProto getProto() {
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
      builder = JobReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
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
  @Override
  public JobState getJobState() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasJobState()) {
      return null;
    }
    return convertFromProtoFormat(p.getJobState());
  }

  @Override
  public void setJobState(JobState jobState) {
    maybeInitBuilder();
    if (jobState == null) {
      builder.clearJobState();
      return;
    }
    builder.setJobState(convertToProtoFormat(jobState));
  }
  @Override
  public float getMapProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMapProgress());
  }

  @Override
  public void setMapProgress(float mapProgress) {
    maybeInitBuilder();
    builder.setMapProgress((mapProgress));
  }
  @Override
  public float getReduceProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getReduceProgress());
  }

  @Override
  public void setReduceProgress(float reduceProgress) {
    maybeInitBuilder();
    builder.setReduceProgress((reduceProgress));
  }
  @Override
  public float getCleanupProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getCleanupProgress());
  }

  @Override
  public void setCleanupProgress(float cleanupProgress) {
    maybeInitBuilder();
    builder.setCleanupProgress((cleanupProgress));
  }
  @Override
  public float getSetupProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSetupProgress());
  }

  @Override
  public void setSetupProgress(float setupProgress) {
    maybeInitBuilder();
    builder.setSetupProgress((setupProgress));
  }
  @Override
  public long getStartTime() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  @Override
  public long getFinishTime() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }

  @Override
  public String getUser() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getUser());
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    builder.setUser((user));
  }

  @Override
  public String getJobName() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getJobName());
  }

  @Override
  public void setJobName(String jobName) {
    maybeInitBuilder();
    builder.setJobName((jobName));
  }

  @Override
  public String getTrackingUrl() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getTrackingUrl());
  }

  @Override
  public void setTrackingUrl(String trackingUrl) {
    maybeInitBuilder();
    builder.setTrackingUrl(trackingUrl);
  }

  @Override
  public String getDiagnostics() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }
  
  @Override
  public String getJobFile() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getJobFile();
  }

  @Override
  public void setJobFile(String jobFile) {
    maybeInitBuilder();
    builder.setJobFile(jobFile);
  }
  
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

  private JobStateProto convertToProtoFormat(JobState e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  private JobState convertFromProtoFormat(JobStateProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }



}  
