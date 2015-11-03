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
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.AMInfoProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;


    
public class JobReportPBImpl extends ProtoBase<JobReportProto> implements
    JobReport {
  JobReportProto proto = JobReportProto.getDefaultInstance();
  JobReportProto.Builder builder = null;
  boolean viaProto = false;

  private JobId jobId = null;
  private List<AMInfo> amInfos = null;
  private Priority jobPriority = null;

  public JobReportPBImpl() {
    builder = JobReportProto.newBuilder();
  }

  public JobReportPBImpl(JobReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized JobReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
    if (this.amInfos != null) {
      addAMInfosToProto();
    }
    if (this.jobPriority != null) {
      builder.setJobPriority(convertToProtoFormat(this.jobPriority));
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
      builder = JobReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized JobId getJobId() {
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
  public synchronized void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null) 
      builder.clearJobId();
    this.jobId = jobId;
  }
  @Override
  public synchronized JobState getJobState() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasJobState()) {
      return null;
    }
    return convertFromProtoFormat(p.getJobState());
  }

  @Override
  public synchronized void setJobState(JobState jobState) {
    maybeInitBuilder();
    if (jobState == null) {
      builder.clearJobState();
      return;
    }
    builder.setJobState(convertToProtoFormat(jobState));
  }
  @Override
  public synchronized float getMapProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMapProgress());
  }

  @Override
  public synchronized void setMapProgress(float mapProgress) {
    maybeInitBuilder();
    builder.setMapProgress((mapProgress));
  }
  @Override
  public synchronized float getReduceProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getReduceProgress());
  }

  @Override
  public synchronized void setReduceProgress(float reduceProgress) {
    maybeInitBuilder();
    builder.setReduceProgress((reduceProgress));
  }
  @Override
  public synchronized float getCleanupProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getCleanupProgress());
  }

  @Override
  public synchronized void setCleanupProgress(float cleanupProgress) {
    maybeInitBuilder();
    builder.setCleanupProgress((cleanupProgress));
  }
  @Override
  public synchronized float getSetupProgress() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSetupProgress());
  }

  @Override
  public synchronized void setSetupProgress(float setupProgress) {
    maybeInitBuilder();
    builder.setSetupProgress((setupProgress));
  }

  @Override
  public synchronized long getSubmitTime() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSubmitTime());
  }

  @Override
  public synchronized void setSubmitTime(long submitTime) {
    maybeInitBuilder();
    builder.setSubmitTime((submitTime));
  }

  @Override
  public synchronized long getStartTime() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public synchronized void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  @Override
  public synchronized long getFinishTime() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public synchronized void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }

  @Override
  public synchronized String getUser() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getUser());
  }

  @Override
  public synchronized void setUser(String user) {
    maybeInitBuilder();
    builder.setUser((user));
  }

  @Override
  public synchronized String getJobName() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getJobName());
  }

  @Override
  public synchronized void setJobName(String jobName) {
    maybeInitBuilder();
    builder.setJobName((jobName));
  }

  @Override
  public synchronized String getTrackingUrl() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getTrackingUrl());
  }

  @Override
  public synchronized void setTrackingUrl(String trackingUrl) {
    maybeInitBuilder();
    builder.setTrackingUrl(trackingUrl);
  }

  @Override
  public synchronized String getDiagnostics() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getDiagnostics();
  }

  @Override
  public synchronized void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    builder.setDiagnostics(diagnostics);
  }
  
  @Override
  public synchronized String getJobFile() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getJobFile();
  }

  @Override
  public synchronized void setJobFile(String jobFile) {
    maybeInitBuilder();
    builder.setJobFile(jobFile);
  }
  
  @Override
  public synchronized List<AMInfo> getAMInfos() {
    initAMInfos();
    return this.amInfos;
  }
  
  @Override
  public synchronized void setAMInfos(List<AMInfo> amInfos) {
    maybeInitBuilder();
    if (amInfos == null) {
      this.builder.clearAmInfos();
      this.amInfos = null;
      return;
    }
    initAMInfos();
    this.amInfos.clear();
    this.amInfos.addAll(amInfos);
  }
  
  
  private synchronized void initAMInfos() {
    if (this.amInfos != null) {
      return;
    }
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    List<AMInfoProto> list = p.getAmInfosList();
    
    this.amInfos = new ArrayList<AMInfo>();

    for (AMInfoProto amInfoProto : list) {
      this.amInfos.add(convertFromProtoFormat(amInfoProto));
    }
  }

  private synchronized void addAMInfosToProto() {
    maybeInitBuilder();
    builder.clearAmInfos();
    if (this.amInfos == null)
      return;
    for (AMInfo amInfo : this.amInfos) {
      builder.addAmInfos(convertToProtoFormat(amInfo));
    }
  }

  private AMInfoPBImpl convertFromProtoFormat(AMInfoProto p) {
    return new AMInfoPBImpl(p);
  }

  private AMInfoProto convertToProtoFormat(AMInfo t) {
    return ((AMInfoPBImpl)t).getProto();
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

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  @Override
  public synchronized boolean isUber() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsUber();
  }

  @Override
  public synchronized void setIsUber(boolean isUber) {
    maybeInitBuilder();
    builder.setIsUber(isUber);
  }

  @Override
  public synchronized Priority getJobPriority() {
    JobReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobPriority != null) {
      return this.jobPriority;
    }
    if (!p.hasJobPriority()) {
      return null;
    }
    this.jobPriority = convertFromProtoFormat(p.getJobPriority());
    return this.jobPriority;
  }

  @Override
  public synchronized void setJobPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearJobPriority();
    }
    this.jobPriority = priority;
  }
}
