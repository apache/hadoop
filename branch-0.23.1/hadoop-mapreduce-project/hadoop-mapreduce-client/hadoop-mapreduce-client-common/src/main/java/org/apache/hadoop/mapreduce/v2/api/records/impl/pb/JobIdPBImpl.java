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
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
    
public class JobIdPBImpl extends JobId {

  JobIdProto proto = JobIdProto.getDefaultInstance();
  JobIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;

  public JobIdPBImpl() {
    builder = JobIdProto.newBuilder();
  }

  public JobIdPBImpl(JobIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized JobIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
            builder.getAppId())) {
      builder.setAppId(convertToProtoFormat(this.applicationId));
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
      builder = JobIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized ApplicationId getAppId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    if (applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasAppId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getAppId());
    return applicationId;
  }

  @Override
  public synchronized void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
    }
    this.applicationId = appId;
  }
  @Override
  public synchronized int getId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
}