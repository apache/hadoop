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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProtoOrBuilder;

    
public class ContainerIdPBImpl extends ContainerId {
  ContainerIdProto proto = ContainerIdProto.getDefaultInstance();
  ContainerIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private ApplicationAttemptId appAttemptId = null;

  public ContainerIdPBImpl() {
    builder = ContainerIdProto.newBuilder();
  }

  public ContainerIdPBImpl(ContainerIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ContainerIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.applicationId != null && !((ApplicationIdPBImpl)applicationId).getProto().equals(builder.getAppId())) {
      builder.setAppId(convertToProtoFormat(this.applicationId));
    }
    if (this.appAttemptId != null && !((ApplicationAttemptIdPBImpl)appAttemptId).getProto().equals(builder.getAppAttemptId())) {
      builder.setAppAttemptId(convertToProtoFormat(this.appAttemptId));
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
      builder = ContainerIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }
  @Override
  public synchronized ApplicationId getAppId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasAppId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getAppId());
    return this.applicationId;
  }

  @Override
  public synchronized ApplicationAttemptId getAppAttemptId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.appAttemptId != null) {
      return this.appAttemptId;
    }
    if (!p.hasAppAttemptId()) {
      return null;
    }
    this.appAttemptId = convertFromProtoFormat(p.getAppAttemptId());
    return this.appAttemptId;
  }

  @Override
  public synchronized void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) 
      builder.clearAppId();
    this.applicationId = appId;
  }

  @Override
  public synchronized void setAppAttemptId(ApplicationAttemptId atId) {
    maybeInitBuilder();
    if (atId == null) 
      builder.clearAppAttemptId();
    this.appAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}  
