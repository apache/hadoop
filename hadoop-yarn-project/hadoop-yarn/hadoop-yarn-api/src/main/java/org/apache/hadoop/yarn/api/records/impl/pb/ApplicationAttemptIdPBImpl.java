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
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

public class ApplicationAttemptIdPBImpl extends ApplicationAttemptId {
  ApplicationAttemptIdProto proto = ApplicationAttemptIdProto
      .getDefaultInstance();
  ApplicationAttemptIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;

  public ApplicationAttemptIdPBImpl() {
    builder = ApplicationAttemptIdProto.newBuilder();
  }

  public ApplicationAttemptIdPBImpl(ApplicationAttemptIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized ApplicationAttemptIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) applicationId).getProto().equals(
            builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
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
      builder = ApplicationAttemptIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getAttemptId() {
    ApplicationAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAttemptId());
  }

  @Override
  public synchronized void setAttemptId(int attemptId) {
    maybeInitBuilder();
    builder.setAttemptId((attemptId));
  }
  @Override
  public synchronized ApplicationId getApplicationId() {
    ApplicationAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public synchronized void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) 
      builder.clearApplicationId();
    this.applicationId = appId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}  
