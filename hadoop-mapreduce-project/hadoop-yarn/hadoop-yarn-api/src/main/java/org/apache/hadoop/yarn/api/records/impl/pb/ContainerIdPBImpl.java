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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProtoOrBuilder;

    
public class ContainerIdPBImpl extends ContainerId {
  ContainerIdProto proto = ContainerIdProto.getDefaultInstance();
  ContainerIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId applicationAttemptId = null;

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
    if (this.applicationAttemptId != null && !
        ((ApplicationAttemptIdPBImpl)applicationAttemptId).getProto().equals(
            builder.getAppAttemptId())) {
      builder.setAppAttemptId(convertToProtoFormat(this.applicationAttemptId));
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
  public synchronized ApplicationAttemptId getApplicationAttemptId() {
    ContainerIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    if (!p.hasAppAttemptId()) {
      return null;
    }
    this.applicationAttemptId = convertFromProtoFormat(p.getAppAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public synchronized void setApplicationAttemptId(ApplicationAttemptId atId) {
    maybeInitBuilder();
    if (atId == null) 
      builder.clearAppAttemptId();
    this.applicationAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }
}  
