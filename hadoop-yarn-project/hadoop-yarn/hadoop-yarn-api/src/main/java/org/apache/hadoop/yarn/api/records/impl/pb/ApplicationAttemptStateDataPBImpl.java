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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptStateDataProtoOrBuilder;

public class ApplicationAttemptStateDataPBImpl
extends ProtoBase<ApplicationAttemptStateDataProto> 
implements ApplicationAttemptStateData {
  
  ApplicationAttemptStateDataProto proto = 
      ApplicationAttemptStateDataProto.getDefaultInstance();
  ApplicationAttemptStateDataProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId attemptId = null;
  private Container masterContainer = null;
  
  public ApplicationAttemptStateDataPBImpl() {
    builder = ApplicationAttemptStateDataProto.newBuilder();
  }

  public ApplicationAttemptStateDataPBImpl(
      ApplicationAttemptStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationAttemptStateDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.attemptId != null) {
      builder.setAttemptId(((ApplicationAttemptIdPBImpl)attemptId).getProto());
    }
    if(this.masterContainer != null) {
      builder.setMasterContainer(((ContainerPBImpl)masterContainer).getProto());
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
      builder = ApplicationAttemptStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationAttemptId getAttemptId() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(attemptId != null) {
      return attemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    attemptId = new ApplicationAttemptIdPBImpl(p.getAttemptId());
    return attemptId;
  }

  @Override
  public void setAttemptId(ApplicationAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) {
      builder.clearAttemptId();
    }
    this.attemptId = attemptId;
  }

  @Override
  public Container getMasterContainer() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(masterContainer != null) {
      return masterContainer;
    }
    if (!p.hasMasterContainer()) {
      return null;
    }
    masterContainer = new ContainerPBImpl(p.getMasterContainer());
    return masterContainer;
  }

  @Override
  public void setMasterContainer(Container container) {
    maybeInitBuilder();
    if (container == null) {
      builder.clearMasterContainer();
    }
    this.masterContainer = container;
  }

}
