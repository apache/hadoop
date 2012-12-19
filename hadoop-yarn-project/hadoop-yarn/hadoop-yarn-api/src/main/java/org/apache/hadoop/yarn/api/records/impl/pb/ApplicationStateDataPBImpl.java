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

import org.apache.hadoop.yarn.api.records.ApplicationStateData;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateDataProtoOrBuilder;

public class ApplicationStateDataPBImpl 
extends ProtoBase<ApplicationStateDataProto> 
implements ApplicationStateData {
  
  ApplicationStateDataProto proto = 
            ApplicationStateDataProto.getDefaultInstance();
  ApplicationStateDataProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationSubmissionContext applicationSubmissionContext = null;
  
  public ApplicationStateDataPBImpl() {
    builder = ApplicationStateDataProto.newBuilder();
  }

  public ApplicationStateDataPBImpl(
      ApplicationStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationStateDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationSubmissionContext != null) {
      builder.setApplicationSubmissionContext(
          ((ApplicationSubmissionContextPBImpl)applicationSubmissionContext)
          .getProto());
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
      builder = ApplicationStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getSubmitTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubmitTime()) {
      return -1;
    }
    return (p.getSubmitTime());
  }

  @Override
  public void setSubmitTime(long submitTime) {
    maybeInitBuilder();
    builder.setSubmitTime(submitTime);
  }

  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(applicationSubmissionContext != null) {
      return applicationSubmissionContext;
    }
    if (!p.hasApplicationSubmissionContext()) {
      return null;
    }
    applicationSubmissionContext = 
        new ApplicationSubmissionContextPBImpl(
                                          p.getApplicationSubmissionContext());
    return applicationSubmissionContext;
  }

  @Override
  public void setApplicationSubmissionContext(
      ApplicationSubmissionContext context) {
    maybeInitBuilder();
    if (context == null) {
      builder.clearApplicationSubmissionContext();
    }
    this.applicationSubmissionContext = context;
  }

}
