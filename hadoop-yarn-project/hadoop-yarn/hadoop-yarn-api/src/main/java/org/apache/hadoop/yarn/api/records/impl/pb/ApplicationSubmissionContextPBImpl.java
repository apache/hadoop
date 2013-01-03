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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
    
public class ApplicationSubmissionContextPBImpl 
extends ProtoBase<ApplicationSubmissionContextProto> 
implements ApplicationSubmissionContext {
  ApplicationSubmissionContextProto proto = 
      ApplicationSubmissionContextProto.getDefaultInstance();
  ApplicationSubmissionContextProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationId applicationId = null;
  private Priority priority = null;
  private ContainerLaunchContext amContainer = null;
  
  public ApplicationSubmissionContextPBImpl() {
    builder = ApplicationSubmissionContextProto.newBuilder();
  }

  public ApplicationSubmissionContextPBImpl(
      ApplicationSubmissionContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ApplicationSubmissionContextProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.amContainer != null) {
      builder.setAmContainerSpec(convertToProtoFormat(this.amContainer));
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
      builder = ApplicationSubmissionContextProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Priority getPriority() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearPriority();
    this.priority = priority;
  }
  
  @Override
  public ApplicationId getApplicationId() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return applicationId;
    } // Else via proto
    if (!p.hasApplicationId()) {
      return null;
    }
    applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }
  
  @Override
  public String getApplicationName() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationName()) {
      return null;
    }
    return (p.getApplicationName());
  }

  @Override
  public void setApplicationName(String applicationName) {
    maybeInitBuilder();
    if (applicationName == null) {
      builder.clearApplicationName();
      return;
    }
    builder.setApplicationName((applicationName));
  }

  @Override
  public String getQueue() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return (p.getQueue());
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue((queue));
  }
  
  @Override
  public String getUser() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return (p.getUser());
  }

  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser((user));
  }

  @Override
  public ContainerLaunchContext getAMContainerSpec() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.amContainer != null) {
      return amContainer;
    } // Else via proto
    if (!p.hasAmContainerSpec()) {
      return null;
    }
    amContainer = convertFromProtoFormat(p.getAmContainerSpec());
    return amContainer;
  }

  @Override
  public void setAMContainerSpec(ContainerLaunchContext amContainer) {
    maybeInitBuilder();
    if (amContainer == null) {
      builder.clearAmContainerSpec();
    }
    this.amContainer = amContainer;
  }
  
  @Override
  public boolean getUnmanagedAM() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUnmanagedAm();
  }
  
  @Override
  public void setUnmanagedAM(boolean value) {
    maybeInitBuilder();
    builder.setUnmanagedAm(value);
  }
  
  @Override
  public boolean getCancelTokensWhenComplete() {
    ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
    //There is a default so cancelTokens should never be null
    return p.getCancelTokensWhenComplete();
  }
  
  @Override
  public void setCancelTokensWhenComplete(boolean cancel) {
    maybeInitBuilder();
    builder.setCancelTokensWhenComplete(cancel);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  private ContainerLaunchContextPBImpl convertFromProtoFormat(
      ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }

  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl)t).getProto();
  }
}  
