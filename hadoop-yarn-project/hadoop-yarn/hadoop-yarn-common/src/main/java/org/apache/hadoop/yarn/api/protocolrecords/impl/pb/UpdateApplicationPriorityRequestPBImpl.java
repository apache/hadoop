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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class UpdateApplicationPriorityRequestPBImpl extends
    UpdateApplicationPriorityRequest {

  UpdateApplicationPriorityRequestProto proto =
      UpdateApplicationPriorityRequestProto
      .getDefaultInstance();
  UpdateApplicationPriorityRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId = null;
  private Priority applicationPriority = null;

  public UpdateApplicationPriorityRequestPBImpl() {
    builder = UpdateApplicationPriorityRequestProto.newBuilder();
  }

  public UpdateApplicationPriorityRequestPBImpl(
      UpdateApplicationPriorityRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateApplicationPriorityRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
      builder = UpdateApplicationPriorityRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.applicationPriority != null) {
      builder
          .setApplicationPriority(convertToProtoFormat(this.applicationPriority));
    }
  }

  @Override
  public Priority getApplicationPriority() {
    UpdateApplicationPriorityRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.applicationPriority != null) {
      return this.applicationPriority;
    }
    if (!p.hasApplicationPriority()) {
      return null;
    }
    this.applicationPriority =
        convertFromProtoFormat(p.getApplicationPriority());
    return this.applicationPriority;
  }

  @Override
  public void setApplicationPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null)
      builder.clearApplicationPriority();
    this.applicationPriority = priority;
  }

  @Override
  public ApplicationId getApplicationId() {
    UpdateApplicationPriorityRequestProtoOrBuilder p =
        viaProto ? proto : builder;
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

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl) t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
