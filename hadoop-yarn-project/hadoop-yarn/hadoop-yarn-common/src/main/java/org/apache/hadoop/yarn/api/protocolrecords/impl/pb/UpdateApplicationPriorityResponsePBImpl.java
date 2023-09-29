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

import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateApplicationPriorityResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

public class UpdateApplicationPriorityResponsePBImpl extends
    UpdateApplicationPriorityResponse {

  UpdateApplicationPriorityResponseProto proto =
      UpdateApplicationPriorityResponseProto.getDefaultInstance();
  UpdateApplicationPriorityResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Priority updatedAppPriority = null;

  public UpdateApplicationPriorityResponsePBImpl() {
    builder = UpdateApplicationPriorityResponseProto.newBuilder();
  }

  public UpdateApplicationPriorityResponsePBImpl(
      UpdateApplicationPriorityResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateApplicationPriorityResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateApplicationPriorityResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.updatedAppPriority != null) {
      builder
          .setApplicationPriority(
              convertToProtoFormat(this.updatedAppPriority));
    }
  }

  @Override
  public Priority getApplicationPriority() {
    UpdateApplicationPriorityResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.updatedAppPriority != null) {
      return this.updatedAppPriority;
    }
    if (!p.hasApplicationPriority()) {
      return null;
    }
    this.updatedAppPriority =
        convertFromProtoFormat(p.getApplicationPriority());
    return this.updatedAppPriority;
  }

  @Override
  public void setApplicationPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearApplicationPriority();
    }
    this.updatedAppPriority = priority;
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl) t).getProto();
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
