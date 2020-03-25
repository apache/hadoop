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
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetQueueInfoRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class GetQueueInfoRequestPBImpl extends GetQueueInfoRequest {

  GetQueueInfoRequestProto proto = 
    GetQueueInfoRequestProto.getDefaultInstance();
  GetQueueInfoRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetQueueInfoRequestPBImpl() {
    builder = GetQueueInfoRequestProto.newBuilder();
  }
  
  public GetQueueInfoRequestPBImpl(GetQueueInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public boolean getIncludeApplications() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasIncludeApplications()) ? p.getIncludeApplications() : false;
  }

  @Override
  public boolean getIncludeChildQueues() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasIncludeChildQueues()) ? p.getIncludeChildQueues() : false;
  }

  @Override
  public String getQueueName() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasQueueName()) ? p.getQueueName() : null;
  }

  @Override
  public boolean getRecursive() {
    GetQueueInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRecursive()) ? p.getRecursive() : false;
  }

  @Override
  public void setIncludeApplications(boolean includeApplications) {
    maybeInitBuilder();
    builder.setIncludeApplications(includeApplications);
  }

  @Override
  public void setIncludeChildQueues(boolean includeChildQueues) {
    maybeInitBuilder();
    builder.setIncludeChildQueues(includeChildQueues);
  }

  @Override
  public void setQueueName(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueueName();
      return;
    }
    builder.setQueueName((queueName));
  }

  @Override
  public void setRecursive(boolean recursive) {
    maybeInitBuilder();
    builder.setRecursive(recursive);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetQueueInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public GetQueueInfoRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
