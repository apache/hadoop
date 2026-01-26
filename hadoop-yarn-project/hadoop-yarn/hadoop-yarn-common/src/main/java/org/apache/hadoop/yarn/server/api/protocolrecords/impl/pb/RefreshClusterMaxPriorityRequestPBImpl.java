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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshClusterMaxPriorityRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshClusterMaxPriorityRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class RefreshClusterMaxPriorityRequestPBImpl extends RefreshClusterMaxPriorityRequest {

  RefreshClusterMaxPriorityRequestProto proto =
      RefreshClusterMaxPriorityRequestProto.getDefaultInstance();
  RefreshClusterMaxPriorityRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public RefreshClusterMaxPriorityRequestPBImpl() {
    builder = RefreshClusterMaxPriorityRequestProto.newBuilder();
  }

  public RefreshClusterMaxPriorityRequestPBImpl(
      RefreshClusterMaxPriorityRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RefreshClusterMaxPriorityRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RefreshClusterMaxPriorityRequestProto.newBuilder(proto);
    }
    viaProto = false;
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

  @Override
  public String getSubClusterId() {
    RefreshClusterMaxPriorityRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasSubClusterId()) ? p.getSubClusterId() : null;
  }

  @Override
  public void setSubClusterId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(subClusterId);
  }
}
