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

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetNodesToAttributesRequestProto;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Request to get hostname to attributes mapping.
 */
public class GetNodesToAttributesRequestPBImpl
    extends GetNodesToAttributesRequest {

  private GetNodesToAttributesRequestProto proto =
      GetNodesToAttributesRequestProto.getDefaultInstance();
  private GetNodesToAttributesRequestProto.Builder builder = null;

  private Set<String> hostNames = null;
  private boolean viaProto = false;

  public GetNodesToAttributesRequestPBImpl() {
    builder = GetNodesToAttributesRequestProto.newBuilder();
  }

  public GetNodesToAttributesRequestPBImpl(
      GetNodesToAttributesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetNodesToAttributesRequestProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (hostNames != null && !hostNames.isEmpty()) {
      builder.clearHostnames();
      builder.addAllHostnames(hostNames);
    }
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
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
  public void setHostNames(Set<String> hostnames) {
    maybeInitBuilder();
    if (hostNames == null) {
      builder.clearHostnames();
    }
    this.hostNames = hostnames;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          YarnServiceProtos.GetNodesToAttributesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Set<String> getHostNames() {
    initNodeToAttributes();
    return this.hostNames;
  }

  private void initNodeToAttributes() {
    if (this.hostNames != null) {
      return;
    }
    YarnServiceProtos.GetNodesToAttributesRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<String> hostNamesList = p.getHostnamesList();
    this.hostNames = new HashSet<>();
    this.hostNames.addAll(hostNamesList);
  }

}
