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

import static org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import static org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerLaunchContextResponseProto;

import org.apache.hadoop.yarn.api.protocolrecords.GetContainerLaunchContextResponse;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnServiceProtos
    .GetContainerLaunchContextResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class GetContainerLaunchContextResponsePBImpl extends GetContainerLaunchContextResponse {
  GetContainerLaunchContextResponseProto proto =
      GetContainerLaunchContextResponseProto
          .getDefaultInstance();
  GetContainerLaunchContextResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerLaunchContext containerLaunchContext = null;
  
  public GetContainerLaunchContextResponsePBImpl() {
    builder = GetContainerLaunchContextResponseProto.newBuilder();
  }
  
  public GetContainerLaunchContextResponsePBImpl(GetContainerLaunchContextResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public GetContainerLaunchContextResponseProto getProto() {
    mergeLocalToProto();
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
  
  private void mergeLocalToBuilder() {
    if (this.containerLaunchContext != null) {
      builder.setContainerLaunchContext(convertToProtoFormat(this.containerLaunchContext));
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
      builder = GetContainerLaunchContextResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public ContainerLaunchContext getContainerLaunchContext() {
    GetContainerLaunchContextResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerLaunchContext != null) {
      return this.containerLaunchContext;
    }
    if (!p.hasContainerLaunchContext()) {
      return null;
    }
    this.containerLaunchContext = convertFromProtoFormat(p.getContainerLaunchContext());
    return this.containerLaunchContext;
  }
  
  @Override
  public void setContainerLaunchContext(ContainerLaunchContext containerLaunchContext) {
    maybeInitBuilder();
    if (containerLaunchContext == null)
      builder.clearContainerLaunchContext();
    this.containerLaunchContext = containerLaunchContext;
  }
  
  private ContainerLaunchContextPBImpl convertFromProtoFormat(ContainerLaunchContextProto p) {
    return new ContainerLaunchContextPBImpl(p);
  }
  
  private ContainerLaunchContextProto convertToProtoFormat(ContainerLaunchContext t) {
    return ((ContainerLaunchContextPBImpl) t).getProto();
  }
}