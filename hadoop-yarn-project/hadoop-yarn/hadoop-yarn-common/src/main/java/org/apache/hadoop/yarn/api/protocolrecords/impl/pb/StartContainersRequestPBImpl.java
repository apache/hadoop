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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProtoOrBuilder;

public class StartContainersRequestPBImpl extends StartContainersRequest {
  StartContainersRequestProto proto = StartContainersRequestProto
    .getDefaultInstance();
  StartContainersRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<StartContainerRequest> requests = null;

  public StartContainersRequestPBImpl() {
    builder = StartContainersRequestProto.newBuilder();
  }

  public StartContainersRequestPBImpl(StartContainersRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public StartContainersRequestProto getProto() {
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (requests != null) {
      addLocalRequestsToProto();
    }
  }


  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainersRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalRequestsToProto() {
    maybeInitBuilder();
    builder.clearStartContainerRequest();
    List<StartContainerRequestProto> protoList =
        new ArrayList<StartContainerRequestProto>();
    for (StartContainerRequest r : this.requests) {
      protoList.add(convertToProtoFormat(r));
    }
    builder.addAllStartContainerRequest(protoList);
  }

  private void initLocalRequests() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<StartContainerRequestProto> requestList =
        p.getStartContainerRequestList();
    this.requests = new ArrayList<StartContainerRequest>();
    for (StartContainerRequestProto r : requestList) {
      this.requests.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public void setStartContainerRequests(List<StartContainerRequest> requests) {
    maybeInitBuilder();
    if (requests == null) {
      builder.clearStartContainerRequest();
    }
    this.requests = requests;
  }

  @Override
  public List<StartContainerRequest> getStartContainerRequests() {
    if (this.requests != null) {
      return this.requests;
    }
    initLocalRequests();
    return this.requests;
  }

  private StartContainerRequestPBImpl convertFromProtoFormat(
      StartContainerRequestProto p) {
    return new StartContainerRequestPBImpl(p);
  }

  private StartContainerRequestProto convertToProtoFormat(
      StartContainerRequest t) {
    return ((StartContainerRequestPBImpl) t).getProto();
  }
}
