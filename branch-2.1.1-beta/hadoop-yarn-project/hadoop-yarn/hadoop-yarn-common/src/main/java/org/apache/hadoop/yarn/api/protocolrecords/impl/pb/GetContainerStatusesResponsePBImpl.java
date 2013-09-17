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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerExceptionMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetContainerStatusesResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class GetContainerStatusesResponsePBImpl extends
    GetContainerStatusesResponse {
  GetContainerStatusesResponseProto proto = GetContainerStatusesResponseProto
    .getDefaultInstance();
  GetContainerStatusesResponseProto.Builder builder = null;
  boolean viaProto = false;

  private List<ContainerStatus> containerStatuses = null;
  private Map<ContainerId, SerializedException> failedRequests = null;

  public GetContainerStatusesResponsePBImpl() {
    builder = GetContainerStatusesResponseProto.newBuilder();
  }

  public GetContainerStatusesResponsePBImpl(
      GetContainerStatusesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetContainerStatusesResponseProto getProto() {
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
    if (this.containerStatuses != null) {
      addLocalContainerStatusesToProto();
    }
    if (this.failedRequests != null) {
      addFailedRequestsToProto();
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
      builder = GetContainerStatusesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalContainerStatusesToProto() {
    maybeInitBuilder();
    builder.clearStatus();
    if (this.containerStatuses == null)
      return;
    List<ContainerStatusProto> protoList =
        new ArrayList<ContainerStatusProto>();
    for (ContainerStatus status : containerStatuses) {
      protoList.add(convertToProtoFormat(status));
    }
    builder.addAllStatus(protoList);
  }

  private void addFailedRequestsToProto() {
    maybeInitBuilder();
    builder.clearFailedRequests();
    if (this.failedRequests == null)
      return;
    List<ContainerExceptionMapProto> protoList =
        new ArrayList<ContainerExceptionMapProto>();
    for (Map.Entry<ContainerId, SerializedException> entry : this.failedRequests
      .entrySet()) {
      protoList.add(ContainerExceptionMapProto.newBuilder()
        .setContainerId(convertToProtoFormat(entry.getKey()))
        .setException(convertToProtoFormat(entry.getValue())).build());
    }
    builder.addAllFailedRequests(protoList);
  }

  private void initLocalContainerStatuses() {
    if (this.containerStatuses != null) {
      return;
    }
    GetContainerStatusesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerStatusProto> statuses = p.getStatusList();
    this.containerStatuses = new ArrayList<ContainerStatus>();
    for (ContainerStatusProto status : statuses) {
      this.containerStatuses.add(convertFromProtoFormat(status));
    }
  }

  private void initFailedRequests() {
    if (this.failedRequests != null) {
      return;
    }
    GetContainerStatusesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerExceptionMapProto> protoList = p.getFailedRequestsList();
    this.failedRequests = new HashMap<ContainerId, SerializedException>();
    for (ContainerExceptionMapProto ce : protoList) {
      this.failedRequests.put(convertFromProtoFormat(ce.getContainerId()),
        convertFromProtoFormat(ce.getException()));
    }
  }

  @Override
  public List<ContainerStatus> getContainerStatuses() {
    initLocalContainerStatuses();
    return this.containerStatuses;
  }

  @Override
  public void setContainerStatuses(List<ContainerStatus> statuses) {
    maybeInitBuilder();
    if (statuses == null)
      builder.clearStatus();
    this.containerStatuses = statuses;
  }

  @Override
  public Map<ContainerId, SerializedException> getFailedRequests() {
    initFailedRequests();
    return this.failedRequests;
  }

  @Override
  public void setFailedRequests(
      Map<ContainerId, SerializedException> failedRequests) {
    maybeInitBuilder();
    if (failedRequests == null)
      builder.clearFailedRequests();
    this.failedRequests = failedRequests;
  }

  private ContainerStatusPBImpl convertFromProtoFormat(ContainerStatusProto p) {
    return new ContainerStatusPBImpl(p);
  }

  private ContainerStatusProto convertToProtoFormat(ContainerStatus t) {
    return ((ContainerStatusPBImpl) t).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private SerializedExceptionPBImpl convertFromProtoFormat(
      SerializedExceptionProto p) {
    return new SerializedExceptionPBImpl(p);
  }

  private SerializedExceptionProto convertToProtoFormat(SerializedException t) {
    return ((SerializedExceptionPBImpl) t).getProto();
  }
}
