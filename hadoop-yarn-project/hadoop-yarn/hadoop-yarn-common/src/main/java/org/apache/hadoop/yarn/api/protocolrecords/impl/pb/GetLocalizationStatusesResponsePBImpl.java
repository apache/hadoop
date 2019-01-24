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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalizationStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerExceptionMapProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ContainerLocalizationStatusesProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.LocalizationStatusProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PB Impl of {@link GetLocalizationStatusesResponse}.
 */
@Private
@Unstable
public class GetLocalizationStatusesResponsePBImpl extends
    GetLocalizationStatusesResponse {
  private GetLocalizationStatusesResponseProto proto =
      GetLocalizationStatusesResponseProto.getDefaultInstance();
  private GetLocalizationStatusesResponseProto.Builder builder;
  private boolean viaProto = false;

  private Map<ContainerId, List<LocalizationStatus>> localizationStatuses;
  private Map<ContainerId, SerializedException> failedRequests;

  public GetLocalizationStatusesResponsePBImpl() {
    builder = GetLocalizationStatusesResponseProto.newBuilder();
  }

  public GetLocalizationStatusesResponsePBImpl(
      GetLocalizationStatusesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetLocalizationStatusesResponseProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.localizationStatuses != null) {
      addLocalStatusesToProto();
    }
    if (this.failedRequests != null) {
      addFailedRequestsToProto();
    }
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
      builder = GetLocalizationStatusesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalStatusesToProto() {
    maybeInitBuilder();
    builder.clearCntnLocalizationStatuses();
    if (this.localizationStatuses == null) {
      return;
    }
    List<ContainerLocalizationStatusesProto> protos =
        new ArrayList<ContainerLocalizationStatusesProto>();

    this.localizationStatuses.forEach((containerId, statuses) -> {
      if (statuses != null && !statuses.isEmpty()) {
        ContainerLocalizationStatusesProto.Builder clProtoBuilder =
            ContainerLocalizationStatusesProto.newBuilder();
        statuses.forEach(status -> {
          clProtoBuilder.addLocalizationStatuses(convertToProtoFormat(status));
        });
        clProtoBuilder.setContainerId(convertToProtoFormat(containerId));
        protos.add(clProtoBuilder.build());
      }
    });
    builder.addAllCntnLocalizationStatuses(protos);
  }

  private void addFailedRequestsToProto() {
    maybeInitBuilder();
    builder.clearFailedRequests();
    if (this.failedRequests == null) {
      return;
    }
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
    if (localizationStatuses != null) {
      return;
    }
    GetLocalizationStatusesResponseProtoOrBuilder p = viaProto ? proto :
        builder;
    List<ContainerLocalizationStatusesProto> protoList =
        p.getCntnLocalizationStatusesList();
    localizationStatuses = new HashMap<>();

    for (ContainerLocalizationStatusesProto clProto : protoList) {
      List<LocalizationStatusProto> lsProtos =
          clProto.getLocalizationStatusesList();

      List<LocalizationStatus> statusesPerCntn = new ArrayList<>();
      lsProtos.forEach(lsProto -> {
        statusesPerCntn.add(convertFromProtoFormat(lsProto));
      });

      localizationStatuses.put(convertFromProtoFormat(clProto.getContainerId()),
          statusesPerCntn);
    }
  }

  private void initFailedRequests() {
    if (this.failedRequests != null) {
      return;
    }
    GetLocalizationStatusesResponseProtoOrBuilder p = viaProto ? proto :
        builder;
    List<ContainerExceptionMapProto> protoList = p.getFailedRequestsList();
    this.failedRequests = new HashMap<>();
    for (ContainerExceptionMapProto ce : protoList) {
      this.failedRequests.put(convertFromProtoFormat(ce.getContainerId()),
          convertFromProtoFormat(ce.getException()));
    }
  }

  @Override
  public Map<ContainerId, List<LocalizationStatus>> getLocalizationStatuses() {
    initLocalContainerStatuses();
    return this.localizationStatuses;
  }

  @Override
  public void setLocalizationStatuses(
      Map<ContainerId, List<LocalizationStatus>> statuses) {
    maybeInitBuilder();
    if (statuses == null) {
      builder.clearCntnLocalizationStatuses();
    }
    this.localizationStatuses = statuses;
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
    if (failedRequests == null) {
      builder.clearFailedRequests();
    }
    this.failedRequests = failedRequests;
  }

  private LocalizationStatusPBImpl convertFromProtoFormat(
      LocalizationStatusProto p) {
    return new LocalizationStatusPBImpl(p);
  }

  private LocalizationStatusProto convertToProtoFormat(
      LocalizationStatus t) {
    return ((LocalizationStatusPBImpl) t).getProto();
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
