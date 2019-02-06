/*
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
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetLocalizationStatusesRequestProtoOrBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * PB Impl of {@link GetLocalizationStatusesRequest}.
 */
@Private
@Unstable
public class GetLocalizationStatusesRequestPBImpl extends
    GetLocalizationStatusesRequest {
  private GetLocalizationStatusesRequestProto proto =
      GetLocalizationStatusesRequestProto.getDefaultInstance();
  private GetLocalizationStatusesRequestProto.Builder builder;
  private boolean viaProto = false;

  private List<ContainerId> containerIds;

  public GetLocalizationStatusesRequestPBImpl() {
    builder = GetLocalizationStatusesRequestProto.newBuilder();
  }

  public GetLocalizationStatusesRequestPBImpl(
      GetLocalizationStatusesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetLocalizationStatusesRequestProto getProto() {
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
    if (this.containerIds != null) {
      addLocalContainerIdsToProto();
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
      builder = GetLocalizationStatusesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalContainerIdsToProto() {
    maybeInitBuilder();
    builder.clearContainerId();
    if (this.containerIds == null) {
      return;
    }
    List<ContainerIdProto> protoList = new ArrayList<ContainerIdProto>();
    for (ContainerId id : containerIds) {
      protoList.add(convertToProtoFormat(id));
    }
    builder.addAllContainerId(protoList);
  }

  private void initLocalContainerIds() {
    if (this.containerIds != null) {
      return;
    }
    GetLocalizationStatusesRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> toAdd = p.getContainerIdList();
    this.containerIds = new ArrayList<>();
    for (ContainerIdProto id : toAdd) {
      this.containerIds.add(convertFromProtoFormat(id));
    }
  }

  @Override
  public List<ContainerId> getContainerIds() {
    initLocalContainerIds();
    return this.containerIds;
  }

  @Override
  public void setContainerIds(List<ContainerId> containerIds) {
    maybeInitBuilder();
    if (containerIds == null) {
      builder.clearContainerId();
    }
    this.containerIds = containerIds;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

}
