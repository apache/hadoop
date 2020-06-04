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


import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;

public class GetTimelineCollectorContextRequestPBImpl extends
    GetTimelineCollectorContextRequest {

  private GetTimelineCollectorContextRequestProto
      proto = GetTimelineCollectorContextRequestProto.getDefaultInstance();
  private GetTimelineCollectorContextRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private ApplicationId appId = null;

  public GetTimelineCollectorContextRequestPBImpl() {
    builder = GetTimelineCollectorContextRequestProto.newBuilder();
  }

  public GetTimelineCollectorContextRequestPBImpl(
      GetTimelineCollectorContextRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetTimelineCollectorContextRequestProto getProto() {
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
    if (appId != null) {
      builder.setAppId(convertToProtoFormat(this.appId));
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
      builder = GetTimelineCollectorContextRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.appId != null) {
      return this.appId;
    }

    GetTimelineCollectorContextRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAppId()) {
      return null;
    }

    this.appId = convertFromProtoFormat(p.getAppId());
    return this.appId;
  }

  @Override
  public void setApplicationId(ApplicationId id) {
    maybeInitBuilder();
    if (id == null) {
      builder.clearAppId();
    }
    this.appId = id;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      YarnProtos.ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private YarnProtos.ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}
