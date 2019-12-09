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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

/**
 * Implementation of <code>UpdateContainerError</code>.
 */
public class UpdateContainerErrorPBImpl extends UpdateContainerError {
  private YarnServiceProtos.UpdateContainerErrorProto proto =
      YarnServiceProtos.UpdateContainerErrorProto.getDefaultInstance();
  private YarnServiceProtos.UpdateContainerErrorProto.Builder builder = null;
  private boolean viaProto = false;

  private String reason = null;
  private UpdateContainerRequest updateRequest = null;

  public UpdateContainerErrorPBImpl() {
    builder = YarnServiceProtos.UpdateContainerErrorProto.newBuilder();
  }

  public UpdateContainerErrorPBImpl(YarnServiceProtos
      .UpdateContainerErrorProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServiceProtos.UpdateContainerErrorProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getReason() {
    YarnServiceProtos.UpdateContainerErrorProtoOrBuilder p = viaProto ? proto
        : builder;
    if (this.reason != null) {
      return this.reason;
    }
    if (p.hasReason()) {
      this.reason = p.getReason();
    }
    return this.reason;
  }

  @Override
  public void setReason(String reason) {
    maybeInitBuilder();
    if (reason == null) {
      builder.clearReason();
    }
    this.reason = reason;
  }

  @Override
  public int getCurrentContainerVersion() {
    YarnServiceProtos.UpdateContainerErrorProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasCurrentContainerVersion()) {
      return 0;
    }
    return p.getCurrentContainerVersion();
  }

  @Override
  public void setCurrentContainerVersion(int containerVersion) {
    maybeInitBuilder();
    builder.setCurrentContainerVersion(containerVersion);
  }

  @Override
  public UpdateContainerRequest getUpdateContainerRequest() {
    YarnServiceProtos.UpdateContainerErrorProtoOrBuilder p = viaProto ? proto
        : builder;
    if (this.updateRequest != null) {
      return this.updateRequest;
    }
    if (p.hasUpdateRequest()) {
      this.updateRequest =
          ProtoUtils.convertFromProtoFormat(p.getUpdateRequest());
    }
    return this.updateRequest;
  }

  @Override
  public void setUpdateContainerRequest(
      UpdateContainerRequest updateContainerRequest) {
    maybeInitBuilder();
    if (updateContainerRequest == null) {
      builder.clearUpdateRequest();
    }
    this.updateRequest = updateContainerRequest;
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
      builder = YarnServiceProtos.UpdateContainerErrorProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.reason != null) {
      builder.setReason(this.reason);
    }
    if (this.updateRequest != null) {
      builder.setUpdateRequest(
          ProtoUtils.convertToProtoFormat(this.updateRequest));
    }
  }
}
