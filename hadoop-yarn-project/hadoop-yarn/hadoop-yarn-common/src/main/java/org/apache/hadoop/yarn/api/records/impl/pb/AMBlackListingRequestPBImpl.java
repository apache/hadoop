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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.AMBlackListingRequest;
import org.apache.hadoop.yarn.proto.YarnProtos.AMBlackListingRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.AMBlackListingRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AMBlackListingRequestPBImpl extends AMBlackListingRequest {
  AMBlackListingRequestProto proto = AMBlackListingRequestProto
      .getDefaultInstance();
  AMBlackListingRequestProto.Builder builder = null;
  boolean viaProto = false;

  public AMBlackListingRequestPBImpl() {
    builder = AMBlackListingRequestProto.newBuilder();
  }

  public AMBlackListingRequestPBImpl(AMBlackListingRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public AMBlackListingRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AMBlackListingRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public boolean isAMBlackListingEnabled() {
    AMBlackListingRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getBlacklistingEnabled();
  }

  @Override
  public float getBlackListingDisableFailureThreshold() {
    AMBlackListingRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getBlacklistingFailureThreshold();
  }

  @Override
  public void setBlackListingEnabled(boolean isAMBlackListingEnabled) {
    maybeInitBuilder();
    builder.setBlacklistingEnabled(isAMBlackListingEnabled);
  }

  @Override
  public void setBlackListingDisableFailureThreshold(
      float disableFailureThreshold) {
    maybeInitBuilder();
    builder.setBlacklistingFailureThreshold(disableFailureThreshold);
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
}
