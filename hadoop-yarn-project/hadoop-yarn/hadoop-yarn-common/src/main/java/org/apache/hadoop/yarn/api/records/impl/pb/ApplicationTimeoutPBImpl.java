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

import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * PB implementation for ApplicationTimeout class.
 */
public class ApplicationTimeoutPBImpl extends ApplicationTimeout {

  private ApplicationTimeoutProto proto =
      ApplicationTimeoutProto.getDefaultInstance();
  private ApplicationTimeoutProto.Builder builder = null;
  private boolean viaProto = false;

  public ApplicationTimeoutPBImpl() {
    builder = ApplicationTimeoutProto.newBuilder();
  }

  public ApplicationTimeoutPBImpl(ApplicationTimeoutProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ApplicationTimeoutProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationTimeoutProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationTimeoutType getTimeoutType() {
    ApplicationTimeoutProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationTimeoutType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getApplicationTimeoutType());
  }

  @Override
  public void setTimeoutType(ApplicationTimeoutType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearApplicationTimeoutType();
      return;
    }
    builder.setApplicationTimeoutType(ProtoUtils.convertToProtoFormat(type));
  }

  @Override
  public String getExpiryTime() {
    ApplicationTimeoutProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasExpireTime()) {
      return null;
    }
    return p.getExpireTime();
  }

  @Override
  public void setExpiryTime(String expiryTime) {
    maybeInitBuilder();
    if (expiryTime == null) {
      builder.clearExpireTime();
      return;
    }
    builder.setExpireTime(expiryTime);
  }

  @Override
  public long getRemainingTime() {
    ApplicationTimeoutProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRemainingTime();
  }

  @Override
  public void setRemainingTime(long remainingTime) {
    maybeInitBuilder();
    builder.setRemainingTime(remainingTime);
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
