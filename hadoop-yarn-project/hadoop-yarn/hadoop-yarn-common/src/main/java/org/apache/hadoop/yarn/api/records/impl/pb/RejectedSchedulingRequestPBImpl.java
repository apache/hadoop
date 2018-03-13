/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.proto.YarnProtos;

/**
 * Implementation of RejectedSchedulingRequest.
 */
public class RejectedSchedulingRequestPBImpl extends RejectedSchedulingRequest {

  private YarnProtos.RejectedSchedulingRequestProto proto =
      YarnProtos.RejectedSchedulingRequestProto.getDefaultInstance();
  private YarnProtos.RejectedSchedulingRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private SchedulingRequest request;

  public RejectedSchedulingRequestPBImpl() {
    builder = YarnProtos.RejectedSchedulingRequestProto.newBuilder();
  }

  public RejectedSchedulingRequestPBImpl(
      YarnProtos.RejectedSchedulingRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized YarnProtos.RejectedSchedulingRequestProto getProto() {
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

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.request != null) {
      builder.setRequest(convertToProtoFormat(this.request));
    }
  }
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnProtos.RejectedSchedulingRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized RejectionReason getReason() {
    YarnProtos.RejectedSchedulingRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasReason()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getReason());
  }

  @Override
  public synchronized void setReason(RejectionReason reason) {
    maybeInitBuilder();
    if (reason == null) {
      builder.clearReason();
      return;
    }
    builder.setReason(ProtoUtils.convertToProtoFormat(reason));
  }

  @Override
  public synchronized SchedulingRequest getRequest() {
    YarnProtos.RejectedSchedulingRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.request != null) {
      return this.request;
    }
    if (!p.hasRequest()) {
      return null;
    }
    this.request = convertFromProtoFormat(p.getRequest());
    return this.request;
  }

  @Override
  public synchronized void setRequest(SchedulingRequest req) {
    maybeInitBuilder();
    if (null == req) {
      builder.clearRequest();
    }
    this.request = req;
  }

  private synchronized YarnProtos.SchedulingRequestProto convertToProtoFormat(
      SchedulingRequest r) {
    return ((SchedulingRequestPBImpl)r).getProto();
  }

  private synchronized SchedulingRequestPBImpl convertFromProtoFormat(
      YarnProtos.SchedulingRequestProto p) {
    return new SchedulingRequestPBImpl(p);
  }
}
