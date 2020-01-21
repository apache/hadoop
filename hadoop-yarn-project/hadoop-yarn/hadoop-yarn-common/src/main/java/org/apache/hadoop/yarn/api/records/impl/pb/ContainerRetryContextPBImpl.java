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


import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryPolicyProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerRetryContextProtoOrBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of ContainerRetryContext.
 */
public class ContainerRetryContextPBImpl extends ContainerRetryContext {
  private ContainerRetryContextProto proto =
      ContainerRetryContextProto.getDefaultInstance();
  private ContainerRetryContextProto.Builder builder = null;
  private boolean viaProto = false;

  private Set<Integer> errorCodes = null;

  public ContainerRetryContextPBImpl() {
    builder = ContainerRetryContextProto.newBuilder();
  }

  public ContainerRetryContextPBImpl(ContainerRetryContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ContainerRetryContextProto getProto() {
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
    if (this.errorCodes != null) {
      builder.clearErrorCodes();
      builder.addAllErrorCodes(this.errorCodes);
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
      builder = ContainerRetryContextProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public ContainerRetryPolicy getRetryPolicy() {
    ContainerRetryContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRetryPolicy()) {
      return ContainerRetryPolicy.NEVER_RETRY;
    }
    return convertFromProtoFormat(p.getRetryPolicy());
  }

  public void setRetryPolicy(ContainerRetryPolicy containerRetryPolicy) {
    maybeInitBuilder();
    if (containerRetryPolicy == null) {
      builder.clearRetryPolicy();
      return;
    }
    builder.setRetryPolicy(convertToProtoFormat(containerRetryPolicy));
  }

  private void initErrorCodes() {
    if (this.errorCodes != null) {
      return;
    }
    ContainerRetryContextProtoOrBuilder p = viaProto ? proto : builder;
    this.errorCodes = new HashSet<>();
    this.errorCodes.addAll(p.getErrorCodesList());
  }

  public Set<Integer> getErrorCodes() {
    initErrorCodes();
    return this.errorCodes;
  }

  public void setErrorCodes(Set<Integer> errCodes) {
    maybeInitBuilder();
    if (errCodes == null || errCodes.isEmpty()) {
      builder.clearErrorCodes();
    }
    this.errorCodes = errCodes;
  }

  public int getMaxRetries() {
    ContainerRetryContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaxRetries()) {
      return 0;
    }
    return p.getMaxRetries();
  }

  public void setMaxRetries(int maxRetries) {
    maybeInitBuilder();
    builder.setMaxRetries(maxRetries);
  }

  public int getRetryInterval() {
    ContainerRetryContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRetryInterval()) {
      return 0;
    }
    return p.getRetryInterval();
  }

  public void setRetryInterval(int retryInterval) {
    maybeInitBuilder();
    builder.setRetryInterval(retryInterval);
  }

  @Override
  public long getFailuresValidityInterval() {
    ContainerRetryContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFailuresValidityInterval()) {
      return -1;
    }
    return p.getFailuresValidityInterval();
  }

  @Override
  public void setFailuresValidityInterval(long failuresValidityInterval) {
    maybeInitBuilder();
    builder.setFailuresValidityInterval(failuresValidityInterval);
  }

  private ContainerRetryPolicyProto convertToProtoFormat(
      ContainerRetryPolicy containerRetryPolicy) {
    return ProtoUtils.convertToProtoFormat(containerRetryPolicy);
  }

  private ContainerRetryPolicy convertFromProtoFormat(
      ContainerRetryPolicyProto containerRetryPolicyProto) {
    return ProtoUtils.convertFromProtoFormat(containerRetryPolicyProto);
  }
}
