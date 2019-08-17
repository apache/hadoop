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


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.LocalizationState;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.LocalizationStateProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.LocalizationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.LocalizationStatusProtoOrBuilder;

/**
 * PB Impl of {@link LocalizationStatus}.
 */
@Private
@Unstable
public class LocalizationStatusPBImpl extends LocalizationStatus {
  private LocalizationStatusProto proto =
      LocalizationStatusProto.getDefaultInstance();
  private LocalizationStatusProto.Builder builder;
  private boolean viaProto = false;

  private String resourceKey;
  private LocalizationState localizationState;
  private String diagnostics;

  public LocalizationStatusPBImpl() {
    builder = LocalizationStatusProto.newBuilder();
  }

  public LocalizationStatusPBImpl(LocalizationStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized LocalizationStatusProto getProto() {
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
    StringBuilder sb = new StringBuilder();
    sb.append("LocalizationStatus: [")
        .append("ResourceKey: ").append(getResourceKey()).append(", ")
        .append("LocalizationState: ").append(getLocalizationState())
        .append(", ")
        .append("Diagnostics: ").append(getDiagnostics()).append(", ")
        .append("]");
    return sb.toString();
  }

  private void mergeLocalToBuilder() {
    if (resourceKey != null) {
      builder.setResourceKey(this.resourceKey);
    }
    if (localizationState != null) {
      builder.setLocalizationState(convertToProtoFormat(localizationState));
    }
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LocalizationStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized String getResourceKey() {
    LocalizationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resourceKey != null) {
      return this.resourceKey;
    }
    if (!p.hasResourceKey()) {
      return null;
    }
    this.resourceKey = p.getResourceKey();
    return this.resourceKey;
  }

  @Override
  public synchronized void setResourceKey(String resourceKey) {
    maybeInitBuilder();
    if (resourceKey == null) {
      builder.clearResourceKey();
    }
    this.resourceKey = resourceKey;
  }

  @Override
  public synchronized LocalizationState getLocalizationState() {
    LocalizationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.localizationState != null) {
      return this.localizationState;
    }
    if (!p.hasLocalizationState()) {
      return null;
    }
    this.localizationState = convertFromProtoFormat(p.getLocalizationState());
    return localizationState;
  }

  @Override
  public synchronized void setLocalizationState(
      LocalizationState localizationState) {
    maybeInitBuilder();
    if (localizationState == null) {
      builder.clearLocalizationState();
    }
    this.localizationState = localizationState;
  }

  @Override
  public synchronized String getDiagnostics() {
    LocalizationStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.diagnostics != null) {
      return this.diagnostics;
    }
    if (!p.hasDiagnostics()) {
      return null;
    }
    this.diagnostics = p.getDiagnostics();
    return diagnostics;
  }

  @Override
  public synchronized void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
    }
    this.diagnostics = diagnostics;
  }

  private LocalizationStateProto convertToProtoFormat(LocalizationState e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private LocalizationState convertFromProtoFormat(LocalizationStateProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

}
