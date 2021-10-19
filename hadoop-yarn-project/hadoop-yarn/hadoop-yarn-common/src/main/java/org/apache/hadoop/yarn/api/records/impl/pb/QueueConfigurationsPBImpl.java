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

import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

public class QueueConfigurationsPBImpl extends QueueConfigurations {

  QueueConfigurationsProto proto = QueueConfigurationsProto
      .getDefaultInstance();
  QueueConfigurationsProto.Builder builder = null;
  Resource configuredMinResource = null;
  Resource configuredMaxResource = null;
  Resource effMinResource = null;
  Resource effMaxResource = null;
  boolean viaProto = false;

  public QueueConfigurationsPBImpl() {
    builder = QueueConfigurationsProto.newBuilder();
  }

  public QueueConfigurationsPBImpl(QueueConfigurationsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public QueueConfigurationsProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.effMinResource != null) {
      builder
          .setEffectiveMinCapacity(convertToProtoFormat(this.effMinResource));
    }
    if (this.effMaxResource != null) {
      builder
          .setEffectiveMaxCapacity(convertToProtoFormat(this.effMaxResource));
    }
    if (this.configuredMinResource != null) {
      builder.setEffectiveMinCapacity(
          convertToProtoFormat(this.configuredMinResource));
    }
    if (this.configuredMaxResource != null) {
      builder.setEffectiveMaxCapacity(
          convertToProtoFormat(this.configuredMaxResource));
    }
  }

  @Override
  public float getCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapacity()) ? p.getCapacity() : 0f;
  }

  @Override
  public void setCapacity(float capacity) {
    maybeInitBuilder();
    builder.setCapacity(capacity);
  }

  @Override
  public float getAbsoluteCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAbsoluteCapacity()) ? p.getAbsoluteCapacity() : 0f;
  }

  @Override
  public void setAbsoluteCapacity(float absoluteCapacity) {
    maybeInitBuilder();
    builder.setAbsoluteCapacity(absoluteCapacity);
  }

  @Override
  public float getMaxCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasMaxCapacity()) ? p.getMaxCapacity() : 0f;
  }

  @Override
  public void setMaxCapacity(float maxCapacity) {
    maybeInitBuilder();
    builder.setMaxCapacity(maxCapacity);
  }

  @Override
  public float getAbsoluteMaxCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAbsoluteMaxCapacity()) ? p.getAbsoluteMaxCapacity() : 0f;
  }

  @Override
  public void setAbsoluteMaxCapacity(float absoluteMaxCapacity) {
    maybeInitBuilder();
    builder.setAbsoluteMaxCapacity(absoluteMaxCapacity);
  }

  @Override
  public float getMaxAMPercentage() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasMaxAMPercentage()) ? p.getMaxAMPercentage() : 0f;
  }

  @Override
  public void setMaxAMPercentage(float maxAMPercentage) {
    maybeInitBuilder();
    builder.setMaxAMPercentage(maxAMPercentage);
  }

  @Override
  public Resource getEffectiveMinCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    if (this.effMinResource != null) {
      return this.effMinResource;
    }
    if (!p.hasEffectiveMinCapacity()) {
      return null;
    }
    this.effMinResource = convertFromProtoFormat(p.getEffectiveMinCapacity());
    return this.effMinResource;
  }

  @Override
  public void setEffectiveMinCapacity(Resource capacity) {
    maybeInitBuilder();
    if (capacity == null) {
      builder.clearEffectiveMinCapacity();
    }
    this.effMinResource = capacity;
  }

  @Override
  public Resource getEffectiveMaxCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    if (this.effMaxResource != null) {
      return this.effMaxResource;
    }
    if (!p.hasEffectiveMaxCapacity()) {
      return null;
    }
    this.effMaxResource = convertFromProtoFormat(p.getEffectiveMaxCapacity());
    return this.effMaxResource;
  }

  @Override
  public void setEffectiveMaxCapacity(Resource capacity) {
    maybeInitBuilder();
    if (capacity == null) {
      builder.clearEffectiveMaxCapacity();
    }
    this.effMaxResource = capacity;
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueueConfigurationsProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
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
  public Resource getConfiguredMinCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    if (this.configuredMinResource != null) {
      return this.configuredMinResource;
    }
    if (!p.hasConfiguredMinCapacity()) {
      return null;
    }
    this.configuredMinResource = convertFromProtoFormat(
        p.getConfiguredMinCapacity());
    return this.configuredMinResource;
  }

  @Override
  public void setConfiguredMinCapacity(Resource minResource) {
    maybeInitBuilder();
    if (minResource == null) {
      builder.clearConfiguredMinCapacity();
    }
    this.configuredMinResource = minResource;
  }

  @Override
  public Resource getConfiguredMaxCapacity() {
    QueueConfigurationsProtoOrBuilder p = viaProto ? proto : builder;
    if (this.configuredMaxResource != null) {
      return this.configuredMaxResource;
    }
    if (!p.hasConfiguredMaxCapacity()) {
      return null;
    }
    this.configuredMaxResource = convertFromProtoFormat(
        p.getConfiguredMaxCapacity());
    return this.configuredMaxResource;
  }

  @Override
  public void setConfiguredMaxCapacity(Resource maxResource) {
    maybeInitBuilder();
    if (configuredMaxResource == null) {
      builder.clearConfiguredMaxCapacity();
    }
    this.configuredMaxResource = maxResource;
  }
}
