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
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsProto;
import org.apache.hadoop.yarn.proto.YarnProtos.QueueConfigurationsProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class QueueConfigurationsPBImpl extends QueueConfigurations {

  QueueConfigurationsProto proto =
      QueueConfigurationsProto.getDefaultInstance();
  QueueConfigurationsProto.Builder builder = null;
  boolean viaProto = false;

  public QueueConfigurationsPBImpl() {
    builder = QueueConfigurationsProto.newBuilder();
  }

  public QueueConfigurationsPBImpl(QueueConfigurationsProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public QueueConfigurationsProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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

}
