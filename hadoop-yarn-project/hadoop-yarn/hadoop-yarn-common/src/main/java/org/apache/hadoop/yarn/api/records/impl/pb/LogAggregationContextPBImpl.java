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

import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class LogAggregationContextPBImpl extends LogAggregationContext{

  LogAggregationContextProto proto = LogAggregationContextProto.getDefaultInstance();
  LogAggregationContextProto.Builder builder = null;
  boolean viaProto = false;

  public LogAggregationContextPBImpl() {
    builder = LogAggregationContextProto.newBuilder();
  }

  public LogAggregationContextPBImpl(LogAggregationContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public LogAggregationContextProto getProto() {
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
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LogAggregationContextProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public String getIncludePattern() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasIncludePattern()) {
      return null;
    }
    return p.getIncludePattern();
  }

  @Override
  public void setIncludePattern(String includePattern) {
    maybeInitBuilder();
    if (includePattern == null) {
      builder.clearIncludePattern();
      return;
    }
    builder.setIncludePattern(includePattern);
  }

  @Override
  public String getExcludePattern() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasExcludePattern()) {
      return null;
    }
    return p.getExcludePattern();
  }

  @Override
  public void setExcludePattern(String excludePattern) {
    maybeInitBuilder();
    if (excludePattern == null) {
      builder.clearExcludePattern();
      return;
    }
    builder.setExcludePattern(excludePattern);
  }

  @Override
  public String getRolledLogsIncludePattern() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasRolledLogsIncludePattern()) {
      return null;
    }
    return p.getRolledLogsIncludePattern();
  }

  @Override
  public void setRolledLogsIncludePattern(String rolledLogsIncludePattern) {
    maybeInitBuilder();
    if (rolledLogsIncludePattern == null) {
      builder.clearRolledLogsIncludePattern();
      return;
    }
    builder.setRolledLogsIncludePattern(rolledLogsIncludePattern);
  }

  @Override
  public String getRolledLogsExcludePattern() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasRolledLogsExcludePattern()) {
      return null;
    }
    return p.getRolledLogsExcludePattern();
  }

  @Override
  public void setRolledLogsExcludePattern(String rolledLogsExcludePattern) {
    maybeInitBuilder();
    if (rolledLogsExcludePattern == null) {
      builder.clearRolledLogsExcludePattern();
      return;
    }
    builder.setRolledLogsExcludePattern(rolledLogsExcludePattern);
  }

  @Override
  public String getLogAggregationPolicyClassName() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasLogAggregationPolicyClassName()) {
      return null;
    }
    return p.getLogAggregationPolicyClassName();
  }

  @Override
  public void setLogAggregationPolicyClassName(
      String className) {
    maybeInitBuilder();
    if (className == null) {
      builder.clearLogAggregationPolicyClassName();
      return;
    }
    builder.setLogAggregationPolicyClassName(className);
  }

  @Override
  public String getLogAggregationPolicyParameters() {
    LogAggregationContextProtoOrBuilder p = viaProto ? proto : builder;
    if (! p.hasLogAggregationPolicyParameters()) {
      return null;
    }
    return p.getLogAggregationPolicyParameters();
  }

  @Override
  public void setLogAggregationPolicyParameters(
      String config) {
    maybeInitBuilder();
    if (config == null) {
      builder.clearLogAggregationPolicyParameters();
      return;
    }
    builder.setLogAggregationPolicyParameters(config);
  }
}
