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

import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeRequestProtoOrBuilder;

/**
 * Implementation of <code>ExecutionTypeRequest</code>.
 */
public class ExecutionTypeRequestPBImpl extends ExecutionTypeRequest {
  private ExecutionTypeRequestProto proto =
      ExecutionTypeRequestProto.getDefaultInstance();
  private ExecutionTypeRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public ExecutionTypeRequestPBImpl() {
    builder = ExecutionTypeRequestProto.newBuilder();
  }

  public ExecutionTypeRequestPBImpl(ExecutionTypeRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ExecutionTypeRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public ExecutionTypeRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public ExecutionType getExecutionType() {
    ExecutionTypeRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasExecutionType()) {
      return null;
    }
    return ProtoUtils.convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public void setExecutionType(ExecutionType execType) {
    maybeInitBuilder();
    if (execType == null) {
      builder.clearExecutionType();
      return;
    }
    builder.setExecutionType(ProtoUtils.convertToProtoFormat(execType));
  }

  @Override
  public void setEnforceExecutionType(boolean enforceExecutionType) {
    maybeInitBuilder();
    builder.setEnforceExecutionType(enforceExecutionType);
  }

  @Override
  public boolean getEnforceExecutionType() {
    ExecutionTypeRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getEnforceExecutionType();
  }

  @Override
  public String toString() {
    return "{Execution Type: " + getExecutionType()
        + ", Enforce Execution Type: " + getEnforceExecutionType() + "}";
  }
}
