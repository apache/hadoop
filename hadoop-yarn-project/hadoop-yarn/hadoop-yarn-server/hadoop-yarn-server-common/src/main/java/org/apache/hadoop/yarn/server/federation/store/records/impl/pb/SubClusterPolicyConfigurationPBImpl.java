/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protobuf based implementation of {@link SubClusterPolicyConfiguration}.
 *
 */
@Private
@Unstable
public class SubClusterPolicyConfigurationPBImpl
    extends SubClusterPolicyConfiguration {

  private SubClusterPolicyConfigurationProto proto =
      SubClusterPolicyConfigurationProto.getDefaultInstance();
  private SubClusterPolicyConfigurationProto.Builder builder = null;
  private boolean viaProto = false;

  public SubClusterPolicyConfigurationPBImpl() {
    builder = SubClusterPolicyConfigurationProto.newBuilder();
  }

  public SubClusterPolicyConfigurationPBImpl(
      SubClusterPolicyConfigurationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SubClusterPolicyConfigurationProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterPolicyConfigurationProto.newBuilder(proto);
    }
    viaProto = false;
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

  @Override
  public String getQueue() {
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getQueue();
  }

  @Override
  public void setQueue(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearType();
      return;
    }
    builder.setQueue(queueName);

  }

  @Override
  public String getType() {
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getType();
  }

  @Override
  public void setType(String policyType) {
    maybeInitBuilder();
    if (policyType == null) {
      builder.clearType();
      return;
    }
    builder.setType(policyType);
  }

  @Override
  public ByteBuffer getParams() {
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getParams());
  }

  @Override
  public void setParams(ByteBuffer policyParams) {
    maybeInitBuilder();
    if (policyParams == null) {
      builder.clearParams();
      return;
    }
    builder.setParams(ProtoUtils.convertToProtoFormat(policyParams));
  }

}