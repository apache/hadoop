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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnProtos.FederationQueueWeightProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.FederationQueueWeightProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;

@Private
@Unstable
public class FederationQueueWeightPBImpl extends FederationQueueWeight {

  private FederationQueueWeightProto proto = FederationQueueWeightProto.getDefaultInstance();
  private FederationQueueWeightProto.Builder builder = null;
  private boolean viaProto = false;

  public FederationQueueWeightPBImpl() {
    this.builder = FederationQueueWeightProto.newBuilder();
  }

  public FederationQueueWeightPBImpl(FederationQueueWeightProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (this.viaProto || this.builder == null) {
      this.builder = FederationQueueWeightProto.newBuilder(proto);
    }
    this.viaProto = false;
  }

  public FederationQueueWeightProto getProto() {
    this.proto = this.viaProto ? this.proto : this.builder.build();
    this.viaProto = true;
    return this.proto;
  }

  @Override
  public String getRouterWeight() {
    FederationQueueWeightProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasRouterWeight = p.hasRouterWeight();
    if (hasRouterWeight) {
      return p.getRouterWeight();
    }
    return null;
  }

  @Override
  public void setRouterWeight(String routerWeight) {
    maybeInitBuilder();
    if (routerWeight == null) {
      builder.clearRouterWeight();
      return;
    }
    builder.setRouterWeight(routerWeight);
  }

  @Override
  public String getAmrmWeight() {
    FederationQueueWeightProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasAmrmWeight = p.hasAmrmWeight();
    if (hasAmrmWeight) {
      return p.getAmrmWeight();
    }
    return null;
  }

  @Override
  public void setAmrmWeight(String amrmWeight) {
    maybeInitBuilder();
    if (amrmWeight == null) {
      builder.clearAmrmWeight();
      return;
    }
    builder.setAmrmWeight(amrmWeight);
  }

  @Override
  public String getHeadRoomAlpha() {
    FederationQueueWeightProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasHeadRoomAlpha = p.hasHeadRoomAlpha();
    if (hasHeadRoomAlpha) {
      return p.getHeadRoomAlpha();
    }
    return null;
  }

  @Override
  public void setHeadRoomAlpha(String headRoomAlpha) {
    maybeInitBuilder();
    if (headRoomAlpha == null) {
      builder.clearHeadRoomAlpha();
      return;
    }
    builder.setHeadRoomAlpha(headRoomAlpha);
  }

  @Override
  public String getQueue() {
    FederationQueueWeightProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasQueue = p.hasQueue();
    if (hasQueue) {
      return p.getQueue();
    }
    return null;
  }

  @Override
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  public String getPolicyManagerClassName() {
    FederationQueueWeightProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasPolicyManagerClassName = p.hasPolicyManagerClassName();
    if (hasPolicyManagerClassName) {
      return p.getPolicyManagerClassName();
    }
    return null;
  }

  @Override
  public void setPolicyManagerClassName(String policyManagerClassName) {
    maybeInitBuilder();
    if (policyManagerClassName == null) {
      builder.clearPolicyManagerClassName();
      return;
    }
    builder.setPolicyManagerClassName(policyManagerClassName);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FederationQueueWeight)) {
      return false;
    }
    FederationQueueWeightPBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }
}
