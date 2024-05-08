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
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnProtos.FederationQueueWeightProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.SaveFederationQueuePolicyRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.SaveFederationQueuePolicyRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;

@Private
@Unstable
public class SaveFederationQueuePolicyRequestPBImpl extends SaveFederationQueuePolicyRequest {

  private SaveFederationQueuePolicyRequestProto proto =
      SaveFederationQueuePolicyRequestProto.getDefaultInstance();
  private SaveFederationQueuePolicyRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private FederationQueueWeight federationQueueWeight = null;

  public SaveFederationQueuePolicyRequestPBImpl() {
    builder = SaveFederationQueuePolicyRequestProto.newBuilder();
  }

  public SaveFederationQueuePolicyRequestPBImpl(SaveFederationQueuePolicyRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SaveFederationQueuePolicyRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public SaveFederationQueuePolicyRequestProto getProto() {
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
    if (!(other instanceof SaveFederationQueuePolicyRequest)) {
      return false;
    }

    SaveFederationQueuePolicyRequestPBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public FederationQueueWeight getFederationQueueWeight() {
    if (this.federationQueueWeight != null) {
      return this.federationQueueWeight;
    }
    SaveFederationQueuePolicyRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFederationQueueWeight()) {
      return null;
    }
    this.federationQueueWeight = convertFromProtoFormat(p.getFederationQueueWeight());
    return this.federationQueueWeight;
  }

  @Override
  public void setFederationQueueWeight(FederationQueueWeight pFederationQueueWeight) {
    if (pFederationQueueWeight == null) {
      throw new IllegalArgumentException("FederationQueueWeight cannot be null.");
    }
    maybeInitBuilder();
    this.federationQueueWeight = pFederationQueueWeight;
    mergeLocalToBuilder();
  }

  private void mergeLocalToBuilder() {
    if (this.federationQueueWeight != null) {
      builder.setFederationQueueWeight(convertToProtoFormat(this.federationQueueWeight));
    }
  }

  @Override
  public String getQueue() {
    SaveFederationQueuePolicyRequestProtoOrBuilder p = viaProto ? proto : builder;
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
    SaveFederationQueuePolicyRequestProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasPolicyManagerClassName = p.hasPolicyManagerClassName();
    if (hasPolicyManagerClassName) {
      return p.getPolicyManagerClassName();
    }
    return null;
  }

  @Override
  public void setPolicyManagerClassName(String className) {
    maybeInitBuilder();
    if (className == null) {
      builder.clearPolicyManagerClassName();
      return;
    }
    builder.setPolicyManagerClassName(className);
  }

  private FederationQueueWeightProto convertToProtoFormat(
      FederationQueueWeight pFederationQueueWeight) {
    return ((FederationQueueWeightPBImpl) pFederationQueueWeight).getProto();
  }

  private FederationQueueWeight convertFromProtoFormat(
      FederationQueueWeightProto federationQueueWeightProto) {
    return new FederationQueueWeightPBImpl(federationQueueWeightProto);
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
