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
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.BatchSaveFederationQueuePoliciesRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.BatchSaveFederationQueuePoliciesRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;

import java.util.ArrayList;
import java.util.List;

/**
 * The class is responsible for batch-saving queue policies requests.
 */
@Private
@Unstable
public class BatchSaveFederationQueuePoliciesRequestPBImpl
    extends BatchSaveFederationQueuePoliciesRequest {

  private BatchSaveFederationQueuePoliciesRequestProto proto =
      BatchSaveFederationQueuePoliciesRequestProto.getDefaultInstance();
  private BatchSaveFederationQueuePoliciesRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private List<FederationQueueWeight> federationQueueWeights = null;

  public BatchSaveFederationQueuePoliciesRequestPBImpl() {
    this.builder = BatchSaveFederationQueuePoliciesRequestProto.newBuilder();
  }

  public BatchSaveFederationQueuePoliciesRequestPBImpl(
      BatchSaveFederationQueuePoliciesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = BatchSaveFederationQueuePoliciesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    if (this.federationQueueWeights != null) {
      for (FederationQueueWeight federationQueueWeight : federationQueueWeights) {
        FederationQueueWeightPBImpl federationQueueWeightPBImpl =
            (FederationQueueWeightPBImpl) federationQueueWeight;
        builder.addFederationQueueWeights(federationQueueWeightPBImpl.getProto());
      }
    }
    proto = builder.build();
    viaProto = true;
  }

  public BatchSaveFederationQueuePoliciesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void initDeregisterSubClustersMapping() {
    if (this.federationQueueWeights != null) {
      return;
    }

    BatchSaveFederationQueuePoliciesRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<FederationQueueWeightProto> batchSaveFederationQueuePoliciesProtoList =
        p.getFederationQueueWeightsList();

    List<FederationQueueWeight> attributes = new ArrayList<>();
    if (batchSaveFederationQueuePoliciesProtoList == null ||
        batchSaveFederationQueuePoliciesProtoList.size() == 0) {
      this.federationQueueWeights = attributes;
      return;
    }

    for (FederationQueueWeightProto federationQueueWeightProto :
        batchSaveFederationQueuePoliciesProtoList) {
      attributes.add(new FederationQueueWeightPBImpl(federationQueueWeightProto));
    }

    this.federationQueueWeights = attributes;
  }

  @Override
  public List<FederationQueueWeight> getFederationQueueWeights() {
    initDeregisterSubClustersMapping();
    return this.federationQueueWeights;
  }

  @Override
  public void setFederationQueueWeights(List<FederationQueueWeight> pFederationQueueWeights) {
    if (federationQueueWeights == null) {
      federationQueueWeights = new ArrayList<>();
    }
    if(federationQueueWeights == null) {
      throw new IllegalArgumentException("federationQueueWeights cannot be null");
    }
    federationQueueWeights.clear();
    federationQueueWeights.addAll(pFederationQueueWeights);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BatchSaveFederationQueuePoliciesRequest)) {
      return false;
    }

    BatchSaveFederationQueuePoliciesRequestPBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
