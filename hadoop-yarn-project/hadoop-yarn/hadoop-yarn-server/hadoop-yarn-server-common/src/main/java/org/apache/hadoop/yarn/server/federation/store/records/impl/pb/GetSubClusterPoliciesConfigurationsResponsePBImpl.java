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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import com.google.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of
 * {@link GetSubClusterPoliciesConfigurationsResponse}.
 */
@Private
@Unstable
public class GetSubClusterPoliciesConfigurationsResponsePBImpl
    extends GetSubClusterPoliciesConfigurationsResponse {

  private GetSubClusterPoliciesConfigurationsResponseProto proto =
      GetSubClusterPoliciesConfigurationsResponseProto.getDefaultInstance();
  private GetSubClusterPoliciesConfigurationsResponseProto.Builder builder =
      null;
  private boolean viaProto = false;

  private List<SubClusterPolicyConfiguration> subClusterPolicies = null;

  public GetSubClusterPoliciesConfigurationsResponsePBImpl() {
    builder = GetSubClusterPoliciesConfigurationsResponseProto.newBuilder();
  }

  public GetSubClusterPoliciesConfigurationsResponsePBImpl(
      GetSubClusterPoliciesConfigurationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetSubClusterPoliciesConfigurationsResponseProto getProto() {
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

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          GetSubClusterPoliciesConfigurationsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.subClusterPolicies != null) {
      addSubClusterPoliciesConfigurationsToProto();
    }
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
  public List<SubClusterPolicyConfiguration> getPoliciesConfigs() {
    initSubClusterPoliciesConfigurationsList();
    return this.subClusterPolicies;
  }

  @Override
  public void setPoliciesConfigs(
      List<SubClusterPolicyConfiguration> policyConfigurations) {
    maybeInitBuilder();
    if (policyConfigurations == null) {
      builder.clearPoliciesConfigurations();
    }
    this.subClusterPolicies = policyConfigurations;
  }

  private void initSubClusterPoliciesConfigurationsList() {
    if (this.subClusterPolicies != null) {
      return;
    }
    GetSubClusterPoliciesConfigurationsResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<SubClusterPolicyConfigurationProto> subClusterPoliciesList =
        p.getPoliciesConfigurationsList();
    subClusterPolicies = new ArrayList<SubClusterPolicyConfiguration>();

    for (SubClusterPolicyConfigurationProto r : subClusterPoliciesList) {
      subClusterPolicies.add(convertFromProtoFormat(r));
    }
  }

  private void addSubClusterPoliciesConfigurationsToProto() {
    maybeInitBuilder();
    builder.clearPoliciesConfigurations();
    if (subClusterPolicies == null) {
      return;
    }
    Iterable<SubClusterPolicyConfigurationProto> iterable =
        new Iterable<SubClusterPolicyConfigurationProto>() {
          @Override
          public Iterator<SubClusterPolicyConfigurationProto> iterator() {
            return new Iterator<SubClusterPolicyConfigurationProto>() {

              private Iterator<SubClusterPolicyConfiguration> iter =
                  subClusterPolicies.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SubClusterPolicyConfigurationProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllPoliciesConfigurations(iterable);
  }

  private SubClusterPolicyConfiguration convertFromProtoFormat(
      SubClusterPolicyConfigurationProto policy) {
    return new SubClusterPolicyConfigurationPBImpl(policy);
  }

  private SubClusterPolicyConfigurationProto convertToProtoFormat(
      SubClusterPolicyConfiguration policy) {
    return ((SubClusterPolicyConfigurationPBImpl) policy).getProto();
  }

}
