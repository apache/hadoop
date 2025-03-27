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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteSubClusterPoliciesConfigurationsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteSubClusterPoliciesConfigurationsRequestProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Protocol buffer based implementation of {@link DeleteSubClusterPoliciesConfigurationsRequest}.
 */
@Private
@Unstable
public class DeleteSubClusterPoliciesConfigurationsRequestPBImpl extends
    DeleteSubClusterPoliciesConfigurationsRequest {

  private DeleteSubClusterPoliciesConfigurationsRequestProto proto =
      DeleteSubClusterPoliciesConfigurationsRequestProto.getDefaultInstance();
  private DeleteSubClusterPoliciesConfigurationsRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private List<String> queues = null;

  public DeleteSubClusterPoliciesConfigurationsRequestPBImpl() {
    builder = DeleteSubClusterPoliciesConfigurationsRequestProto.newBuilder();
  }

  public DeleteSubClusterPoliciesConfigurationsRequestPBImpl(
      DeleteSubClusterPoliciesConfigurationsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DeleteSubClusterPoliciesConfigurationsRequestProto getProto() {
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
    if (this.queues != null) {
      addQueuesToProto();
    }
  }

  private void addQueuesToProto() {
    maybeInitBuilder();
    builder.clearQueues();
    if (this.queues == null) {
      return;
    }
    builder.addAllQueues(this.queues);
  }

  private void initQueues() {
    if (this.queues != null) {
      return;
    }
    DeleteSubClusterPoliciesConfigurationsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getQueuesList();
    this.queues = new ArrayList<>();
    this.queues.addAll(list);
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
  public List<String> getQueues() {
    if (this.queues != null) {
      return this.queues;
    }
    initQueues();
    return this.queues;
  }

  @Override
  public void setQueues(List<String> pQueues) {
    if (pQueues == null || pQueues.isEmpty()) {
      maybeInitBuilder();
      if (this.queues != null) {
        this.queues.clear();
      }
      return;
    }
    if (this.queues == null) {
      this.queues = new ArrayList<>();
    }
    this.queues.clear();
    this.queues.addAll(pQueues);
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DeleteSubClusterPoliciesConfigurationsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
