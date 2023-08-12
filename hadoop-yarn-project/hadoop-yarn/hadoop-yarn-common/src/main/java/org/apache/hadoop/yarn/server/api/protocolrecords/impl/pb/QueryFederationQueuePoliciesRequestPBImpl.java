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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.QueryFederationQueuePoliciesRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.QueryFederationQueuePoliciesRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesRequest;

import java.util.ArrayList;
import java.util.List;

@Private
@Unstable
public class QueryFederationQueuePoliciesRequestPBImpl
    extends QueryFederationQueuePoliciesRequest {

  private QueryFederationQueuePoliciesRequestProto proto =
      QueryFederationQueuePoliciesRequestProto.getDefaultInstance();
  private QueryFederationQueuePoliciesRequestProto.Builder builder = null;
  private boolean viaProto = false;
  List<String> queues = null;

  public QueryFederationQueuePoliciesRequestPBImpl() {
    builder = QueryFederationQueuePoliciesRequestProto.newBuilder();
  }

  public QueryFederationQueuePoliciesRequestPBImpl(
      QueryFederationQueuePoliciesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public void setPageSize(int pageSize) {
    Preconditions.checkNotNull(proto);
    builder.setPageSize(pageSize);
  }

  @Override
  public int getPageSize() {
    Preconditions.checkNotNull(proto);
    return proto.getPageSize();
  }

  @Override
  public void setCurrentPage(int currentPage) {
    Preconditions.checkNotNull(proto);
    builder.setCurrentPage(currentPage);
  }

  @Override
  public int getCurrentPage() {
    Preconditions.checkNotNull(proto);
    return proto.getCurrentPage();
  }

  @Override
  public String getQueue() {
    QueryFederationQueuePoliciesRequestProtoOrBuilder p = viaProto ? proto : builder;
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
  public List<String> getQueues() {
    return null;
  }

  @Override
  public void setQueues(List<String> pQueues) {
    if (pQueues == null || pQueues.isEmpty()) {
      if (this.queues != null) {
        this.queues.clear();
      }
      return;
    }
    initQueues();
    this.queues.clear();
    this.queues.addAll(pQueues);
  }

  private void initQueues() {
    if (this.queues != null) {
      return;
    }
    QueryFederationQueuePoliciesRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  public QueryFederationQueuePoliciesRequestProto getProto() {
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
    builder.clearQueue();
    if (this.queues == null) {
      return;
    }
    builder.addAllQueues(this.queues);
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
        builder = QueryFederationQueuePoliciesRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
