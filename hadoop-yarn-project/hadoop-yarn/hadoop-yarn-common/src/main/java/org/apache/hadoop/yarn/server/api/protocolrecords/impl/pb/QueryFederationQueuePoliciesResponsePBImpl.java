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

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.proto.YarnProtos.FederationQueueWeightProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.QueryFederationQueuePoliciesResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.QueryFederationQueuePoliciesResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesResponse;

import java.util.ArrayList;
import java.util.List;

public class QueryFederationQueuePoliciesResponsePBImpl
    extends QueryFederationQueuePoliciesResponse {

  private QueryFederationQueuePoliciesResponseProto proto =
      QueryFederationQueuePoliciesResponseProto.getDefaultInstance();
  private QueryFederationQueuePoliciesResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private List<FederationQueueWeight> federationQueueWeights = null;

  public QueryFederationQueuePoliciesResponsePBImpl() {
    builder = QueryFederationQueuePoliciesResponseProto.newBuilder();
  }

  public QueryFederationQueuePoliciesResponsePBImpl(
      QueryFederationQueuePoliciesResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
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

  public QueryFederationQueuePoliciesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = QueryFederationQueuePoliciesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getTotalSize() {
    QueryFederationQueuePoliciesResponseProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasTotalSize = p.hasTotalSize();
    if (hasTotalSize) {
      return p.getTotalSize();
    }
    return 0;
  }

  @Override
  public void setTotalSize(int totalSize) {
    maybeInitBuilder();
    Preconditions.checkNotNull(builder);
    builder.setTotalSize(totalSize);
  }

  @Override
  public int getTotalPage() {
    QueryFederationQueuePoliciesResponseProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasTotalPage = p.hasTotalPage();
    if (hasTotalPage) {
      return p.getTotalPage();
    }
    return 0;
  }

  @Override
  public void setTotalPage(int totalPage) {
    maybeInitBuilder();
    Preconditions.checkNotNull(builder);
    builder.setTotalPage(totalPage);
  }

  @Override
  public int getCurrentPage() {
    QueryFederationQueuePoliciesResponseProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasCurrentPage = p.hasCurrentPage();
    if (hasCurrentPage) {
      return p.getCurrentPage();
    }
    return 0;
  }

  @Override
  public void setCurrentPage(int currentPage) {
    maybeInitBuilder();
    Preconditions.checkNotNull(builder);
    builder.setCurrentPage(currentPage);
  }

  @Override
  public int getPageSize() {
    QueryFederationQueuePoliciesResponseProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasPageSize = p.hasPageSize();
    if (hasPageSize) {
      return p.getPageSize();
    }
    return 0;
  }

  @Override
  public void setPageSize(int pageSize) {
    Preconditions.checkNotNull(builder);
    builder.setPageSize(pageSize);
  }

  private void initFederationQueueWeightsMapping() {
    if (this.federationQueueWeights != null) {
      return;
    }

    QueryFederationQueuePoliciesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<FederationQueueWeightProto> queryFederationQueuePoliciesProtoList =
        p.getFederationQueueWeightsList();

    List<FederationQueueWeight> fqWeights = new ArrayList<>();
    if (queryFederationQueuePoliciesProtoList == null ||
            queryFederationQueuePoliciesProtoList.size() == 0) {
      this.federationQueueWeights = fqWeights;
      return;
    }

    for (FederationQueueWeightProto federationQueueWeightProto :
        queryFederationQueuePoliciesProtoList) {
      fqWeights.add(new FederationQueueWeightPBImpl(federationQueueWeightProto));
    }

    this.federationQueueWeights = fqWeights;
  }


  @Override
  public List<FederationQueueWeight> getFederationQueueWeights() {
    initFederationQueueWeightsMapping();
    return this.federationQueueWeights;
  }

  @Override
  public void setFederationQueueWeights(List<FederationQueueWeight> pfederationQueueWeights) {
    maybeInitBuilder();
    if (federationQueueWeights == null) {
      federationQueueWeights = new ArrayList<>();
    }
    if(pfederationQueueWeights == null) {
      builder.clearFederationQueueWeights();
      return;
    }
    federationQueueWeights.clear();
    federationQueueWeights.addAll(pfederationQueueWeights);
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
}
