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
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnProtos.FederationSubClusterProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetSubClustersResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.GetSubClustersResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationSubCluster;

import java.util.ArrayList;
import java.util.List;

/**
 * The class is responsible for get subclusters responses.
 */
public class GetSubClustersResponsePBImpl extends GetSubClustersResponse {

  private GetSubClustersResponseProto proto = GetSubClustersResponseProto.getDefaultInstance();
  private GetSubClustersResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private List<FederationSubCluster> federationSubClusters = null;

  public GetSubClustersResponsePBImpl() {
    this.builder = GetSubClustersResponseProto.newBuilder();
  }

  public GetSubClustersResponsePBImpl(GetSubClustersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClustersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    if (this.federationSubClusters != null) {
      for (FederationSubCluster federationSubCluster : federationSubClusters) {
        FederationSubClusterPBImpl federationSubClusterPBImpl =
            (FederationSubClusterPBImpl) federationSubCluster;
        builder.addSubClusters(federationSubClusterPBImpl.getProto());
      }
    }
    proto = builder.build();
    viaProto = true;
  }

  public GetSubClustersResponseProto getProto() {
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
    if (!(other instanceof GetSubClustersResponse)) {
      return false;
    }
    GetSubClustersResponsePBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public List<FederationSubCluster> getFederationSubClusters() {
    initFederationSubClustersList();
    return this.federationSubClusters;
  }

  private void initFederationSubClustersList() {
    if (this.federationSubClusters != null) {
      return;
    }
    GetSubClustersResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<FederationSubClusterProto> getSubClustersResponseProtoList = p.getSubClustersList();
    List<FederationSubCluster> subClusters = new ArrayList<>();
    if (getSubClustersResponseProtoList == null || getSubClustersResponseProtoList.size() == 0) {
      this.federationSubClusters = subClusters;
      return;
    }
    for (FederationSubClusterProto federationSubClusterProto : getSubClustersResponseProtoList) {
      subClusters.add(new FederationSubClusterPBImpl(federationSubClusterProto));
    }
    this.federationSubClusters = subClusters;
  }

  @Override
  public void setFederationSubClusters(List<FederationSubCluster> pFederationSubClusters) {
    if (federationSubClusters == null) {
      federationSubClusters = new ArrayList<>();
    }
    federationSubClusters.clear();
    federationSubClusters.addAll(pFederationSubClusters);
  }
}
