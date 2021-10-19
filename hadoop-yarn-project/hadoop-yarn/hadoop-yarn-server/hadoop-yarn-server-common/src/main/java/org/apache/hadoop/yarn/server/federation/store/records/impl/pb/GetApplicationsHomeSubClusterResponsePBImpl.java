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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of
 * {@link GetApplicationsHomeSubClusterResponse}.
 */
@Private
@Unstable
public class GetApplicationsHomeSubClusterResponsePBImpl
    extends GetApplicationsHomeSubClusterResponse {

  private GetApplicationsHomeSubClusterResponseProto proto =
      GetApplicationsHomeSubClusterResponseProto.getDefaultInstance();
  private GetApplicationsHomeSubClusterResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ApplicationHomeSubCluster> appsHomeSubCluster;

  public GetApplicationsHomeSubClusterResponsePBImpl() {
    builder = GetApplicationsHomeSubClusterResponseProto.newBuilder();
  }

  public GetApplicationsHomeSubClusterResponsePBImpl(
      GetApplicationsHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetApplicationsHomeSubClusterResponseProto getProto() {
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
      builder = GetApplicationsHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.appsHomeSubCluster != null) {
      addSubClustersInfoToProto();
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
  public List<ApplicationHomeSubCluster> getAppsHomeSubClusters() {
    initSubClustersInfoList();
    return appsHomeSubCluster;
  }

  @Override
  public void setAppsHomeSubClusters(
      List<ApplicationHomeSubCluster> appsHomeSubClusters) {
    maybeInitBuilder();
    if (appsHomeSubClusters == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    this.appsHomeSubCluster = appsHomeSubClusters;
  }

  private void initSubClustersInfoList() {
    if (this.appsHomeSubCluster != null) {
      return;
    }
    GetApplicationsHomeSubClusterResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ApplicationHomeSubClusterProto> subClusterInfosList =
        p.getAppSubclusterMapList();
    appsHomeSubCluster = new ArrayList<ApplicationHomeSubCluster>();

    for (ApplicationHomeSubClusterProto r : subClusterInfosList) {
      appsHomeSubCluster.add(convertFromProtoFormat(r));
    }
  }

  private void addSubClustersInfoToProto() {
    maybeInitBuilder();
    builder.clearAppSubclusterMap();
    if (appsHomeSubCluster == null) {
      return;
    }
    Iterable<ApplicationHomeSubClusterProto> iterable =
        new Iterable<ApplicationHomeSubClusterProto>() {
          @Override
          public Iterator<ApplicationHomeSubClusterProto> iterator() {
            return new Iterator<ApplicationHomeSubClusterProto>() {

              private Iterator<ApplicationHomeSubCluster> iter =
                  appsHomeSubCluster.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ApplicationHomeSubClusterProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllAppSubclusterMap(iterable);
  }

  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}
