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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationsHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationsHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of
 * {@link GetReservationsHomeSubClusterResponse}.
 */
@Private
@Unstable
public class GetReservationsHomeSubClusterResponsePBImpl
    extends GetReservationsHomeSubClusterResponse {

  private GetReservationsHomeSubClusterResponseProto proto =
      GetReservationsHomeSubClusterResponseProto.getDefaultInstance();
  private GetReservationsHomeSubClusterResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private List<ReservationHomeSubCluster> appsHomeSubCluster;

  public GetReservationsHomeSubClusterResponsePBImpl() {
    builder = GetReservationsHomeSubClusterResponseProto.newBuilder();
  }

  public GetReservationsHomeSubClusterResponsePBImpl(
      GetReservationsHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetReservationsHomeSubClusterResponseProto getProto() {
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
      builder = GetReservationsHomeSubClusterResponseProto.newBuilder(proto);
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
  public List<ReservationHomeSubCluster> getAppsHomeSubClusters() {
    initSubClustersInfoList();
    return appsHomeSubCluster;
  }

  @Override
  public void setAppsHomeSubClusters(
      List<ReservationHomeSubCluster> appsHomeSubClusters) {
    maybeInitBuilder();
    if (appsHomeSubClusters == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    this.appsHomeSubCluster = appsHomeSubClusters;
    addSubClustersInfoToProto();
  }

  private void initSubClustersInfoList() {
    if (this.appsHomeSubCluster != null) {
      return;
    }
    GetReservationsHomeSubClusterResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ReservationHomeSubClusterProto> subClusterInfosList = p.getAppSubclusterMapList();
    appsHomeSubCluster = new ArrayList<>();

    for (ReservationHomeSubClusterProto r : subClusterInfosList) {
      appsHomeSubCluster.add(convertFromProtoFormat(r));
    }
  }

  private void addSubClustersInfoToProto() {
    maybeInitBuilder();
    builder.clearAppSubclusterMap();
    if (appsHomeSubCluster == null) {
      return;
    }
    Iterable<ReservationHomeSubClusterProto> iterable =
        new Iterable<ReservationHomeSubClusterProto>() {
          @Override
          public Iterator<ReservationHomeSubClusterProto> iterator() {
            return new Iterator<ReservationHomeSubClusterProto>() {

              private Iterator<ReservationHomeSubCluster> iter = appsHomeSubCluster.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ReservationHomeSubClusterProto next() {
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

  private ReservationHomeSubCluster convertFromProtoFormat(ReservationHomeSubClusterProto sc) {
    return new ReservationHomeSubClusterPBImpl(sc);
  }

  private ReservationHomeSubClusterProto convertToProtoFormat(ReservationHomeSubCluster sc) {
    return ((ReservationHomeSubClusterPBImpl) sc).getProto();
  }
}
