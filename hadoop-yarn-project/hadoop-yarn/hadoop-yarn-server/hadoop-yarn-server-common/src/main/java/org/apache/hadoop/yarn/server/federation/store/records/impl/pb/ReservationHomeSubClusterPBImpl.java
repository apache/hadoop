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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link ReservationHomeSubCluster}.
 */
@Private
@Unstable
public class ReservationHomeSubClusterPBImpl extends ReservationHomeSubCluster {

  private ReservationHomeSubClusterProto proto =
      ReservationHomeSubClusterProto.getDefaultInstance();
  private ReservationHomeSubClusterProto.Builder builder = null;
  private boolean viaProto = false;

  private ReservationId reservationId = null;
  private SubClusterId homeSubCluster = null;

  public ReservationHomeSubClusterPBImpl() {
    builder = ReservationHomeSubClusterProto.newBuilder();
  }

  public ReservationHomeSubClusterPBImpl(ReservationHomeSubClusterProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReservationHomeSubClusterProto getProto() {
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
      builder = ReservationHomeSubClusterProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.reservationId != null) {
      builder.setReservationId(convertToProtoFormat(this.reservationId));
    }
    if (this.homeSubCluster != null) {
      builder.setHomeSubCluster(convertToProtoFormat(this.homeSubCluster));
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
  public ReservationId getReservationId() {
    ReservationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReservationId()) {
      return null;
    }
    this.reservationId = convertFromProtoFormat(p.getReservationId());
    return this.reservationId;
  }

  @Override
  public void setReservationId(ReservationId resId) {
    maybeInitBuilder();
    if (resId == null) {
      builder.clearReservationId();
      return;
    }
    builder.setReservationId(convertToProtoFormat(resId));
    this.reservationId = resId;
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    ReservationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.homeSubCluster != null) {
      return this.homeSubCluster;
    }
    if (!p.hasHomeSubCluster()) {
      return null;
    }
    this.homeSubCluster = convertFromProtoFormat(p.getHomeSubCluster());
    return this.homeSubCluster;
  }

  @Override
  public void setHomeSubCluster(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearHomeSubCluster();
      return;
    }
    this.homeSubCluster = subClusterId;
  }

  private SubClusterId convertFromProtoFormat(SubClusterIdProto subClusterId) {
    return new SubClusterIdPBImpl(subClusterId);
  }

  private SubClusterIdProto convertToProtoFormat(SubClusterId subClusterId) {
    return ((SubClusterIdPBImpl) subClusterId).getProto();
  }

  private ReservationId convertFromProtoFormat(ReservationIdProto appId) {
    return new ReservationIdPBImpl(appId);
  }

  private ReservationIdProto convertToProtoFormat(ReservationId appId) {
    return ((ReservationIdPBImpl) appId).getProto();
  }
}
