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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import com.google.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of {@link ApplicationHomeSubCluster}.
 */
@Private
@Unstable
public class ApplicationHomeSubClusterPBImpl extends ApplicationHomeSubCluster {

  private ApplicationHomeSubClusterProto proto =
      ApplicationHomeSubClusterProto.getDefaultInstance();
  private ApplicationHomeSubClusterProto.Builder builder = null;
  private boolean viaProto = false;

  private ApplicationId applicationId = null;
  private SubClusterId homeSubCluster = null;

  public ApplicationHomeSubClusterPBImpl() {
    builder = ApplicationHomeSubClusterProto.newBuilder();
  }

  public ApplicationHomeSubClusterPBImpl(ApplicationHomeSubClusterProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ApplicationHomeSubClusterProto getProto() {
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
      builder = ApplicationHomeSubClusterProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
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
  public ApplicationId getApplicationId() {
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    this.applicationId = applicationId;
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setHomeSubCluster(SubClusterId homeSubCluster) {
    maybeInitBuilder();
    if (homeSubCluster == null) {
      builder.clearHomeSubCluster();
    }
    this.homeSubCluster = homeSubCluster;
  }

  private SubClusterId convertFromProtoFormat(SubClusterIdProto subClusterId) {
    return new SubClusterIdPBImpl(subClusterId);
  }

  private SubClusterIdProto convertToProtoFormat(SubClusterId subClusterId) {
    return ((SubClusterIdPBImpl) subClusterId).getProto();
  }

  private ApplicationId convertFromProtoFormat(ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }
}
