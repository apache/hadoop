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
import org.apache.hadoop.yarn.proto.YarnProtos.DeregisterSubClustersProto;
import org.apache.hadoop.yarn.proto.YarnProtos.DeregisterSubClustersProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;

@Private
@Unstable
public class DeregisterSubClustersPBImpl extends DeregisterSubClusters {

  private DeregisterSubClustersProto proto = DeregisterSubClustersProto.getDefaultInstance();
  private DeregisterSubClustersProto.Builder builder = null;
  private boolean viaProto = false;

  public DeregisterSubClustersPBImpl() {
    this.builder = DeregisterSubClustersProto.newBuilder();
  }

  public DeregisterSubClustersPBImpl(DeregisterSubClustersProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (this.viaProto || this.builder == null) {
      this.builder = DeregisterSubClustersProto.newBuilder(proto);
    }
    this.viaProto = false;
  }

  public DeregisterSubClustersProto getProto() {
    this.proto = this.viaProto ? this.proto : this.builder.build();
    this.viaProto = true;
    return this.proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DeregisterSubClusters)) {
      return false;
    }
    DeregisterSubClustersPBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public String getSubClusterId() {
    DeregisterSubClustersProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasSubClusterId = p.hasSubClusterId();
    if (hasSubClusterId) {
      return p.getSubClusterId();
    }
    return null;
  }

  @Override
  public void setSubClusterId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(subClusterId);
  }

  @Override
  public String getDeregisterState() {
    DeregisterSubClustersProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasDeregisterState = p.hasDeregisterState();
    if (hasDeregisterState) {
      return p.getDeregisterState();
    }
    return null;
  }

  @Override
  public void setDeregisterState(String deregisterState) {
    maybeInitBuilder();
    if (deregisterState == null) {
      builder.clearDeregisterState();
      return;
    }
    builder.setDeregisterState(deregisterState);
  }

  @Override
  public String getLastHeartBeatTime() {
    DeregisterSubClustersProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasLastHeartBeatTime = p.hasLastHeartBeatTime();
    if (hasLastHeartBeatTime) {
      return p.getLastHeartBeatTime();
    }
    return null;
  }

  @Override
  public void setLastHeartBeatTime(String lastHeartBeatTime) {
    maybeInitBuilder();
    if (lastHeartBeatTime == null) {
      builder.clearLastHeartBeatTime();
      return;
    }
    builder.setLastHeartBeatTime(lastHeartBeatTime);
  }

  @Override
  public String getInformation() {
    DeregisterSubClustersProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasInformation = p.hasInformation();
    if (hasInformation) {
      return p.getInformation();
    }
    return null;
  }

  @Override
  public void setInformation(String info) {
    maybeInitBuilder();
    if (info == null) {
      builder.clearInformation();
      return;
    }
    builder.setInformation(info);
  }

  @Override
  public String getSubClusterState() {
    DeregisterSubClustersProtoOrBuilder p = this.viaProto ? this.proto : this.builder;
    boolean hasSubClusterState = p.hasSubClusterState();
    if (hasSubClusterState) {
      return p.getSubClusterState();
    }
    return null;
  }

  @Override
  public void setSubClusterState(String subClusterState) {
    maybeInitBuilder();
    if (subClusterState == null) {
      builder.clearSubClusterState();
      return;
    }
    builder.setSubClusterState(subClusterState);
  }
}
