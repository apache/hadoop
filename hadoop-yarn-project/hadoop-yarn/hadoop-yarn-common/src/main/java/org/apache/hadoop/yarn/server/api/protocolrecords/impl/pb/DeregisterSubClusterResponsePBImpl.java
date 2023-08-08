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
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DeregisterSubClusterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DeregisterSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;

import java.util.ArrayList;
import java.util.List;

@Private
@Unstable
public class DeregisterSubClusterResponsePBImpl extends DeregisterSubClusterResponse {
  private DeregisterSubClusterResponseProto proto =
      DeregisterSubClusterResponseProto.getDefaultInstance();
  private DeregisterSubClusterResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private List<DeregisterSubClusters> deregisterSubClustersMapping = null;

  public DeregisterSubClusterResponsePBImpl() {
    this.builder = DeregisterSubClusterResponseProto.newBuilder();
  }

  public DeregisterSubClusterResponsePBImpl(DeregisterSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DeregisterSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    if (this.deregisterSubClustersMapping != null) {
      for (DeregisterSubClusters deregisterSubClusters : deregisterSubClustersMapping) {
        DeregisterSubClustersPBImpl deregisterSubClustersPBImpl =
            (DeregisterSubClustersPBImpl) deregisterSubClusters;
        builder.addDeregisterSubClusters(deregisterSubClustersPBImpl.getProto());
      }
    }
    proto = builder.build();
    viaProto = true;
  }

  public DeregisterSubClusterResponseProto getProto() {
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
    if (!(other instanceof DeregisterSubClusterResponse)) {
      return false;
    }
    DeregisterSubClusterResponsePBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public void setDeregisterSubClusters(List<DeregisterSubClusters> deregisterSubClusters) {
    if (deregisterSubClustersMapping == null) {
      deregisterSubClustersMapping = new ArrayList<>();
    }
    if(deregisterSubClusters == null) {
      throw new IllegalArgumentException("deregisterSubClusters cannot be null");
    }
    deregisterSubClustersMapping.clear();
    deregisterSubClustersMapping.addAll(deregisterSubClusters);
  }

  private void initDeregisterSubClustersMapping() {
    if (this.deregisterSubClustersMapping != null) {
      return;
    }
    DeregisterSubClusterResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<DeregisterSubClustersProto> deregisterSubClustersProtoList =
        p.getDeregisterSubClustersList();
    List<DeregisterSubClusters> attributes = new ArrayList<>();
    if (deregisterSubClustersProtoList == null || deregisterSubClustersProtoList.size() == 0) {
      this.deregisterSubClustersMapping = attributes;
      return;
    }
    for (DeregisterSubClustersProto deregisterSubClustersProto : deregisterSubClustersProtoList) {
      attributes.add(new DeregisterSubClustersPBImpl(deregisterSubClustersProto));
    }
    this.deregisterSubClustersMapping = attributes;
  }

  @Override
  public List<DeregisterSubClusters> getDeregisterSubClusters() {
    initDeregisterSubClustersMapping();
    return this.deregisterSubClustersMapping;
  }
}
