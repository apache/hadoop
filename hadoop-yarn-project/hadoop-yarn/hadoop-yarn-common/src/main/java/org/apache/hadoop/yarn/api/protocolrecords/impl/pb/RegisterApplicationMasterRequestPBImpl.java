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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.pb.PlacementConstraintFromProtoConverter;
import org.apache.hadoop.yarn.api.pb.PlacementConstraintToProtoConverter;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;

import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Private
@Unstable
public class RegisterApplicationMasterRequestPBImpl
    extends RegisterApplicationMasterRequest {
  private RegisterApplicationMasterRequestProto proto =
      RegisterApplicationMasterRequestProto.getDefaultInstance();
  private RegisterApplicationMasterRequestProto.Builder builder = null;
  private Map<Set<String>, PlacementConstraint> placementConstraints = null;
  boolean viaProto = false;
  
  public RegisterApplicationMasterRequestPBImpl() {
    builder = RegisterApplicationMasterRequestProto.newBuilder();
  }

  public RegisterApplicationMasterRequestPBImpl(
      RegisterApplicationMasterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterApplicationMasterRequestProto getProto() {
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
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.placementConstraints != null) {
      addPlacementConstraintMap();
    }
  }

  private void addPlacementConstraintMap() {
    maybeInitBuilder();
    builder.clearPlacementConstraints();
    if (this.placementConstraints == null) {
      return;
    }
    List<YarnProtos.PlacementConstraintMapEntryProto> protoList =
        new ArrayList<>();
    for (Map.Entry<Set<String>, PlacementConstraint> entry :
        this.placementConstraints.entrySet()) {
      protoList.add(
          YarnProtos.PlacementConstraintMapEntryProto.newBuilder()
              .addAllAllocationTags(entry.getKey())
              .setPlacementConstraint(
                  new PlacementConstraintToProtoConverter(
                      entry.getValue()).convert())
              .build());
    }
    builder.addAllPlacementConstraints(protoList);
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterApplicationMasterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getHost() {
    RegisterApplicationMasterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getHost();
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      builder.clearHost();
      return;
    }
    builder.setHost(host);
  }

  @Override
  public int getRpcPort() {
    RegisterApplicationMasterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public void setRpcPort(int port) {
    maybeInitBuilder();
    builder.setRpcPort(port);
  }

  @Override
  public String getTrackingUrl() {
    RegisterApplicationMasterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(url);
  }

  private void initPlacementConstraintMap() {
    if (this.placementConstraints != null) {
      return;
    }
    RegisterApplicationMasterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<YarnProtos.PlacementConstraintMapEntryProto> pcmList =
        p.getPlacementConstraintsList();
    this.placementConstraints = new HashMap<>();
    for (YarnProtos.PlacementConstraintMapEntryProto e : pcmList) {
      this.placementConstraints.put(
          new HashSet<>(e.getAllocationTagsList()),
          new PlacementConstraintFromProtoConverter(
              e.getPlacementConstraint()).convert());
    }
  }

  @Override
  public Map<Set<String>, PlacementConstraint> getPlacementConstraints() {
    initPlacementConstraintMap();
    return this.placementConstraints;
  }

  @Override
  public void setPlacementConstraints(
      Map<Set<String>, PlacementConstraint> constraints) {
    maybeInitBuilder();
    if (constraints == null) {
      builder.clearPlacementConstraints();
    } else {
      removeEmptyKeys(constraints);
    }
    this.placementConstraints = constraints;
  }

  private void removeEmptyKeys(
      Map<Set<String>, PlacementConstraint> constraintMap) {
    Iterator<Set<String>> iter = constraintMap.keySet().iterator();
    while (iter.hasNext()) {
      Set<String> aTags = iter.next();
      if (aTags.size() == 0) {
        iter.remove();
      }
    }
  }
}
