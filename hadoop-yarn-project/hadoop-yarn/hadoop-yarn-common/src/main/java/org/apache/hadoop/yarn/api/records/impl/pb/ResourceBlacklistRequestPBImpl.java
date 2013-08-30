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

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceBlacklistRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceBlacklistRequestProtoOrBuilder;

@Private
@Unstable
public class ResourceBlacklistRequestPBImpl extends ResourceBlacklistRequest {

  ResourceBlacklistRequestProto proto = null;
  ResourceBlacklistRequestProto.Builder builder = null;
  boolean viaProto = false;

  List<String> blacklistAdditions = null;
  List<String> blacklistRemovals = null;
  
  public ResourceBlacklistRequestPBImpl() {
    builder = ResourceBlacklistRequestProto.newBuilder();
  }
  
  public ResourceBlacklistRequestPBImpl(ResourceBlacklistRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceBlacklistRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceBlacklistRequestProto.newBuilder(proto);
    }
    viaProto = false;
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
    if (this.blacklistAdditions != null) {
      addBlacklistAdditionsToProto();
    }
    if (this.blacklistRemovals != null) {
      addBlacklistRemovalsToProto();
    }
  }
  
  private void addBlacklistAdditionsToProto() {
    maybeInitBuilder();
    builder.clearBlacklistAdditions();
    if (this.blacklistAdditions == null) { 
      return;
    }
    builder.addAllBlacklistAdditions(this.blacklistAdditions);
  }

  private void addBlacklistRemovalsToProto() {
    maybeInitBuilder();
    builder.clearBlacklistAdditions();
    if (this.blacklistRemovals == null) { 
      return;
    }
    builder.addAllBlacklistRemovals(this.blacklistRemovals);
  }

  private void initBlacklistAdditions() {
    if (this.blacklistAdditions != null) {
      return;
    }
    ResourceBlacklistRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getBlacklistAdditionsList();
    this.blacklistAdditions = new ArrayList<String>();
    this.blacklistAdditions.addAll(list);
  }
  
  private void initBlacklistRemovals() {
    if (this.blacklistRemovals != null) {
      return;
    }
    ResourceBlacklistRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> list = p.getBlacklistRemovalsList();
    this.blacklistRemovals = new ArrayList<String>();
    this.blacklistRemovals.addAll(list);
  }
  
  @Override
  public List<String> getBlacklistAdditions() {
    initBlacklistAdditions();
    return this.blacklistAdditions;
  }

  @Override
  public void setBlacklistAdditions(List<String> resourceNames) {
    if (resourceNames == null || resourceNames.isEmpty()) {
      if (this.blacklistAdditions != null) {
        this.blacklistAdditions.clear();
      }
      return;
    }
    initBlacklistAdditions();
    this.blacklistAdditions.clear();
    this.blacklistAdditions.addAll(resourceNames);
  }

  @Override
  public List<String> getBlacklistRemovals() {
    initBlacklistRemovals();
    return this.blacklistRemovals;
  }

  @Override
  public void setBlacklistRemovals(List<String> resourceNames) {
    if (resourceNames == null || resourceNames.isEmpty()) {
      if (this.blacklistRemovals != null) {
        this.blacklistRemovals.clear();
      }
      return;
    }
    initBlacklistRemovals();
    this.blacklistRemovals.clear();
    this.blacklistRemovals.addAll(resourceNames);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
}
