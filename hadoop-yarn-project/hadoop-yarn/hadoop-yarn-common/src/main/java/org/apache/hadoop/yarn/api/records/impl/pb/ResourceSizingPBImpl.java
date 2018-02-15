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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceSizingProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceSizingProtoOrBuilder;

/**
 * Proto Implementation for {@link ResourceSizing} interface.
 */
@Private
@Unstable
public class ResourceSizingPBImpl extends ResourceSizing {
  ResourceSizingProto proto = ResourceSizingProto.getDefaultInstance();
  ResourceSizingProto.Builder builder = null;
  boolean viaProto = false;

  private Resource resources = null;

  public ResourceSizingPBImpl() {
    builder = ResourceSizingProto.newBuilder();
  }

  public ResourceSizingPBImpl(ResourceSizingProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceSizingProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resources != null) {
      builder.setResources(convertToProtoFormat(this.resources));
    }
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
      builder = ResourceSizingProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getNumAllocations() {
    ResourceSizingProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNumAllocations());
  }

  @Override
  public void setNumAllocations(int numAllocations) {
    maybeInitBuilder();
    builder.setNumAllocations(numAllocations);
  }

  @Override
  public Resource getResources() {
    ResourceSizingProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resources != null) {
      return this.resources;
    }
    if (!p.hasResources()) {
      return null;
    }
    this.resources = convertFromProtoFormat(p.getResources());
    return this.resources;
  }

  @Override
  public void setResources(Resource resources) {
    maybeInitBuilder();
    if (resources == null) {
      builder.clearResources();
    }
    this.resources = resources;
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto r) {
    return new ResourcePBImpl(r);
  }

  private ResourceProto convertToProtoFormat(Resource r) {
    return ProtoUtils.convertToProtoFormat(r);
  }

  @Override
  public String toString() {
    return "ResourceSizingPBImpl{" +
        "numAllocations=" + getNumAllocations() +
        ", resources=" + getResources() +
        '}';
  }
}
