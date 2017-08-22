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
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceTypeInfoProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceTypesProto;

/**
 * {@code ResourceTypeInfoPBImpl} which implements the
 * {@link ResourceTypeInfo} class which represents different resource types
 * supported in YARN.
 */
@Private
@Unstable
public class ResourceTypeInfoPBImpl extends ResourceTypeInfo {

  ResourceTypeInfoProto proto = ResourceTypeInfoProto.getDefaultInstance();
  ResourceTypeInfoProto.Builder builder = null;
  boolean viaProto = false;

  private String name = null;
  private String defaultUnit = null;
  private ResourceTypes resourceTypes = null;

  public ResourceTypeInfoPBImpl() {
    builder = ResourceTypeInfoProto.newBuilder();
  }

  public ResourceTypeInfoPBImpl(ResourceTypeInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceTypeInfoProto getProto() {
    mergeLocalToProto();
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

  private void mergeLocalToBuilder() {
    if (this.name != null) {
      builder.setName(this.name);
    }
    if (this.defaultUnit != null) {
      builder.setUnits(this.defaultUnit);
    }
    if (this.resourceTypes != null) {
      builder.setType(convertToProtoFormat(this.resourceTypes));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnProtos.ResourceTypeInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getName() {
    if (this.name != null) {
      return this.name;
    }

    YarnProtos.ResourceTypeInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  @Override
  public void setName(String rName) {
    maybeInitBuilder();
    if (rName == null) {
      builder.clearName();
    }
    this.name = rName;
  }

  @Override
  public String getDefaultUnit() {
    if (this.defaultUnit != null) {
      return this.defaultUnit;
    }

    YarnProtos.ResourceTypeInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getUnits();
  }

  @Override
  public void setDefaultUnit(String rUnits) {
    maybeInitBuilder();
    if (rUnits == null) {
      builder.clearUnits();
    }
    this.defaultUnit = rUnits;
  }

  @Override
  public ResourceTypes getResourceType() {
    if (this.resourceTypes != null) {
      return this.resourceTypes;
    }

    YarnProtos.ResourceTypeInfoProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getType());
  }

  @Override
  public void setResourceType(ResourceTypes type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
    }
    this.resourceTypes = type;
  }

  public static ResourceTypesProto convertToProtoFormat(ResourceTypes e) {
    return ResourceTypesProto.valueOf(e.name());
  }

  public static ResourceTypes convertFromProtoFormat(ResourceTypesProto e) {
    return ResourceTypes.valueOf(e.name());
  }
}
