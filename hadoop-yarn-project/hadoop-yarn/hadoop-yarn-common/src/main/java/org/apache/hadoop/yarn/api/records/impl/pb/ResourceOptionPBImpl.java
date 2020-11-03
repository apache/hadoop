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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceOptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceOptionProtoOrBuilder;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

public class ResourceOptionPBImpl extends ResourceOption {

  ResourceOptionProto proto = ResourceOptionProto.getDefaultInstance();
  ResourceOptionProto.Builder builder = null;
  boolean viaProto = false;

  public ResourceOptionPBImpl() {
    builder = ResourceOptionProto.newBuilder();
  }

  public ResourceOptionPBImpl(ResourceOptionProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceOptionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  @Override
  public Resource getResource() {
    ResourceOptionProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getResource());
  }

  @Override
  protected void setResource(Resource resource) {
    maybeInitBuilder();
    builder.setResource(convertToProtoFormat(resource));
  }

  @Override
  public int getOverCommitTimeout() {
    ResourceOptionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getOverCommitTimeout();
  }

  @Override
  protected void setOverCommitTimeout(int overCommitTimeout) {
    maybeInitBuilder();
    builder.setOverCommitTimeout(overCommitTimeout);
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceOptionProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  private ResourceProto convertToProtoFormat(
      Resource resource) {
    return ProtoUtils.convertToProtoFormat(resource);
  }
  
  private ResourcePBImpl convertFromProtoFormat(
      ResourceProto p) {
    return new ResourcePBImpl(p);
  }
  
  @Override
  protected void build() {
    proto = builder.build();
    viaProto = true;
    builder = null;
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

}
