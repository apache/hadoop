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

import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReleaseSharedCacheResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReleaseSharedCacheResourceRequestProtoOrBuilder;

public class ReleaseSharedCacheResourceRequestPBImpl extends
    ReleaseSharedCacheResourceRequest {
  ReleaseSharedCacheResourceRequestProto proto =
      ReleaseSharedCacheResourceRequestProto.getDefaultInstance();
  ReleaseSharedCacheResourceRequestProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationId applicationId = null;

  public ReleaseSharedCacheResourceRequestPBImpl() {
    builder = ReleaseSharedCacheResourceRequestProto.newBuilder();
  }

  public ReleaseSharedCacheResourceRequestPBImpl(
      ReleaseSharedCacheResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ReleaseSharedCacheResourceRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public ApplicationId getAppId() {
    ReleaseSharedCacheResourceRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setAppId(ApplicationId id) {
    maybeInitBuilder();
    if (id == null)
      builder.clearApplicationId();
    this.applicationId = id;
  }

  @Override
  public String getResourceKey() {
    ReleaseSharedCacheResourceRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return (p.hasResourceKey()) ? p.getResourceKey() : null;
  }

  @Override
  public void setResourceKey(String key) {
    maybeInitBuilder();
    if (key == null) {
      builder.clearResourceKey();
      return;
    }
    builder.setResourceKey(key);
  }

  private void mergeLocalToBuilder() {
    if (applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
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
      builder = ReleaseSharedCacheResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

}
