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

import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UseSharedCacheResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UseSharedCacheResourceResponseProtoOrBuilder;

public class UseSharedCacheResourceResponsePBImpl extends
    UseSharedCacheResourceResponse {
  UseSharedCacheResourceResponseProto proto =
      UseSharedCacheResourceResponseProto
      .getDefaultInstance();
  UseSharedCacheResourceResponseProto.Builder builder = null;
  boolean viaProto = false;

  public UseSharedCacheResourceResponsePBImpl() {
    builder = UseSharedCacheResourceResponseProto.newBuilder();
  }

  public UseSharedCacheResourceResponsePBImpl(
      UseSharedCacheResourceResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UseSharedCacheResourceResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getPath() {
    UseSharedCacheResourceResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasPath()) ? p.getPath() : null;
  }

  @Override
  public void setPath(String path) {
    maybeInitBuilder();
    if (path == null) {
      builder.clearPath();
      return;
    }
    builder.setPath(path);
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UseSharedCacheResourceResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
