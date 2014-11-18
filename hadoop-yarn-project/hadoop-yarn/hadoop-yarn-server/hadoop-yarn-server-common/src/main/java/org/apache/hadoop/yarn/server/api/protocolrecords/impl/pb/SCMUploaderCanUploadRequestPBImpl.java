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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;

public class SCMUploaderCanUploadRequestPBImpl
    extends SCMUploaderCanUploadRequest {
  SCMUploaderCanUploadRequestProto proto =
      SCMUploaderCanUploadRequestProto.getDefaultInstance();
  SCMUploaderCanUploadRequestProto.Builder builder = null;
  boolean viaProto = false;

  public SCMUploaderCanUploadRequestPBImpl() {
    builder = SCMUploaderCanUploadRequestProto.newBuilder();
  }

  public SCMUploaderCanUploadRequestPBImpl(
      SCMUploaderCanUploadRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SCMUploaderCanUploadRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public String getResourceKey() {
    SCMUploaderCanUploadRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderCanUploadRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
