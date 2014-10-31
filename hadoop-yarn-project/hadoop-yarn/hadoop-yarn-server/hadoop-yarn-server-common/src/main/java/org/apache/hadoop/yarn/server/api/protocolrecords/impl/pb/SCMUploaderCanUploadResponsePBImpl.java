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

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;

public class SCMUploaderCanUploadResponsePBImpl
    extends SCMUploaderCanUploadResponse {
  SCMUploaderCanUploadResponseProto proto =
      SCMUploaderCanUploadResponseProto.getDefaultInstance();
  SCMUploaderCanUploadResponseProto.Builder builder = null;
  boolean viaProto = false;

  public SCMUploaderCanUploadResponsePBImpl() {
    builder = SCMUploaderCanUploadResponseProto.newBuilder();
  }

  public SCMUploaderCanUploadResponsePBImpl(
      SCMUploaderCanUploadResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SCMUploaderCanUploadResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public boolean getUploadable() {
    SCMUploaderCanUploadResponseProtoOrBuilder p = viaProto ? proto : builder;
    // Default to true, when in doubt allow the upload
    return (p.hasUploadable()) ? p.getUploadable() : true;
  }

  @Override
  public void setUploadable(boolean b) {
    maybeInitBuilder();
    builder.setUploadable(b);
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SCMUploaderCanUploadResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
