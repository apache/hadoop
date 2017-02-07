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
package org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.YarnTensorflowClusterProtos.GetClusterSpecResponseProto;
import org.apache.hadoop.yarn.proto.YarnTensorflowClusterProtos.GetClusterSpecResponseProtoOrBuilder;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecResponse;

public class GetClusterSpecResponsePBImpl extends GetClusterSpecResponse {
  GetClusterSpecResponseProto proto = GetClusterSpecResponseProto.getDefaultInstance();
  GetClusterSpecResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private String clusterSpec = null;

  public GetClusterSpecResponsePBImpl() {
    builder = GetClusterSpecResponseProto.newBuilder();
  }

  public GetClusterSpecResponsePBImpl(GetClusterSpecResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetClusterSpecResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
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
    if (this.clusterSpec != null) {
      builder.setClusterSpec(this.clusterSpec);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterSpecResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getClusterSpec() {
    GetClusterSpecResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.clusterSpec != null) {
      return this.clusterSpec;
    }
    if (!p.hasClusterSpec()) {
      return null;
    }
    this.clusterSpec = p.getClusterSpec();
    return this.clusterSpec;
  }

  @Override
  public void setClusterSpec(String clusterSpec) {
    maybeInitBuilder();
    if (clusterSpec == null) {
      builder.clearClusterSpec();
    }
    this.clusterSpec = clusterSpec;
  }
}
