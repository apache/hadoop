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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileResponseProtoOrBuilder;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Protobuf implementation for the GetResourceProfileResponse class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GetResourceProfileResponsePBImpl
    extends GetResourceProfileResponse {

  private GetResourceProfileResponseProto proto =
      GetResourceProfileResponseProto.getDefaultInstance();
  private GetResourceProfileResponseProto.Builder builder = null;
  private boolean viaProto = false;

  private Resource resource;

  public GetResourceProfileResponsePBImpl() {
    builder = GetResourceProfileResponseProto.newBuilder();
  }

  public GetResourceProfileResponsePBImpl(
      GetResourceProfileResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public Resource getResource() {
    if (resource != null) {
      return resource;
    }
    GetResourceProfileResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasResources()) {
      resource = Resource.newInstance(p.getResources().getMemory(),
          p.getResources().getVirtualCores());
    }
    return resource;
  }

  public void setResource(Resource r) {
    resource = Resources.clone(r);
  }

  public GetResourceProfileResponseProto getProto() {
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
    if (resource != null) {
      builder.setResources(convertToProtoFormat(resource));
    }
  }

  private ResourceProto convertToProtoFormat(Resource res) {
    return ProtoUtils.convertToProtoFormat(res);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetResourceProfileResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
}
