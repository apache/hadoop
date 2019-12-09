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
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetResourceProfileRequestProto;

/**
 * Protobuf implementation for the GetResourceProfileRequest class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GetResourceProfileRequestPBImpl extends GetResourceProfileRequest {

  private GetResourceProfileRequestProto proto =
      GetResourceProfileRequestProto.getDefaultInstance();
  private GetResourceProfileRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private String profile;

  public GetResourceProfileRequestPBImpl() {
    builder = GetResourceProfileRequestProto.newBuilder();
  }

  public GetResourceProfileRequestPBImpl(GetResourceProfileRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetResourceProfileRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public void setProfileName(String profileName) {
    this.profile = profileName;
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
    if (profile != null) {
      builder.setProfile(profile);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetResourceProfileRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getProfileName() {
    if (this.profile != null) {
      return profile;
    }
    GetResourceProfileRequestProtoOrBuilder protoOrBuilder =
        viaProto ? proto : builder;
    if (protoOrBuilder.hasProfile()) {
      profile = protoOrBuilder.getProfile();
    }
    return profile;
  }
}
