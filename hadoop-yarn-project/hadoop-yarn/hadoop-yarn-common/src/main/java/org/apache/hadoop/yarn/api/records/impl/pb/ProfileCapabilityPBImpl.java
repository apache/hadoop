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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ProfileCapability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ProfileCapabilityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ProfileCapabilityProtoOrBuilder;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Protobuf implementation for the ProfileCapability class.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProfileCapabilityPBImpl extends ProfileCapability {

  private ProfileCapabilityProto proto =
      ProfileCapabilityProto.getDefaultInstance();
  private ProfileCapabilityProto.Builder builder;

  private boolean viaProto;

  private String profile;
  private Resource profileCapabilityOverride;

  public ProfileCapabilityPBImpl() {
    builder = ProfileCapabilityProto.newBuilder();
  }

  public ProfileCapabilityPBImpl(ProfileCapabilityProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public String getProfileName() {
    if (profile != null) {
      return profile;
    }
    ProfileCapabilityProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasProfile()) {
      profile = p.getProfile();
    }
    return profile;
  }

  @Override
  public Resource getProfileCapabilityOverride() {
    if (profileCapabilityOverride != null) {
      return profileCapabilityOverride;
    }
    ProfileCapabilityProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasProfileCapabilityOverride()) {
      profileCapabilityOverride =
          Resources.clone(new ResourcePBImpl(p.getProfileCapabilityOverride()));
    }
    return profileCapabilityOverride;
  }

  @Override
  public void setProfileName(String profileName) {
    this.profile = profileName;
  }

  @Override
  public void setProfileCapabilityOverride(Resource r) {
    this.profileCapabilityOverride = r;
  }

  public ProfileCapabilityProto getProto() {
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
    if (profile != null) {
      builder.setProfile(profile);
    }
    if (profileCapabilityOverride != null) {
      builder.setProfileCapabilityOverride(
          convertToProtoFormat(profileCapabilityOverride));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ProfileCapabilityProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private YarnProtos.ResourceProto convertToProtoFormat(Resource res) {
    return ProtoUtils.convertToProtoFormat(res);
  }
}
