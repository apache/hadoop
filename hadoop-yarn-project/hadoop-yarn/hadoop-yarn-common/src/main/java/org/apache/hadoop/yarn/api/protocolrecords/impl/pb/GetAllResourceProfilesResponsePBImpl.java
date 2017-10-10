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
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProfilesProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProfileEntry;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceProfilesResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetAllResourceProfilesResponseProto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Protobuf implementation class for the GetAllResourceProfilesResponse.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GetAllResourceProfilesResponsePBImpl
    extends GetAllResourceProfilesResponse {

  private GetAllResourceProfilesResponseProto proto =
      GetAllResourceProfilesResponseProto.getDefaultInstance();
  private GetAllResourceProfilesResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private Map<String, Resource> profiles;

  public GetAllResourceProfilesResponsePBImpl() {
    builder = GetAllResourceProfilesResponseProto.newBuilder();
  }

  public GetAllResourceProfilesResponsePBImpl(
      GetAllResourceProfilesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetAllResourceProfilesResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetAllResourceProfilesResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (profiles != null) {
      addProfilesToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void addProfilesToProto() {
    maybeInitBuilder();
    builder.clearResourceProfiles();
    if (profiles == null) {
      return;
    }
    ResourceProfilesProto.Builder profilesBuilder =
        ResourceProfilesProto.newBuilder();
    for (Map.Entry<String, Resource> entry : profiles.entrySet()) {
      ResourceProfileEntry.Builder profileEntry =
          ResourceProfileEntry.newBuilder();
      profileEntry.setName(entry.getKey());
      profileEntry.setResources(convertToProtoFormat(entry.getValue()));
      profilesBuilder.addResourceProfilesMap(profileEntry);
    }
    builder.setResourceProfiles(profilesBuilder.build());
  }

  public void setResourceProfiles(Map<String, Resource> resourceProfiles) {
    initResourceProfiles();
    profiles.clear();
    profiles.putAll(resourceProfiles);
  }

  public Map<String, Resource> getResourceProfiles() {
    initResourceProfiles();
    return profiles;
  }

  private void initResourceProfiles() {
    if (profiles != null) {
      return;
    }
    profiles = new HashMap<>();
    GetAllResourceProfilesResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceProfileEntry> profilesList =
        p.getResourceProfiles().getResourceProfilesMapList();
    for (ResourceProfileEntry entry : profilesList) {
      profiles.put(entry.getName(), new ResourcePBImpl(entry.getResources()));
    }
  }

  private ResourceProto convertToProtoFormat(Resource res) {
    return ProtoUtils.convertToProtoFormat(res);
  }
}
