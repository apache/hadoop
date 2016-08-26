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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ResourceLocalizationRequestProto;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ResourceLocalizationRequestPBImpl
    extends ResourceLocalizationRequest {
  private ResourceLocalizationRequestProto proto =
      ResourceLocalizationRequestProto.getDefaultInstance();
  private ResourceLocalizationRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private Map<String, LocalResource> localResources = null;
  private ContainerId containerId;

  public ResourceLocalizationRequestPBImpl() {
    builder = ResourceLocalizationRequestProto.newBuilder();
  }

  public ResourceLocalizationRequestPBImpl(
      ResourceLocalizationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
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
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.localResources != null) {
      addLocalResourcesToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceLocalizationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalResourcesToProto() {
    maybeInitBuilder();
    builder.clearLocalResources();
    if (localResources == null) {
      return;
    }
    Iterable<YarnProtos.StringLocalResourceMapProto> iterable =
        new Iterable<YarnProtos.StringLocalResourceMapProto>() {

          @Override
          public Iterator<YarnProtos.StringLocalResourceMapProto> iterator() {
            return new Iterator<YarnProtos.StringLocalResourceMapProto>() {

              Iterator<String> keyIter = localResources.keySet().iterator();

              @Override public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override public YarnProtos.StringLocalResourceMapProto next() {
                String key = keyIter.next();
                return YarnProtos.StringLocalResourceMapProto.newBuilder()
                    .setKey(key).
                        setValue(convertToProtoFormat(localResources.get(key)))
                    .build();
              }

              @Override public boolean hasNext() {
                return keyIter.hasNext();
              }
            };
          }
        };
    builder.addAllLocalResources(iterable);
  }

  public ResourceLocalizationRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void initLocalResources() {
    if (this.localResources != null) {
      return;
    }
    YarnServiceProtos.ResourceLocalizationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<YarnProtos.StringLocalResourceMapProto> list =
        p.getLocalResourcesList();
    this.localResources = new HashMap<>();

    for (YarnProtos.StringLocalResourceMapProto c : list) {
      this.localResources.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }

  private LocalResourcePBImpl convertFromProtoFormat(
      YarnProtos.LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private ContainerIdPBImpl convertFromProtoFormat(
      YarnProtos.ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private YarnProtos.ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private YarnProtos.LocalResourceProto convertToProtoFormat(LocalResource t) {
    return ((LocalResourcePBImpl) t).getProto();
  }

  @Override
  public ContainerId getContainerId() {
    YarnServiceProtos.ResourceLocalizationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public Map<String, LocalResource> getLocalResources() {
    initLocalResources();
    return this.localResources;
  }

  @Override
  public void setLocalResources(Map<String, LocalResource> localResources) {
    if (localResources == null) {
      this.localResources = null;
      builder.clearLocalResources();
      return;
    }
    this.localResources = new HashMap<>(localResources);
  }
}
