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
package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerActionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.ResourceLocalizationSpecPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;

public class LocalizerHeartbeatResponsePBImpl
        extends ProtoBase<LocalizerHeartbeatResponseProto>
        implements LocalizerHeartbeatResponse {

  LocalizerHeartbeatResponseProto proto =
    LocalizerHeartbeatResponseProto.getDefaultInstance();
  LocalizerHeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;

  private List<ResourceLocalizationSpec> resourceSpecs;

  public LocalizerHeartbeatResponsePBImpl() {
    builder = LocalizerHeartbeatResponseProto.newBuilder();
  }

  public LocalizerHeartbeatResponsePBImpl(
      LocalizerHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public LocalizerHeartbeatResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (resourceSpecs != null) {
      addResourcesToProto();
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
      builder = LocalizerHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public LocalizerAction getLocalizerAction() {
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAction()) {
      return null;
    }
    return convertFromProtoFormat(p.getAction());
  }

  @Override
  public List<ResourceLocalizationSpec> getResourceSpecs() {
    initResources();
    return this.resourceSpecs;
  }

  public void setLocalizerAction(LocalizerAction action) {
    maybeInitBuilder();
    if (action == null) {
      builder.clearAction();
      return;
    }
    builder.setAction(convertToProtoFormat(action));
  }

  public void setResourceSpecs(List<ResourceLocalizationSpec> rsrcs) {
    maybeInitBuilder();
    if (rsrcs == null) {
      builder.clearResources();
      return;
    }
    this.resourceSpecs = rsrcs;
  }

  private void initResources() {
    if (this.resourceSpecs != null) {
      return;
    }
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceLocalizationSpecProto> list = p.getResourcesList();
    this.resourceSpecs = new ArrayList<ResourceLocalizationSpec>();
    for (ResourceLocalizationSpecProto c : list) {
      this.resourceSpecs.add(convertFromProtoFormat(c));
    }
  }

  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resourceSpecs == null) 
      return;
    Iterable<ResourceLocalizationSpecProto> iterable =
        new Iterable<ResourceLocalizationSpecProto>() {
      @Override
      public Iterator<ResourceLocalizationSpecProto> iterator() {
        return new Iterator<ResourceLocalizationSpecProto>() {

          Iterator<ResourceLocalizationSpec> iter = resourceSpecs.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceLocalizationSpecProto next() {
            ResourceLocalizationSpec resource = iter.next();
            
            return ((ResourceLocalizationSpecPBImpl)resource).getProto();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllResources(iterable);
  }


  private ResourceLocalizationSpec convertFromProtoFormat(
      ResourceLocalizationSpecProto p) {
    return new ResourceLocalizationSpecPBImpl(p);
  }

  private LocalizerActionProto convertToProtoFormat(LocalizerAction a) {
    return LocalizerActionProto.valueOf(a.name());
  }

  private LocalizerAction convertFromProtoFormat(LocalizerActionProto a) {
    return LocalizerAction.valueOf(a.name());
  }
}