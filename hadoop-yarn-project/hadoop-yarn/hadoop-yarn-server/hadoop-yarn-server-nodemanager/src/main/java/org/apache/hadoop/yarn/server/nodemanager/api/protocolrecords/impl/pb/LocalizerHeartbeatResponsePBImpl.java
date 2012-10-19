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

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerActionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;

public class LocalizerHeartbeatResponsePBImpl
        extends ProtoBase<LocalizerHeartbeatResponseProto>
        implements LocalizerHeartbeatResponse {

  LocalizerHeartbeatResponseProto proto =
    LocalizerHeartbeatResponseProto.getDefaultInstance();
  LocalizerHeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;

  private List<LocalResource> resources;

  public LocalizerHeartbeatResponsePBImpl() {
    builder = LocalizerHeartbeatResponseProto.newBuilder();
  }

  public LocalizerHeartbeatResponsePBImpl(LocalizerHeartbeatResponseProto proto) {
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
    if (resources != null) {
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

  public LocalizerAction getLocalizerAction() {
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAction()) {
      return null;
    }
    return convertFromProtoFormat(p.getAction());
  }

  public List<LocalResource> getAllResources() {
    initResources();
    return this.resources;
  }

  public LocalResource getLocalResource(int i) {
    initResources();
    return this.resources.get(i);
  }

  public void setLocalizerAction(LocalizerAction action) {
    maybeInitBuilder();
    if (action == null) {
      builder.clearAction();
      return;
    }
    builder.setAction(convertToProtoFormat(action));
  }

  private void initResources() {
    if (this.resources != null) {
      return;
    }
    LocalizerHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<LocalResourceProto> list = p.getResourcesList();
    this.resources = new ArrayList<LocalResource>();

    for (LocalResourceProto c : list) {
      this.resources.add(convertFromProtoFormat(c));
    }
  }

  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resources == null) 
      return;
    Iterable<LocalResourceProto> iterable =
        new Iterable<LocalResourceProto>() {
      @Override
      public Iterator<LocalResourceProto> iterator() {
        return new Iterator<LocalResourceProto>() {

          Iterator<LocalResource> iter = resources.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public LocalResourceProto next() {
            return convertToProtoFormat(iter.next());
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

  public void addAllResources(List<LocalResource> resources) {
    if (resources == null)
      return;
    initResources();
    this.resources.addAll(resources);
  }

  public void addResource(LocalResource resource) {
    initResources();
    this.resources.add(resource);
  }

  public void removeResource(int index) {
    initResources();
    this.resources.remove(index);
  }

  public void clearResources() {
    initResources();
    this.resources.clear();
  }

  private LocalResource convertFromProtoFormat(LocalResourceProto p) {
    return new LocalResourcePBImpl(p);
  }

  private LocalResourceProto convertToProtoFormat(LocalResource s) {
    return ((LocalResourcePBImpl)s).getProto();
  }

  private LocalizerActionProto convertToProtoFormat(LocalizerAction a) {
    return LocalizerActionProto.valueOf(a.name());
  }

  private LocalizerAction convertFromProtoFormat(LocalizerActionProto a) {
    return LocalizerAction.valueOf(a.name());
  }

}
