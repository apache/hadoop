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
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalResourceStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalizerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;

public class LocalizerStatusPBImpl
    extends ProtoBase<LocalizerStatusProto> implements LocalizerStatus {

  LocalizerStatusProto proto =
    LocalizerStatusProto.getDefaultInstance();
  LocalizerStatusProto.Builder builder = null;
  boolean viaProto = false;

  private List<LocalResourceStatus> resources = null;

  public LocalizerStatusPBImpl() {
    builder = LocalizerStatusProto.newBuilder();
  }

  public LocalizerStatusPBImpl(LocalizerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public LocalizerStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resources != null) {
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
      builder = LocalizerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getLocalizerId() {
    LocalizerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLocalizerId()) {
      return null;
    }
    return (p.getLocalizerId());
  }

  @Override
  public List<LocalResourceStatus> getResources() {
    initResources();
    return this.resources;
  }

  @Override
  public void setLocalizerId(String localizerId) {
    maybeInitBuilder();
    if (localizerId == null) {
      builder.clearLocalizerId();
      return;
    }
    builder.setLocalizerId(localizerId);
  }

  private void initResources() {
    if (this.resources != null) {
      return;
    }
    LocalizerStatusProtoOrBuilder p = viaProto ? proto : builder;
    List<LocalResourceStatusProto> list = p.getResourcesList();
    this.resources = new ArrayList<LocalResourceStatus>();

    for (LocalResourceStatusProto c : list) {
      this.resources.add(convertFromProtoFormat(c));
    }
  }

  private void addResourcesToProto() {
    maybeInitBuilder();
    builder.clearResources();
    if (this.resources == null) 
      return;
    Iterable<LocalResourceStatusProto> iterable =
        new Iterable<LocalResourceStatusProto>() {
      @Override
      public Iterator<LocalResourceStatusProto> iterator() {
        return new Iterator<LocalResourceStatusProto>() {

          Iterator<LocalResourceStatus> iter = resources.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public LocalResourceStatusProto next() {
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

  @Override
  public void addAllResources(List<LocalResourceStatus> resources) {
    if (resources == null)
      return;
    initResources();
    this.resources.addAll(resources);
  }

  @Override
  public LocalResourceStatus getResourceStatus(int index) {
    initResources();
    return this.resources.get(index);
  }

  @Override
  public void addResourceStatus(LocalResourceStatus resource) {
    initResources();
    this.resources.add(resource);
  }

  @Override
  public void removeResource(int index) {
    initResources();
    this.resources.remove(index);
  }

  @Override
  public void clearResources() {
    initResources();
    this.resources.clear();
  }

  private LocalResourceStatus
      convertFromProtoFormat(LocalResourceStatusProto p) {
    return new LocalResourceStatusPBImpl(p);
  }

  private LocalResourceStatusProto convertToProtoFormat(LocalResourceStatus s) {
    return ((LocalResourceStatusPBImpl)s).getProto();
  }

}
