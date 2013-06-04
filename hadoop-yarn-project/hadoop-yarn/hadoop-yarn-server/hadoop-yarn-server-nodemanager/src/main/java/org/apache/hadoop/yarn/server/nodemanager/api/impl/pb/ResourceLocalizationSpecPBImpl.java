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
package org.apache.hadoop.yarn.server.nodemanager.api.impl.pb;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceLocalizationSpecProtoOrBuilder;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;

public class ResourceLocalizationSpecPBImpl extends
    ProtoBase<ResourceLocalizationSpecProto> implements
    ResourceLocalizationSpec {

  private ResourceLocalizationSpecProto proto = ResourceLocalizationSpecProto
    .getDefaultInstance();
  private ResourceLocalizationSpecProto.Builder builder = null;
  private boolean viaProto;
  private LocalResource resource = null;
  private URL destinationDirectory = null;

  public ResourceLocalizationSpecPBImpl() {
    builder = ResourceLocalizationSpecProto.newBuilder();
  }

  public ResourceLocalizationSpecPBImpl(ResourceLocalizationSpecProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public LocalResource getResource() {
    ResourceLocalizationSpecProtoOrBuilder p = viaProto ? proto : builder;
    if (resource != null) {
      return resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    resource = new LocalResourcePBImpl(p.getResource());
    return resource;
  }

  @Override
  public void setResource(LocalResource rsrc) {
    maybeInitBuilder();
    resource = rsrc;
  }

  @Override
  public URL getDestinationDirectory() {
    ResourceLocalizationSpecProtoOrBuilder p = viaProto ? proto : builder;
    if (destinationDirectory != null) {
      return destinationDirectory;
    }
    if (!p.hasDestinationDirectory()) {
      return null;
    }
    destinationDirectory = new URLPBImpl(p.getDestinationDirectory());
    return destinationDirectory;
  }

  @Override
  public void setDestinationDirectory(URL destinationDirectory) {
    maybeInitBuilder();
    this.destinationDirectory = destinationDirectory;
  }

  @Override
  public ResourceLocalizationSpecProto getProto() {
    mergeLocalToBuilder();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (builder == null || viaProto) {
      builder = ResourceLocalizationSpecProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    ResourceLocalizationSpecProtoOrBuilder l = viaProto ? proto : builder;
    if (this.resource != null
        && !(l.getResource()
          .equals(((LocalResourcePBImpl) resource).getProto()))) {
      maybeInitBuilder();
      builder.setResource(((LocalResourcePBImpl) resource).getProto());
    }
    if (this.destinationDirectory != null
        && !(l.getDestinationDirectory()
          .equals(((URLPBImpl) destinationDirectory).getProto()))) {
      maybeInitBuilder();
      builder.setDestinationDirectory(((URLPBImpl) destinationDirectory)
        .getProto());
    }
  }
}