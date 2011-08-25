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


import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceVisibilityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.util.ProtoUtils;


    
public class LocalResourcePBImpl extends ProtoBase<LocalResourceProto> implements LocalResource {
  LocalResourceProto proto = LocalResourceProto.getDefaultInstance();
  LocalResourceProto.Builder builder = null;
  boolean viaProto = false;
  
  private URL url = null;
  
  
  public LocalResourcePBImpl() {
    builder = LocalResourceProto.newBuilder();
  }

  public LocalResourcePBImpl(LocalResourceProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public LocalResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.url != null) {
      builder.setResource(convertToProtoFormat(this.url));
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
      builder = LocalResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public long getSize() {
    LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSize());
  }

  @Override
  public void setSize(long size) {
    maybeInitBuilder();
    builder.setSize((size));
  }
  @Override
  public long getTimestamp() {
    LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getTimestamp());
  }

  @Override
  public void setTimestamp(long timestamp) {
    maybeInitBuilder();
    builder.setTimestamp((timestamp));
  }
  @Override
  public LocalResourceType getType() {
    LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasType()) {
      return null;
    }
    return convertFromProtoFormat(p.getType());
  }

  @Override
  public void setType(LocalResourceType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
      return;
    }
    builder.setType(convertToProtoFormat(type));
  }
  @Override
  public URL getResource() {
    LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
    if (this.url != null) {
      return this.url;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.url = convertFromProtoFormat(p.getResource());
    return this.url;
  }

  @Override
  public void setResource(URL resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.url = resource;
  }
  @Override
  public LocalResourceVisibility getVisibility() {
    LocalResourceProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasVisibility()) {
      return null;
    }
    return convertFromProtoFormat(p.getVisibility());
  }

  @Override
  public void setVisibility(LocalResourceVisibility visibility) {
    maybeInitBuilder();
    if (visibility == null) {
      builder.clearVisibility();
      return;
    }
    builder.setVisibility(convertToProtoFormat(visibility));
  }

  private LocalResourceTypeProto convertToProtoFormat(LocalResourceType e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private LocalResourceType convertFromProtoFormat(LocalResourceTypeProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private URLPBImpl convertFromProtoFormat(URLProto p) {
    return new URLPBImpl(p);
  }

  private URLProto convertToProtoFormat(URL t) {
    return ((URLPBImpl)t).getProto();
  }

  private LocalResourceVisibilityProto convertToProtoFormat(LocalResourceVisibility e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private LocalResourceVisibility convertFromProtoFormat(LocalResourceVisibilityProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }



}  
