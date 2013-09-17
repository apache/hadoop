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

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.URLPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.URLProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalResourceStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.LocalResourceStatusProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerServiceProtos.ResourceStatusTypeProto;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;

public class LocalResourceStatusPBImpl
  extends ProtoBase<LocalResourceStatusProto> implements LocalResourceStatus {

  LocalResourceStatusProto proto =
    LocalResourceStatusProto.getDefaultInstance();
  LocalResourceStatusProto.Builder builder = null;
  boolean viaProto = false;

  private LocalResource resource;
  private URL localPath;
  private SerializedException exception;

  public LocalResourceStatusPBImpl() {
    builder = LocalResourceStatusProto.newBuilder();
  }

  public LocalResourceStatusPBImpl(LocalResourceStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public LocalResourceStatusProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.resource != null &&
        !((LocalResourcePBImpl)this.resource).getProto()
          .equals(builder.getResource())) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.localPath != null &&
        !((URLPBImpl)this.localPath).getProto()
          .equals(builder.getLocalPath())) {
      builder.setLocalPath(convertToProtoFormat(this.localPath));
    }
    if (this.exception != null &&
        !((SerializedExceptionPBImpl)this.exception).getProto()
          .equals(builder.getException())) {
      builder.setException(convertToProtoFormat(this.exception));
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
      builder = LocalResourceStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public LocalResource getResource() {
    LocalResourceStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public ResourceStatusType getStatus() {
    LocalResourceStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getStatus());
  }

  @Override
  public URL getLocalPath() {
    LocalResourceStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.localPath != null) {
      return this.localPath;
    }
    if (!p.hasLocalPath()) {
      return null;
    }
    this.localPath = convertFromProtoFormat(p.getLocalPath());
    return this.localPath;
  }

  @Override
  public long getLocalSize() {
    LocalResourceStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getLocalSize());
  }

  @Override
  public SerializedException getException() {
    LocalResourceStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.exception != null) {
      return this.exception;
    }
    if (!p.hasException()) {
      return null;
    }
    this.exception = convertFromProtoFormat(p.getException());
    return this.exception;
  }


  @Override
  public void setResource(LocalResource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  @Override
  public void setStatus(ResourceStatusType status) {
    maybeInitBuilder();
    if (status == null) {
      builder.clearStatus();
      return;
    }
    builder.setStatus(convertToProtoFormat(status));
  }

  @Override
  public void setLocalPath(URL localPath) {
    maybeInitBuilder();
    if (localPath == null)
      builder.clearLocalPath();
    this.localPath = localPath;
  }

  @Override
  public void setLocalSize(long size) {
    maybeInitBuilder();
    builder.setLocalSize(size);
  }

  @Override
  public void setException(SerializedException exception) {
    maybeInitBuilder();
    if (exception == null)
      builder.clearException();
    this.exception = exception;
  }

  private LocalResourceProto convertToProtoFormat(LocalResource rsrc) {
    return ((LocalResourcePBImpl)rsrc).getProto();
  }

  private LocalResourcePBImpl convertFromProtoFormat(LocalResourceProto rsrc) {
    return new LocalResourcePBImpl(rsrc);
  }

  private URLPBImpl convertFromProtoFormat(URLProto p) {
    return new URLPBImpl(p);
  }

  private URLProto convertToProtoFormat(URL t) {
    return ((URLPBImpl)t).getProto();
  }

  private ResourceStatusTypeProto convertToProtoFormat(ResourceStatusType e) {
    return ResourceStatusTypeProto.valueOf(e.name());
  }

  private ResourceStatusType convertFromProtoFormat(ResourceStatusTypeProto e) {
    return ResourceStatusType.valueOf(e.name());
  }

  private SerializedExceptionPBImpl convertFromProtoFormat(SerializedExceptionProto p) {
    return new SerializedExceptionPBImpl(p);
  }

  private SerializedExceptionProto convertToProtoFormat(SerializedException t) {
    return ((SerializedExceptionPBImpl)t).getProto();
  }

}
