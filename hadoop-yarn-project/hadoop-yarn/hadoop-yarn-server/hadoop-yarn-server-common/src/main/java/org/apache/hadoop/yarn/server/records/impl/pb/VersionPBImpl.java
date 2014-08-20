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

package org.apache.hadoop.yarn.server.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProtoOrBuilder;

import org.apache.hadoop.yarn.server.records.Version;

public class VersionPBImpl extends Version {

  VersionProto proto = VersionProto.getDefaultInstance();
  VersionProto.Builder builder = null;
  boolean viaProto = false;

  public VersionPBImpl() {
    builder = VersionProto.newBuilder();
  }

  public VersionPBImpl(VersionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public VersionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = VersionProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getMajorVersion() {
    VersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMajorVersion();
  }

  @Override
  public void setMajorVersion(int major) {
    maybeInitBuilder();
    builder.setMajorVersion(major);
  }

  @Override
  public int getMinorVersion() {
    VersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMinorVersion();
  }

  @Override
  public void setMinorVersion(int minor) {
    maybeInitBuilder();
    builder.setMinorVersion(minor);
  }
}
