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
package org.apache.hadoop.yarn.server.nodemanager.recovery.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.NMDBSchemaVersionProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.NMDBSchemaVersionProtoOrBuilder;

import org.apache.hadoop.yarn.server.nodemanager.recovery.records.NMDBSchemaVersion;

@Private
@Evolving
public class NMDBSchemaVersionPBImpl extends NMDBSchemaVersion {

  NMDBSchemaVersionProto proto = NMDBSchemaVersionProto.getDefaultInstance();
  NMDBSchemaVersionProto.Builder builder = null;
  boolean viaProto = false;

  public NMDBSchemaVersionPBImpl() {
    builder = NMDBSchemaVersionProto.newBuilder();
  }

  public NMDBSchemaVersionPBImpl(NMDBSchemaVersionProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public NMDBSchemaVersionProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NMDBSchemaVersionProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public int getMajorVersion() {
    NMDBSchemaVersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMajorVersion();
  }

  @Override
  public void setMajorVersion(int majorVersion) {
    maybeInitBuilder();
    builder.setMajorVersion(majorVersion);
  }

  @Override
  public int getMinorVersion() {
    NMDBSchemaVersionProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMinorVersion();
  }

  @Override
  public void setMinorVersion(int minorVersion) {
    maybeInitBuilder();
    builder.setMinorVersion(minorVersion);
  }

}
