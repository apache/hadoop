/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RemoveStoredMasterKeyRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RemoveStoredMasterKeyRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.RemoveStoredMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;

@Private
@Unstable
public class RemoveStoredMasterKeyRequestPBImpl extends RemoveStoredMasterKeyRequest {

  private RemoveStoredMasterKeyRequestProto proto =
      RemoveStoredMasterKeyRequestProto.getDefaultInstance();
  private RemoveStoredMasterKeyRequestProto.Builder builder = null;
  private boolean viaProto = false;
  private RouterMasterKey routerMasterKey = null;

  public RemoveStoredMasterKeyRequestPBImpl() {
    builder = RemoveStoredMasterKeyRequestProto.newBuilder();
  }

  public RemoveStoredMasterKeyRequestPBImpl(RemoveStoredMasterKeyRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RemoveStoredMasterKeyRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RemoveStoredMasterKeyRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    RouterMasterKeyPBImpl masterKeyRequest =
        (RouterMasterKeyPBImpl) this.routerMasterKey;
    RouterMasterKeyProto routerMasterKeyProto = builder.getRouterMasterKey();
    if (this.routerMasterKey != null && !masterKeyRequest.getProto().equals(routerMasterKeyProto)) {
      builder.setRouterMasterKey(convertToProtoFormat(this.routerMasterKey));
    }
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

  @Override
  public RouterMasterKey getRouterMasterKey() {
    RemoveStoredMasterKeyRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.routerMasterKey != null) {
      return this.routerMasterKey;
    }
    if (!p.hasRouterMasterKey()) {
      return null;
    }
    this.routerMasterKey = convertFromProtoFormat(p.getRouterMasterKey());
    return this.routerMasterKey;
  }

  @Override
  public void setRouterMasterKey(RouterMasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) {
      builder.clearRouterMasterKey();
      return;
    }
    this.routerMasterKey = masterKey;
  }

  private RouterMasterKey convertFromProtoFormat(RouterMasterKeyProto masterKeyProto) {
    return new RouterMasterKeyPBImpl(masterKeyProto);
  }

  private RouterMasterKeyProto convertToProtoFormat(RouterMasterKey masterKey) {
    return ((RouterMasterKeyPBImpl) masterKey).getProto();
  }
}
