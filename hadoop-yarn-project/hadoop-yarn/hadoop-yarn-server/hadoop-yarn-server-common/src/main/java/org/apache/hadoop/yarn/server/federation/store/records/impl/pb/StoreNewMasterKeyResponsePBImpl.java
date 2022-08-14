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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.StoreNewMasterKeyResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.StoreNewMasterKeyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.StoreNewMasterKeyResponse;

@Private
@Unstable
public class StoreNewMasterKeyResponsePBImpl extends StoreNewMasterKeyResponse {

  private StoreNewMasterKeyResponseProto proto =
      StoreNewMasterKeyResponseProto.getDefaultInstance();

  private StoreNewMasterKeyResponseProto.Builder builder = null;

  private boolean viaProto = false;

  private RouterMasterKey routerMasterKey = null;

  public StoreNewMasterKeyResponsePBImpl() {
    builder = StoreNewMasterKeyResponseProto.newBuilder();
  }

  public StoreNewMasterKeyResponsePBImpl(StoreNewMasterKeyResponseProto responseProto) {
    this.proto = responseProto;
    viaProto = true;
  }

  public StoreNewMasterKeyResponseProto getProto() {
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
      builder = StoreNewMasterKeyResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.routerMasterKey != null && !((RouterMasterKeyPBImpl) this.routerMasterKey).
        getProto().equals(builder.getRouterMasterKey())) {
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

  private RouterMasterKeyProto convertToProtoFormat(
      RouterMasterKey masterKey) {
    return ((RouterMasterKeyPBImpl) masterKey).getProto();
  }

  @Override
  public RouterMasterKey getRouterMasterKey() {
    StoreNewMasterKeyResponseProtoOrBuilder p = viaProto ? proto : builder;
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

  private RouterMasterKey convertFromProtoFormat(RouterMasterKeyProto masterKey) {
    return new RouterMasterKeyPBImpl(masterKey);
  }
}
