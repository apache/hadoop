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

import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RemoveStoredMasterKeyResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RemoveStoredMasterKeyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.RemoveStoredMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;

public class RemoveStoredMasterKeyResponsePBImpl extends RemoveStoredMasterKeyResponse {

  private RemoveStoredMasterKeyResponseProto proto =
      RemoveStoredMasterKeyResponseProto.getDefaultInstance();

  private RemoveStoredMasterKeyResponseProto.Builder builder = null;

  private boolean viaProto = false;

  private RouterMasterKey routerMasterKey = null;

  public RemoveStoredMasterKeyResponsePBImpl() {
    builder = RemoveStoredMasterKeyResponseProto.newBuilder();
  }

  public RemoveStoredMasterKeyResponsePBImpl(RemoveStoredMasterKeyResponseProto responseProto) {
    this.proto = responseProto;
    viaProto = true;
  }

  public RemoveStoredMasterKeyResponseProto getProto() {
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
      builder = RemoveStoredMasterKeyResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.routerMasterKey != null && !((RouterMasterKeyPBImpl) this.routerMasterKey).
            getProto().equals(builder.getRouterMasterKey())) {
      builder.setRouterMasterKey(convertToProtoFormat(this.routerMasterKey));
    }
  }

  private YarnServerFederationProtos.RouterMasterKeyProto convertToProtoFormat(
      RouterMasterKey masterKey) {
    return ((RouterMasterKeyPBImpl) masterKey).getProto();
  }


  @Override
  public RouterMasterKey getRouterMasterKey() {
    RemoveStoredMasterKeyResponseProtoOrBuilder p = viaProto ? proto : builder;
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
