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
package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;

@Private
@Unstable
public class RouterRMTokenResponsePBImpl extends RouterRMTokenResponse {

  private RouterRMTokenResponseProto proto = RouterRMTokenResponseProto.getDefaultInstance();
  private RouterRMTokenResponseProto.Builder builder = null;
  private boolean viaProto = false;
  private RouterStoreToken routerStoreToken = null;

  public RouterRMTokenResponsePBImpl() {
    builder = RouterRMTokenResponseProto.newBuilder();
  }

  public RouterRMTokenResponsePBImpl(RouterRMTokenResponseProto requestProto) {
    this.proto = requestProto;
    viaProto = true;
  }

  public RouterRMTokenResponseProto getProto() {
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
      builder = RouterRMTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.routerStoreToken != null) {
      RouterStoreTokenPBImpl routerStoreTokenPBImpl =
          (RouterStoreTokenPBImpl) this.routerStoreToken;
      RouterStoreTokenProto storeTokenProto = routerStoreTokenPBImpl.getProto();
      if (!storeTokenProto.equals(builder.getRouterStoreToken())) {
        builder.setRouterStoreToken(convertToProtoFormat(this.routerStoreToken));
      }
    }
  }

  @Override
  public RouterStoreToken getRouterStoreToken() {
    RouterRMTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.routerStoreToken != null) {
      return this.routerStoreToken;
    }
    if (!p.hasRouterStoreToken()) {
      return null;
    }
    this.routerStoreToken = convertFromProtoFormat(p.getRouterStoreToken());
    return this.routerStoreToken;
  }

  @Override
  public void setRouterStoreToken(RouterStoreToken storeToken) {
    maybeInitBuilder();
    if (storeToken == null) {
      builder.clearRouterStoreToken();
    }
    this.routerStoreToken = storeToken;
  }

  private RouterStoreTokenProto convertToProtoFormat(RouterStoreToken storeToken) {
    return ((RouterStoreTokenPBImpl) storeToken).getProto();
  }

  private RouterStoreToken convertFromProtoFormat(RouterStoreTokenProto storeTokenProto) {
    return new RouterStoreTokenPBImpl(storeTokenProto);
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
}
