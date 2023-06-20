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
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProtoOrBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Protocol buffer based implementation of {@link RouterStoreToken}.
 */
@Private
@Unstable
public class RouterStoreTokenPBImpl extends RouterStoreToken {

  private RouterStoreTokenProto proto = RouterStoreTokenProto.getDefaultInstance();

  private RouterStoreTokenProto.Builder builder = null;

  private boolean viaProto = false;

  private YARNDelegationTokenIdentifier rMDelegationTokenIdentifier = null;
  private Long renewDate;
  private String tokenInfo;

  public RouterStoreTokenPBImpl() {
    builder = RouterStoreTokenProto.newBuilder();
  }

  public RouterStoreTokenPBImpl(RouterStoreTokenProto storeTokenProto) {
    this.proto = storeTokenProto;
    viaProto = true;
  }

  public RouterStoreTokenProto getProto() {
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

  private void mergeLocalToBuilder() {
    if (this.rMDelegationTokenIdentifier != null) {
      YARNDelegationTokenIdentifierProto idProto = this.rMDelegationTokenIdentifier.getProto();
      if (!idProto.equals(builder.getTokenIdentifier())) {
        builder.setTokenIdentifier(convertToProtoFormat(this.rMDelegationTokenIdentifier));
      }
    }

    if (this.renewDate != null) {
      builder.setRenewDate(this.renewDate);
    }

    if (this.tokenInfo != null) {
      builder.setTokenInfo(this.tokenInfo);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterStoreTokenProto.newBuilder(proto);
    }
    viaProto = false;
  }

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
  public YARNDelegationTokenIdentifier getTokenIdentifier() throws IOException {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (rMDelegationTokenIdentifier != null) {
      return rMDelegationTokenIdentifier;
    }
    if(!p.hasTokenIdentifier()){
      return null;
    }
    YARNDelegationTokenIdentifierProto identifierProto = p.getTokenIdentifier();
    ByteArrayInputStream in = new ByteArrayInputStream(identifierProto.toByteArray());
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier();
    identifier.readFields(new DataInputStream(in));
    this.rMDelegationTokenIdentifier = identifier;
    return identifier;
  }

  @Override
  public Long getRenewDate() {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.renewDate != null) {
      return this.renewDate;
    }
    if (!p.hasRenewDate()) {
      return null;
    }
    this.renewDate = p.getRenewDate();
    return this.renewDate;
  }

  @Override
  public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
    maybeInitBuilder();
    if(identifier == null) {
      builder.clearTokenIdentifier();
      return;
    }
    this.rMDelegationTokenIdentifier = identifier;
    this.builder.setTokenIdentifier(identifier.getProto());
  }

  @Override
  public void setRenewDate(Long renewDate) {
    maybeInitBuilder();
    if(renewDate == null) {
      builder.clearRenewDate();
      return;
    }
    this.renewDate = renewDate;
    this.builder.setRenewDate(renewDate);
  }
  @Override
  public String getTokenInfo() {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.tokenInfo != null) {
      return this.tokenInfo;
    }
    if (!p.hasTokenInfo()) {
      return null;
    }
    this.tokenInfo = p.getTokenInfo();
    return this.tokenInfo;
  }

  @Override
  public void setTokenInfo(String tokenInfo) {
    maybeInitBuilder();
    if (tokenInfo == null) {
      builder.clearTokenInfo();
      return;
    }
    this.tokenInfo = tokenInfo;
    this.builder.setTokenInfo(tokenInfo);
  }

  private YARNDelegationTokenIdentifierProto convertToProtoFormat(
      YARNDelegationTokenIdentifier delegationTokenIdentifier) {
    return delegationTokenIdentifier.getProto();
  }

  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }
}
