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

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;

import java.nio.ByteBuffer;

public class RouterMasterKeyPBImpl extends RouterMasterKey {

  private RouterMasterKeyProto proto = RouterMasterKeyProto.getDefaultInstance();
  private RouterMasterKeyProto.Builder builder = null;
  private boolean viaProto = false;

  public RouterMasterKeyPBImpl() {
    builder = RouterMasterKeyProto.newBuilder();
  }

  public RouterMasterKeyPBImpl(RouterMasterKeyProto masterKeyProto) {
    this.proto = masterKeyProto;
    viaProto = true;
  }

  public RouterMasterKeyProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterMasterKeyProto.newBuilder(proto);
    }
    viaProto = false;
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
  public Integer getKeyId() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getKeyId();
  }

  @Override
  public void setKeyId(Integer keyId) {
    maybeInitBuilder();
    if (keyId == null) {
      builder.clearKeyId();
      return;
    }
    builder.setKeyId(keyId);
  }

  @Override
  public ByteBuffer getKeyBytes() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getKeyBytes());
  }

  @Override
  public void setKeyBytes(ByteBuffer keyBytes) {
    maybeInitBuilder();
    if (keyBytes == null) {
      builder.clearKeyBytes();
      return;
    }
    builder.setKeyBytes(convertToProtoFormat(keyBytes));
  }

  @Override
  public Long getExpiryDate() {
    RouterMasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return p.getExpiryDate();
  }

  @Override
  public void setExpiryDate(Long expiryDate) {
    maybeInitBuilder();
    if (expiryDate == null) {
      builder.clearExpiryDate();
      return;
    }
    builder.setExpiryDate(expiryDate);
  }

  protected final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }

  protected final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }
}
