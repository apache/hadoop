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
package org.apache.hadoop.ozone.security;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SecretKeyProto;

/**
 * Wrapper class for Ozone/Hdds secret keys. Used in delegation tokens and block
 * tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneSecretKey implements Writable {

  private int keyId;
  private long expiryDate;
  private PrivateKey privateKey;
  private PublicKey publicKey;
  private SecurityConfig securityConfig;

  public OzoneSecretKey(int keyId, long expiryDate, KeyPair keyPair) {
    Preconditions.checkNotNull(keyId);
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    this.privateKey = keyPair.getPrivate();
    this.publicKey = keyPair.getPublic();
  }

  /*
   * Create new instance using default signature algorithm and provider.
   * */
  public OzoneSecretKey(int keyId, long expiryDate, byte[] pvtKey,
      byte[] publicKey) {
    Preconditions.checkNotNull(pvtKey);
    Preconditions.checkNotNull(publicKey);

    this.securityConfig = new SecurityConfig(new OzoneConfiguration());
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    this.privateKey = SecurityUtil.getPrivateKey(pvtKey, securityConfig);
    this.publicKey = SecurityUtil.getPublicKey(publicKey, securityConfig);
  }

  public int getKeyId() {
    return keyId;
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  public PublicKey getPublicKey() {
    return publicKey;
  }

  public byte[] getEncodedPrivateKey() {
    return privateKey.getEncoded();
  }

  public byte[] getEncodedPubliceKey() {
    return publicKey.getEncoded();
  }

  public void setExpiryDate(long expiryDate) {
    this.expiryDate = expiryDate;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    SecretKeyProto token = SecretKeyProto.newBuilder()
        .setKeyId(getKeyId())
        .setExpiryDate(getExpiryDate())
        .setPrivateKeyBytes(ByteString.copyFrom(getEncodedPrivateKey()))
        .setPublicKeyBytes(ByteString.copyFrom(getEncodedPubliceKey()))
        .build();
    out.write(token.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    SecretKeyProto secretKey = SecretKeyProto.parseFrom((DataInputStream) in);
    expiryDate = secretKey.getExpiryDate();
    keyId = secretKey.getKeyId();
    privateKey = SecurityUtil.getPrivateKey(secretKey.getPrivateKeyBytes()
        .toByteArray(), securityConfig);
    publicKey = SecurityUtil.getPublicKey(secretKey.getPublicKeyBytes()
        .toByteArray(), securityConfig);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(537, 963);
    hashCodeBuilder.append(getExpiryDate())
        .append(getKeyId())
        .append(getEncodedPrivateKey())
        .append(getEncodedPubliceKey());

    return hashCodeBuilder.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof OzoneSecretKey) {
      OzoneSecretKey that = (OzoneSecretKey) obj;
      return new EqualsBuilder()
          .append(this.keyId, that.keyId)
          .append(this.expiryDate, that.expiryDate)
          .append(this.privateKey, that.privateKey)
          .append(this.publicKey, that.publicKey)
          .build();
    }
    return false;
  }

  /**
   * Reads protobuf encoded input stream to construct {@link OzoneSecretKey}.
   */
  static OzoneSecretKey readProtoBuf(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);
    SecretKeyProto key = SecretKeyProto.parseFrom((DataInputStream) in);
    return new OzoneSecretKey(key.getKeyId(), key.getExpiryDate(),
        key.getPrivateKeyBytes().toByteArray(),
        key.getPublicKeyBytes().toByteArray());
  }

  /**
   * Reads protobuf encoded input stream to construct {@link OzoneSecretKey}.
   */
  static OzoneSecretKey readProtoBuf(byte[] identifier) throws IOException {
    Preconditions.checkNotNull(identifier);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(
        identifier));
    return readProtoBuf(in);
  }

}