/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.Builder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

/**
 * Block token identifier for Ozone/HDDS. Ozone block access token is similar
 * to HDFS block access token, which is meant to be lightweight and
 * short-lived. No need to renew or revoke a block access token. when a
 * cached block access token expires, the client simply get a new one.
 * Block access token should be cached only in memory and never write to disk.
 */
@InterfaceAudience.Private
public class OzoneBlockTokenIdentifier extends TokenIdentifier {

  static final Text KIND_NAME = new Text("HDDS_BLOCK_TOKEN");
  private long expiryDate;
  private String ownerId;
  private String blockId;
  private final EnumSet<AccessModeProto> modes;
  private final String omCertSerialId;

  public OzoneBlockTokenIdentifier(String ownerId, String blockId,
      EnumSet<AccessModeProto> modes, long expiryDate, String omCertSerialId) {
    this.ownerId = ownerId;
    this.blockId = blockId;
    this.expiryDate = expiryDate;
    this.modes = modes == null ? EnumSet.noneOf(AccessModeProto.class) : modes;
    this.omCertSerialId = omCertSerialId;
  }

  @Override
  public UserGroupInformation getUser() {
    if (this.getOwnerId() == null || "".equals(this.getOwnerId())) {
      return UserGroupInformation.createRemoteUser(blockId);
    }
    return UserGroupInformation.createRemoteUser(ownerId);
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public String getBlockId() {
    return blockId;
  }

  public EnumSet<AccessModeProto> getAccessModes() {
    return modes;
  }

  public String getOmCertSerialId(){
    return omCertSerialId;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public String toString() {
    return "block_token_identifier (expiryDate=" + this.getExpiryDate()
        + ", ownerId=" + this.getOwnerId()
        + ", omCertSerialId=" + this.getOmCertSerialId()
        + ", blockId=" + this.getBlockId() + ", access modes="
        + this.getAccessModes() + ")";
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof OzoneBlockTokenIdentifier) {
      OzoneBlockTokenIdentifier that = (OzoneBlockTokenIdentifier) obj;
      return new EqualsBuilder()
          .append(this.expiryDate, that.expiryDate)
          .append(this.ownerId, that.ownerId)
          .append(this.blockId, that.blockId)
          .append(this.modes, that.modes)
          .append(this.omCertSerialId, that.omCertSerialId)
          .build();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(133, 567)
        .append(this.expiryDate)
        .append(this.blockId)
        .append(this.ownerId)
        .append(this.modes)
        .append(this.omCertSerialId)
        .build();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    final DataInputStream dis = (DataInputStream) in;
    if (!dis.markSupported()) {
      throw new IOException("Could not peek first byte.");
    }
    readFieldsProtobuf(dis);
  }

  @VisibleForTesting
  public static OzoneBlockTokenIdentifier readFieldsProtobuf(DataInput in)
      throws IOException {
    BlockTokenSecretProto tokenPtoto =
        BlockTokenSecretProto.parseFrom((DataInputStream) in);
    return new OzoneBlockTokenIdentifier(tokenPtoto.getOwnerId(),
        tokenPtoto.getBlockId(), EnumSet.copyOf(tokenPtoto.getModesList()),
        tokenPtoto.getExpiryDate(), tokenPtoto.getOmCertSerialId());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeProtobuf(out);
  }

  @VisibleForTesting
  void writeProtobuf(DataOutput out) throws IOException {
    Builder builder = BlockTokenSecretProto.newBuilder()
        .setBlockId(this.getBlockId())
        .setOwnerId(this.getOwnerId())
        .setOmCertSerialId(this.getOmCertSerialId())
        .setExpiryDate(this.getExpiryDate());
    // Add access mode allowed
    for (AccessModeProto mode : this.getAccessModes()) {
      builder.addModes(AccessModeProto.valueOf(mode.name()));
    }
    out.write(builder.build().toByteArray());
  }
}

