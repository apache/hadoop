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

package org.apache.hadoop.hdfs.security.token.block;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.AccessModeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockTokenSecretProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

@InterfaceAudience.Private
public class BlockTokenIdentifier extends TokenIdentifier {
  static final Text KIND_NAME = new Text("HDFS_BLOCK_TOKEN");

  public enum AccessMode {
    READ, WRITE, COPY, REPLACE
  }

  private long expiryDate;
  private int keyId;
  private String userId;
  private String blockPoolId;
  private long blockId;
  private final EnumSet<AccessMode> modes;
  private StorageType[] storageTypes;
  private String[] storageIds;
  private boolean useProto;
  private byte[] handshakeMsg;

  private byte [] cache;

  public BlockTokenIdentifier() {
    this(null, null, 0, EnumSet.noneOf(AccessMode.class), null, null,
        false);
  }

  public BlockTokenIdentifier(String userId, String bpid, long blockId,
      EnumSet<AccessMode> modes, StorageType[] storageTypes,
      String[] storageIds, boolean useProto) {
    this.cache = null;
    this.userId = userId;
    this.blockPoolId = bpid;
    this.blockId = blockId;
    this.modes = modes == null ? EnumSet.noneOf(AccessMode.class) : modes;
    this.storageTypes = Optional.ofNullable(storageTypes)
                                .orElse(StorageType.EMPTY_ARRAY);
    this.storageIds = Optional.ofNullable(storageIds)
                              .orElse(new String[0]);
    this.useProto = useProto;
    this.handshakeMsg = new byte[0];
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    if (userId == null || "".equals(userId)) {
      String user = blockPoolId + ":" + Long.toString(blockId);
      return UserGroupInformation.createRemoteUser(user);
    }
    return UserGroupInformation.createRemoteUser(userId);
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public void setExpiryDate(long expiryDate) {
    this.cache = null;
    this.expiryDate = expiryDate;
  }

  public int getKeyId() {
    return this.keyId;
  }

  public void setKeyId(int keyId) {
    this.cache = null;
    this.keyId = keyId;
  }

  public String getUserId() {
    return userId;
  }

  public String getBlockPoolId() {
    return blockPoolId;
  }

  public long getBlockId() {
    return blockId;
  }

  public EnumSet<AccessMode> getAccessModes() {
    return modes;
  }

  public StorageType[] getStorageTypes(){
    return storageTypes;
  }

  public String[] getStorageIds(){
    return storageIds;
  }

  public byte[] getHandshakeMsg() {
    return handshakeMsg;
  }

  public void setHandshakeMsg(byte[] bytes) {
    cache = null; // invalidate the cache
    handshakeMsg = bytes;
  }

  @Override
  public String toString() {
    return "block_token_identifier (expiryDate=" + this.getExpiryDate()
        + ", keyId=" + this.getKeyId() + ", userId=" + this.getUserId()
        + ", blockPoolId=" + this.getBlockPoolId()
        + ", blockId=" + this.getBlockId() + ", access modes="
        + this.getAccessModes() + ", storageTypes= "
        + Arrays.toString(this.getStorageTypes()) + ", storageIds= "
        + Arrays.toString(this.getStorageIds()) + ")";
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof BlockTokenIdentifier) {
      BlockTokenIdentifier that = (BlockTokenIdentifier) obj;
      return this.expiryDate == that.expiryDate && this.keyId == that.keyId
          && isEqual(this.userId, that.userId)
          && isEqual(this.blockPoolId, that.blockPoolId)
          && this.blockId == that.blockId
          && isEqual(this.modes, that.modes)
          && Arrays.equals(this.storageTypes, that.storageTypes)
          && Arrays.equals(this.storageIds, that.storageIds);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) expiryDate ^ keyId ^ (int) blockId ^ modes.hashCode()
        ^ (userId == null ? 0 : userId.hashCode())
        ^ (blockPoolId == null ? 0 : blockPoolId.hashCode())
        ^ (storageTypes == null ? 0 : Arrays.hashCode(storageTypes))
        ^ (storageIds == null ? 0 : Arrays.hashCode(storageIds));
  }

  /**
   * readFields peeks at the first byte of the DataInput and determines if it
   * was written using WritableUtils ("Legacy") or Protobuf. We can do this
   * because we know the first field is the Expiry date.
   *
   * In the case of the legacy buffer, the expiry date is a VInt, so the size
   * (which should always be &gt;1) is encoded in the first byte - which is
   * always negative due to this encoding. However, there are sometimes null
   * BlockTokenIdentifier written so we also need to handle the case there
   * the first byte is also 0.
   *
   * In the case of protobuf, the first byte is a type tag for the expiry date
   * which is written as <code>field_number &lt;&lt; 3 | wire_type</code>.
   * So as long as the field_number  is less than 16, but also positive, then
   * we know we have a Protobuf.
   *
   * @param in <code>DataInput</code> to deserialize this object from.
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.cache = null;

    final DataInputStream dis = (DataInputStream)in;
    if (!dis.markSupported()) {
      throw new IOException("Could not peek first byte.");
    }

    // this.cache should be assigned the raw bytes from the input data for
    // upgrading compatibility. If we won't mutate fields and call getBytes()
    // for something (e.g retrieve password), we should return the raw bytes
    // instead of serializing the instance self fields to bytes, because we may
    // lose newly added fields which we can't recognize
    this.cache = IOUtils.readFullyToByteArray(dis);
    dis.reset();

    dis.mark(1);
    final byte firstByte = dis.readByte();
    dis.reset();
    if (firstByte <= 0) {
      readFieldsLegacy(dis);
    } else {
      readFieldsProtobuf(dis);
    }
  }

  @VisibleForTesting
  void readFieldsLegacy(DataInput in) throws IOException {
    expiryDate = WritableUtils.readVLong(in);
    keyId = WritableUtils.readVInt(in);
    userId = WritableUtils.readString(in);
    blockPoolId = WritableUtils.readString(in);
    blockId = WritableUtils.readVLong(in);
    int length = WritableUtils.readVIntInRange(in, 0,
        AccessMode.class.getEnumConstants().length);
    for (int i = 0; i < length; i++) {
      modes.add(WritableUtils.readEnum(in, AccessMode.class));
    }

    try {
      length = WritableUtils.readVInt(in);
      StorageType[] readStorageTypes = new StorageType[length];
      for (int i = 0; i < length; i++) {
        readStorageTypes[i] = WritableUtils.readEnum(in, StorageType.class);
      }
      storageTypes = readStorageTypes;

      length = WritableUtils.readVInt(in);
      String[] readStorageIds = new String[length];
      for (int i = 0; i < length; i++) {
        readStorageIds[i] = WritableUtils.readString(in);
      }
      storageIds = readStorageIds;

      int handshakeMsgLen = WritableUtils.readVInt(in);
      if (handshakeMsgLen != 0) {
        handshakeMsg = new byte[handshakeMsgLen];
        in.readFully(handshakeMsg);
      }
    } catch (EOFException eof) {
      // If the NameNode is on a version before HDFS-6708 and HDFS-9807, then
      // the block token won't have storage types or storage IDs. For backward
      // compatibility, swallow the EOF that we get when we try to read those
      // fields. Same for the handshake secret field from HDFS-14611.
    }

    useProto = false;
  }

  @VisibleForTesting
  void readFieldsProtobuf(DataInput in) throws IOException {
    BlockTokenSecretProto blockTokenSecretProto =
        BlockTokenSecretProto.parseFrom((DataInputStream)in);
    expiryDate = blockTokenSecretProto.getExpiryDate();
    keyId = blockTokenSecretProto.getKeyId();
    if (blockTokenSecretProto.hasUserId()) {
      userId = blockTokenSecretProto.getUserId();
    } else {
      userId = null;
    }
    if (blockTokenSecretProto.hasBlockPoolId()) {
      blockPoolId = blockTokenSecretProto.getBlockPoolId();
    } else {
      blockPoolId = null;
    }
    blockId = blockTokenSecretProto.getBlockId();
    for (int i = 0; i < blockTokenSecretProto.getModesCount(); i++) {
      AccessModeProto accessModeProto = blockTokenSecretProto.getModes(i);
      modes.add(PBHelperClient.convert(accessModeProto));
    }

    storageTypes = blockTokenSecretProto.getStorageTypesList().stream()
        .map(PBHelperClient::convertStorageType)
        .toArray(StorageType[]::new);
    storageIds = blockTokenSecretProto.getStorageIdsList().stream()
        .toArray(String[]::new);
    useProto = true;

    if(blockTokenSecretProto.hasHandshakeSecret()) {
      handshakeMsg = blockTokenSecretProto
          .getHandshakeSecret().toByteArray();
    } else {
      handshakeMsg = new byte[0];
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (useProto) {
      writeProtobuf(out);
    } else {
      writeLegacy(out);
    }
  }

  @VisibleForTesting
  void writeLegacy(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, expiryDate);
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeString(out, userId);
    WritableUtils.writeString(out, blockPoolId);
    WritableUtils.writeVLong(out, blockId);
    WritableUtils.writeVInt(out, modes.size());
    for (AccessMode aMode : modes) {
      WritableUtils.writeEnum(out, aMode);
    }
    if (storageTypes != null) {
      WritableUtils.writeVInt(out, storageTypes.length);
      for (StorageType type : storageTypes) {
        WritableUtils.writeEnum(out, type);
      }
    }
    if (storageIds != null) {
      WritableUtils.writeVInt(out, storageIds.length);
      for (String id : storageIds) {
        WritableUtils.writeString(out, id);
      }
    }
    if (handshakeMsg != null && handshakeMsg.length > 0) {
      WritableUtils.writeVInt(out, handshakeMsg.length);
      out.write(handshakeMsg);
    }
  }

  @VisibleForTesting
  void writeProtobuf(DataOutput out) throws IOException {
    BlockTokenSecretProto secret = PBHelperClient.convert(this);
    out.write(secret.toByteArray());
  }

  @Override
  public byte[] getBytes() {
    if(cache == null) cache = super.getBytes();

    return cache;
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}
