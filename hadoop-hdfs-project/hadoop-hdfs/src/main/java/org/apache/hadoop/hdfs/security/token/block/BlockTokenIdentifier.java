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
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

@InterfaceAudience.Private
public class BlockTokenIdentifier extends TokenIdentifier {
  static final Text KIND_NAME = new Text("HDFS_BLOCK_TOKEN");

  private long expiryDate;
  private int keyId;
  private String userId;
  private String blockPoolId;
  private long blockId;
  private final EnumSet<AccessMode> modes;

  private byte [] cache;
  
  public BlockTokenIdentifier() {
    this(null, null, 0, EnumSet.noneOf(AccessMode.class));
  }

  public BlockTokenIdentifier(String userId, String bpid, long blockId,
      EnumSet<AccessMode> modes) {
    this.cache = null;
    this.userId = userId;
    this.blockPoolId = bpid;
    this.blockId = blockId;
    this.modes = modes == null ? EnumSet.noneOf(AccessMode.class) : modes;
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

  @Override
  public String toString() {
    return "block_token_identifier (expiryDate=" + this.getExpiryDate()
        + ", keyId=" + this.getKeyId() + ", userId=" + this.getUserId()
        + ", blockPoolId=" + this.getBlockPoolId()
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
    if (obj instanceof BlockTokenIdentifier) {
      BlockTokenIdentifier that = (BlockTokenIdentifier) obj;
      return this.expiryDate == that.expiryDate && this.keyId == that.keyId
          && isEqual(this.userId, that.userId) 
          && isEqual(this.blockPoolId, that.blockPoolId)
          && this.blockId == that.blockId
          && isEqual(this.modes, that.modes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) expiryDate ^ keyId ^ (int) blockId ^ modes.hashCode()
        ^ (userId == null ? 0 : userId.hashCode())
        ^ (blockPoolId == null ? 0 : blockPoolId.hashCode());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.cache = null;
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
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, expiryDate);
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeString(out, userId);
    WritableUtils.writeString(out, blockPoolId);
    WritableUtils.writeVLong(out, blockId);
    WritableUtils.writeVInt(out, modes.size());
    for (AccessMode aMode : modes) {
      WritableUtils.writeEnum(out, aMode);
    }
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
