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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class OmKeyLocationInfo {
  private final BlockID blockID;
  // the id of this subkey in all the subkeys.
  private long length;
  private final long offset;
  // the version number indicating when this block was added
  private long createVersion;

  private OmKeyLocationInfo(BlockID blockID, long length, long offset) {
    this.blockID = blockID;
    this.length = length;
    this.offset = offset;
  }

  public void setCreateVersion(long version) {
    createVersion = version;
  }

  public long getCreateVersion() {
    return createVersion;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public long getContainerID() {
    return blockID.getContainerID();
  }

  public long getLocalID() {
    return blockID.getLocalID();
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getBlockCommitSequenceId() {
    return blockID.getBlockCommitSequenceId();
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private long length;
    private long offset;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setOffset(long off) {
      this.offset = off;
      return this;
    }

    public OmKeyLocationInfo build() {
      return new OmKeyLocationInfo(blockID, length, offset);
    }
  }

  public KeyLocation getProtobuf() {
    return KeyLocation.newBuilder()
        .setBlockID(blockID.getProtobuf())
        .setLength(length)
        .setOffset(offset)
        .setCreateVersion(createVersion)
        .build();
  }

  public static OmKeyLocationInfo getFromProtobuf(KeyLocation keyLocation) {
    OmKeyLocationInfo info = new OmKeyLocationInfo(
        BlockID.getFromProtobuf(keyLocation.getBlockID()),
        keyLocation.getLength(),
        keyLocation.getOffset());
    info.setCreateVersion(keyLocation.getCreateVersion());
    return info;
  }

  @Override
  public String toString() {
    return "{blockID={containerID=" + blockID.getContainerID() +
        ", localID=" + blockID.getLocalID() + "}" +
        ", length=" + length +
        ", offset=" + offset +
        ", createVersion=" + createVersion + '}';
  }
}
