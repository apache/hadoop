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
  private final boolean shouldCreateContainer;
  // the id of this subkey in all the subkeys.
  private long length;
  private final long offset;
  // the version number indicating when this block was added
  private long createVersion;

  private OmKeyLocationInfo(BlockID blockID, boolean shouldCreateContainer,
                            long length, long offset) {
    this.blockID = blockID;
    this.shouldCreateContainer = shouldCreateContainer;
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

  public boolean getShouldCreateContainer() {
    return shouldCreateContainer;
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

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private boolean shouldCreateContainer;
    private long length;
    private long offset;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    public Builder setShouldCreateContainer(boolean create) {
      this.shouldCreateContainer = create;
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
      return new OmKeyLocationInfo(blockID,
          shouldCreateContainer, length, offset);
    }
  }

  public KeyLocation getProtobuf() {
    return KeyLocation.newBuilder()
        .setBlockID(blockID.getProtobuf())
        .setShouldCreateContainer(shouldCreateContainer)
        .setLength(length)
        .setOffset(offset)
        .setCreateVersion(createVersion)
        .build();
  }

  public static OmKeyLocationInfo getFromProtobuf(KeyLocation keyLocation) {
    OmKeyLocationInfo info = new OmKeyLocationInfo(
        BlockID.getFromProtobuf(keyLocation.getBlockID()),
        keyLocation.getShouldCreateContainer(),
        keyLocation.getLength(),
        keyLocation.getOffset());
    info.setCreateVersion(keyLocation.getCreateVersion());
    return info;
  }
}
