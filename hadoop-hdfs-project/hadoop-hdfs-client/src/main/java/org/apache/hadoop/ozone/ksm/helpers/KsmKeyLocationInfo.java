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
package org.apache.hadoop.ozone.ksm.helpers;

import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyLocation;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class KsmKeyLocationInfo {
  private final String containerName;
  // name of the block id SCM assigned for the key
  private final String blockID;
  private final boolean shouldCreateContainer;
  // the id of this subkey in all the subkeys.
  private final long length;
  private final long offset;
  // the version number indicating when this block was added
  private long createVersion;

  private KsmKeyLocationInfo(String containerName,
      String blockID, boolean shouldCreateContainer,
      long length, long offset) {
    this.containerName = containerName;
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

  public String getContainerName() {
    return containerName;
  }

  public String getBlockID() {
    return blockID;
  }

  public boolean getShouldCreateContainer() {
    return shouldCreateContainer;
  }

  public long getLength() {
    return length;
  }

  public long getOffset() {
    return offset;
  }

  /**
   * Builder of KsmKeyLocationInfo.
   */
  public static class Builder {
    private String containerName;
    private String blockID;
    private boolean shouldCreateContainer;
    private long length;
    private long offset;

    public Builder setContainerName(String container) {
      this.containerName = container;
      return this;
    }

    public Builder setBlockID(String block) {
      this.blockID = block;
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

    public KsmKeyLocationInfo build() {
      return new KsmKeyLocationInfo(containerName, blockID,
          shouldCreateContainer, length, offset);
    }
  }

  public KeyLocation getProtobuf() {
    return KeyLocation.newBuilder()
        .setContainerName(containerName)
        .setBlockID(blockID)
        .setShouldCreateContainer(shouldCreateContainer)
        .setLength(length)
        .setOffset(offset)
        .setCreateVersion(createVersion)
        .build();
  }

  public static KsmKeyLocationInfo getFromProtobuf(KeyLocation keyLocation) {
    KsmKeyLocationInfo info = new KsmKeyLocationInfo(
        keyLocation.getContainerName(),
        keyLocation.getBlockID(),
        keyLocation.getShouldCreateContainer(),
        keyLocation.getLength(),
        keyLocation.getOffset());
    info.setCreateVersion(keyLocation.getCreateVersion());
    return info;
  }
}
