/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ksm.helpers;


import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;

/**
 * Args for key block. The block instance for the key requested in putKey.
 * This is returned from KSM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to ksm.db on server side.
 */
public final class KsmKeyInfo {
  private final String volumeName;
  private final String bucketName;
  // name of key client specified
  private final String keyName;
  private final String containerName;
  // name of the block id SCM assigned for the key
  private final String blockID;
  private final long dataSize;
  private final boolean shouldCreateContainer;
  private final long creationTime;
  private final long modificationTime;

  private KsmKeyInfo(String volumeName, String bucketName, String keyName,
      long dataSize, String blockID, String containerName,
      boolean shouldCreateContainer, long creationTime,
      long modificationTime) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.containerName = containerName;
    this.blockID = blockID;
    this.dataSize = dataSize;
    this.shouldCreateContainer = shouldCreateContainer;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getBlockID() {
    return blockID;
  }

  public String getContainerName() {
    return containerName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public boolean getShouldCreateContainer() {
    return shouldCreateContainer;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Builder of KsmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private String containerName;
    private String blockID;
    private long dataSize;
    private boolean shouldCreateContainer;
    private long creationTime;
    private long modificationTime;

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setKeyName(String key) {
      this.keyName = key;
      return this;
    }

    public Builder setBlockID(String block) {
      this.blockID = block;
      return this;
    }

    public Builder setContainerName(String container) {
      this.containerName = container;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setShouldCreateContainer(boolean create) {
      this.shouldCreateContainer = create;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder setModificationTime(long modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public KsmKeyInfo build() {
      return new KsmKeyInfo(
          volumeName, bucketName, keyName, dataSize, blockID, containerName,
          shouldCreateContainer, creationTime, modificationTime);
    }
  }

  public KeyInfo getProtobuf() {
    return KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setBlockKey(blockID)
        .setContainerName(containerName)
        .setShouldCreateContainer(shouldCreateContainer)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .build();
  }

  public static KsmKeyInfo getFromProtobuf(KeyInfo keyInfo) {
    return new KsmKeyInfo(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getKeyName(),
        keyInfo.getDataSize(),
        keyInfo.getBlockKey(),
        keyInfo.getContainerName(),
        keyInfo.getShouldCreateContainer(),
        keyInfo.getCreationTime(),
        keyInfo.getModificationTime());
  }

}
