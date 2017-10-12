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
package org.apache.hadoop.ozone.ksm.helpers;

import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;

import java.util.List;
import java.util.stream.Collectors;

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
  private long dataSize;
  private List<KsmKeyLocationInfo> keyLocationList;
  private final long creationTime;
  private long modificationTime;

  private KsmKeyInfo(String volumeName, String bucketName, String keyName,
      List<KsmKeyLocationInfo> locationInfos, long dataSize, long creationTime,
      long modificationTime) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    this.keyLocationList = locationInfos;
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

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public List<KsmKeyLocationInfo> getKeyLocationList() {
    return keyLocationList;
  }

  public void appendKeyLocation(KsmKeyLocationInfo newLocation) {
    keyLocationList.add(newLocation);
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  /**
   * Builder of KsmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private List<KsmKeyLocationInfo> ksmKeyLocationInfos;
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

    public Builder setKsmKeyLocationInfos(
        List<KsmKeyLocationInfo> ksmKeyLocationInfoList) {
      this.ksmKeyLocationInfos = ksmKeyLocationInfoList;
      return this;
    }

    public Builder setDataSize(long size) {
      this.dataSize = size;
      return this;
    }

    public Builder setCreationTime(long crTime) {
      this.creationTime = crTime;
      return this;
    }

    public Builder setModificationTime(long mTime) {
      this.modificationTime = mTime;
      return this;
    }

    public KsmKeyInfo build() {
      return new KsmKeyInfo(
          volumeName, bucketName, keyName, ksmKeyLocationInfos,
          dataSize, creationTime, modificationTime);
    }
  }

  public KeyInfo getProtobuf() {
    return KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .addAllKeyLocationList(keyLocationList.stream()
            .map(KsmKeyLocationInfo::getProtobuf).collect(Collectors.toList()))
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .build();
  }

  public static KsmKeyInfo getFromProtobuf(KeyInfo keyInfo) {
    return new KsmKeyInfo(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getKeyName(),
        keyInfo.getKeyLocationListList().stream()
            .map(KsmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()),
        keyInfo.getDataSize(),
        keyInfo.getCreationTime(),
        keyInfo.getModificationTime());
  }

}
