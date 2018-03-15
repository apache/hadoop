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

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.util.Time;

import java.io.IOException;
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
  private List<KsmKeyLocationInfoGroup> keyLocationVersions;
  private final long creationTime;
  private long modificationTime;

  private KsmKeyInfo(String volumeName, String bucketName, String keyName,
      List<KsmKeyLocationInfoGroup> versions, long dataSize,
      long creationTime, long modificationTime) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    // it is important that the versions are ordered from old to new.
    // Do this sanity check when versions got loaded on creating KsmKeyInfo.
    // TODO : this is not necessary, here only because versioning is still a
    // work in-progress, remove this following check when versioning is
    // complete and prove correctly functioning
    long currentVersion = -1;
    for (KsmKeyLocationInfoGroup version : versions) {
      Preconditions.checkArgument(
            currentVersion + 1 == version.getVersion());
      currentVersion = version.getVersion();
    }
    this.keyLocationVersions = versions;
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

  public synchronized KsmKeyLocationInfoGroup getLatestVersionLocations()
      throws IOException {
    return keyLocationVersions.size() == 0? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<KsmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void updateModifcationTime() {
    this.modificationTime = Time.monotonicNow();
  }

  /**
   * Append a set of blocks to the latest version. Note that these blocks are
   * part of the latest version, not a new version.
   *
   * @param newLocationList the list of new blocks to be added.
   * @throws IOException
   */
  public synchronized void appendNewBlocks(
      List<KsmKeyLocationInfo> newLocationList) throws IOException {
    if (keyLocationVersions.size() == 0) {
      throw new IOException("Appending new block, but no version exist");
    }
    KsmKeyLocationInfoGroup currentLatestVersion =
        keyLocationVersions.get(keyLocationVersions.size() - 1);
    currentLatestVersion.appendNewBlocks(newLocationList);
    setModificationTime(Time.now());
  }

  /**
   * Add a new set of blocks. The new blocks will be added as appending a new
   * version to the all version list.
   *
   * @param newLocationList the list of new blocks to be added.
   * @throws IOException
   */
  public synchronized long addNewVersion(
      List<KsmKeyLocationInfo> newLocationList) throws IOException {
    long latestVersionNum;
    if (keyLocationVersions.size() == 0) {
      // no version exist, these blocks are the very first version.
      keyLocationVersions.add(new KsmKeyLocationInfoGroup(0, newLocationList));
      latestVersionNum = 0;
    } else {
      // it is important that the new version are always at the tail of the list
      KsmKeyLocationInfoGroup currentLatestVersion =
          keyLocationVersions.get(keyLocationVersions.size() - 1);
      // the new version is created based on the current latest version
      KsmKeyLocationInfoGroup newVersion =
          currentLatestVersion.generateNextVersion(newLocationList);
      keyLocationVersions.add(newVersion);
      latestVersionNum = newVersion.getVersion();
    }
    setModificationTime(Time.now());
    return latestVersionNum;
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
    private List<KsmKeyLocationInfoGroup> ksmKeyLocationInfoGroups;
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
        List<KsmKeyLocationInfoGroup> ksmKeyLocationInfoList) {
      this.ksmKeyLocationInfoGroups = ksmKeyLocationInfoList;
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
          volumeName, bucketName, keyName, ksmKeyLocationInfoGroups,
          dataSize, creationTime, modificationTime);
    }
  }

  public KeyInfo getProtobuf() {
    long latestVersion = keyLocationVersions.size() == 0 ? -1 :
        keyLocationVersions.get(keyLocationVersions.size() - 1).getVersion();
    return KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .addAllKeyLocationList(keyLocationVersions.stream()
            .map(KsmKeyLocationInfoGroup::getProtobuf)
            .collect(Collectors.toList()))
        .setLatestVersion(latestVersion)
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
            .map(KsmKeyLocationInfoGroup::getFromProtobuf)
            .collect(Collectors.toList()),
        keyInfo.getDataSize(),
        keyInfo.getCreationTime(),
        keyInfo.getModificationTime());
  }

}
