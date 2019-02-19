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
package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

/**
 * Args for key block. The block instance for the key requested in putKey.
 * This is returned from OM to client, and client use class to talk to
 * datanode. Also, this is the metadata written to om.db on server side.
 */
public final class OmKeyInfo extends WithMetadata {
  private final String volumeName;
  private final String bucketName;
  // name of key client specified
  private String keyName;
  private long dataSize;
  private List<OmKeyLocationInfoGroup> keyLocationVersions;
  private final long creationTime;
  private long modificationTime;
  private HddsProtos.ReplicationType type;
  private HddsProtos.ReplicationFactor factor;
  private FileEncryptionInfo encInfo;

  @SuppressWarnings("parameternumber")
  OmKeyInfo(String volumeName, String bucketName, String keyName,
      List<OmKeyLocationInfoGroup> versions, long dataSize,
      long creationTime, long modificationTime,
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor,
      Map<String, String> metadata,
      FileEncryptionInfo encInfo) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.dataSize = dataSize;
    // it is important that the versions are ordered from old to new.
    // Do this sanity check when versions got loaded on creating OmKeyInfo.
    // TODO : this is not necessary, here only because versioning is still a
    // work in-progress, remove this following check when versioning is
    // complete and prove correctly functioning
    long currentVersion = -1;
    for (OmKeyLocationInfoGroup version : versions) {
      Preconditions.checkArgument(
            currentVersion + 1 == version.getVersion());
      currentVersion = version.getVersion();
    }
    this.keyLocationVersions = versions;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.factor = factor;
    this.type = type;
    this.metadata = metadata;
    this.encInfo = encInfo;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public HddsProtos.ReplicationType getType() {
    return type;
  }

  public HddsProtos.ReplicationFactor getFactor() {
    return factor;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long size) {
    this.dataSize = size;
  }

  public synchronized OmKeyLocationInfoGroup getLatestVersionLocations() {
    return keyLocationVersions.size() == 0? null :
        keyLocationVersions.get(keyLocationVersions.size() - 1);
  }

  public List<OmKeyLocationInfoGroup> getKeyLocationVersions() {
    return keyLocationVersions;
  }

  public void updateModifcationTime() {
    this.modificationTime = Time.monotonicNow();
  }

  /**
   * updates the length of the each block in the list given.
   * This will be called when the key is being committed to OzoneManager.
   *
   * @param locationInfoList list of locationInfo
   */
  public void updateLocationInfoList(List<OmKeyLocationInfo> locationInfoList) {
    long latestVersion = getLatestVersionLocations().getVersion();
    OmKeyLocationInfoGroup keyLocationInfoGroup = getLatestVersionLocations();
    List<OmKeyLocationInfo> currentList =
        keyLocationInfoGroup.getLocationList();
    List<OmKeyLocationInfo> latestVersionList =
        keyLocationInfoGroup.getBlocksLatestVersionOnly();
    // Updates the latest locationList in the latest version only with
    // given locationInfoList here.
    // TODO : The original allocated list and the updated list here may vary
    // as the containers on the Datanode on which the blocks were pre allocated
    // might get closed. The diff of blocks between these two lists here
    // need to be garbage collected in case the ozone client dies.
    currentList.removeAll(latestVersionList);
    // set each of the locationInfo object to the latest version
    locationInfoList.stream().forEach(omKeyLocationInfo -> omKeyLocationInfo
        .setCreateVersion(latestVersion));
    currentList.addAll(locationInfoList);
  }

  /**
   * Append a set of blocks to the latest version. Note that these blocks are
   * part of the latest version, not a new version.
   *
   * @param newLocationList the list of new blocks to be added.
   * @throws IOException
   */
  public synchronized void appendNewBlocks(
      List<OmKeyLocationInfo> newLocationList) throws IOException {
    if (keyLocationVersions.size() == 0) {
      throw new IOException("Appending new block, but no version exist");
    }
    OmKeyLocationInfoGroup currentLatestVersion =
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
      List<OmKeyLocationInfo> newLocationList) throws IOException {
    long latestVersionNum;
    if (keyLocationVersions.size() == 0) {
      // no version exist, these blocks are the very first version.
      keyLocationVersions.add(new OmKeyLocationInfoGroup(0, newLocationList));
      latestVersionNum = 0;
    } else {
      // it is important that the new version are always at the tail of the list
      OmKeyLocationInfoGroup currentLatestVersion =
          keyLocationVersions.get(keyLocationVersions.size() - 1);
      // the new version is created based on the current latest version
      OmKeyLocationInfoGroup newVersion =
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

  public FileEncryptionInfo getFileEncryptionInfo() {
    return encInfo;
  }

  /**
   * Builder of OmKeyInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private long dataSize;
    private List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups =
        new ArrayList<>();
    private long creationTime;
    private long modificationTime;
    private HddsProtos.ReplicationType type;
    private HddsProtos.ReplicationFactor factor;
    private Map<String, String> metadata;
    private FileEncryptionInfo encInfo;

    public Builder() {
      this.metadata = new HashMap<>();
      omKeyLocationInfoGroups = new ArrayList<>();
    }

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

    public Builder setOmKeyLocationInfos(
        List<OmKeyLocationInfoGroup> omKeyLocationInfoList) {
      this.omKeyLocationInfoGroups = omKeyLocationInfoList;
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

    public Builder setReplicationFactor(HddsProtos.ReplicationFactor replFact) {
      this.factor = replFact;
      return this;
    }

    public Builder setReplicationType(HddsProtos.ReplicationType replType) {
      this.type = replType;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> newMetadata) {
      metadata.putAll(newMetadata);
      return this;
    }

    public Builder setFileEncryptionInfo(FileEncryptionInfo feInfo) {
      this.encInfo = feInfo;
      return this;
    }

    public OmKeyInfo build() {
      return new OmKeyInfo(
          volumeName, bucketName, keyName, omKeyLocationInfoGroups,
          dataSize, creationTime, modificationTime, type, factor, metadata,
          encInfo);
    }
  }

  public KeyInfo getProtobuf() {
    long latestVersion = keyLocationVersions.size() == 0 ? -1 :
        keyLocationVersions.get(keyLocationVersions.size() - 1).getVersion();
    KeyInfo.Builder kb = KeyInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(dataSize)
        .setFactor(factor)
        .setType(type)
        .addAllKeyLocationList(keyLocationVersions.stream()
            .map(OmKeyLocationInfoGroup::getProtobuf)
            .collect(Collectors.toList()))
        .setLatestVersion(latestVersion)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata));
    if (encInfo != null) {
      kb.setFileEncryptionInfo(OMPBHelper.convert(encInfo));
    }
    return kb.build();
  }

  public static OmKeyInfo getFromProtobuf(KeyInfo keyInfo) {
    return new OmKeyInfo(
        keyInfo.getVolumeName(),
        keyInfo.getBucketName(),
        keyInfo.getKeyName(),
        keyInfo.getKeyLocationListList().stream()
            .map(OmKeyLocationInfoGroup::getFromProtobuf)
            .collect(Collectors.toList()),
        keyInfo.getDataSize(),
        keyInfo.getCreationTime(),
        keyInfo.getModificationTime(),
        keyInfo.getType(),
        keyInfo.getFactor(),
        KeyValueUtil.getFromProtobuf(keyInfo.getMetadataList()),
        keyInfo.hasFileEncryptionInfo() ? OMPBHelper.convert(keyInfo
            .getFileEncryptionInfo()): null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmKeyInfo omKeyInfo = (OmKeyInfo) o;
    return dataSize == omKeyInfo.dataSize &&
        creationTime == omKeyInfo.creationTime &&
        modificationTime == omKeyInfo.modificationTime &&
        volumeName.equals(omKeyInfo.volumeName) &&
        bucketName.equals(omKeyInfo.bucketName) &&
        keyName.equals(omKeyInfo.keyName) &&
        Objects
            .equals(keyLocationVersions, omKeyInfo.keyLocationVersions) &&
        type == omKeyInfo.type &&
        factor == omKeyInfo.factor &&
        Objects.equals(metadata, omKeyInfo.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName, keyName);
  }
}
