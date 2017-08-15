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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocolPB.KSMPBHelper;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that encapsulates Bucket Info.
 */
public final class KsmBucketInfo {
  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;
  /**
   * Bucket Version flag.
   */
  private Boolean isVersionEnabled;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;
  /**
   * Creation time of bucket.
   */
  private final long creationTime;

  /**
   * Private constructor, constructed via builder.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @param acls - list of ACLs.
   * @param isVersionEnabled - Bucket version flag.
   * @param storageType - Storage type to be used.
   * @param creationTime - Bucket creation time.
   */
  private KsmBucketInfo(String volumeName, String bucketName,
                        List<OzoneAcl> acls, boolean isVersionEnabled,
                        StorageType storageType, long creationTime) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.acls = acls;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.creationTime = creationTime;
  }

  /**
   * Returns the Volume Name.
   * @return String.
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns the Bucket Name.
   * @return String
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns the ACL's associated with this bucket.
   * @return List<OzoneAcl>
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public boolean getIsVersionEnabled() {
    return isVersionEnabled;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns creation time.
   *
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns new builder class that builds a KsmBucketInfo.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for KsmBucketInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private List<OzoneAcl> acls;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private long creationTime;

    Builder() {
      //Default values
      this.acls = new LinkedList<>();
      this.isVersionEnabled = false;
      this.storageType = StorageType.DISK;
    }

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public Builder setIsVersionEnabled(Boolean versionFlag) {
      this.isVersionEnabled = versionFlag;
      return this;
    }

    public Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    /**
     * Constructs the KsmBucketInfo.
     * @return instance of KsmBucketInfo.
     */
    public KsmBucketInfo build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);
      Preconditions.checkNotNull(acls);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);

      return new KsmBucketInfo(volumeName, bucketName, acls,
          isVersionEnabled, storageType, creationTime);
    }
  }

  /**
   * Creates BucketInfo protobuf from KsmBucketInfo.
   */
  public BucketInfo getProtobuf() {
    return BucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllAcls(acls.stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()))
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(PBHelperClient.convertStorageType(
            storageType))
        .setCreationTime(creationTime)
        .build();
  }

  /**
   * Parses BucketInfo protobuf and creates KsmBucketInfo.
   * @param bucketInfo
   * @return instance of KsmBucketInfo
   */
  public static KsmBucketInfo getFromProtobuf(BucketInfo bucketInfo) {
    return new KsmBucketInfo(
        bucketInfo.getVolumeName(),
        bucketInfo.getBucketName(),
        bucketInfo.getAclsList().stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()),
        bucketInfo.getIsVersionEnabled(),
        PBHelperClient.convertStorageType(
            bucketInfo.getStorageType()), bucketInfo.getCreationTime());
  }
}