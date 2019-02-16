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


import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

import com.google.common.base.Preconditions;

/**
 * A class that encapsulates Bucket Info.
 */
public final class OmBucketInfo extends WithMetadata implements Auditable {
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
   * Bucket encryption key info if encryption is enabled.
   */
  private BucketEncryptionKeyInfo bekInfo;

  /**
   * Private constructor, constructed via builder.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @param acls - list of ACLs.
   * @param isVersionEnabled - Bucket version flag.
   * @param storageType - Storage type to be used.
   * @param creationTime - Bucket creation time.
   * @param metadata - metadata.
   * @param bekInfo - bucket encryption key info.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private OmBucketInfo(String volumeName,
                       String bucketName,
                       List<OzoneAcl> acls,
                       boolean isVersionEnabled,
                       StorageType storageType,
                       long creationTime,
                       Map<String, String> metadata,
                       BucketEncryptionKeyInfo bekInfo) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.acls = acls;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
    this.creationTime = creationTime;
    this.metadata = metadata;
    this.bekInfo = bekInfo;
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
   * @return {@literal List<OzoneAcl>}
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
   * Returns bucket encryption key info.
   * @return bucket encryption key info
   */
  public BucketEncryptionKeyInfo getEncryptionKeyInfo() {
    return bekInfo;
  }


  /**
   * Returns new builder class that builds a OmBucketInfo.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volumeName);
    auditMap.put(OzoneConsts.BUCKET, this.bucketName);
    auditMap.put(OzoneConsts.ACLS,
        (this.acls != null) ? this.acls.toString() : null);
    auditMap.put(OzoneConsts.IS_VERSION_ENABLED,
        String.valueOf(this.isVersionEnabled));
    auditMap.put(OzoneConsts.STORAGE_TYPE,
        (this.storageType != null) ? this.storageType.name() : null);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    return auditMap;
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private List<OzoneAcl> acls;
    private Boolean isVersionEnabled;
    private StorageType storageType;
    private long creationTime;
    private Map<String, String> metadata;
    private BucketEncryptionKeyInfo bekInfo;

    public Builder() {
      //Default values
      this.acls = new LinkedList<>();
      this.isVersionEnabled = false;
      this.storageType = StorageType.DISK;
      this.metadata = new HashMap<>();
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

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetadata) {
      if (additionalMetadata != null) {
        metadata.putAll(additionalMetadata);
      }
      return this;
    }

    public Builder setBucketEncryptionKey(
        BucketEncryptionKeyInfo info) {
      this.bekInfo = info;
      return this;
    }

    /**
     * Constructs the OmBucketInfo.
     * @return instance of OmBucketInfo.
     */
    public OmBucketInfo build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);
      Preconditions.checkNotNull(acls);
      Preconditions.checkNotNull(isVersionEnabled);
      Preconditions.checkNotNull(storageType);

      return new OmBucketInfo(volumeName, bucketName, acls,
          isVersionEnabled, storageType, creationTime, metadata, bekInfo);
    }
  }

  /**
   * Creates BucketInfo protobuf from OmBucketInfo.
   */
  public BucketInfo getProtobuf() {
    BucketInfo.Builder bib =  BucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .addAllAcls(acls.stream().map(
            OMPBHelper::convertOzoneAcl).collect(Collectors.toList()))
        .setIsVersionEnabled(isVersionEnabled)
        .setStorageType(storageType.toProto())
        .setCreationTime(creationTime)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata));
    if (bekInfo != null && bekInfo.getKeyName() != null) {
      bib.setBeinfo(OMPBHelper.convert(bekInfo));
    }
    return bib.build();
  }

  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getFromProtobuf(BucketInfo bucketInfo) {
    OmBucketInfo.Builder obib = OmBucketInfo.newBuilder()
        .setVolumeName(bucketInfo.getVolumeName())
        .setBucketName(bucketInfo.getBucketName())
        .setAcls(bucketInfo.getAclsList().stream().map(
            OMPBHelper::convertOzoneAcl).collect(Collectors.toList()))
        .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(bucketInfo.getStorageType()))
        .setCreationTime(bucketInfo.getCreationTime());
    if (bucketInfo.getMetadataList() != null) {
      obib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(bucketInfo.getMetadataList()));
    }
    if (bucketInfo.hasBeinfo()) {
      obib.setBucketEncryptionKey(OMPBHelper.convert(bucketInfo.getBeinfo()));
    }
    return obib.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmBucketInfo that = (OmBucketInfo) o;
    return creationTime == that.creationTime &&
        volumeName.equals(that.volumeName) &&
        bucketName.equals(that.bucketName) &&
        Objects.equals(acls, that.acls) &&
        Objects.equals(isVersionEnabled, that.isVersionEnabled) &&
        storageType == that.storageType &&
        Objects.equals(metadata, that.metadata) &&
        Objects.equals(bekInfo, that.bekInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(volumeName, bucketName);
  }
}
