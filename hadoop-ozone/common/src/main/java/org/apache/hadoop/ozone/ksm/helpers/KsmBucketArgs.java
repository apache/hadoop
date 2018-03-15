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

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocolPB.KSMPBHelper;

/**
 * A class that encapsulates Bucket Arguments.
 */
public final class KsmBucketArgs {
  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * ACL's that are to be added for the bucket.
   */
  private List<OzoneAcl> addAcls;
  /**
   * ACL's that are to be removed from the bucket.
   */
  private List<OzoneAcl> removeAcls;
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
   * Private constructor, constructed via builder.
   * @param volumeName - Volume name.
   * @param bucketName - Bucket name.
   * @param addAcls - ACL's to be added.
   * @param removeAcls - ACL's to be removed.
   * @param isVersionEnabled - Bucket version flag.
   * @param storageType - Storage type to be used.
   */
  private KsmBucketArgs(String volumeName, String bucketName,
      List<OzoneAcl> addAcls, List<OzoneAcl> removeAcls,
      Boolean isVersionEnabled, StorageType storageType) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.addAcls = addAcls;
    this.removeAcls = removeAcls;
    this.isVersionEnabled = isVersionEnabled;
    this.storageType = storageType;
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
   * Returns the ACL's that are to be added.
   * @return List<OzoneAclInfo>
   */
  public List<OzoneAcl> getAddAcls() {
    return addAcls;
  }

  /**
   * Returns the ACL's that are to be removed.
   * @return List<OzoneAclInfo>
   */
  public List<OzoneAcl> getRemoveAcls() {
    return removeAcls;
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getIsVersionEnabled() {
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
   * Returns new builder class that builds a KsmBucketArgs.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for KsmBucketArgs.
   */
  public static class Builder {
    private String volumeName;
    private String bucketName;
    private List<OzoneAcl> addAcls;
    private List<OzoneAcl> removeAcls;
    private Boolean isVersionEnabled;
    private StorageType storageType;

    public Builder setVolumeName(String volume) {
      this.volumeName = volume;
      return this;
    }

    public Builder setBucketName(String bucket) {
      this.bucketName = bucket;
      return this;
    }

    public Builder setAddAcls(List<OzoneAcl> acls) {
      this.addAcls = acls;
      return this;
    }

    public Builder setRemoveAcls(List<OzoneAcl> acls) {
      this.removeAcls = acls;
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

    /**
     * Constructs the KsmBucketArgs.
     * @return instance of KsmBucketArgs.
     */
    public KsmBucketArgs build() {
      Preconditions.checkNotNull(volumeName);
      Preconditions.checkNotNull(bucketName);
      return new KsmBucketArgs(volumeName, bucketName, addAcls,
          removeAcls, isVersionEnabled, storageType);
    }
  }

  /**
   * Creates BucketArgs protobuf from KsmBucketArgs.
   */
  public BucketArgs getProtobuf() {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVolumeName(volumeName)
        .setBucketName(bucketName);
    if(addAcls != null && !addAcls.isEmpty()) {
      builder.addAllAddAcls(addAcls.stream().map(
          KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
    }
    if(removeAcls != null && !removeAcls.isEmpty()) {
      builder.addAllRemoveAcls(removeAcls.stream().map(
          KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()));
    }
    if(isVersionEnabled != null) {
      builder.setIsVersionEnabled(isVersionEnabled);
    }
    if(storageType != null) {
      builder.setStorageType(
          PBHelperClient.convertStorageType(storageType));
    }
    return builder.build();
  }

  /**
   * Parses BucketInfo protobuf and creates KsmBucketArgs.
   * @param bucketArgs
   * @return instance of KsmBucketArgs
   */
  public static KsmBucketArgs getFromProtobuf(BucketArgs bucketArgs) {
    return new KsmBucketArgs(bucketArgs.getVolumeName(),
        bucketArgs.getBucketName(),
        bucketArgs.getAddAclsList().stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()),
        bucketArgs.getRemoveAclsList().stream().map(
            KSMPBHelper::convertOzoneAcl).collect(Collectors.toList()),
        bucketArgs.hasIsVersionEnabled() ?
            bucketArgs.getIsVersionEnabled() : null,
        bucketArgs.hasStorageType() ? PBHelperClient.convertStorageType(
            bucketArgs.getStorageType()) : null);
  }
}