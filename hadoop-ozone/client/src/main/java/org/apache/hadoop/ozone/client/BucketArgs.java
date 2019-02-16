/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a bucket.
 */
public final class BucketArgs {

  /**
   * ACL Information.
   */
  private List<OzoneAcl> acls;
  /**
   * Bucket Version flag.
   */
  private Boolean versioning;
  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private StorageType storageType;

  /**
   * Custom key/value metadata.
   */
  private Map<String, String> metadata;

  /**
   * Bucket encryption key name.
   */
  private String bucketEncryptionKey;

  /**
   * Private constructor, constructed via builder.
   * @param versioning Bucket version flag.
   * @param storageType Storage type to be used.
   * @param acls list of ACLs.
   * @param metadata map of bucket metadata
   * @param bucketEncryptionKey bucket encryption key name
   */
  private BucketArgs(Boolean versioning, StorageType storageType,
                     List<OzoneAcl> acls, Map<String, String> metadata,
                     String bucketEncryptionKey) {
    this.acls = acls;
    this.versioning = versioning;
    this.storageType = storageType;
    this.metadata = metadata;
    this.bucketEncryptionKey = bucketEncryptionKey;
  }

  /**
   * Returns true if bucket version is enabled, else false.
   * @return isVersionEnabled
   */
  public Boolean getVersioning() {
    return versioning;
  }

  /**
   * Returns the type of storage to be used.
   * @return StorageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns the ACL's associated with this bucket.
   * @return {@literal List<OzoneAcl>}
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Custom metadata for the buckets.
   *
   * @return key value map
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Returns the bucket encryption key name.
   * @return bucket encryption key
   */
  public String getEncryptionKey() {
    return bucketEncryptionKey;
  }

  /**
   * Returns new builder class that builds a OmBucketInfo.
   *
   * @return Builder
   */
  public static BucketArgs.Builder newBuilder() {
    return new BucketArgs.Builder();
  }

  /**
   * Builder for OmBucketInfo.
   */
  public static class Builder {
    private Boolean versioning;
    private StorageType storageType;
    private List<OzoneAcl> acls;
    private Map<String, String> metadata;
    private String bucketEncryptionKey;

    public Builder() {
      metadata = new HashMap<>();
    }

    public BucketArgs.Builder setVersioning(Boolean versionFlag) {
      this.versioning = versionFlag;
      return this;
    }

    public BucketArgs.Builder setStorageType(StorageType storage) {
      this.storageType = storage;
      return this;
    }

    public BucketArgs.Builder setAcls(List<OzoneAcl> listOfAcls) {
      this.acls = listOfAcls;
      return this;
    }

    public BucketArgs.Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public BucketArgs.Builder setBucketEncryptionKey(String bek) {
      this.bucketEncryptionKey = bek;
      return this;
    }
    /**
     * Constructs the BucketArgs.
     * @return instance of BucketArgs.
     */
    public BucketArgs build() {
      return new BucketArgs(versioning, storageType, acls, metadata,
          bucketEncryptionKey);
    }
  }
}
