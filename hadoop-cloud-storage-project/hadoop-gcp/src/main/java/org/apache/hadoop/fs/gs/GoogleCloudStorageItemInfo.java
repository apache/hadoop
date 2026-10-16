/*
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

package org.apache.hadoop.fs.gs;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Contains information about an item in Google Cloud Storage.
 */
final class GoogleCloudStorageItemInfo {
  // Info about the root of GCS namespace.
  public static final GoogleCloudStorageItemInfo ROOT_INFO =
      new GoogleCloudStorageItemInfo(StorageResourceId.ROOT,
          /* creationTime= */ 0,
          /* modificationTime= */ 0,
          /* size= */ 0,
          /* location= */ null,
          /* storageClass= */ null,
          /* contentType= */ null,
          /* contentEncoding= */ null,
          /* metadata= */ null,
          /* contentGeneration= */ 0,
          /* metaGeneration= */ 0,
          /* verificationAttributes= */ null);

  /**
   * Factory method for creating a GoogleCloudStorageItemInfo for a bucket.
   *
   * @param resourceId       Resource ID that identifies a bucket
   * @param creationTime     Time when a bucket was created (milliseconds since January 1, 1970
   *                         UTC).
   * @param modificationTime Time when a bucket was last modified (milliseconds since January 1,
   *                         1970 UTC).
   * @param location         Location of a bucket.
   * @param storageClass     Storage class of a bucket.
   */
  static GoogleCloudStorageItemInfo createBucket(StorageResourceId resourceId,
      long creationTime, long modificationTime, String location, String storageClass) {
    checkNotNull(resourceId, "resourceId must not be null");
    checkArgument(resourceId.isBucket(), "expected bucket but got '%s'", resourceId);
    return new GoogleCloudStorageItemInfo(resourceId, creationTime, modificationTime,
        /* size= */ 0, location, storageClass,
        /* contentType= */ null,
        /* contentEncoding= */ null,
        /* metadata= */ null,
        /* contentGeneration= */ 0,
        /* metaGeneration= */ 0,
        /* verificationAttributes= */ null);
  }

  /**
   * Factory method for creating a GoogleCloudStorageItemInfo for an object.
   *
   * @param resourceId   identifies either root, a Bucket, or a StorageObject
   * @param creationTime Time when object was created (milliseconds since January 1, 1970
   *                     UTC).
   * @param size         Size of the given object (number of bytes) or -1 if the object
   *                     does not exist.
   * @param metadata     User-supplied object metadata for this object.
   */
  static GoogleCloudStorageItemInfo createObject(StorageResourceId resourceId,
      long creationTime, long modificationTime, long size, String contentType,
      String contentEncoding, Map<String, byte[]> metadata, long contentGeneration,
      long metaGeneration, VerificationAttributes verificationAttributes) {
    checkNotNull(resourceId, "resourceId must not be null");
    checkArgument(
        !resourceId.isRoot(),
        "expected object or directory but got '%s'", resourceId);
    checkArgument(
        !resourceId.isBucket(),
        "expected object or directory but got '%s'", resourceId);
    return new GoogleCloudStorageItemInfo(resourceId, creationTime, modificationTime, size,
        /* location= */ null,
        /* storageClass= */ null, contentType, contentEncoding, metadata, contentGeneration,
        metaGeneration, verificationAttributes);
  }

  /**
   * Factory method for creating a "found" GoogleCloudStorageItemInfo for an inferred directory.
   *
   * @param resourceId Resource ID that identifies an inferred directory
   */
  static GoogleCloudStorageItemInfo createInferredDirectory(StorageResourceId resourceId) {
    return new GoogleCloudStorageItemInfo(resourceId,
        /* creationTime= */ 0,
        /* modificationTime= */ 0,
        /* size= */ 0,
        /* location= */ null,
        /* storageClass= */ null,
        /* contentType= */ null,
        /* contentEncoding= */ null,
        /* metadata= */ null,
        /* contentGeneration= */ 0,
        /* metaGeneration= */ 0,
        /* verificationAttributes= */ null);
  }

  /**
   * Factory method for creating a "not found" GoogleCloudStorageItemInfo for a bucket or an object.
   *
   * @param resourceId Resource ID that identifies an inferred directory
   */
  static GoogleCloudStorageItemInfo createNotFound(StorageResourceId resourceId) {
    return new GoogleCloudStorageItemInfo(resourceId,
        /* creationTime= */ 0,
        /* modificationTime= */ 0,
        /* size= */ -1,
        /* location= */ null,
        /* storageClass= */ null,
        /* contentType= */ null,
        /* contentEncoding= */ null,
        /* metadata= */ null,
        /* contentGeneration= */ 0,
        /* metaGeneration= */ 0,
        /* verificationAttributes= */ null);
  }

  // The Bucket and maybe StorageObject names of the GCS "item" referenced by this object. Not
  // null.
  private final StorageResourceId resourceId;

  // Creation time of this item.
  // Time is expressed as milliseconds since January 1, 1970 UTC.
  private final long creationTime;

  // Modification time of this item.
  // Time is expressed as milliseconds since January 1, 1970 UTC.
  private final long modificationTime;

  // Size of an object (number of bytes).
  // Size is -1 for items that do not exist.
  private final long size;

  // Location of this item.
  private final String location;

  // Storage class of this item.
  private final String storageClass;

  // Content-Type of this item
  private final String contentType;

  private final String contentEncoding;

  // User-supplied metadata.
  private final Map<String, byte[]> metadata;

  private final long contentGeneration;

  private final long metaGeneration;

  private final VerificationAttributes verificationAttributes;

  private GoogleCloudStorageItemInfo(StorageResourceId resourceId, long creationTime,
      long modificationTime, long size, String location, String storageClass, String contentType,
      String contentEncoding, Map<String, byte[]> metadata, long contentGeneration,
      long metaGeneration, VerificationAttributes verificationAttributes) {
    this.resourceId = checkNotNull(resourceId, "resourceId must not be null");
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.size = size;
    this.location = location;
    this.storageClass = storageClass;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.metadata = (metadata == null) ? ImmutableMap.of() : metadata;
    this.contentGeneration = contentGeneration;
    this.metaGeneration = metaGeneration;
    this.verificationAttributes = verificationAttributes;
  }

  /**
   * Gets bucket name of this item.
   */
  String getBucketName() {
    return resourceId.getBucketName();
  }

  /**
   * Gets object name of this item.
   */
  String getObjectName() {
    return resourceId.getObjectName();
  }

  /**
   * Gets the resourceId that holds the (possibly null) bucketName and objectName of this object.
   */
  StorageResourceId getResourceId() {
    return resourceId;
  }

  /**
   * Gets creation time of this item.
   *
   * <p>Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  long getCreationTime() {
    return creationTime;
  }

  /**
   * Gets modification time of this item.
   *
   * <p>Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  long getModificationTime() {
    return modificationTime;
  }

  /**
   * Gets size of this item (number of bytes). Returns -1 if the object does not exist.
   */
  long getSize() {
    return size;
  }

  /**
   * Gets location of this item.
   *
   * <p>Note: Location is only supported for buckets. The value is always null for objects.
   */
  String getLocation() {
    return location;
  }

  /**
   * Gets storage class of this item.
   *
   * <p>Note: Storage-class is only supported for buckets. The value is always null for objects.
   */
  String getStorageClass() {
    return storageClass;
  }

  /**
   * Gets the content-type of this item, or null if unknown or inapplicable.
   *
   * <p>Note: content-type is only supported for objects, and will always be null for buckets.
   */
  String getContentType() {
    return contentType;
  }

  /**
   * Gets the content-encoding of this item, or null if unknown or inapplicable.
   *
   * <p>Note: content-encoding is only supported for objects, and will always be null for buckets.
   */
  String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * Gets user-supplied metadata for this item.
   *
   * <p>Note: metadata is only supported for objects. This value is always an empty map for buckets.
   */
  Map<String, byte[]> getMetadata() {
    return metadata;
  }

  /**
   * Indicates whether this item is a bucket. Root is not considered to be a bucket.
   */
  boolean isBucket() {
    return resourceId.isBucket();
  }

  /**
   * Indicates whether this item refers to the GCS root (gs://).
   */
  boolean isRoot() {
    return resourceId.isRoot();
  }

  /**
   * Indicates whether this instance has information about the unique, shared root of the underlying
   * storage system.
   */
  boolean isGlobalRoot() {
    return isRoot() && exists();
  }

  /**
   * Indicates whether {@code itemInfo} is a directory.
   */
  boolean isDirectory() {
    return isGlobalRoot() || isBucket() || resourceId.isDirectory();
  }

  /**
   * Indicates whether {@code itemInfo} is an inferred directory.
   */
  boolean isInferredDirectory() {
    return creationTime == 0 && modificationTime == 0 && size == 0 && contentGeneration == 0
        && metaGeneration == 0;
  }

  /**
   * Get the content generation of the object.
   */
  long getContentGeneration() {
    return contentGeneration;
  }

  /**
   * Get the meta generation of the object.
   */
  long getMetaGeneration() {
    return metaGeneration;
  }

  /**
   * Get object validation attributes.
   */
  VerificationAttributes getVerificationAttributes() {
    return verificationAttributes;
  }

  /**
   * Indicates whether this item exists.
   */
  boolean exists() {
    return size >= 0;
  }

  /**
   * Helper for checking logical equality of metadata maps, checking equality of keySet() between
   * this.metadata and otherMetadata, and then using Arrays.equals to compare contents of
   * corresponding byte arrays.
   */
  @VisibleForTesting
  public boolean metadataEquals(Map<String, byte[]> otherMetadata) {
    if (metadata == otherMetadata) {
      // Fast-path for common cases where the same actual default metadata instance may be
      // used in
      // multiple different item infos.
      return true;
    }
    // No need to check if other `metadata` is not null,
    // because previous `if` checks if both of them are null.
    if (metadata == null || otherMetadata == null) {
      return false;
    }
    if (!metadata.keySet().equals(otherMetadata.keySet())) {
      return false;
    }

    // Compare each byte[] with Arrays.equals.
    for (Map.Entry<String, byte[]> metadataEntry : metadata.entrySet()) {
      if (!Arrays.equals(metadataEntry.getValue(), otherMetadata.get(metadataEntry.getKey()))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets string representation of this instance.
   */
  @Override
  public String toString() {
    return exists() ?
        String.format("%s: created on: %s", resourceId, Instant.ofEpochMilli(creationTime)) :
        String.format("%s: exists: no", resourceId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GoogleCloudStorageItemInfo) {
      GoogleCloudStorageItemInfo other = (GoogleCloudStorageItemInfo) obj;
      return resourceId.equals(other.resourceId) && creationTime == other.creationTime
          && modificationTime == other.modificationTime && size == other.size && Objects.equals(
          location, other.location) && Objects.equals(storageClass, other.storageClass)
          && Objects.equals(verificationAttributes, other.verificationAttributes)
          && metaGeneration == other.metaGeneration && contentGeneration == other.contentGeneration
          && metadataEquals(other.getMetadata());
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + resourceId.hashCode();
    result = prime * result + (int) creationTime;
    result = prime * result + (int) modificationTime;
    result = prime * result + (int) size;
    result = prime * result + Objects.hashCode(location);
    result = prime * result + Objects.hashCode(storageClass);
    result = prime * result + Objects.hashCode(verificationAttributes);
    result = prime * result + (int) metaGeneration;
    result = prime * result + (int) contentGeneration;
    result = prime * result + metadata.entrySet().stream()
        .mapToInt(e -> Objects.hash(e.getKey()) + Arrays.hashCode(e.getValue())).sum();
    return result;
  }
}
