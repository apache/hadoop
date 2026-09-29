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
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.hadoop.fs.gs.Constants.SCHEME;

import com.google.cloud.storage.BlobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data struct representing either a GCS StorageObject, a GCS Bucket or the GCS root (gs://). If
 * both bucketName and objectName are null, the StorageResourceId refers to GCS root (gs://). If
 * bucketName is non-null, and objectName is null, then this refers to a GCS Bucket. Otherwise, if
 * bucketName and objectName are both non-null, this refers to a GCS StorageObject.
 */
class StorageResourceId {

  private static final Logger LOG = LoggerFactory.getLogger(StorageResourceId.class);

  // The generationId used to denote "unknown"; if given to a method expecting generationId
  // constraints, the method may perform extra low-level GETs to determine an existing
  // generationId
  // if idempotency constraints require doing so.
  static final long UNKNOWN_GENERATION_ID = -1L;

  // Pattern that parses out bucket and object names.
  // Given 'gs://foo-bucket/foo/bar/baz', matcher.group(x) will return:
  // 0 = gs://foo-bucket/foo/bar/baz
  // 1 = foo-bucket/foo/bar/baz
  // 2 = foo-bucket
  // 3 = /foo/bar/baz
  // 4 = foo/bar/baz
  // Groups 2 and 4 can be used to create an instance.
  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("gs://(([^/]+)(/(.+)?)?)?");

  // The singleton instance identifying the GCS root (gs://). Both getObjectName() and
  // getBucketName() will return null.
  static final StorageResourceId ROOT = new StorageResourceId();

  // Bucket name of this storage resource to be used with the Google Cloud Storage API.
  private final String bucketName;

  // Object name of this storage resource to be used with the Google Cloud Storage API.
  private final String objectName;

  // Human-readable String to be returned by toString(); kept as 'final' member for efficiency.
  private final String stringPath;

  // The generationId to be used with precondition checks when using this StorageResourceId
  // as an identifier for mutation requests.
  private final long generationId;

  /**
   * Constructor for a StorageResourceId that refers to the GCS root (gs://). Private because all
   * external users should just use the singleton StorageResourceId.ROOT.
   */
  private StorageResourceId() {
    this.bucketName = null;
    this.objectName = null;
    this.stringPath = StringPaths.fromComponents(bucketName, objectName);
    this.generationId = UNKNOWN_GENERATION_ID;
  }

  /**
   * Constructor for a StorageResourceId representing a Bucket; {@code getObjectName()} will return
   * null for a StorageResourceId that represents a Bucket.
   *
   * @param bucketName The bucket name of the resource. Must be non-empty and non-null.
   */
  StorageResourceId(String bucketName) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");

    this.bucketName = bucketName;
    this.objectName = null;
    this.stringPath = StringPaths.fromComponents(bucketName, objectName);
    this.generationId = UNKNOWN_GENERATION_ID;
  }

  /**
   * Constructor for a StorageResourceId representing a full StorageObject, including bucketName and
   * objectName.
   *
   * @param bucketName The bucket name of the resource. Must be non-empty and non-null.
   * @param objectName The object name of the resource. Must be non-empty and non-null.
   */
  StorageResourceId(String bucketName, String objectName) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkArgument(!isNullOrEmpty(objectName), "objectName must not be null or empty");

    this.bucketName = bucketName;
    this.objectName = objectName;
    this.stringPath = StringPaths.fromComponents(bucketName, objectName);
    this.generationId = UNKNOWN_GENERATION_ID;
  }

  /**
   * Constructor for a StorageResourceId representing a full StorageObject, including bucketName and
   * objectName.
   *
   * @param bucketName   The bucket name of the resource. Must be non-empty and non-null.
   * @param objectName   The object name of the resource. Must be non-empty and non-null.
   * @param generationId The generationId to be used with precondition checks when using this
   *                     StorageResourceId as an identifier for mutation requests.
   */
  StorageResourceId(String bucketName, String objectName, long generationId) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkArgument(!isNullOrEmpty(objectName), "objectName must not be null or empty");

    this.bucketName = bucketName;
    this.objectName = objectName;
    this.stringPath = StringPaths.fromComponents(bucketName, objectName);
    this.generationId = generationId;
  }

  /**
   * Constructor for a StorageResourceId representing a full StorageObject, including bucketName and
   * objectName.
   *
   * @param bucketName   The bucket name of the resource. Must be non-empty and non-null.
   * @param generationId The generationId to be used with precondition checks when using this
   *                     StorageResourceId as an identifier for mutation requests.
   */
  StorageResourceId(String bucketName, long generationId) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    this.bucketName = bucketName;
    this.objectName = null;
    this.stringPath = StringPaths.fromComponents(bucketName, objectName);
    this.generationId = generationId;
  }

  /**
   * Returns true if this StorageResourceId represents a GCS StorageObject; if true, both {@code
   * getBucketName} and {@code getObjectName} will be non-empty and non-null.
   */
  boolean isStorageObject() {
    return bucketName != null && objectName != null;
  }

  /**
   * Returns true if this StorageResourceId represents a GCS Bucket; if true, then {@code
   * getObjectName} will return null.
   */
  boolean isBucket() {
    return bucketName != null && objectName == null;
  }

  /**
   * Returns true if this StorageResourceId represents the GCS root (gs://); if true, then both
   * {@code getBucketName} and {@code getObjectName} will be null.
   */
  boolean isRoot() {
    return bucketName == null && objectName == null;
  }

  /**
   * Indicates if this StorageResourceId corresponds to a 'directory'; similar to {@link
   * FileInfo#isDirectory} except deals entirely with pathnames instead of also checking for
   * exists() to be true on a corresponding GoogleCloudStorageItemInfo.
   */
  boolean isDirectory() {
    return isRoot() || isBucket() || StringPaths.isDirectoryPath(objectName);
  }

  /**
   * Gets the bucket name component of this resource identifier.
   */
  String getBucketName() {
    return bucketName;
  }

  /**
   * Gets the object name component of this resource identifier.
   */
  String getObjectName() {
    return objectName;
  }

  /**
   * The generationId to be used with precondition checks when using this StorageResourceId as an
   * identifier for mutation requests. The generationId is *not* used when determining equals() or
   * hashCode().
   */
  long getGenerationId() {
    return generationId;
  }

  /**
   * Returns true if generationId is not UNKNOWN_GENERATION_ID.
   */
  boolean hasGenerationId() {
    return generationId != UNKNOWN_GENERATION_ID;
  }

  /**
   * Returns a string of the form {@code gs://<bucketName>/<objectName>}.
   */
  @Override
  public String toString() {
    return stringPath;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StorageResourceId) {
      StorageResourceId other = (StorageResourceId) obj;
      return Objects.equals(bucketName, other.bucketName) && Objects.equals(objectName,
          other.objectName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return stringPath.hashCode();
  }

  /**
   * Converts StorageResourceId instance to look like a directory path. If the path already looks
   * like a directory path then this call is a no-op.
   *
   * @return A resourceId with a directory path corresponding to the given resourceId.
   */
  StorageResourceId toDirectoryId() {
    if (isStorageObject() && !StringPaths.isDirectoryPath(getObjectName())) {
      return new StorageResourceId(getBucketName(), StringPaths.toDirectoryPath(getObjectName()));
    }
    return this;
  }

  /**
   * Parses {@link StorageResourceId} from specified string.
   */
  static StorageResourceId fromStringPath(String path) {
    return fromStringPath(path, UNKNOWN_GENERATION_ID);
  }

  /**
   * Parses {@link StorageResourceId} from specified string and generationId.
   */
  static StorageResourceId fromStringPath(String path, long generationId) {
    checkArgument(path != null, "path must not be null");

    Matcher matcher = GCS_PATH_PATTERN.matcher(path);
    checkArgument(matcher.matches(), "'%s' is not a valid GCS object name.", path);

    String bucketName = matcher.group(2);
    String relativePath = matcher.group(4);
    if (bucketName == null) {
      checkArgument(generationId == UNKNOWN_GENERATION_ID,
          "Cannot specify generationId '%s' for root object '%s'", generationId, path);
      return ROOT;
    } else if (relativePath != null) {
      return new StorageResourceId(bucketName, relativePath, generationId);
    }
    checkArgument(generationId == UNKNOWN_GENERATION_ID,
        "Cannot specify generationId '%s' for bucket '%s'", generationId, path);
    return new StorageResourceId(bucketName);
  }

  /**
   * Validates the given URI and if valid, returns the associated StorageResourceId.
   *
   * @param path                 The GCS URI to validate.
   * @param allowEmptyObjectName If true, a missing object name is not considered invalid.
   * @return a StorageResourceId that may be the GCS root, a Bucket, or a StorageObject.
   */
  static StorageResourceId fromUriPath(URI path, boolean allowEmptyObjectName) {
    return fromUriPath(path, allowEmptyObjectName, UNKNOWN_GENERATION_ID);
  }

  /**
   * Validates the given URI and if valid, returns the associated StorageResourceId.
   *
   * @param path                 The GCS URI to validate.
   * @param allowEmptyObjectName If true, a missing object name is not considered invalid.
   * @param generationId         The generationId to be used with precondition checks when
   *                             using this
   * @return a StorageResourceId that may be the GCS root, a Bucket, or a StorageObject.
   */
  static StorageResourceId fromUriPath(URI path, boolean allowEmptyObjectName,
      long generationId) {
    LOG.trace("fromUriPath('{}', {})", path, allowEmptyObjectName);
    checkNotNull(path);

    if (!SCHEME.equals(path.getScheme())) {
      throw new IllegalArgumentException(
          String.format("GCS path supports only '%s' scheme, instead got '%s' from '%s'.", SCHEME,
              path.getScheme(), path));
    }

    if (path.equals(GoogleCloudStorageFileSystem.GCSROOT)) {
      return ROOT;
    }

    String bucketName = StringPaths.validateBucketName(path.getAuthority());
    // Note that we're using getPath here instead of rawPath, etc. This is because it is assumed
    // that the path was properly encoded in getPath (or another similar method):
    String objectName = StringPaths.validateObjectName(path.getPath(), allowEmptyObjectName);

    return isNullOrEmpty(objectName) ?
        new StorageResourceId(bucketName, generationId) :
        new StorageResourceId(bucketName, objectName, generationId);
  }

  BlobId toBlobId() {
    return BlobId.of(bucketName, objectName);
  }
}
