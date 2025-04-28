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

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.*;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;

import com.google.cloud.storage.*;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around <a href="https://github.com/googleapis/java-storage">Google cloud storage client</a>
 */
class GoogleCloudStorage {
  public static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopFileSystem.class);
  static final List<Storage.BlobField> BLOB_FIELDS =
      ImmutableList.of(Storage.BlobField.BUCKET, Storage.BlobField.CONTENT_ENCODING,
          Storage.BlobField.CONTENT_TYPE, Storage.BlobField.CRC32C, Storage.BlobField.GENERATION,
          Storage.BlobField.METADATA, Storage.BlobField.MD5HASH, Storage.BlobField.METAGENERATION,
          Storage.BlobField.NAME, Storage.BlobField.SIZE, Storage.BlobField.TIME_CREATED,
          Storage.BlobField.UPDATED);
  private final Storage storage;
  private final GoogleHadoopFileSystemConfiguration configuration;

  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  GoogleCloudStorage(GoogleHadoopFileSystemConfiguration configuration) throws IOException {
    // TODO: Set projectId
    // TODO: Set credentials
    this.storage = StorageOptions.newBuilder().build().getService();
    this.configuration = configuration;
  }

  public WritableByteChannel create(final StorageResourceId resourceId, final CreateOptions options)
      throws IOException {
    LOG.trace("create({})", resourceId);

    checkArgument(resourceId.isStorageObject(), "Expected full StorageObject id, got %s",
        resourceId);
    // Update resourceId if generationId is missing
    StorageResourceId resourceIdWithGeneration = resourceId;
    if (!resourceId.hasGenerationId()) {
      resourceIdWithGeneration =
          new StorageResourceId(resourceId.getBucketName(), resourceId.getObjectName(),
              getWriteGeneration(resourceId, options.isOverwriteExisting()));
    }

    return new GoogleCloudStorageClientWriteChannel(storage, resourceIdWithGeneration, options);
  }

  /**
   * Gets the object generation for a write operation
   *
   * <p>making getItemInfo call even if overwrite is disabled to fail fast in case file is existing.
   *
   * @param resourceId object for which generation info is requested
   * @param overwrite  whether existing object should be overwritten
   * @return the generation of the object
   * @throws IOException if the object already exists and cannot be overwritten
   */
  private long getWriteGeneration(StorageResourceId resourceId, boolean overwrite)
      throws IOException {
    LOG.trace("getWriteGeneration({}, {})", resourceId, overwrite);
    GoogleCloudStorageItemInfo info = getItemInfo(resourceId);
    if (!info.exists()) {
      return 0L;
    }
    if (info.exists() && overwrite) {
      long generation = info.getContentGeneration();
      checkState(generation != 0, "Generation should not be 0 for an existing item");
      return generation;
    }

    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
  }

  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.warn("Error occurred while closing the storage client", e);
    }
  }

  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    LOG.trace("getItemInfo({})", resourceId);

    // Handle ROOT case first.
    if (resourceId.isRoot()) {
      return GoogleCloudStorageItemInfo.ROOT_INFO;
    }
    GoogleCloudStorageItemInfo itemInfo = null;

    if (resourceId.isBucket()) {
      Bucket bucket = getBucket(resourceId.getBucketName());
      if (bucket != null) {
        itemInfo = createItemInfoForBucket(resourceId, bucket);
      } else {
        LOG.debug("getBucket({}): not found", resourceId.getBucketName());
      }
    } else {
      Blob blob = getBlob(resourceId);
      if (blob != null) {
        itemInfo = createItemInfoForBlob(resourceId, blob);
      } else {
        LOG.debug("getObject({}): not found", resourceId);
      }
    }

    if (itemInfo == null) {
      itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    LOG.debug("getItemInfo: {}", itemInfo);
    return itemInfo;
  }

  /**
   * Gets the bucket with the given name.
   *
   * @param bucketName name of the bucket to get
   * @return the bucket with the given name or null if bucket not found
   * @throws IOException if the bucket exists but cannot be accessed
   */
  @Nullable
  private Bucket getBucket(String bucketName) throws IOException {
    LOG.debug("getBucket({})", bucketName);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    try {
      return storage.get(bucketName);
    } catch (StorageException e) {
      if (ErrorTypeExtractor.getErrorType(e) == ErrorTypeExtractor.ErrorType.NOT_FOUND) {
        return null;
      }
      throw new IOException("Error accessing Bucket " + bucketName, e);
    }
  }

  private static GoogleCloudStorageItemInfo createItemInfoForBlob(StorageResourceId resourceId,
      Blob blob) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(blob != null, "object must not be null");
    checkArgument(resourceId.isStorageObject(),
        "resourceId must be a StorageObject. resourceId: %s", resourceId);
    checkArgument(resourceId.getBucketName().equals(blob.getBucket()),
        "resourceId.getBucketName() must equal object.getBucket(): '%s' vs '%s'",
        resourceId.getBucketName(), blob.getBucket());
    checkArgument(resourceId.getObjectName().equals(blob.getName()),
        "resourceId.getObjectName() must equal object.getName(): '%s' vs '%s'",
        resourceId.getObjectName(), blob.getName());

    Map<String, byte[]> decodedMetadata =
        blob.getMetadata() == null ? null : decodeMetadata(blob.getMetadata());

    byte[] md5Hash = null;
    byte[] crc32c = null;

    if (!isNullOrEmpty(blob.getCrc32c())) {
      crc32c = BaseEncoding.base64().decode(blob.getCrc32c());
    }

    if (!isNullOrEmpty(blob.getMd5())) {
      md5Hash = BaseEncoding.base64().decode(blob.getMd5());
    }

    return GoogleCloudStorageItemInfo.createObject(resourceId,
        blob.getCreateTimeOffsetDateTime() == null ?
            0 :
            blob.getCreateTimeOffsetDateTime().toInstant().toEpochMilli(),
        blob.getUpdateTimeOffsetDateTime() == null ?
            0 :
            blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli(),
        blob.getSize() == null ? 0 : blob.getSize(), blob.getContentType(),
        blob.getContentEncoding(), decodedMetadata,
        blob.getGeneration() == null ? 0 : blob.getGeneration(),
        blob.getMetageneration() == null ? 0 : blob.getMetageneration(),
        new VerificationAttributes(md5Hash, crc32c));
  }

  static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorage::decodeMetadataValues);
  }

  @Nullable
  private static byte[] decodeMetadataValues(String value) {
    try {
      return BaseEncoding.base64().decode(value);
    } catch (IllegalArgumentException iae) {
      LOG.error("Failed to parse base64 encoded attribute value {}", value, iae);
      return null;
    }
  }

  /**
   * Gets the object with the given resourceId.
   *
   * @param resourceId identifies a StorageObject
   * @return the object with the given name or null if object not found
   * @throws IOException if the object exists but cannot be accessed
   */
  @Nullable
  Blob getBlob(StorageResourceId resourceId) throws IOException {
    checkArgument(resourceId.isStorageObject(), "Expected full StorageObject id, got %s",
        resourceId);
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    Blob blob;
    try {
      blob = storage.get(BlobId.of(bucketName, objectName),
          Storage.BlobGetOption.fields(BLOB_FIELDS.toArray(new Storage.BlobField[0])));
    } catch (StorageException e) {
      throw new IOException("Error accessing " + resourceId, e);
    }
    return blob;
  }

  private static GoogleCloudStorageItemInfo createItemInfoForBucket(StorageResourceId resourceId,
      Bucket bucket) {
    checkArgument(resourceId != null, "resourceId must not be null");
    checkArgument(bucket != null, "bucket must not be null");
    checkArgument(resourceId.isBucket(), "resourceId must be a Bucket. resourceId: %s", resourceId);
    checkArgument(resourceId.getBucketName().equals(bucket.getName()),
        "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
        resourceId.getBucketName(), bucket.getName());

    return GoogleCloudStorageItemInfo.createBucket(resourceId,
        bucket.asBucketInfo().getCreateTimeOffsetDateTime().toInstant().toEpochMilli(),
        bucket.asBucketInfo().getUpdateTimeOffsetDateTime().toInstant().toEpochMilli(),
        bucket.getLocation(),
        bucket.getStorageClass() == null ? null : bucket.getStorageClass().name());
  }
}
