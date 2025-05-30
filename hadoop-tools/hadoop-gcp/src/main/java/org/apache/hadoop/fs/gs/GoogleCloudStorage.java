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
import static java.lang.Math.toIntExact;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.gax.paging.Page;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper around <a href="https://github.com/googleapis/java-storage">Google cloud storage
 * client</a>.
 */
class GoogleCloudStorage {
  static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopFileSystem.class);
  static final List<Storage.BlobField> BLOB_FIELDS =
      ImmutableList.of(
          Storage.BlobField.BUCKET, Storage.BlobField.CONTENT_ENCODING,
          Storage.BlobField.CONTENT_TYPE, Storage.BlobField.CRC32C, Storage.BlobField.GENERATION,
          Storage.BlobField.METADATA, Storage.BlobField.MD5HASH, Storage.BlobField.METAGENERATION,
          Storage.BlobField.NAME, Storage.BlobField.SIZE, Storage.BlobField.TIME_CREATED,
          Storage.BlobField.UPDATED);

  static final CreateObjectOptions EMPTY_OBJECT_CREATE_OPTIONS =
      CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
          .setEnsureEmptyObjectsMetadataMatch(false)
          .build();

  private final Storage storage;
  private final GoogleHadoopFileSystemConfiguration configuration;

  /**
   * Having an instance of gscImpl to redirect calls to Json client while new client implementation
   * is in WIP.
   */
  GoogleCloudStorage(GoogleHadoopFileSystemConfiguration configuration) throws IOException {
    // TODO: Set credentials
    this.storage = createStorage(configuration.getProjectId());
    this.configuration = configuration;
  }

  private static Storage createStorage(String projectId) {
    if (projectId != null) {
      return StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    }

    return StorageOptions.newBuilder().build().getService();
  }

  WritableByteChannel create(final StorageResourceId resourceId, final CreateOptions options)
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

  void close() {
    try {
      storage.close();
    } catch (Exception e) {
      LOG.warn("Error occurred while closing the storage client", e);
    }
  }

  GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
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

  List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName,
      String objectNamePrefix,
      ListObjectOptions listOptions) {
    long maxResults = listOptions.getMaxResults() > 0 ?
        listOptions.getMaxResults() + (listOptions.isIncludePrefix() ? 0 : 1) :
        listOptions.getMaxResults();

    Storage.BlobListOption[] blobListOptions =
        getBlobListOptions(objectNamePrefix, listOptions, maxResults);
    Page<Blob> blobs = storage.list(bucketName, blobListOptions);
    ListDirectoryResult result = new ListDirectoryResult(maxResults);
    for (Blob blob : blobs.iterateAll()) {
      result.add(blob);
    }

    return result.getItems();
  }

  private Storage.BlobListOption[] getBlobListOptions(
      String objectNamePrefix, ListObjectOptions listOptions, long maxResults) {
    List<Storage.BlobListOption> options = new ArrayList<>();

    options.add(Storage.BlobListOption.fields(BLOB_FIELDS.toArray(new Storage.BlobField[0])));
    options.add(Storage.BlobListOption.prefix(objectNamePrefix));
    // TODO: set max results as a BlobListOption
    if ("/".equals(listOptions.getDelimiter())) {
      options.add(Storage.BlobListOption.currentDirectory());
    }

    if (listOptions.getDelimiter() != null) {
      options.add(Storage.BlobListOption.includeTrailingDelimiter());
    }

    return options.toArray(new Storage.BlobListOption[0]);
  }

  private GoogleCloudStorageItemInfo createItemInfoForBlob(Blob blob) {
    long generationId = blob.getGeneration() == null ? 0L : blob.getGeneration();
    StorageResourceId resourceId =
        new StorageResourceId(blob.getBucket(), blob.getName(), generationId);
    return createItemInfoForBlob(resourceId, blob);
  }

  void createBucket(String bucketName, CreateBucketOptions options) throws IOException {
    LOG.trace("createBucket({})", bucketName);
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkNotNull(options, "options must not be null");

    BucketInfo.Builder bucketInfoBuilder =
        BucketInfo.newBuilder(bucketName).setLocation(options.getLocation());

    if (options.getStorageClass() != null) {
      bucketInfoBuilder.setStorageClass(
          StorageClass.valueOfStrict(options.getStorageClass().toUpperCase()));
    }
    if (options.getTtl() != null) {
      bucketInfoBuilder.setLifecycleRules(
          Collections.singletonList(
              new BucketInfo.LifecycleRule(
                  BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction(),
                  BucketInfo.LifecycleRule.LifecycleCondition.newBuilder()
                      .setAge(toIntExact(options.getTtl().toDays()))
                      .build())));
    }
    try {
      storage.create(bucketInfoBuilder.build());
    } catch (StorageException e) {
      if (ErrorTypeExtractor.bucketAlreadyExists(e)) {
        throw (FileAlreadyExistsException)
            new FileAlreadyExistsException(String.format("Bucket '%s' already exists.", bucketName))
                .initCause(e);
      }
      throw new IOException(e);
    }
  }

  void createEmptyObject(StorageResourceId resourceId) throws IOException {
    LOG.trace("createEmptyObject({})", resourceId);
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    createEmptyObject(resourceId, EMPTY_OBJECT_CREATE_OPTIONS);
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObject(StorageResourceId, CreateObjectOptions)} for
   * details about expected behavior.
   */
  void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    try {
      createEmptyObjectInternal(resourceId, options);
    } catch (StorageException e) {
      if (canIgnoreExceptionForEmptyObject(e, resourceId, options)) {
        LOG.info(
            "Ignoring exception of type %s; verified object already exists with desired state.",
            e.getClass().getSimpleName());
        LOG.trace("Ignored exception while creating empty object: {}", resourceId, e);
      } else {
        if (ErrorTypeExtractor.getErrorType(e) == ErrorTypeExtractor.ErrorType.ALREADY_EXISTS) {
          throw (FileAlreadyExistsException)
              new FileAlreadyExistsException(
                  String.format("Object '%s' already exists.", resourceId)
              ).initCause(e);
        }
        throw new IOException(e);
      }
    }
  }

  /**
   * Helper to check whether an empty object already exists with the expected metadata specified in
   * {@code options}, to be used to determine whether it's safe to ignore an exception that was
   * thrown when trying to create the object, {@code exceptionOnCreate}.
   */
  private boolean canIgnoreExceptionForEmptyObject(
      StorageException exceptionOnCreate, StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    ErrorTypeExtractor.ErrorType errorType = ErrorTypeExtractor.getErrorType(exceptionOnCreate);
    if (shouldBackoff(resourceId, errorType)) {
      GoogleCloudStorageItemInfo existingInfo;
      Duration maxWaitTime = Duration.ofSeconds(3); // TODO: make this configurable

      BackOff backOff =
          !maxWaitTime.isZero() && !maxWaitTime.isNegative()
              ? new ExponentialBackOff.Builder()
              .setMaxElapsedTimeMillis(toIntExact(maxWaitTime.toMillis()))
              .setMaxIntervalMillis(500)
              .setInitialIntervalMillis(100)
              .setMultiplier(1.5)
              .setRandomizationFactor(0.15)
              .build()
              : BackOff.STOP_BACKOFF;
      long nextSleep = 0L;
      do {
        if (nextSleep > 0) {
          try {
            Sleeper.DEFAULT.sleep(nextSleep);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            nextSleep = BackOff.STOP;
          }
        }
        existingInfo = getItemInfo(resourceId);
        nextSleep = nextSleep == BackOff.STOP ? BackOff.STOP : backOff.nextBackOffMillis();
      } while (!existingInfo.exists() && nextSleep != BackOff.STOP);

      // Compare existence, size, and metadata; for 429 errors creating an empty object,
      // we don't care about metaGeneration/contentGeneration as long as the metadata
      // matches, since we don't know for sure whether our low-level request succeeded
      // first or some other client succeeded first.
      if (existingInfo.exists() && existingInfo.getSize() == 0) {
        if (options.isEnsureEmptyObjectsMetadataMatch()) {
          return existingInfo.metadataEquals(options.getMetadata());
        }
        return true;
      }
    }
    return false;
  }

  private static boolean shouldBackoff(StorageResourceId resourceId,
      ErrorTypeExtractor.ErrorType errorType) {
    return errorType == ErrorTypeExtractor.ErrorType.RESOURCE_EXHAUSTED
        || errorType == ErrorTypeExtractor.ErrorType.INTERNAL ||
        (resourceId.isDirectory() && errorType == ErrorTypeExtractor.ErrorType.FAILED_PRECONDITION);
  }

  private void createEmptyObjectInternal(
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions) {
    Map<String, String> rewrittenMetadata = encodeMetadata(createObjectOptions.getMetadata());

    List<Storage.BlobTargetOption> blobTargetOptions = new ArrayList<>();
    blobTargetOptions.add(Storage.BlobTargetOption.disableGzipContent());
    if (resourceId.hasGenerationId()) {
      blobTargetOptions.add(Storage.BlobTargetOption.generationMatch(resourceId.getGenerationId()));
    } else if (resourceId.isDirectory() || !createObjectOptions.isOverwriteExisting()) {
      blobTargetOptions.add(Storage.BlobTargetOption.doesNotExist());
    }

    // TODO: Set encryption key and related properties
    storage.create(
        BlobInfo.newBuilder(BlobId.of(resourceId.getBucketName(), resourceId.getObjectName()))
            .setMetadata(rewrittenMetadata)
            .setContentEncoding(createObjectOptions.getContentEncoding())
            .setContentType(createObjectOptions.getContentType())
            .build(),
        blobTargetOptions.toArray(new Storage.BlobTargetOption[0]));
  }

  private static Map<String, String> encodeMetadata(Map<String, byte[]> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorage::encodeMetadataValues);
  }

  private static String encodeMetadataValues(byte[] bytes) {
    return bytes == null ? null : BaseEncoding.base64().encode(bytes);
  }

  private class ListDirectoryResult {
    private final Map<String, Blob> prefixes = new HashMap<>();
    private final List<Blob> objects = new ArrayList<>();

    private  final Set<String> objectsSet = new HashSet<>();

    private final long maxResults;

    ListDirectoryResult(long maxResults) {
      this.maxResults = maxResults;
    }

    void add(Blob blob) {
      String path = blob.getBlobId().toGsUtilUri();
      if (blob.getGeneration() != null) {
        prefixes.remove(path);
        objects.add(blob);

        objectsSet.add(path);
      } else if (!objectsSet.contains(path)) {
        prefixes.put(path, blob);
      }
    }

    List<GoogleCloudStorageItemInfo> getItems() {
      List<GoogleCloudStorageItemInfo> result = new ArrayList<>(prefixes.size() + objects.size());

      for (Blob blob : objects) {
        result.add(createItemInfoForBlob(blob));

        if (result.size() == maxResults) {
          return result;
        }
      }

      for (Blob blob : prefixes.values()) {
        if (result.size() == maxResults) {
          return result;
        }

        result.add(createItemInfoForBlob(blob));
      }

      return result;
    }
  }
}
