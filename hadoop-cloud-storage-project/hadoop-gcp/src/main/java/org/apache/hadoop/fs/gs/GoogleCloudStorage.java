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
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.*;
import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Math.toIntExact;
import static org.apache.hadoop.fs.gs.GoogleCloudStorageExceptions.createFileNotFoundException;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.storage.*;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
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
import java.util.stream.Collectors;

/**
 * A wrapper around <a href="https://github.com/googleapis/java-storage">Google cloud storage
 * client</a>.
 */
class GoogleCloudStorage {
  static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorage.class);
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
  GoogleCloudStorage(GoogleHadoopFileSystemConfiguration configuration, Credentials credentials)
          throws IOException {
    this.storage = createStorage(configuration.getProjectId(), credentials);
    this.configuration = configuration;
  }

  private static Storage createStorage(String projectId, Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    if (projectId != null) {
      builder.setProjectId(projectId);
    }

    return builder.setCredentials(credentials).build().getService();
  }

  WritableByteChannel create(final StorageResourceId resourceId, final CreateFileOptions options)
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

  void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    try {
      createEmptyObjectInternal(resourceId, options);
    } catch (StorageException e) {
      if (canIgnoreExceptionForEmptyObject(e, resourceId, options)) {
        LOG.info(
            "Ignoring exception of type {}; verified object already exists with desired state.",
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

  GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    LOG.trace("composeObjects({}, {}, {})", sources, destination, options);
    for (StorageResourceId inputId : sources) {
      if (!destination.getBucketName().equals(inputId.getBucketName())) {
        throw new IOException(
            String.format(
                "Bucket doesn't match for source '%s' and destination '%s'!",
                inputId, destination));
      }
    }
    Storage.ComposeRequest request =
        Storage.ComposeRequest.newBuilder()
            .addSource(
                sources.stream().map(StorageResourceId::getObjectName).collect(Collectors.toList()))
            .setTarget(
                BlobInfo.newBuilder(destination.getBucketName(), destination.getObjectName())
                    .setContentType(options.getContentType())
                    .setContentEncoding(options.getContentEncoding())
                    .setMetadata(encodeMetadata(options.getMetadata()))
                    .build())
            .setTargetOptions(
                Storage.BlobTargetOption.generationMatch(
                    destination.hasGenerationId()
                        ? destination.getGenerationId()
                        : getWriteGeneration(destination, true)))
            .build();

    Blob composedBlob;
    try {
      composedBlob = storage.compose(request);
    } catch (StorageException e) {
      throw new IOException(e);
    }
    GoogleCloudStorageItemInfo compositeInfo = createItemInfoForBlob(destination, composedBlob);
    LOG.trace("composeObjects() done, returning: {}", compositeInfo);
    return compositeInfo;
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
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions) throws IOException {
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

  List<GoogleCloudStorageItemInfo> listDirectoryRecursive(String bucketName, String objectName)
      throws IOException {
    // TODO: Take delimiter from config
    // TODO: Set specific fields

    checkArgument(
            objectName == null || objectName.endsWith("/"),
            String.format("%s should end with /", objectName));
    try {
      List<Blob> blobs = new GcsListOperation.Builder(bucketName, objectName, storage)
          .forRecursiveListing().build()
          .execute();

      List<GoogleCloudStorageItemInfo> result = new ArrayList<>();
      for (Blob blob : blobs) {
        result.add(createItemInfoForBlob(blob));
      }

      return result;
    } catch (StorageException e) {
      throw new IOException(
          String.format("Listing '%s' failed", BlobId.of(bucketName, objectName)), e);
    }
  }

  void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    LOG.trace("deleteObjects({})", fullObjectNames);

    if (fullObjectNames.isEmpty()) {
      return;
    }

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId toDelete : fullObjectNames) {
      checkArgument(
          toDelete.isStorageObject(),
          "Expected full StorageObject names only, got: %s",
          toDelete);
    }

    // TODO: Do this concurrently
    // TODO: There is duplication. fix it
    for (StorageResourceId toDelete : fullObjectNames) {
      try {
        LOG.trace("Deleting Object ({})", toDelete);
        if (toDelete.hasGenerationId() && toDelete.getGenerationId() != 0) {
          storage.delete(
              BlobId.of(toDelete.getBucketName(), toDelete.getObjectName()),
              Storage.BlobSourceOption.generationMatch(toDelete.getGenerationId()));
        } else {
          // TODO: Remove delete without generationId
          storage.delete(BlobId.of(toDelete.getBucketName(), toDelete.getObjectName()));

          LOG.trace("Deleting Object without generationId ({})", toDelete);
        }
      } catch (StorageException e) {
        throw new IOException(String.format("Deleting resource %s failed.", toDelete), e);
      }
    }
  }

  List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    List<Bucket> allBuckets = listBucketsInternal();
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketInfos.add(createItemInfoForBucket(new StorageResourceId(bucket.getName()), bucket));
    }
    return bucketInfos;
  }


  private List<Bucket> listBucketsInternal() throws IOException {
    checkNotNull(configuration.getProjectId(), "projectId must not be null");
    List<Bucket> allBuckets = new ArrayList<>();
    try {
      Page<Bucket> buckets =
          storage.list(
              Storage.BucketListOption.pageSize(configuration.getMaxListItemsPerCall()),
              Storage.BucketListOption.fields(
                  Storage.BucketField.LOCATION,
                  Storage.BucketField.STORAGE_CLASS,
                  Storage.BucketField.TIME_CREATED,
                  Storage.BucketField.UPDATED));

      // Loop to fetch all the items.
      for (Bucket bucket : buckets.iterateAll()) {
        allBuckets.add(bucket);
      }
    } catch (StorageException e) {
      throw new IOException(e);
    }
    return allBuckets;
  }

  SeekableByteChannel open(GoogleCloudStorageItemInfo itemInfo,
      GoogleHadoopFileSystemConfiguration config) throws IOException {
    LOG.trace("open({})", itemInfo);
    checkNotNull(itemInfo, "itemInfo should not be null");

    StorageResourceId resourceId = itemInfo.getResourceId();
    checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return open(resourceId, itemInfo, config);
  }

  private SeekableByteChannel open(
      StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo,
      GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    return new GoogleCloudStorageClientReadChannel(
        storage,
        itemInfo == null ? getItemInfo(resourceId) : itemInfo,
        config);
  }

  void move(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {
    validateMoveArguments(sourceToDestinationObjectsMap);

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId srcObject = entry.getKey();
      StorageResourceId dstObject = entry.getValue();
      // TODO: Do this concurrently
      moveInternal(
          srcObject.getBucketName(),
          srcObject.getGenerationId(),
          srcObject.getObjectName(),
          dstObject.getGenerationId(),
          dstObject.getObjectName());
    }
  }

  private void moveInternal(
      String srcBucketName,
      long srcContentGeneration,
      String srcObjectName,
      long dstContentGeneration,
      String dstObjectName) throws IOException {
    Storage.MoveBlobRequest.Builder moveRequestBuilder =
        createMoveRequestBuilder(
            srcBucketName,
            srcObjectName,
            dstObjectName,
            srcContentGeneration,
            dstContentGeneration);
    try {
      String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
      String dstString = StringPaths.fromComponents(srcBucketName, dstObjectName);

      Blob movedBlob = storage.moveBlob(moveRequestBuilder.build());
      if (movedBlob != null) {
        LOG.trace("Successfully moved {} to {}", srcString, dstString);
      }
    } catch (StorageException e) {
      if (ErrorTypeExtractor.getErrorType(e) == ErrorTypeExtractor.ErrorType.NOT_FOUND) {
        throw createFileNotFoundException(srcBucketName, srcObjectName, new IOException(e));
      } else {
        throw
            new IOException(
                String.format(
                    "Error moving '%s'",
                    StringPaths.fromComponents(srcBucketName, srcObjectName)),
                e);
      }
    }
  }

  /** Creates a builder for a blob move request. */
  private Storage.MoveBlobRequest.Builder createMoveRequestBuilder(
      String srcBucketName,
      String srcObjectName,
      String dstObjectName,
      long srcContentGeneration,
      long dstContentGeneration) {

    Storage.MoveBlobRequest.Builder moveRequestBuilder =
        Storage.MoveBlobRequest.newBuilder().setSource(BlobId.of(srcBucketName, srcObjectName));
    moveRequestBuilder.setTarget(BlobId.of(srcBucketName, dstObjectName));

    List<Storage.BlobTargetOption> blobTargetOptions = new ArrayList<>();
    List<Storage.BlobSourceOption> blobSourceOptions = new ArrayList<>();

    if (srcContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      blobSourceOptions.add(Storage.BlobSourceOption.generationMatch(srcContentGeneration));
    }

    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      blobTargetOptions.add(Storage.BlobTargetOption.generationMatch(dstContentGeneration));
    }

    // TODO: Add encryption support

    moveRequestBuilder.setSourceOptions(blobSourceOptions);
    moveRequestBuilder.setTargetOptions(blobTargetOptions);

    return moveRequestBuilder;
  }

  /**
   * Validates basic argument constraints like non-null, non-empty Strings, using {@code
   * Preconditions} in addition to checking for src/dst bucket equality.
   */
  static void validateMoveArguments(
      Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap) throws IOException {
    checkNotNull(sourceToDestinationObjectsMap, "srcObjects must not be null");

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId source = entry.getKey();
      StorageResourceId destination = entry.getValue();
      String srcBucketName = source.getBucketName();
      String dstBucketName = destination.getBucketName();
      // Avoid move across buckets.
      if (!srcBucketName.equals(dstBucketName)) {
        throw new UnsupportedOperationException(
            "This operation is not supported across two different buckets.");
      }
      checkArgument(
          !isNullOrEmpty(source.getObjectName()), "srcObjectName must not be null or empty");
      checkArgument(
          !isNullOrEmpty(destination.getObjectName()), "dstObjectName must not be null or empty");
      if (srcBucketName.equals(dstBucketName)
          && source.getObjectName().equals(destination.getObjectName())) {
        throw new IllegalArgumentException(
            String.format(
                "Move destination must be different from source for %s.",
                StringPaths.fromComponents(srcBucketName, source.getObjectName())));
      }
    }
  }

  void copy(Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap)
      throws IOException {
    validateCopyArguments(sourceToDestinationObjectsMap, this);

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId srcObject = entry.getKey();
      StorageResourceId dstObject = entry.getValue();
      // TODO: Do this concurrently
      copyInternal(
          srcObject.getBucketName(),
          srcObject.getObjectName(),
          dstObject.getGenerationId(),
          dstObject.getBucketName(),
          dstObject.getObjectName());
    }
  }

  private void copyInternal(
      String srcBucketName,
      String srcObjectName,
      long dstContentGeneration,
      String dstBucketName,
      String dstObjectName) throws IOException {
    Storage.CopyRequest.Builder copyRequestBuilder =
        Storage.CopyRequest.newBuilder().setSource(BlobId.of(srcBucketName, srcObjectName));
    if (dstContentGeneration != StorageResourceId.UNKNOWN_GENERATION_ID) {
      copyRequestBuilder.setTarget(
          BlobId.of(dstBucketName, dstObjectName),
          Storage.BlobTargetOption.generationMatch(dstContentGeneration));
    } else {
      copyRequestBuilder.setTarget(BlobId.of(dstBucketName, dstObjectName));
    }

    // TODO: Add support for encryption key
    if (configuration.getMaxRewriteChunkSize() > 0) {
      copyRequestBuilder.setMegabytesCopiedPerChunk(
          // Convert raw byte size into Mib.
          configuration.getMaxRewriteChunkSize() / (1024 * 1024));
    }

    String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
    String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);

    try {
      CopyWriter copyWriter = storage.copy(copyRequestBuilder.build());
      while (!copyWriter.isDone()) {
        copyWriter.copyChunk();
        LOG.trace(
            "Copy ({} to {}) did not complete. Resuming...", srcString, dstString);
      }
      LOG.trace("Successfully copied {} to {}", srcString, dstString);
    } catch (StorageException e) {
      if (ErrorTypeExtractor.getErrorType(e) == ErrorTypeExtractor.ErrorType.NOT_FOUND) {
        throw createFileNotFoundException(srcBucketName, srcObjectName, new IOException(e));
      } else {
        throw new IOException(String.format("copy(%s->%s) failed.", srcString, dstString), e);
      }
    }
  }

  static void validateCopyArguments(
      Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap,
      GoogleCloudStorage gcsImpl)
      throws IOException {
    checkNotNull(sourceToDestinationObjectsMap, "srcObjects must not be null");

    if (sourceToDestinationObjectsMap.isEmpty()) {
      return;
    }

    Map<StorageResourceId, GoogleCloudStorageItemInfo> bucketInfoCache = new HashMap<>();

    for (Map.Entry<StorageResourceId, StorageResourceId> entry :
        sourceToDestinationObjectsMap.entrySet()) {
      StorageResourceId source = entry.getKey();
      StorageResourceId destination = entry.getValue();
      String srcBucketName = source.getBucketName();
      String dstBucketName = destination.getBucketName();
      // Avoid copy across locations or storage classes.
      if (!srcBucketName.equals(dstBucketName)) {
        StorageResourceId srcBucketResourceId = new StorageResourceId(srcBucketName);
        GoogleCloudStorageItemInfo srcBucketInfo =
            getGoogleCloudStorageItemInfo(gcsImpl, bucketInfoCache, srcBucketResourceId);
        if (!srcBucketInfo.exists()) {
          throw new FileNotFoundException("Bucket not found: " + srcBucketName);
        }

        StorageResourceId dstBucketResourceId = new StorageResourceId(dstBucketName);
        GoogleCloudStorageItemInfo dstBucketInfo =
            getGoogleCloudStorageItemInfo(gcsImpl, bucketInfoCache, dstBucketResourceId);
        if (!dstBucketInfo.exists()) {
          throw new FileNotFoundException("Bucket not found: " + dstBucketName);
        }

        // TODO: Restrict this only when copy-with-rewrite is enabled
        if (!srcBucketInfo.getLocation().equals(dstBucketInfo.getLocation())) {
          throw new UnsupportedOperationException(
              "This operation is not supported across two different storage locations.");
        }

        if (!srcBucketInfo.getStorageClass().equals(dstBucketInfo.getStorageClass())) {
          throw new UnsupportedOperationException(
              "This operation is not supported across two different storage classes.");
        }
      }
      checkArgument(
          !isNullOrEmpty(source.getObjectName()), "srcObjectName must not be null or empty");
      checkArgument(
          !isNullOrEmpty(destination.getObjectName()), "dstObjectName must not be null or empty");
      if (srcBucketName.equals(dstBucketName)
          && source.getObjectName().equals(destination.getObjectName())) {
        throw new IllegalArgumentException(
            String.format(
                "Copy destination must be different from source for %s.",
                StringPaths.fromComponents(srcBucketName, source.getObjectName())));
      }
    }
  }

  private static GoogleCloudStorageItemInfo getGoogleCloudStorageItemInfo(
      GoogleCloudStorage gcsImpl,
      Map<StorageResourceId, GoogleCloudStorageItemInfo> bucketInfoCache,
      StorageResourceId resourceId)
      throws IOException {
    GoogleCloudStorageItemInfo storageItemInfo = bucketInfoCache.get(resourceId);
    if (storageItemInfo != null) {
      return storageItemInfo;
    }
    storageItemInfo = gcsImpl.getItemInfo(resourceId);
    bucketInfoCache.put(resourceId, storageItemInfo);
    return storageItemInfo;
  }

  List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    LOG.trace("getItemInfos({})", resourceIds);

    if (resourceIds.isEmpty()) {
      return new ArrayList<>();
    }

    List<GoogleCloudStorageItemInfo> result = new ArrayList<>(resourceIds.size());
    for (StorageResourceId resourceId : resourceIds) {
      // TODO: Do this concurrently
      result.add(getItemInfo(resourceId));
    }

    return result;
  }

  List<GoogleCloudStorageItemInfo> listDirectory(String bucketName, String objectNamePrefix)
      throws IOException {
    checkArgument(
        objectNamePrefix == null || objectNamePrefix.endsWith("/"),
        String.format("%s should end with /", objectNamePrefix));

    try {
      List<Blob> blobs = new GcsListOperation.Builder(bucketName, objectNamePrefix, storage)
          .forCurrentDirectoryListing().build()
          .execute();

      ListOperationResult result = new ListOperationResult();
      for (Blob blob : blobs) {
        result.add(blob);
      }

      return result.getItems();
    } catch (StorageException e) {
      throw new IOException(
          String.format("listing object '%s' failed.", BlobId.of(bucketName, objectNamePrefix)),
          e);
    }
  }

  void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    LOG.trace("compose({}, {}, {}, {})", bucketName, sources, destination, contentType);
    List<StorageResourceId> sourceIds =
        sources.stream()
            .map(objectName -> new StorageResourceId(bucketName, objectName))
            .collect(Collectors.toList());
    StorageResourceId destinationId = new StorageResourceId(bucketName, destination);
    CreateObjectOptions options =
        CreateObjectOptions.DEFAULT_OVERWRITE.toBuilder()
            .setContentType(contentType)
            .setEnsureEmptyObjectsMetadataMatch(false)
            .build();
    composeObjects(sourceIds, destinationId, options);
  }

  /**
   * Get metadata for the given resourceId. The resourceId can be a file or a directory.
   *
   * For a resourceId gs://b/foo/a, it can be a file or a directory (gs:/b/foo/a/).
   * This method checks for both and return the one that is found. "NotFound" is returned
   * if not found.
   */
  GoogleCloudStorageItemInfo getFileOrDirectoryInfo(StorageResourceId resourceId) {
    BlobId blobId = resourceId.toBlobId();
    if (resourceId.isDirectory()) {
      // Do not check for "file" for directory paths.
      Blob blob = storage.get(blobId);
      if (blob != null) {
        return createItemInfoForBlob(blob);
      }
    } else {
      BlobId dirId = resourceId.toDirectoryId().toBlobId();

      // Check for both file and directory.
      List<Blob> blobs = storage.get(blobId, dirId);
      for (Blob blob : blobs) {
        if (blob != null) {
          return createItemInfoForBlob(blob);
        }
      }
    }

    return GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  /**
   * Check if any "implicit" directory exists for the given resourceId.
   *
   * Note that GCS object store does not have a concept of directories for non-HNS buckets.
   * For e.g. one could create an object gs://bucket/foo/bar/a.txt, without creating the
   * parent directories (i.e. placeholder emtpy files ending with a /). In this case we might
   * want to treat gs://bucket/foo/ and gs://bucket/foo/bar/ as directories.
   *
   * This method helps check if a given resourceId (e.g. gs://bucket/foo/bar/) is an "implicit"
   * directory.
   *
   * Note that this will result in a list operation and is more expensive than "get metadata".
   */
  GoogleCloudStorageItemInfo getImplicitDirectory(StorageResourceId resourceId) {
    List<Blob> blobs = new GcsListOperation
        .Builder(resourceId.getBucketName(), resourceId.getObjectName(), storage)
        .forImplicitDirectoryCheck().build()
        .execute();

    if (blobs.isEmpty()) {
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }

    return GoogleCloudStorageItemInfo.createInferredDirectory(resourceId.toDirectoryId());
  }

  public void deleteBuckets(List<String> bucketNames) throws IOException {
    LOG.trace("deleteBuckets({})", bucketNames);

    // Validate all the inputs first.
    for (String bucketName : bucketNames) {
      checkArgument(!Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    }

    // Gather exceptions to wrap in a composite exception at the end.
    List<IOException> innerExceptions = new ArrayList<>();

    for (String bucketName : bucketNames) {
      try {
        boolean isDeleted = storage.delete(bucketName);
        if (!isDeleted) {
          innerExceptions.add(createFileNotFoundException(bucketName, null, null));
        }
      } catch (StorageException e) {
        innerExceptions.add(
                new IOException(String.format("Error deleting '%s' bucket", bucketName), e));
      }
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  // Helper class to capture the results of list operation.
  private class ListOperationResult {
    private final Map<String, Blob> prefixes = new HashMap<>();
    private final List<Blob> objects = new ArrayList<>();

    private  final Set<String> objectsSet = new HashSet<>();

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
      }

      for (Blob blob : prefixes.values()) {
        result.add(createItemInfoForBlob(blob));
      }

      return result;
    }
  }
}
