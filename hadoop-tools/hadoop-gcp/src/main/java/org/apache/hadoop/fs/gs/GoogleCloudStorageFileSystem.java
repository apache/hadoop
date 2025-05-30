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
import static org.apache.hadoop.fs.gs.Constants.SCHEME;

import com.google.auth.Credentials;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * Provides FS semantics over GCS based on Objects API.
 */
class GoogleCloudStorageFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(StorageResourceId.class);

  private static final ListObjectOptions GET_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.builder().setIncludePrefix(true).setMaxResults(1).build();

  private static final ListObjectOptions LIST_FILE_INFO_LIST_OPTIONS =
      ListObjectOptions.DEFAULT.builder().setIncludePrefix(true).build();


  // URI of the root path.
  static final URI GCSROOT = URI.create(SCHEME + ":/");

  // GCS access instance.
  private GoogleCloudStorage gcs;

  private static GoogleCloudStorage createCloudStorage(
      final GoogleHadoopFileSystemConfiguration configuration, final Credentials credentials)
      throws IOException {
    checkNotNull(configuration, "configuration must not be null");

    return new GoogleCloudStorage(configuration);
  }

  GoogleCloudStorageFileSystem(final GoogleHadoopFileSystemConfiguration configuration,
      final Credentials credentials) throws IOException {
    gcs = createCloudStorage(configuration, credentials);
  }

  WritableByteChannel create(final URI path, final CreateOptions createOptions)
      throws IOException {
    LOG.trace("create(path: {}, createOptions: {})", path, createOptions);
    checkNotNull(path, "path could not be null");
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName=*/ true);

    if (resourceId.isDirectory()) {
      throw new IOException(
          String.format("Cannot create a file whose name looks like a directory: '%s'",
              resourceId));
    }

    if (createOptions.getOverwriteGenerationId() != StorageResourceId.UNKNOWN_GENERATION_ID) {
      resourceId = new StorageResourceId(resourceId.getBucketName(), resourceId.getObjectName(),
          createOptions.getOverwriteGenerationId());
    }

    return gcs.create(resourceId, createOptions);
  }

  void close() {
    if (gcs == null) {
      return;
    }
    LOG.trace("close()");
    try {
      gcs.close();
    } finally {
      gcs = null;
    }
  }

  public FileInfo getFileInfo(URI path) throws IOException {
    checkArgument(path != null, "path must not be null");
    // Validate the given path. true == allow empty object name.
    // One should be able to get info about top level directory (== bucket),
    // therefore we allow object name to be empty.
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);
    FileInfo fileInfo =
        FileInfo.fromItemInfo(
            getFileInfoInternal(resourceId, /* inferImplicitDirectories= */ true));
    LOG.trace("getFileInfo(path: {}): {}", path, fileInfo);
    return fileInfo;
  }

  private GoogleCloudStorageItemInfo getFileInfoInternal(
      StorageResourceId resourceId,
      boolean inferImplicitDirectories)
      throws IOException {
    if (resourceId.isRoot() || resourceId.isBucket()) {
      return gcs.getItemInfo(resourceId);
    }

    StorageResourceId dirId = resourceId.toDirectoryId();
    if (!resourceId.isDirectory()) {
      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(resourceId);
      if (itemInfo.exists()) {
        return itemInfo;
      }

      if (inferImplicitDirectories) {
        // TODO: Set max result
        List<GoogleCloudStorageItemInfo> listDirResult = gcs.listObjectInfo(
            resourceId.getBucketName(),
            resourceId.getObjectName(),
            GET_FILE_INFO_LIST_OPTIONS);
        LOG.info("List for getMetadat returned {}. {}", listDirResult.size(), listDirResult);
        if (!listDirResult.isEmpty()) {
          LOG.info("Get metadata for directory returned non empty{}", listDirResult);
          return GoogleCloudStorageItemInfo.createInferredDirectory(resourceId.toDirectoryId());
        }
      }
    }

    List<GoogleCloudStorageItemInfo> listDirInfo = ImmutableList.of(gcs.getItemInfo(dirId));
    if (listDirInfo.isEmpty()) {
      return GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    checkState(listDirInfo.size() <= 2, "listed more than 2 objects: '%s'", listDirInfo);
    GoogleCloudStorageItemInfo dirInfo = Iterables.get(listDirInfo, /* position= */ 0);
    checkState(
        dirInfo.getResourceId().equals(dirId) || !inferImplicitDirectories,
        "listed wrong object '%s', but should be '%s'",
        dirInfo.getResourceId(),
        resourceId);
    return dirInfo.getResourceId().equals(dirId) && dirInfo.exists()
        ? dirInfo
        : GoogleCloudStorageItemInfo.createNotFound(resourceId);
  }

  public void mkdirs(URI path) throws IOException {
    LOG.trace("mkdirs(path: {})", path);
    checkNotNull(path, "path should not be null");

    /* allowEmptyObjectName= */
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true);
    if (resourceId.isRoot()) {
      // GCS_ROOT directory always exists, no need to go through the rest of the method.
      return;
    }

    // In case path is a bucket we just attempt to create it without additional checks
    if (resourceId.isBucket()) {
      try {
        gcs.createBucket(resourceId.getBucketName(), CreateBucketOptions.DEFAULT);
      } catch (FileAlreadyExistsException e) {
        // This means that bucket already exist, and we do not need to do anything.
        LOG.trace("mkdirs: {} already exists, ignoring creation failure", resourceId, e);
      }
      return;
    }

    resourceId = resourceId.toDirectoryId();

    // Before creating a leaf directory we need to check if there are no conflicting files
    // with the same name as any subdirectory
//    if (options.isEnsureNoConflictingItems()) {
//      checkNoFilesConflictingWithDirs(resourceId);
//    }

    // Create only a leaf directory because subdirectories will be inferred
    // if leaf directory exists
    try {
      gcs.createEmptyObject(resourceId);
    } catch (FileAlreadyExistsException e) {
      // This means that directory object already exist, and we do not need to do anything.
      LOG.trace("mkdirs: {} already exists, ignoring creation failure", resourceId, e);
    }
  }
}
