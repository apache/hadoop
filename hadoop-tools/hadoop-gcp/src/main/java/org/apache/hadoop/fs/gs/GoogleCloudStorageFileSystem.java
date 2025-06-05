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
import static java.util.Comparator.comparing;
import static org.apache.hadoop.fs.gs.Constants.PATH_DELIMITER;
import static org.apache.hadoop.fs.gs.Constants.SCHEME;

import com.google.auth.Credentials;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Provides FS semantics over GCS based on Objects API.
 */
class GoogleCloudStorageFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(StorageResourceId.class);
  // Comparator used for sorting paths.
  //
  // For some bulk operations, we need to operate on parent directories before
  // we operate on their children. To achieve this, we sort paths such that
  // shorter paths appear before longer paths. Also, we sort lexicographically
  // within paths of the same length (this is not strictly required but helps when
  // debugging/testing).
  @VisibleForTesting
  static final Comparator<URI> PATH_COMPARATOR =
      comparing(
          URI::toString,
          (as, bs) ->
              (as.length() == bs.length())
                  ? as.compareTo(bs)
                  : Integer.compare(as.length(), bs.length()));

  static final Comparator<FileInfo> FILE_INFO_PATH_COMPARATOR =
      comparing(FileInfo::getPath, PATH_COMPARATOR);

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

  void delete(URI path, boolean recursive) throws IOException {
    checkNotNull(path, "path should not be null");
    checkArgument(!path.equals(GCSROOT), "Cannot delete root path (%s)", path);

    FileInfo fileInfo = getFileInfo(path);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    List<FileInfo> itemsToDelete;
    // Delete sub-items if it is a directory.
    if (fileInfo.isDirectory()) {
      itemsToDelete =
          recursive
              ? listRecursive(fileInfo.getPath()) // TODO: Get only one result
              : listDirectory(fileInfo.getPath());

      if (!itemsToDelete.isEmpty() && !recursive) {
        throw new DirectoryNotEmptyException("Cannot delete a non-empty directory. : " + path);
      }
    } else {
      itemsToDelete = new ArrayList<>();
    }

    List<FileInfo> bucketsToDelete = new ArrayList<>();
    (fileInfo.getItemInfo().isBucket() ? bucketsToDelete : itemsToDelete).add(fileInfo);

    deleteObjects(itemsToDelete, bucketsToDelete);

    StorageResourceId parentId =
        StorageResourceId.fromUriPath(UriPaths.getParentPath(path), true);
    GoogleCloudStorageItemInfo parentInfo =
        getFileInfoInternal(parentId, /* inferImplicitDirectories= */ false);

    StorageResourceId resourceId = parentInfo.getResourceId();
    if (parentInfo.exists()
        || resourceId.isRoot()
        || resourceId.isBucket()
        || PATH_DELIMITER.equals(resourceId.getObjectName())) {
      return;
    }

    // TODO: Keep the repair parent step behind a flag
    gcs.createEmptyObject(parentId);
  }

  private List<FileInfo> listRecursive(URI prefix) throws IOException {
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listDirectoryRecursive(prefixId.getBucketName(), prefixId.getObjectName());
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  private List<FileInfo> listDirectory(URI prefix) throws IOException {
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.listObjectInfo(
        prefixId.getBucketName(),
        prefixId.getObjectName(),
        ListObjectOptions.DEFAULT_FLAT_LIST);

    List<FileInfo> fileInfos = FileInfo.fromItemInfos(itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  private StorageResourceId getPrefixId(URI prefix) {
    checkNotNull(prefix, "prefix could not be null");

    StorageResourceId prefixId = StorageResourceId.fromUriPath(prefix, true);
    checkArgument(!prefixId.isRoot(), "prefix must not be global root, got '%s'", prefix);

    return prefixId;
  }

  private void deleteObjects(
      List<FileInfo> itemsToDelete, List<FileInfo> bucketsToDelete)
      throws IOException {
    LOG.trace("deleteInternalWithFolders; fileSize={} bucketSize={}",
        itemsToDelete.size(), bucketsToDelete.size());
    deleteObjects(itemsToDelete);
    deleteBucket(bucketsToDelete);
  }

  private void deleteObjects(List<FileInfo> itemsToDelete) throws IOException {
    // Delete children before their parents.
    //
    // Note: we modify the input list, which is ok for current usage.
    // We should make a copy in case that changes in future.
    itemsToDelete.sort(FILE_INFO_PATH_COMPARATOR.reversed());

    if (!itemsToDelete.isEmpty()) {
      List<StorageResourceId> objectsToDelete = new ArrayList<>(itemsToDelete.size());
      for (FileInfo fileInfo : itemsToDelete) {
        if (!fileInfo.isInferredDirectory()) {
          objectsToDelete.add(
              new StorageResourceId(
                  fileInfo.getItemInfo().getBucketName(),
                  fileInfo.getItemInfo().getObjectName(),
                  fileInfo.getItemInfo().getContentGeneration()));
        }
      }

      gcs.deleteObjects(objectsToDelete);
    }
  }

  private void deleteBucket(List<FileInfo> bucketsToDelete) throws IOException {
    if (bucketsToDelete == null || bucketsToDelete.isEmpty()) {
      return;
    }

    // TODO: Add support for deleting bucket
    throw new UnsupportedOperationException("deleteBucket is not supported.");
  }

  public List<FileInfo> listFileInfo(URI path, ListFileOptions listOptions) throws IOException {
    checkNotNull(path, "path can not be null");
    LOG.trace("listStatus(path: {})", path);

    StorageResourceId pathId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true);

    if (!pathId.isDirectory()) {
      GoogleCloudStorageItemInfo pathInfo = gcs.getItemInfo(pathId);
      if (pathInfo.exists()) {
        List<FileInfo> listedInfo = new ArrayList<>();
        listedInfo.add(FileInfo.fromItemInfo(pathInfo));

        return listedInfo;
      }
    }

    StorageResourceId dirId = pathId.toDirectoryId();
    List<GoogleCloudStorageItemInfo> dirItemInfos = dirId.isRoot() ?
        gcs.listBucketInfo() :
        gcs.listObjectInfo(
            dirId.getBucketName(), dirId.getObjectName(), LIST_FILE_INFO_LIST_OPTIONS);

    if (pathId.isStorageObject() && dirItemInfos.isEmpty()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    if (!dirItemInfos.isEmpty() && Objects.equals(dirItemInfos.get(0).getResourceId(), dirId)) {
      dirItemInfos.remove(0);
    }

    List<FileInfo> fileInfos = FileInfo.fromItemInfos(dirItemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }
}
