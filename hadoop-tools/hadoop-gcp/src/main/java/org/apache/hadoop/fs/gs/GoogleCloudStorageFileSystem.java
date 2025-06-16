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
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

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
  private final GoogleHadoopFileSystemConfiguration configuration;

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
    this.configuration = configuration;
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
        if (!listDirResult.isEmpty()) {
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

    // TODO: Before creating a leaf directory we need to check if there are no conflicting files
    // TODO: with the same name as any subdirectory

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

  FileInfo getFileInfoObject(URI path) throws IOException {
    checkArgument(path != null, "path must not be null");
    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);
    checkArgument(
        !resourceId.isDirectory(),
        String.format(
            "path must be an object and not a directory, path: %s, resourceId: %s",
            path, resourceId));
    FileInfo fileInfo = FileInfo.fromItemInfo(gcs.getItemInfo(resourceId));
    LOG.trace("getFileInfoObject(path: {}): {}", path, fileInfo);
    return fileInfo;
  }

  SeekableByteChannel open(FileInfo fileInfo, GoogleHadoopFileSystemConfiguration config)
      throws IOException {
    checkNotNull(fileInfo, "fileInfo should not be null");
    checkArgument(
        !fileInfo.isDirectory(), "Cannot open a directory for reading: %s", fileInfo.getPath());

    return gcs.open(fileInfo.getItemInfo(), config);
  }

  void rename(URI src, URI dst) throws IOException {
    LOG.trace("rename(src: {}, dst: {})", src, dst);
    checkNotNull(src);
    checkNotNull(dst);
    checkArgument(!src.equals(GCSROOT), "Root path cannot be renamed.");

    // Parent of the destination path.
    URI dstParent = UriPaths.getParentPath(dst);

    // Obtain info on source, destination and destination-parent.
    List<URI> paths = new ArrayList<>();
    paths.add(src);
    paths.add(dst);
    if (dstParent != null) {
      // dstParent is null if dst is GCS_ROOT.
      paths.add(dstParent);
    }
    List<FileInfo> fileInfos = getFileInfos(paths);
    FileInfo srcInfo = fileInfos.get(0);
    FileInfo dstInfo = fileInfos.get(1);
    FileInfo dstParentInfo = dstParent == null ? null : fileInfos.get(2);

    // Throw if the source file does not exist.
    if (!srcInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + src);
    }

    // Make sure paths match what getFileInfo() returned (it can add / at the end).
    src = srcInfo.getPath();
    dst = getDstUri(srcInfo, dstInfo, dstParentInfo);

    // if src and dst are equal then do nothing
    if (src.equals(dst)) {
      return;
    }

    if (srcInfo.isDirectory()) {
      renameDirectoryInternal(srcInfo, dst);
    } else {
      renameObject(src, dst, srcInfo);
    }
  }

  private void renameObject(URI src, URI dst, FileInfo srcInfo) throws IOException {
    StorageResourceId srcResourceId =
        StorageResourceId.fromUriPath(src, /* allowEmptyObjectName= */ true);
    StorageResourceId dstResourceId =
        StorageResourceId.fromUriPath(dst, /* allowEmptyObjectName= */ true, /* generationId= */ 0L);

    if (srcResourceId.getBucketName().equals(dstResourceId.getBucketName())) {
      gcs.move(
          ImmutableMap.of(
              new StorageResourceId(
                  srcInfo.getItemInfo().getBucketName(),
                  srcInfo.getItemInfo().getObjectName(),
                  srcInfo.getItemInfo().getContentGeneration()),
              dstResourceId));
    } else {
      gcs.copy(ImmutableMap.of(srcResourceId, dstResourceId));

      gcs.deleteObjects(
          ImmutableList.of(
              new StorageResourceId(
                  srcInfo.getItemInfo().getBucketName(),
                  srcInfo.getItemInfo().getObjectName(),
                  srcInfo.getItemInfo().getContentGeneration())));
    }
  }

  /**
   * Renames given directory without checking any parameters.
   *
   * <p>GCS does not support atomic renames therefore rename is implemented as copying source
   * metadata to destination and then deleting source metadata. Note that only the metadata is
   * copied and not the content of any file.
   */
  private void renameDirectoryInternal(FileInfo srcInfo, URI dst) throws IOException {
    checkArgument(srcInfo.isDirectory(), "'%s' should be a directory", srcInfo);
    checkArgument(dst.toString().endsWith(PATH_DELIMITER), "'%s' should be a directory", dst);

    URI src = srcInfo.getPath();

    // Mapping from each src to its respective dst.
    // Sort src items so that parent directories appear before their children.
    // That allows us to copy parent directories before we copy their children.
    Map<FileInfo, URI> srcToDstItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);
    Map<FileInfo, URI> srcToDstMarkerItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);

    // List of individual paths to rename;
    // we will try to carry out the copies in this list's order.
    List<FileInfo> srcItemInfos =
        listFileInfoForPrefix(src, ListFileOptions.DELETE_RENAME_LIST_OPTIONS);

    // Create a list of sub-items to copy.
    Pattern markerFilePattern = configuration.getMarkerFilePattern();
    String prefix = src.toString();
    for (FileInfo srcItemInfo : srcItemInfos) {
      String relativeItemName = srcItemInfo.getPath().toString().substring(prefix.length());
      URI dstItemName = dst.resolve(relativeItemName);
      if (markerFilePattern != null && markerFilePattern.matcher(relativeItemName).matches()) {
        srcToDstMarkerItemNames.put(srcItemInfo, dstItemName);
      } else {
        srcToDstItemNames.put(srcItemInfo, dstItemName);
      }
    }

    StorageResourceId srcResourceId =
        StorageResourceId.fromUriPath(src, /* allowEmptyObjectName= */ true);
    StorageResourceId dstResourceId =
        StorageResourceId.fromUriPath(
            dst, /* allowEmptyObjectName= */ true, /* generationId= */ 0L);
    if (srcResourceId.getBucketName().equals(dstResourceId.getBucketName())) {
      // First, move all items except marker items
      moveInternal(srcToDstItemNames);
      // Finally, move marker items (if any) to mark rename operation success
      moveInternal(srcToDstMarkerItemNames);

      if (srcInfo.getItemInfo().isBucket()) {
        deleteBucket(Collections.singletonList(srcInfo));
      } else {
        // If src is a directory then srcItemInfos does not contain its own name,
        // we delete item separately in the list.
        deleteObjects(Collections.singletonList(srcInfo));
      }
      return;
    }

    // TODO: Add support for across bucket moves
    throw new UnsupportedOperationException(String.format(
        "Moving object from bucket '%s' to '%s' is not supported",
        srcResourceId.getBucketName(),
        dstResourceId.getBucketName()));
  }

  List<FileInfo> listFileInfoForPrefix(URI prefix, ListFileOptions listOptions)
      throws IOException {
    LOG.trace("listAllFileInfoForPrefix(prefix: {})", prefix);
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listObjectInfo(
            prefixId.getBucketName(),
            prefixId.getObjectName(),
            updateListObjectOptions(ListObjectOptions.DEFAULT_FLAT_LIST, listOptions));
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /** Moves items in given map that maps source items to destination items. */
  private void moveInternal(Map<FileInfo, URI> srcToDstItemNames) throws IOException {
    if (srcToDstItemNames.isEmpty()) {
      return;
    }

    Map<StorageResourceId, StorageResourceId> sourceToDestinationObjectsMap = new HashMap<>();

    // Prepare list of items to move.
    for (Map.Entry<FileInfo, URI> srcToDstItemName : srcToDstItemNames.entrySet()) {
      StorageResourceId srcResourceId = srcToDstItemName.getKey().getItemInfo().getResourceId();

      StorageResourceId dstResourceId =
          StorageResourceId.fromUriPath(srcToDstItemName.getValue(), true);
      sourceToDestinationObjectsMap.put(srcResourceId, dstResourceId);
    }

    // Perform move.
    gcs.move(sourceToDestinationObjectsMap);
  }

  private static ListObjectOptions updateListObjectOptions(
      ListObjectOptions listObjectOptions, ListFileOptions listFileOptions) {
    return listObjectOptions.builder().setFields(listFileOptions.getFields()).build();
  }

  private List<FileInfo> getFileInfos(List<URI> paths) throws IOException {
    List<FileInfo> result = new ArrayList<>(paths.size());
    for (URI path : paths) {
      // TODO: Do this concurrently
      result.add(getFileInfo(path));
    }

    return result;
  }

  private URI getDstUri(FileInfo srcInfo, FileInfo dstInfo, @Nullable FileInfo dstParentInfo)
      throws IOException {
    URI src = srcInfo.getPath();
    URI dst = dstInfo.getPath();

    // Throw if src is a file and dst == GCS_ROOT
    if (!srcInfo.isDirectory() && dst.equals(GCSROOT)) {
      throw new IOException("A file cannot be created in root.");
    }

    // Throw if the destination is a file that already exists, and it's not a source file.
    if (dstInfo.exists() && !dstInfo.isDirectory() && (srcInfo.isDirectory() || !dst.equals(src))) {
      throw new IOException("Cannot overwrite an existing file: " + dst);
    }

    // Rename operation cannot be completed if parent of destination does not exist.
    if (dstParentInfo != null && !dstParentInfo.exists()) {
      throw new IOException(
          "Cannot rename because path does not exist: " + dstParentInfo.getPath());
    }

    // Leaf item of the source path.
    String srcItemName = getItemName(src);

    // Having taken care of the initial checks, apply the regular rules.
    // After applying the rules, we will be left with 2 paths such that:
    // -- either both are files or both are directories
    // -- src exists and dst leaf does not exist
    if (srcInfo.isDirectory()) {
      // -- if src is a directory
      //    -- dst is an existing file => disallowed
      //    -- dst is a directory => rename the directory.

      // The first case (dst is an existing file) is already checked earlier.
      // If the destination path looks like a file, make it look like a
      // directory path. This is because users often type 'mv foo bar'
      // rather than 'mv foo bar/'.
      if (!dstInfo.isDirectory()) {
        dst = UriPaths.toDirectory(dst);
      }

      // Throw if renaming directory to self - this is forbidden
      if (src.equals(dst)) {
        throw new IOException("Rename dir to self is forbidden");
      }

      URI dstRelativeToSrc = src.relativize(dst);
      // Throw if dst URI relative to src is not equal to dst,
      // because this means that src is a parent directory of dst
      // and src cannot be "renamed" to its subdirectory
      if (!dstRelativeToSrc.equals(dst)) {
        throw new IOException("Rename to subdir is forbidden");
      }

      if (dstInfo.exists()) {
        dst =
            dst.equals(GCSROOT)
                ? UriPaths.fromStringPathComponents(
                srcItemName, /* objectName= */ null, /* allowEmptyObjectName= */ true)
                : UriPaths.toDirectory(dst.resolve(srcItemName));
      }
    } else {
      // -- src is a file
      //    -- dst is a file => rename the file.
      //    -- dst is a directory => similar to the previous case after
      //                             appending src file-name to dst

      if (dstInfo.isDirectory()) {
        if (!dstInfo.exists()) {
          throw new IOException("Cannot rename because path does not exist: " + dstInfo.getPath());
        } else {
          dst = dst.resolve(srcItemName);
        }
      }
    }

    return dst;
  }

  @Nullable
  static String getItemName(URI path) {
    checkNotNull(path, "path can not be null");

    // There is no leaf item for the root path.
    if (path.equals(GCSROOT)) {
      return null;
    }

    StorageResourceId resourceId = StorageResourceId.fromUriPath(path, true);

    if (resourceId.isBucket()) {
      return resourceId.getBucketName();
    }

    String objectName = resourceId.getObjectName();
    int index =
        StringPaths.isDirectoryPath(objectName)
            ? objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2)
            : objectName.lastIndexOf(PATH_DELIMITER);
    return index < 0 ? objectName : objectName.substring(index + 1);
  }
}
