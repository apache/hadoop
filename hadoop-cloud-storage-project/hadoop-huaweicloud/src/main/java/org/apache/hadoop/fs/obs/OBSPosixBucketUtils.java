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

package org.apache.hadoop.fs.obs;

import com.obs.services.exception.ObsException;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.fs.GetAttributeRequest;
import com.obs.services.model.fs.NewFolderRequest;
import com.obs.services.model.fs.ObsFSAttribute;
import com.obs.services.model.fs.RenameRequest;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Posix bucket specific utils for {@link OBSFileSystem}.
 */
final class OBSPosixBucketUtils {
  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSPosixBucketUtils.class);

  private OBSPosixBucketUtils() {
  }

  /**
   * Get the depth of an absolute path, that is the number of '/' in the path.
   *
   * @param key object key
   * @return depth
   */
  static int fsGetObjectKeyDepth(final String key) {
    int depth = 0;
    for (int idx = key.indexOf('/');
        idx >= 0; idx = key.indexOf('/', idx + 1)) {
      depth++;
    }
    return key.endsWith("/") ? depth - 1 : depth;
  }

  /**
   * Used to judge that an object is a file or folder.
   *
   * @param attr posix object attribute
   * @return is posix folder
   */
  static boolean fsIsFolder(final ObsFSAttribute attr) {
    final int ifDir = 0x004000;
    int mode = attr.getMode();
    // object mode is -1 when the object is migrated from
    // object bucket to posix bucket.
    // -1 is a file, not folder.
    if (mode < 0) {
      return false;
    }

    return (mode & ifDir) != 0;
  }

  /**
   * The inner rename operation based on Posix bucket.
   *
   * @param owner OBS File System instance
   * @param src   source path to be renamed from
   * @param dst   destination path to be renamed to
   * @return boolean
   * @throws RenameFailedException if some criteria for a state changing rename
   *                               was not met. This means work didn't happen;
   *                               it's not something which is reported upstream
   *                               to the FileSystem APIs, for which the
   *                               semantics of "false" are pretty vague.
   * @throws IOException           on IO failure.
   */
  static boolean renameBasedOnPosix(final OBSFileSystem owner, final Path src,
      final Path dst) throws IOException {
    Path dstPath = dst;
    String srcKey = OBSCommonUtils.pathToKey(owner, src);
    String dstKey = OBSCommonUtils.pathToKey(owner, dstPath);

    if (srcKey.isEmpty()) {
      LOG.error("rename: src [{}] is root directory", src);
      return false;
    }

    try {
      FileStatus dstStatus = owner.getFileStatus(dstPath);
      if (dstStatus.isDirectory()) {
        String newDstString = OBSCommonUtils.maybeAddTrailingSlash(
            dstPath.toString());
        String filename = srcKey.substring(
            OBSCommonUtils.pathToKey(owner, src.getParent())
                .length() + 1);
        dstPath = new Path(newDstString + filename);
        dstKey = OBSCommonUtils.pathToKey(owner, dstPath);
        LOG.debug(
            "rename: dest is an existing directory and will be "
                + "changed to [{}]", dstPath);

        if (owner.exists(dstPath)) {
          LOG.error("rename: failed to rename " + src + " to "
              + dstPath
              + " because destination exists");
          return false;
        }
      } else {
        if (srcKey.equals(dstKey)) {
          LOG.warn(
              "rename: src and dest refer to the same "
                  + "file or directory: {}", dstPath);
          return true;
        } else {
          LOG.error("rename: failed to rename " + src + " to "
              + dstPath
              + " because destination exists");
          return false;
        }
      }
    } catch (FileNotFoundException e) {
      // if destination does not exist, do not change the
      // destination key, and just do rename.
      LOG.debug("rename: dest [{}] does not exist", dstPath);
    } catch (FileConflictException e) {
      Path parent = dstPath.getParent();
      if (!OBSCommonUtils.pathToKey(owner, parent).isEmpty()) {
        FileStatus dstParentStatus = owner.getFileStatus(parent);
        if (!dstParentStatus.isDirectory()) {
          throw new ParentNotDirectoryException(
              parent + " is not a directory");
        }
      }
    }

    if (dstKey.startsWith(srcKey) && (dstKey.equals(srcKey)
        || dstKey.charAt(srcKey.length()) == Path.SEPARATOR_CHAR)) {
      LOG.error("rename: dest [{}] cannot be a descendant of src [{}]",
          dstPath, src);
      return false;
    }

    return innerFsRenameWithRetry(owner, src, dstPath, srcKey, dstKey);
  }

  private static boolean innerFsRenameWithRetry(final OBSFileSystem owner,
      final Path src,
      final Path dst, final String srcKey, final String dstKey)
      throws IOException {
    boolean renameResult = true;
    int retryTime = 1;
    while (retryTime <= OBSCommonUtils.MAX_RETRY_TIME) {
      try {
        LOG.debug("rename: {}-st rename from [{}] to [{}] ...",
            retryTime, srcKey, dstKey);
        innerFsRenameFile(owner, srcKey, dstKey);
        renameResult = true;
        break;
      } catch (FileNotFoundException e) {
        if (owner.exists(dst)) {
          LOG.warn(
              "rename: successfully {}-st rename src [{}] "
                  + "to dest [{}] with SDK retry",
              retryTime, src, dst, e);
          renameResult = true;
        } else {
          LOG.error(
              "rename: failed {}-st rename src [{}] to dest [{}]",
              retryTime, src, dst, e);
          renameResult = false;
        }
        break;
      } catch (IOException e) {
        if (retryTime == OBSCommonUtils.MAX_RETRY_TIME) {
          LOG.error(
              "rename: failed {}-st rename src [{}] to dest [{}]",
              retryTime, src, dst, e);
          throw e;
        } else {
          LOG.warn(
              "rename: failed {}-st rename src [{}] to dest [{}]",
              retryTime, src, dst, e);
          if (owner.exists(dst) && owner.exists(src)) {
            LOG.warn(
                "rename: failed {}-st rename src [{}] to "
                    + "dest [{}] with SDK retry", retryTime, src,
                dst, e);
            renameResult = false;
            break;
          }

          try {
            Thread.sleep(OBSCommonUtils.DELAY_TIME);
          } catch (InterruptedException ie) {
            throw e;
          }
        }
      }

      retryTime++;
    }

    return renameResult;
  }

  /**
   * Used to rename a source folder to a destination folder that is not existed
   * before rename.
   *
   * @param owner OBS File System instance
   * @param src   source folder key
   * @param dst   destination folder key that not existed before rename
   * @throws IOException  any io exception
   * @throws ObsException any obs operation exception
   */
  static void fsRenameToNewFolder(final OBSFileSystem owner, final String src,
      final String dst)
      throws IOException, ObsException {
    LOG.debug("RenameFolder path {} to {}", src, dst);

    try {
      RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(owner.getBucket());
      renameObjectRequest.setObjectKey(src);
      renameObjectRequest.setNewObjectKey(dst);
      owner.getObsClient().renameFolder(renameObjectRequest);
      owner.getSchemeStatistics().incrementWriteOps(1);
    } catch (ObsException e) {
      throw OBSCommonUtils.translateException(
          "renameFile(" + src + ", " + dst + ")", src, e);
    }
  }

  static void innerFsRenameFile(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey) throws IOException {
    LOG.debug("RenameFile path {} to {}", srcKey, dstKey);

    try {
      final RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(owner.getBucket());
      renameObjectRequest.setObjectKey(srcKey);
      renameObjectRequest.setNewObjectKey(dstKey);
      owner.getObsClient().renameFile(renameObjectRequest);
      owner.getSchemeStatistics().incrementWriteOps(1);
    } catch (ObsException e) {
      if (e.getResponseCode() == OBSCommonUtils.NOT_FOUND_CODE) {
        throw new FileNotFoundException(
            "No such file or directory: " + srcKey);
      }
      if (e.getResponseCode() == OBSCommonUtils.CONFLICT_CODE) {
        throw new FileConflictException(
            "File conflicts during rename, " + e.getResponseStatus());
      }
      throw OBSCommonUtils.translateException(
          "renameFile(" + srcKey + ", " + dstKey + ")", srcKey, e);
    }
  }

  /**
   * Used to rename a source object to a destination object which is not existed
   * before rename.
   *
   * @param owner  OBS File System instance
   * @param srcKey source object key
   * @param dstKey destination object key
   * @throws IOException io exception
   */
  static void fsRenameToNewObject(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey) throws IOException {
    String newSrcKey = srcKey;
    String newdstKey = dstKey;
    newSrcKey = OBSCommonUtils.maybeDeleteBeginningSlash(newSrcKey);
    newdstKey = OBSCommonUtils.maybeDeleteBeginningSlash(newdstKey);
    if (newSrcKey.endsWith("/")) {
      // Rename folder.
      fsRenameToNewFolder(owner, newSrcKey, newdstKey);
    } else {
      // Rename file.
      innerFsRenameFile(owner, newSrcKey, newdstKey);
    }
  }

  // Delete a file.
  private static int fsRemoveFile(final OBSFileSystem owner,
      final String sonObjectKey,
      final List<KeyAndVersion> files)
      throws IOException {
    files.add(new KeyAndVersion(sonObjectKey));
    if (files.size() == owner.getMaxEntriesToDelete()) {
      // batch delete files.
      OBSCommonUtils.removeKeys(owner, files, true, false);
      return owner.getMaxEntriesToDelete();
    }
    return 0;
  }

  // Recursively delete a folder that might be not empty.
  static boolean fsDelete(final OBSFileSystem owner, final FileStatus status,
      final boolean recursive)
      throws IOException, ObsException {
    long startTime = System.currentTimeMillis();
    long threadId = Thread.currentThread().getId();
    Path f = status.getPath();
    String key = OBSCommonUtils.pathToKey(owner, f);

    if (!status.isDirectory()) {
      LOG.debug("delete: Path is a file");
      trashObjectIfNeed(owner, key);
    } else {
      LOG.debug("delete: Path is a directory: {} - recursive {}", f,
          recursive);
      key = OBSCommonUtils.maybeAddTrailingSlash(key);
      boolean isEmptyDir = OBSCommonUtils.isFolderEmpty(owner, key);
      if (key.equals("")) {
        return OBSCommonUtils.rejectRootDirectoryDelete(
            owner.getBucket(), isEmptyDir, recursive);
      }
      if (!recursive && !isEmptyDir) {
        LOG.warn("delete: Path is not empty: {} - recursive {}", f,
            recursive);
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
      if (isEmptyDir) {
        LOG.debug(
            "delete: Deleting fake empty directory {} - recursive {}",
            f, recursive);
        OBSCommonUtils.deleteObject(owner, key);
      } else {
        LOG.debug(
            "delete: Deleting objects for directory prefix {} to "
                + "delete - recursive {}", f, recursive);
        trashFolderIfNeed(owner, key, f);
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.debug("delete Path:{} thread:{}, timeUsedInMilliSec:{}", f,
        threadId, endTime - startTime);
    return true;
  }

  private static void trashObjectIfNeed(final OBSFileSystem owner,
      final String key)
      throws ObsException, IOException {
    if (needToTrash(owner, key)) {
      mkTrash(owner, key);
      StringBuilder sb = new StringBuilder(owner.getTrashDir());
      sb.append(key);
      if (owner.exists(new Path(sb.toString()))) {
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMddHHmmss");
        sb.append(df.format(new Date()));
      }
      fsRenameToNewObject(owner, key, sb.toString());
      LOG.debug("Moved: '" + key + "' to trash at: " + sb.toString());
    } else {
      OBSCommonUtils.deleteObject(owner, key);
    }
  }

  private static void trashFolderIfNeed(final OBSFileSystem owner,
      final String key,
      final Path f) throws ObsException, IOException {
    if (needToTrash(owner, key)) {
      mkTrash(owner, key);
      StringBuilder sb = new StringBuilder(owner.getTrashDir());
      String subKey = OBSCommonUtils.maybeAddTrailingSlash(key);
      sb.append(subKey);
      if (owner.exists(new Path(sb.toString()))) {
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMddHHmmss");
        sb.insert(sb.length() - 1, df.format(new Date()));
      }

      String srcKey = OBSCommonUtils.maybeDeleteBeginningSlash(key);
      String dstKey = OBSCommonUtils.maybeDeleteBeginningSlash(
          sb.toString());
      fsRenameToNewFolder(owner, srcKey, dstKey);
      LOG.debug("Moved: '" + key + "' to trash at: " + sb.toString());
    } else {
      if (owner.isEnableMultiObjectDeleteRecursion()) {
        long delNum = fsRecursivelyDeleteDir(owner, key, true);
        LOG.debug("Recursively delete {} files/dirs when deleting {}",
            delNum, key);
      } else {
        fsNonRecursivelyDelete(owner, f);
      }
    }
  }

  static long fsRecursivelyDeleteDir(final OBSFileSystem owner,
      final String parentKey,
      final boolean deleteParent) throws IOException {
    long delNum = 0;
    List<KeyAndVersion> subdirList = new ArrayList<>(
        owner.getMaxEntriesToDelete());
    List<KeyAndVersion> fileList = new ArrayList<>(
        owner.getMaxEntriesToDelete());

    ListObjectsRequest request = OBSCommonUtils.createListObjectsRequest(
        owner, parentKey, "/", owner.getMaxKeys());
    ObjectListing objects = OBSCommonUtils.listObjects(owner, request);
    while (true) {
      for (String commonPrefix : objects.getCommonPrefixes()) {
        if (commonPrefix.equals(parentKey)) {
          // skip prefix itself
          continue;
        }

        delNum += fsRemoveSubdir(owner, commonPrefix, subdirList);
      }

      for (ObsObject sonObject : objects.getObjects()) {
        String sonObjectKey = sonObject.getObjectKey();

        if (sonObjectKey.equals(parentKey)) {
          // skip prefix itself
          continue;
        }

        if (!sonObjectKey.endsWith("/")) {
          delNum += fsRemoveFile(owner, sonObjectKey, fileList);
        } else {
          delNum += fsRemoveSubdir(owner, sonObjectKey, subdirList);
        }
      }

      if (!objects.isTruncated()) {
        break;
      }

      objects = OBSCommonUtils.continueListObjects(owner, objects);
    }

    delNum += fileList.size();
    OBSCommonUtils.removeKeys(owner, fileList, true, false);

    delNum += subdirList.size();
    OBSCommonUtils.removeKeys(owner, subdirList, true, false);

    if (deleteParent) {
      OBSCommonUtils.deleteObject(owner, parentKey);
      delNum++;
    }

    return delNum;
  }

  private static boolean needToTrash(final OBSFileSystem owner,
      final String key) {
    String newKey = key;
    newKey = OBSCommonUtils.maybeDeleteBeginningSlash(newKey);
    if (owner.isEnableTrash() && newKey.startsWith(owner.getTrashDir())) {
      return false;
    }
    return owner.isEnableTrash();
  }

  // Delete a sub dir.
  private static int fsRemoveSubdir(final OBSFileSystem owner,
      final String subdirKey,
      final List<KeyAndVersion> subdirList)
      throws IOException {
    fsRecursivelyDeleteDir(owner, subdirKey, false);

    subdirList.add(new KeyAndVersion(subdirKey));
    if (subdirList.size() == owner.getMaxEntriesToDelete()) {
      // batch delete subdirs.
      OBSCommonUtils.removeKeys(owner, subdirList, true, false);
      return owner.getMaxEntriesToDelete();
    }

    return 0;
  }

  private static void mkTrash(final OBSFileSystem owner, final String key)
      throws ObsException, IOException {
    String newKey = key;
    StringBuilder sb = new StringBuilder(owner.getTrashDir());
    newKey = OBSCommonUtils.maybeAddTrailingSlash(newKey);
    sb.append(newKey);
    sb.deleteCharAt(sb.length() - 1);
    sb.delete(sb.lastIndexOf("/"), sb.length());
    Path fastDeleteRecycleDirPath = new Path(sb.toString());
    // keep the parent directory of the target path exists
    if (!owner.exists(fastDeleteRecycleDirPath)) {
      owner.mkdirs(fastDeleteRecycleDirPath);
    }
  }

  // List all sub objects at first, delete sub objects in batch secondly.
  private static void fsNonRecursivelyDelete(final OBSFileSystem owner,
      final Path parent)
      throws IOException, ObsException {
    // List sub objects sorted by path depth.
    FileStatus[] arFileStatus = OBSCommonUtils.innerListStatus(owner,
        parent, true);
    // Remove sub objects one depth by one depth to avoid that parents and
    // children in a same batch.
    fsRemoveKeys(owner, arFileStatus);
    // Delete parent folder that should has become empty.
    OBSCommonUtils.deleteObject(owner,
        OBSCommonUtils.pathToKey(owner, parent));
  }

  // Remove sub objects of each depth one by one to avoid that parents and
  // children in a same batch.
  private static void fsRemoveKeys(final OBSFileSystem owner,
      final FileStatus[] arFileStatus)
      throws ObsException, IOException {
    if (arFileStatus.length <= 0) {
      // exit fast if there are no keys to delete
      return;
    }

    String key;
    for (FileStatus fileStatus : arFileStatus) {
      key = OBSCommonUtils.pathToKey(owner, fileStatus.getPath());
      OBSCommonUtils.blockRootDelete(owner.getBucket(), key);
    }

    fsRemoveKeysByDepth(owner, arFileStatus);
  }

  // Batch delete sub objects one depth by one depth to avoid that parents and
  // children in a same
  // batch.
  // A batch deletion might be split into some concurrent deletions to promote
  // the performance, but
  // it
  // can't make sure that an object is deleted before it's children.
  private static void fsRemoveKeysByDepth(final OBSFileSystem owner,
      final FileStatus[] arFileStatus)
      throws ObsException, IOException {
    if (arFileStatus.length <= 0) {
      // exit fast if there is no keys to delete
      return;
    }

    // Find all leaf keys in the list.
    String key;
    int depth = Integer.MAX_VALUE;
    List<KeyAndVersion> leafKeys = new ArrayList<>(
        owner.getMaxEntriesToDelete());
    for (int idx = arFileStatus.length - 1; idx >= 0; idx--) {
      if (leafKeys.size() >= owner.getMaxEntriesToDelete()) {
        OBSCommonUtils.removeKeys(owner, leafKeys, true, false);
      }

      key = OBSCommonUtils.pathToKey(owner, arFileStatus[idx].getPath());

      // Check file.
      if (!arFileStatus[idx].isDirectory()) {
        // A file must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }

      // Check leaf folder at current depth.
      int keyDepth = fsGetObjectKeyDepth(key);
      if (keyDepth == depth) {
        // Any key at current depth must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }
      if (keyDepth < depth) {
        // The last batch delete at current depth.
        OBSCommonUtils.removeKeys(owner, leafKeys, true, false);
        // Go on at the upper depth.
        depth = keyDepth;
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }
      LOG.warn(
          "The objects list is invalid because it isn't sorted by"
              + " path depth.");
      throw new ObsException("System failure");
    }

    // The last batch delete at the minimum depth of all keys.
    OBSCommonUtils.removeKeys(owner, leafKeys, true, false);
  }

  // Used to create a folder
  static void fsCreateFolder(final OBSFileSystem owner,
      final String objectName)
      throws ObsException {
    for (int retryTime = 1;
        retryTime < OBSCommonUtils.MAX_RETRY_TIME; retryTime++) {
      try {
        innerFsCreateFolder(owner, objectName);
        return;
      } catch (ObsException e) {
        LOG.warn("Failed to create folder [{}], retry time [{}], "
            + "exception [{}]", objectName, retryTime, e);
        try {
          Thread.sleep(OBSCommonUtils.DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }

    innerFsCreateFolder(owner, objectName);
  }

  private static void innerFsCreateFolder(final OBSFileSystem owner,
      final String objectName)
      throws ObsException {
    final NewFolderRequest newFolderRequest = new NewFolderRequest(
        owner.getBucket(), objectName);
    newFolderRequest.setAcl(owner.getCannedACL());
    long len = newFolderRequest.getObjectKey().length();
    owner.getObsClient().newFolder(newFolderRequest);
    owner.getSchemeStatistics().incrementWriteOps(1);
    owner.getSchemeStatistics().incrementBytesWritten(len);
  }

  // Used to get the status of a file or folder in a file-gateway bucket.
  static OBSFileStatus innerFsGetObjectStatus(final OBSFileSystem owner,
      final Path f) throws IOException {
    final Path path = OBSCommonUtils.qualify(owner, f);
    String key = OBSCommonUtils.pathToKey(owner, path);
    LOG.debug("Getting path status for {}  ({})", path, key);

    if (key.isEmpty()) {
      LOG.debug("Found root directory");
      return new OBSFileStatus(path, owner.getUsername());
    }

    try {
      final GetAttributeRequest getAttrRequest = new GetAttributeRequest(
          owner.getBucket(), key);
      ObsFSAttribute meta = owner.getObsClient()
          .getAttribute(getAttrRequest);
      owner.getSchemeStatistics().incrementReadOps(1);
      if (fsIsFolder(meta)) {
        LOG.debug("Found file (with /): fake directory");
        return new OBSFileStatus(path,
            OBSCommonUtils.dateToLong(meta.getLastModified()),
            owner.getUsername());
      } else {
        LOG.debug(
            "Found file (with /): real file? should not happen: {}",
            key);
        return new OBSFileStatus(
            meta.getContentLength(),
            OBSCommonUtils.dateToLong(meta.getLastModified()),
            path,
            owner.getDefaultBlockSize(path),
            owner.getUsername());
      }
    } catch (ObsException e) {
      if (e.getResponseCode() == OBSCommonUtils.NOT_FOUND_CODE) {
        LOG.debug("Not Found: {}", path);
        throw new FileNotFoundException(
            "No such file or directory: " + path);
      }
      if (e.getResponseCode() == OBSCommonUtils.CONFLICT_CODE) {
        throw new FileConflictException(
            "file conflicts: " + e.getResponseStatus());
      }
      throw OBSCommonUtils.translateException("getFileStatus", path, e);
    }
  }

  static ContentSummary fsGetDirectoryContentSummary(
      final OBSFileSystem owner,
      final String key) throws IOException {
    String newKey = key;
    newKey = OBSCommonUtils.maybeAddTrailingSlash(newKey);
    long[] summary = {0, 0, 1};
    LOG.debug("Summary key {}", newKey);
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(owner.getBucket());
    request.setPrefix(newKey);
    request.setMaxKeys(owner.getMaxKeys());
    ObjectListing objects = OBSCommonUtils.listObjects(owner, request);
    while (true) {
      if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjects()
          .isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as directory (with /): {}/{}",
              objects.getCommonPrefixes().size(),
              objects.getObjects().size());
        }
        for (String prefix : objects.getCommonPrefixes()) {
          if (!prefix.equals(newKey)) {
            summary[2]++;
          }
        }

        for (ObsObject obj : objects.getObjects()) {
          if (!obj.getObjectKey().endsWith("/")) {
            summary[0] += obj.getMetadata().getContentLength();
            summary[1] += 1;
          } else if (!obj.getObjectKey().equals(newKey)) {
            summary[2]++;
          }
        }
      }
      if (!objects.isTruncated()) {
        break;
      }
      objects = OBSCommonUtils.continueListObjects(owner, objects);
    }
    LOG.debug(String.format(
        "file size [%d] - file count [%d] - directory count [%d] - "
            + "file path [%s]",
        summary[0], summary[1], summary[2], newKey));
    return new ContentSummary.Builder().length(summary[0])
        .fileCount(summary[1]).directoryCount(summary[2])
        .spaceConsumed(summary[0]).build();
  }
}
