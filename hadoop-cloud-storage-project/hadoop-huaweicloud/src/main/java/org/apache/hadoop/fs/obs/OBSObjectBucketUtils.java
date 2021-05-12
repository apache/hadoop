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
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.CopyObjectRequest;
import com.obs.services.model.CopyObjectResult;
import com.obs.services.model.CopyPartRequest;
import com.obs.services.model.CopyPartResult;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.GetObjectMetadataRequest;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.InitiateMultipartUploadResult;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Object bucket specific utils for {@link OBSFileSystem}.
 */
final class OBSObjectBucketUtils {
  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSObjectBucketUtils.class);

  private OBSObjectBucketUtils() {

  }

  /**
   * The inner rename operation.
   *
   * @param owner OBS File System instance
   * @param src   path to be renamed
   * @param dst   new path after rename
   * @return boolean
   * @throws RenameFailedException if some criteria for a state changing rename
   *                               was not met. This means work didn't happen;
   *                               it's not something which is reported upstream
   *                               to the FileSystem APIs, for which the
   *                               semantics of "false" are pretty vague.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException           on IO failure.
   * @throws ObsException          on failures inside the OBS SDK
   */
  static boolean renameBasedOnObject(final OBSFileSystem owner,
      final Path src, final Path dst) throws RenameFailedException,
      FileNotFoundException, IOException,
      ObsException {
    String srcKey = OBSCommonUtils.pathToKey(owner, src);
    String dstKey = OBSCommonUtils.pathToKey(owner, dst);

    if (srcKey.isEmpty()) {
      LOG.error("rename: src [{}] is root directory", src);
      throw new IOException(src + " is root directory");
    }

    // get the source file status; this raises a FNFE if there is no source
    // file.
    FileStatus srcStatus = owner.getFileStatus(src);

    FileStatus dstStatus;
    try {
      dstStatus = owner.getFileStatus(dst);
      // if there is no destination entry, an exception is raised.
      // hence this code sequence can assume that there is something
      // at the end of the path; the only detail being what it is and
      // whether or not it can be the destination of the rename.
      if (dstStatus.isDirectory()) {
        String newDstKey = OBSCommonUtils.maybeAddTrailingSlash(dstKey);
        String filename = srcKey.substring(
            OBSCommonUtils.pathToKey(owner, src.getParent()).length()
                + 1);
        newDstKey = newDstKey + filename;
        dstKey = newDstKey;
        dstStatus = owner.getFileStatus(
            OBSCommonUtils.keyToPath(dstKey));
        if (dstStatus.isDirectory()) {
          throw new RenameFailedException(src, dst,
              "new destination is an existed directory")
              .withExitCode(false);
        } else {
          throw new RenameFailedException(src, dst,
              "new destination is an existed file")
              .withExitCode(false);
        }
      } else {

        if (srcKey.equals(dstKey)) {
          LOG.warn(
              "rename: src and dest refer to the same file or"
                  + " directory: {}",
              dst);
          return true;
        } else {
          throw new RenameFailedException(src, dst,
              "destination is an existed file")
              .withExitCode(false);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);

      // Parent must exist
      checkDestinationParent(owner, src, dst);
    }

    if (dstKey.startsWith(srcKey)
        && dstKey.charAt(srcKey.length()) == Path.SEPARATOR_CHAR) {
      LOG.error("rename: dest [{}] cannot be a descendant of src [{}]",
          dst, src);
      return false;
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.debug("rename: renaming file {} to {}", src, dst);

      renameFile(owner, srcKey, dstKey, srcStatus);
    } else {
      LOG.debug("rename: renaming directory {} to {}", src, dst);

      // This is a directory to directory copy
      dstKey = OBSCommonUtils.maybeAddTrailingSlash(dstKey);
      srcKey = OBSCommonUtils.maybeAddTrailingSlash(srcKey);

      renameFolder(owner, srcKey, dstKey);
    }

    if (src.getParent() != dst.getParent()) {
      // deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(owner, src.getParent());
    }

    return true;
  }

  private static void checkDestinationParent(final OBSFileSystem owner,
      final Path src,
      final Path dst) throws IOException {
    Path parent = dst.getParent();
    if (!OBSCommonUtils.pathToKey(owner, parent).isEmpty()) {
      try {
        FileStatus dstParentStatus = owner.getFileStatus(
            dst.getParent());
        if (!dstParentStatus.isDirectory()) {
          throw new ParentNotDirectoryException(
              "destination parent [" + dst.getParent()
                  + "] is not a directory");
        }
      } catch (FileNotFoundException e2) {
        throw new RenameFailedException(src, dst,
            "destination has no parent ");
      }
    }
  }

  /**
   * Implement rename file.
   *
   * @param owner     OBS File System instance
   * @param srcKey    source object key
   * @param dstKey    destination object key
   * @param srcStatus source object status
   * @throws IOException any problem with rename operation
   */
  private static void renameFile(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey,
      final FileStatus srcStatus)
      throws IOException {
    long startTime = System.nanoTime();

    copyFile(owner, srcKey, dstKey, srcStatus.getLen());
    objectDelete(owner, srcStatus, false);

    if (LOG.isDebugEnabled()) {
      long delay = System.nanoTime() - startTime;
      LOG.debug("OBSFileSystem rename: "
          + ", {src="
          + srcKey
          + ", dst="
          + dstKey
          + ", delay="
          + delay
          + "}");
    }
  }

  static boolean objectDelete(final OBSFileSystem owner,
      final FileStatus status,
      final boolean recursive) throws IOException {
    Path f = status.getPath();
    String key = OBSCommonUtils.pathToKey(owner, f);

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {} - recursive {}", f,
          recursive);

      key = OBSCommonUtils.maybeAddTrailingSlash(key);
      if (!key.endsWith("/")) {
        key = key + "/";
      }

      boolean isEmptyDir = OBSCommonUtils.isFolderEmpty(owner, key);
      if (key.equals("/")) {
        return OBSCommonUtils.rejectRootDirectoryDelete(
            owner.getBucket(), isEmptyDir, recursive);
      }

      if (!recursive && !isEmptyDir) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (isEmptyDir) {
        LOG.debug(
            "delete: Deleting fake empty directory {} - recursive {}",
            f, recursive);
        OBSCommonUtils.deleteObject(owner, key);
      } else {
        LOG.debug(
            "delete: Deleting objects for directory prefix {} "
                + "- recursive {}",
            f, recursive);
        deleteNonEmptyDir(owner, recursive, key);
      }

    } else {
      LOG.debug("delete: Path is a file");
      OBSCommonUtils.deleteObject(owner, key);
    }

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(owner, parent);
    }
    return true;
  }

  /**
   * Implement rename folder.
   *
   * @param owner  OBS File System instance
   * @param srcKey source folder key
   * @param dstKey destination folder key
   * @throws IOException any problem with rename folder
   */
  static void renameFolder(final OBSFileSystem owner, final String srcKey,
      final String dstKey)
      throws IOException {
    long startTime = System.nanoTime();

    List<KeyAndVersion> keysToDelete = new ArrayList<>();

    createFakeDirectory(owner, dstKey);

    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(owner.getBucket());
    request.setPrefix(srcKey);
    request.setMaxKeys(owner.getMaxKeys());

    ObjectListing objects = OBSCommonUtils.listObjects(owner, request);

    List<Future<CopyObjectResult>> copyfutures = new LinkedList<>();
    while (true) {
      for (ObsObject summary : objects.getObjects()) {
        if (summary.getObjectKey().equals(srcKey)) {
          // skip prefix itself
          continue;
        }

        keysToDelete.add(new KeyAndVersion(summary.getObjectKey()));
        String newDstKey = dstKey + summary.getObjectKey()
            .substring(srcKey.length());
        // copyFile(summary.getObjectKey(), newDstKey,
        // summary.getMetadata().getContentLength());
        copyfutures.add(
            copyFileAsync(owner, summary.getObjectKey(), newDstKey,
                summary.getMetadata().getContentLength()));

        if (keysToDelete.size() == owner.getMaxEntriesToDelete()) {
          waitAllCopyFinished(copyfutures);
          copyfutures.clear();
        }
      }

      if (!objects.isTruncated()) {
        if (!keysToDelete.isEmpty()) {
          waitAllCopyFinished(copyfutures);
          copyfutures.clear();
        }
        break;
      }
      objects = OBSCommonUtils.continueListObjects(owner, objects);
    }

    keysToDelete.add(new KeyAndVersion(srcKey));

    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(
        owner.getBucket());
    deleteObjectsRequest.setKeyAndVersions(
        keysToDelete.toArray(new KeyAndVersion[0]));
    OBSCommonUtils.deleteObjects(owner, deleteObjectsRequest);

    if (LOG.isDebugEnabled()) {
      long delay = System.nanoTime() - startTime;
      LOG.debug(
          "OBSFileSystem rename: "
              + ", {src="
              + srcKey
              + ", dst="
              + dstKey
              + ", delay="
              + delay
              + "}");
    }
  }

  private static void waitAllCopyFinished(
      final List<Future<CopyObjectResult>> copyFutures)
      throws IOException {
    try {
      for (Future<CopyObjectResult> copyFuture : copyFutures) {
        copyFuture.get();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while copying objects (copy)");
      throw new InterruptedIOException(
          "Interrupted while copying objects (copy)");
    } catch (ExecutionException e) {
      for (Future<CopyObjectResult> future : copyFutures) {
        future.cancel(true);
      }

      throw OBSCommonUtils.extractException(
          "waitAllCopyFinished", copyFutures.toString(), e);
    }
  }

  /**
   * Request object metadata; increments counters in the process.
   *
   * @param owner OBS File System instance
   * @param key   key
   * @return the metadata
   */
  protected static ObjectMetadata getObjectMetadata(final OBSFileSystem owner,
      final String key) {
    GetObjectMetadataRequest request = new GetObjectMetadataRequest();
    request.setBucketName(owner.getBucket());
    request.setObjectKey(key);
    if (owner.getSse().isSseCEnable()) {
      request.setSseCHeader(owner.getSse().getSseCHeader());
    }
    ObjectMetadata meta = owner.getObsClient().getObjectMetadata(request);
    owner.getSchemeStatistics().incrementReadOps(1);
    return meta;
  }

  /**
   * Create a new object metadata instance. Any standard metadata headers are
   * added here, for example: encryption.
   *
   * @param length length of data to set in header.
   * @return a new metadata instance
   */
  static ObjectMetadata newObjectMetadata(final long length) {
    final ObjectMetadata om = new ObjectMetadata();
    if (length >= 0) {
      om.setContentLength(length);
    }
    return om;
  }

  private static void deleteNonEmptyDir(final OBSFileSystem owner,
      final boolean recursive, final String key) throws IOException {
    String delimiter = recursive ? null : "/";
    ListObjectsRequest request = OBSCommonUtils.createListObjectsRequest(
        owner, key, delimiter);

    ObjectListing objects = OBSCommonUtils.listObjects(owner, request);
    List<KeyAndVersion> keys = new ArrayList<>(objects.getObjects().size());
    while (true) {
      for (ObsObject summary : objects.getObjects()) {
        if (summary.getObjectKey().equals(key)) {
          // skip prefix itself
          continue;
        }

        keys.add(new KeyAndVersion(summary.getObjectKey()));
        LOG.debug("Got object to delete {}", summary.getObjectKey());

        if (keys.size() == owner.getMaxEntriesToDelete()) {
          OBSCommonUtils.removeKeys(owner, keys, true, true);
        }
      }

      if (!objects.isTruncated()) {
        keys.add(new KeyAndVersion(key));
        OBSCommonUtils.removeKeys(owner, keys, false, true);

        break;
      }
      objects = OBSCommonUtils.continueListObjects(owner, objects);
    }
  }

  static void createFakeDirectoryIfNecessary(final OBSFileSystem owner,
      final Path f)
      throws IOException, ObsException {

    String key = OBSCommonUtils.pathToKey(owner, f);
    if (!key.isEmpty() && !owner.exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(owner, key);
    }
  }

  static void createFakeDirectory(final OBSFileSystem owner,
      final String objectName)
      throws ObsException, IOException {
    String newObjectName = objectName;
    newObjectName = OBSCommonUtils.maybeAddTrailingSlash(newObjectName);
    createEmptyObject(owner, newObjectName);
  }

  // Used to create an empty file that represents an empty directory
  private static void createEmptyObject(final OBSFileSystem owner,
      final String objectName)
      throws ObsException, IOException {
    for (int retryTime = 1;
        retryTime < OBSCommonUtils.MAX_RETRY_TIME; retryTime++) {
      try {
        innerCreateEmptyObject(owner, objectName);
        return;
      } catch (ObsException e) {
        LOG.warn("Failed to create empty object [{}], retry time [{}], "
            + "exception [{}]", objectName, retryTime, e);
        try {
          Thread.sleep(OBSCommonUtils.DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }

    innerCreateEmptyObject(owner, objectName);
  }

  // Used to create an empty file that represents an empty directory
  private static void innerCreateEmptyObject(final OBSFileSystem owner,
      final String objectName)
      throws ObsException, IOException {
    final InputStream im =
        new InputStream() {
          @Override
          public int read() {
            return -1;
          }
        };

    PutObjectRequest putObjectRequest = OBSCommonUtils
        .newPutObjectRequest(owner, objectName, newObjectMetadata(0L), im);

    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }

    try {
      owner.getObsClient().putObject(putObjectRequest);
      owner.getSchemeStatistics().incrementWriteOps(1);
      owner.getSchemeStatistics().incrementBytesWritten(len);
    } finally {
      im.close();
    }
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   *
   * @param owner  OBS File System instance
   * @param srcKey source object path
   * @param dstKey destination object path
   * @param size   object size
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException            Other IO problems
   */
  private static void copyFile(final OBSFileSystem owner, final String srcKey,
      final String dstKey, final long size)
      throws IOException, InterruptedIOException {
    for (int retryTime = 1;
        retryTime < OBSCommonUtils.MAX_RETRY_TIME; retryTime++) {
      try {
        innerCopyFile(owner, srcKey, dstKey, size);
        return;
      } catch (InterruptedIOException e) {
        throw e;
      } catch (IOException e) {
        LOG.warn(
            "Failed to copy file from [{}] to [{}] with size [{}], "
                + "retry time [{}], exception [{}]", srcKey, dstKey,
            size, retryTime, e);
        try {
          Thread.sleep(OBSCommonUtils.DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }

    innerCopyFile(owner, srcKey, dstKey, size);
  }

  private static void innerCopyFile(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey, final long size)
      throws IOException {
    LOG.debug("copyFile {} -> {} ", srcKey, dstKey);
    try {
      // 100MB per part
      if (size > owner.getCopyPartSize()) {
        // initial copy part task
        InitiateMultipartUploadRequest request
            = new InitiateMultipartUploadRequest(owner.getBucket(),
            dstKey);
        request.setAcl(owner.getCannedACL());
        if (owner.getSse().isSseCEnable()) {
          request.setSseCHeader(owner.getSse().getSseCHeader());
        } else if (owner.getSse().isSseKmsEnable()) {
          request.setSseKmsHeader(owner.getSse().getSseKmsHeader());
        }
        InitiateMultipartUploadResult result = owner.getObsClient()
            .initiateMultipartUpload(request);

        final String uploadId = result.getUploadId();
        LOG.debug("Multipart copy file, uploadId: {}", uploadId);
        // count the parts
        long partCount = calPartCount(owner.getCopyPartSize(), size);

        final List<PartEtag> partEtags =
            getCopyFilePartEtags(owner, srcKey, dstKey, size, uploadId,
                partCount);
        // merge the copy parts
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            new CompleteMultipartUploadRequest(owner.getBucket(),
                dstKey, uploadId, partEtags);
        owner.getObsClient()
            .completeMultipartUpload(completeMultipartUploadRequest);
      } else {
        ObjectMetadata srcom = getObjectMetadata(owner, srcKey);
        ObjectMetadata dstom = cloneObjectMetadata(srcom);
        final CopyObjectRequest copyObjectRequest =
            new CopyObjectRequest(owner.getBucket(), srcKey,
                owner.getBucket(), dstKey);
        copyObjectRequest.setAcl(owner.getCannedACL());
        copyObjectRequest.setNewObjectMetadata(dstom);
        if (owner.getSse().isSseCEnable()) {
          copyObjectRequest.setSseCHeader(
              owner.getSse().getSseCHeader());
          copyObjectRequest.setSseCHeaderSource(
              owner.getSse().getSseCHeader());
        } else if (owner.getSse().isSseKmsEnable()) {
          copyObjectRequest.setSseKmsHeader(
              owner.getSse().getSseKmsHeader());
        }
        owner.getObsClient().copyObject(copyObjectRequest);
      }

      owner.getSchemeStatistics().incrementWriteOps(1);
    } catch (ObsException e) {
      throw OBSCommonUtils.translateException(
          "copyFile(" + srcKey + ", " + dstKey + ")", srcKey, e);
    }
  }

  static int calPartCount(final long partSize, final long cloudSize) {
    // get user setting of per copy part size ,default is 100MB
    // calculate the part count
    long partCount = cloudSize % partSize == 0
        ? cloudSize / partSize
        : cloudSize / partSize + 1;
    return (int) partCount;
  }

  static List<PartEtag> getCopyFilePartEtags(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey,
      final long objectSize,
      final String uploadId,
      final long partCount)
      throws IOException {
    final List<PartEtag> partEtags = Collections.synchronizedList(
        new ArrayList<>());
    final List<Future<?>> partCopyFutures = new ArrayList<>();
    submitCopyPartTasks(owner, srcKey, dstKey, objectSize, uploadId,
        partCount, partEtags, partCopyFutures);

    // wait the tasks for completing
    try {
      for (Future<?> partCopyFuture : partCopyFutures) {
        partCopyFuture.get();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while copying objects (copy)");
      throw new InterruptedIOException(
          "Interrupted while copying objects (copy)");
    } catch (ExecutionException e) {
      LOG.error("Multipart copy file exception.", e);
      for (Future<?> future : partCopyFutures) {
        future.cancel(true);
      }

      owner.getObsClient()
          .abortMultipartUpload(
              new AbortMultipartUploadRequest(owner.getBucket(), dstKey,
                  uploadId));

      throw OBSCommonUtils.extractException(
          "Multi-part copy with id '" + uploadId + "' from " + srcKey
              + "to " + dstKey, dstKey, e);
    }

    // Make part numbers in ascending order
    partEtags.sort(Comparator.comparingInt(PartEtag::getPartNumber));
    return partEtags;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private static void submitCopyPartTasks(final OBSFileSystem owner,
      final String srcKey,
      final String dstKey,
      final long objectSize,
      final String uploadId,
      final long partCount,
      final List<PartEtag> partEtags,
      final List<Future<?>> partCopyFutures) {
    for (int i = 0; i < partCount; i++) {
      final long rangeStart = i * owner.getCopyPartSize();
      final long rangeEnd = (i + 1 == partCount)
          ? objectSize - 1
          : rangeStart + owner.getCopyPartSize() - 1;
      final int partNumber = i + 1;
      partCopyFutures.add(
          owner.getBoundedCopyPartThreadPool().submit(() -> {
            CopyPartRequest request = new CopyPartRequest();
            request.setUploadId(uploadId);
            request.setSourceBucketName(owner.getBucket());
            request.setSourceObjectKey(srcKey);
            request.setDestinationBucketName(owner.getBucket());
            request.setDestinationObjectKey(dstKey);
            request.setByteRangeStart(rangeStart);
            request.setByteRangeEnd(rangeEnd);
            request.setPartNumber(partNumber);
            if (owner.getSse().isSseCEnable()) {
              request.setSseCHeaderSource(
                  owner.getSse().getSseCHeader());
              request.setSseCHeaderDestination(
                  owner.getSse().getSseCHeader());
            }
            CopyPartResult result = owner.getObsClient()
                .copyPart(request);
            partEtags.add(
                new PartEtag(result.getEtag(), result.getPartNumber()));
            LOG.debug(
                "Multipart copy file, uploadId: {}, Part#{} done.",
                uploadId, partNumber);
          }));
    }
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}. Does so without using
   * the {@link ObjectMetadata#clone()} method, to avoid copying unnecessary
   * headers.
   *
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private static ObjectMetadata cloneObjectMetadata(
      final ObjectMetadata source) {
    // This approach may be too brittle, especially if
    // in future there are new attributes added to ObjectMetadata
    // that we do not explicitly call to set here
    ObjectMetadata ret = newObjectMetadata(source.getContentLength());

    if (source.getContentEncoding() != null) {
      ret.setContentEncoding(source.getContentEncoding());
    }
    return ret;
  }

  static OBSFileStatus innerGetObjectStatus(final OBSFileSystem owner,
      final Path f)
      throws IOException {
    final Path path = OBSCommonUtils.qualify(owner, f);
    String key = OBSCommonUtils.pathToKey(owner, path);
    LOG.debug("Getting path status for {}  ({})", path, key);
    if (!StringUtils.isEmpty(key)) {
      try {
        ObjectMetadata meta = getObjectMetadata(owner, key);

        if (OBSCommonUtils.objectRepresentsDirectory(key,
            meta.getContentLength())) {
          LOG.debug("Found exact file: fake directory");
          return new OBSFileStatus(path, owner.getUsername());
        } else {
          LOG.debug("Found exact file: normal file");
          return new OBSFileStatus(meta.getContentLength(),
              OBSCommonUtils.dateToLong(meta.getLastModified()),
              path, owner.getDefaultBlockSize(path),
              owner.getUsername());
        }
      } catch (ObsException e) {
        if (e.getResponseCode() != OBSCommonUtils.NOT_FOUND_CODE) {
          throw OBSCommonUtils.translateException("getFileStatus",
              path, e);
        }
      }

      if (!key.endsWith("/")) {
        String newKey = key + "/";
        try {
          ObjectMetadata meta = getObjectMetadata(owner, newKey);

          if (OBSCommonUtils.objectRepresentsDirectory(newKey,
              meta.getContentLength())) {
            LOG.debug("Found file (with /): fake directory");
            return new OBSFileStatus(path, owner.getUsername());
          } else {
            LOG.debug(
                "Found file (with /): real file? should not "
                    + "happen: {}",
                key);

            return new OBSFileStatus(meta.getContentLength(),
                OBSCommonUtils.dateToLong(meta.getLastModified()),
                path,
                owner.getDefaultBlockSize(path),
                owner.getUsername());
          }
        } catch (ObsException e) {
          if (e.getResponseCode() != OBSCommonUtils.NOT_FOUND_CODE) {
            throw OBSCommonUtils.translateException("getFileStatus",
                newKey, e);
          }
        }
      }
    }

    try {
      boolean isEmpty = OBSCommonUtils.innerIsFolderEmpty(owner, key);
      LOG.debug("Is dir ({}) empty? {}", path, isEmpty);
      return new OBSFileStatus(path, owner.getUsername());
    } catch (ObsException e) {
      if (e.getResponseCode() != OBSCommonUtils.NOT_FOUND_CODE) {
        throw OBSCommonUtils.translateException("getFileStatus", key,
            e);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  static ContentSummary getDirectoryContentSummary(final OBSFileSystem owner,
      final String key) throws IOException {
    String newKey = key;
    newKey = OBSCommonUtils.maybeAddTrailingSlash(newKey);
    long[] summary = {0, 0, 1};
    LOG.debug("Summary key {}", newKey);
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(owner.getBucket());
    request.setPrefix(newKey);
    Set<String> directories = new TreeSet<>();
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
          LOG.debug("Objects in folder [" + prefix + "]:");
          getDirectories(prefix, newKey, directories);
        }

        for (ObsObject obj : objects.getObjects()) {
          LOG.debug("Summary: {} {}", obj.getObjectKey(),
              obj.getMetadata().getContentLength());
          if (!obj.getObjectKey().endsWith("/")) {
            summary[0] += obj.getMetadata().getContentLength();
            summary[1] += 1;
          }
          getDirectories(obj.getObjectKey(), newKey, directories);
        }
      }
      if (!objects.isTruncated()) {
        break;
      }
      objects = OBSCommonUtils.continueListObjects(owner, objects);
    }
    summary[2] += directories.size();
    LOG.debug(String.format(
        "file size [%d] - file count [%d] - directory count [%d] - "
            + "file path [%s]",
        summary[0],
        summary[1], summary[2], newKey));
    return new ContentSummary.Builder().length(summary[0])
        .fileCount(summary[1]).directoryCount(summary[2])
        .spaceConsumed(summary[0]).build();
  }

  private static void getDirectories(final String key, final String sourceKey,
      final Set<String> directories) {
    Path p = new Path(key);
    Path sourcePath = new Path(sourceKey);
    // directory must add first
    if (key.endsWith("/") && p.compareTo(sourcePath) > 0) {
      directories.add(p.toString());
    }
    while (p.compareTo(sourcePath) > 0) {
      Optional<Path> parent = p.getOptionalParentPath();
      if (!parent.isPresent()) {
        break;
      }
      p = parent.get();
      if (p.compareTo(sourcePath) == 0) {
        break;
      }
      directories.add(p.toString());
    }
  }

  private static Future<CopyObjectResult> copyFileAsync(
      final OBSFileSystem owner,
      final String srcKey,
      final String dstKey, final long size) {
    return owner.getBoundedCopyThreadPool().submit(() -> {
      copyFile(owner, srcKey, dstKey, size);
      return null;
    });
  }
}
