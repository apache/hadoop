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

import org.apache.hadoop.util.Preconditions;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListMultipartUploadsRequest;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.MultipartUpload;
import com.obs.services.model.MultipartUploadListing;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.PutObjectResult;
import com.obs.services.model.UploadPartRequest;
import com.obs.services.model.UploadPartResult;
import com.obs.services.model.fs.FSStatusEnum;
import com.obs.services.model.fs.GetAttributeRequest;
import com.obs.services.model.fs.GetBucketFSStatusRequest;
import com.obs.services.model.fs.GetBucketFSStatusResult;
import com.obs.services.model.fs.ObsFSAttribute;
import com.obs.services.model.fs.WriteFileRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Common utils for {@link OBSFileSystem}.
 */
final class OBSCommonUtils {
  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSCommonUtils.class);

  /**
   * Moved permanently response code.
   */
  static final int MOVED_PERMANENTLY_CODE = 301;

  /**
   * Unauthorized response code.
   */
  static final int UNAUTHORIZED_CODE = 401;

  /**
   * Forbidden response code.
   */
  static final int FORBIDDEN_CODE = 403;

  /**
   * Not found response code.
   */
  static final int NOT_FOUND_CODE = 404;

  /**
   * File conflict.
   */
  static final int CONFLICT_CODE = 409;

  /**
   * Gone response code.
   */
  static final int GONE_CODE = 410;

  /**
   * EOF response code.
   */
  static final int EOF_CODE = 416;

  /**
   * Core property for provider path. Duplicated here for consistent code across
   * Hadoop version: {@value}.
   */
  static final String CREDENTIAL_PROVIDER_PATH
      = "hadoop.security.credential.provider.path";

  /**
   * Max number of retry times.
   */
  static final int MAX_RETRY_TIME = 3;

  /**
   * Delay time between two retries.
   */
  static final int DELAY_TIME = 10;

  /**
   * Max number of listing keys for checking folder empty.
   */
  static final int MAX_KEYS_FOR_CHECK_FOLDER_EMPTY = 3;

  /**
   * Max number of listing keys for checking folder empty.
   */
  static final int BYTE_TO_INT_MASK = 0xFF;

  private OBSCommonUtils() {
  }

  /**
   * Get the fs status of the bucket.
   *
   * @param obs        OBS client instance
   * @param bucketName bucket name
   * @return boolean value indicating if this bucket is a posix bucket
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException           any other problem talking to OBS
   */
  static boolean getBucketFsStatus(final ObsClient obs,
      final String bucketName)
      throws FileNotFoundException, IOException {
    try {
      GetBucketFSStatusRequest getBucketFsStatusRequest
          = new GetBucketFSStatusRequest();
      getBucketFsStatusRequest.setBucketName(bucketName);
      GetBucketFSStatusResult getBucketFsStatusResult =
          obs.getBucketFSStatus(getBucketFsStatusRequest);
      FSStatusEnum fsStatus = getBucketFsStatusResult.getStatus();
      return fsStatus == FSStatusEnum.ENABLED;
    } catch (ObsException e) {
      LOG.error(e.toString());
      throw translateException("getBucketFsStatus", bucketName, e);
    }
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key.
   *
   * @param owner the owner OBSFileSystem instance
   * @param path  input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  static String pathToKey(final OBSFileSystem owner, final Path path) {
    Path absolutePath = path;
    if (!path.isAbsolute()) {
      absolutePath = new Path(owner.getWorkingDirectory(), path);
    }

    if (absolutePath.toUri().getScheme() != null && absolutePath.toUri()
        .getPath()
        .isEmpty()) {
      return "";
    }

    return absolutePath.toUri().getPath().substring(1);
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key, adding a trailing "/"
   * if the path is not the root <i>and</i> does not already have a "/" at the
   * end.
   *
   * @param key obs key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  static String maybeAddTrailingSlash(final String key) {
    if (!StringUtils.isEmpty(key) && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Convert a path back to a key.
   *
   * @param key input key
   * @return the path from this key
   */
  static Path keyToPath(final String key) {
    return new Path("/" + key);
  }

  /**
   * Convert a key to a fully qualified path.
   *
   * @param owner the owner OBSFileSystem instance
   * @param key   input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  static Path keyToQualifiedPath(final OBSFileSystem owner,
      final String key) {
    return qualify(owner, keyToPath(key));
  }

  /**
   * Qualify a path.
   *
   * @param owner the owner OBSFileSystem instance
   * @param path  path to qualify
   * @return a qualified path.
   */
  static Path qualify(final OBSFileSystem owner, final Path path) {
    return path.makeQualified(owner.getUri(), owner.getWorkingDirectory());
  }

  /**
   * Delete obs key started '/'.
   *
   * @param key object key
   * @return new key
   */
  static String maybeDeleteBeginningSlash(final String key) {
    return !StringUtils.isEmpty(key) && key.startsWith("/") ? key.substring(
        1) : key;
  }

  /**
   * Add obs key started '/'.
   *
   * @param key object key
   * @return new key
   */
  static String maybeAddBeginningSlash(final String key) {
    return !StringUtils.isEmpty(key) && !key.startsWith("/")
        ? "/" + key
        : key;
  }

  /**
   * Translate an exception raised in an operation into an IOException. HTTP
   * error codes are examined and can be used to build a more specific
   * response.
   *
   * @param operation operation
   * @param path      path operated on (may be null)
   * @param exception obs exception raised
   * @return an IOE which wraps the caught exception.
   */
  static IOException translateException(
      final String operation, final String path,
      final ObsException exception) {
    String message = String.format("%s%s: status [%d] - request id [%s] "
            + "- error code [%s] - error message [%s] - trace :%s ",
        operation, path != null ? " on " + path : "",
        exception.getResponseCode(), exception.getErrorRequestId(),
        exception.getErrorCode(),
        exception.getErrorMessage(), exception);

    IOException ioe;

    int status = exception.getResponseCode();
    switch (status) {
    case MOVED_PERMANENTLY_CODE:
      message =
          String.format("Received permanent redirect response, "
                  + "status [%d] - request id [%s] - "
                  + "error code [%s] - message [%s]",
              exception.getResponseCode(),
              exception.getErrorRequestId(), exception.getErrorCode(),
              exception.getErrorMessage());
      ioe = new OBSIOException(message, exception);
      break;
    // permissions
    case UNAUTHORIZED_CODE:
    case FORBIDDEN_CODE:
      ioe = new AccessDeniedException(path, null, message);
      ioe.initCause(exception);
      break;

    // the object isn't there
    case NOT_FOUND_CODE:
    case GONE_CODE:
      ioe = new FileNotFoundException(message);
      ioe.initCause(exception);
      break;

    // out of range. This may happen if an object is overwritten with
    // a shorter one while it is being read.
    case EOF_CODE:
      ioe = new EOFException(message);
      break;

    default:
      // no specific exit code. Choose an IOE subclass based on the
      // class
      // of the caught exception
      ioe = new OBSIOException(message, exception);
      break;
    }
    return ioe;
  }

  /**
   * Reject any request to delete an object where the key is root.
   *
   * @param bucket bucket name
   * @param key    key to validate
   * @throws InvalidRequestException if the request was rejected due to a
   *                                 mistaken attempt to delete the root
   *                                 directory.
   */
  static void blockRootDelete(final String bucket, final String key)
      throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException(
          "Bucket " + bucket + " cannot be deleted");
    }
  }

  /**
   * Delete an object. Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   *
   * @param owner the owner OBSFileSystem instance
   * @param key   key to blob to delete.
   * @throws IOException on any failure to delete object
   */
  static void deleteObject(final OBSFileSystem owner, final String key)
      throws IOException {
    blockRootDelete(owner.getBucket(), key);
    ObsException lastException = null;
    for (int retryTime = 1; retryTime <= MAX_RETRY_TIME; retryTime++) {
      try {
        owner.getObsClient().deleteObject(owner.getBucket(), key);
        owner.getSchemeStatistics().incrementWriteOps(1);
        return;
      } catch (ObsException e) {
        lastException = e;
        LOG.warn("Delete path failed with [{}], "
                + "retry time [{}] - request id [{}] - "
                + "error code [{}] - error message [{}]",
            e.getResponseCode(), retryTime, e.getErrorRequestId(),
            e.getErrorCode(), e.getErrorMessage());
        if (retryTime < MAX_RETRY_TIME) {
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            throw translateException("delete", key, e);
          }
        }
      }
    }
    throw translateException(
        String.format("retry max times [%s] delete failed", MAX_RETRY_TIME),
        key, lastException);
  }

  /**
   * Perform a bulk object delete operation. Increments the {@code
   * OBJECT_DELETE_REQUESTS} and write operation statistics.
   *
   * @param owner         the owner OBSFileSystem instance
   * @param deleteRequest keys to delete on the obs-backend
   * @throws IOException on any failure to delete objects
   */
  static void deleteObjects(final OBSFileSystem owner,
      final DeleteObjectsRequest deleteRequest) throws IOException {
    DeleteObjectsResult result;
    deleteRequest.setQuiet(true);
    try {
      result = owner.getObsClient().deleteObjects(deleteRequest);
      owner.getSchemeStatistics().incrementWriteOps(1);
    } catch (ObsException e) {
      LOG.warn("delete objects failed, request [{}], request id [{}] - "
              + "error code [{}] - error message [{}]",
          deleteRequest, e.getErrorRequestId(), e.getErrorCode(),
          e.getErrorMessage());
      for (KeyAndVersion keyAndVersion
          : deleteRequest.getKeyAndVersionsList()) {
        deleteObject(owner, keyAndVersion.getKey());
      }
      return;
    }

    // delete one by one if there is errors
    if (result != null) {
      List<DeleteObjectsResult.ErrorResult> errorResults
          = result.getErrorResults();
      if (!errorResults.isEmpty()) {
        LOG.warn("bulk delete {} objects, {} failed, begin to delete "
                + "one by one.",
            deleteRequest.getKeyAndVersionsList().size(),
            errorResults.size());
        for (DeleteObjectsResult.ErrorResult errorResult
            : errorResults) {
          deleteObject(owner, errorResult.getObjectKey());
        }
      }
    }
  }

  /**
   * Create a putObject request. Adds the ACL and metadata
   *
   * @param owner    the owner OBSFileSystem instance
   * @param key      key of object
   * @param metadata metadata header
   * @param srcfile  source file
   * @return the request
   */
  static PutObjectRequest newPutObjectRequest(final OBSFileSystem owner,
      final String key, final ObjectMetadata metadata, final File srcfile) {
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(
        owner.getBucket(), key, srcfile);
    putObjectRequest.setAcl(owner.getCannedACL());
    putObjectRequest.setMetadata(metadata);
    if (owner.getSse().isSseCEnable()) {
      putObjectRequest.setSseCHeader(owner.getSse().getSseCHeader());
    } else if (owner.getSse().isSseKmsEnable()) {
      putObjectRequest.setSseKmsHeader(owner.getSse().getSseKmsHeader());
    }
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request. The metadata is assumed to have
   * been configured with the size of the operation.
   *
   * @param owner       the owner OBSFileSystem instance
   * @param key         key of object
   * @param metadata    metadata header
   * @param inputStream source data.
   * @return the request
   */
  static PutObjectRequest newPutObjectRequest(final OBSFileSystem owner,
      final String key, final ObjectMetadata metadata,
      final InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    PutObjectRequest putObjectRequest = new PutObjectRequest(
        owner.getBucket(), key, inputStream);
    putObjectRequest.setAcl(owner.getCannedACL());
    putObjectRequest.setMetadata(metadata);
    if (owner.getSse().isSseCEnable()) {
      putObjectRequest.setSseCHeader(owner.getSse().getSseCHeader());
    } else if (owner.getSse().isSseKmsEnable()) {
      putObjectRequest.setSseKmsHeader(owner.getSse().getSseKmsHeader());
    }
    return putObjectRequest;
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager). Byte length is
   * calculated from the file length, or, if there is no file, from the content
   * length of the header. <i>Important: this call will close any input stream
   * in the request.</i>
   *
   * @param owner            the owner OBSFileSystem instance
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws ObsException on problems
   */
  static PutObjectResult putObjectDirect(final OBSFileSystem owner,
      final PutObjectRequest putObjectRequest) throws ObsException {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }

    PutObjectResult result = owner.getObsClient()
        .putObject(putObjectRequest);
    owner.getSchemeStatistics().incrementWriteOps(1);
    owner.getSchemeStatistics().incrementBytesWritten(len);
    return result;
  }

  /**
   * Upload part of a multi-partition file. Increments the write and put
   * counters. <i>Important: this call does not close any input stream in the
   * request.</i>
   *
   * @param owner   the owner OBSFileSystem instance
   * @param request request
   * @return the result of the operation.
   * @throws ObsException on problems
   */
  static UploadPartResult uploadPart(final OBSFileSystem owner,
      final UploadPartRequest request) throws ObsException {
    long len = request.getPartSize();
    UploadPartResult uploadPartResult = owner.getObsClient()
        .uploadPart(request);
    owner.getSchemeStatistics().incrementWriteOps(1);
    owner.getSchemeStatistics().incrementBytesWritten(len);
    return uploadPartResult;
  }

  static void removeKeys(final OBSFileSystem owner,
      final List<KeyAndVersion> keysToDelete, final boolean clearKeys,
      final boolean checkRootDelete) throws IOException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return;
    }

    if (checkRootDelete) {
      for (KeyAndVersion keyVersion : keysToDelete) {
        blockRootDelete(owner.getBucket(), keyVersion.getKey());
      }
    }

    if (!owner.isEnableMultiObjectDelete()
        || keysToDelete.size() < owner.getMultiDeleteThreshold()) {
      // delete one by one.
      for (KeyAndVersion keyVersion : keysToDelete) {
        deleteObject(owner, keyVersion.getKey());
      }
    } else if (keysToDelete.size() <= owner.getMaxEntriesToDelete()) {
      // Only one batch.
      DeleteObjectsRequest deleteObjectsRequest
          = new DeleteObjectsRequest(owner.getBucket());
      deleteObjectsRequest.setKeyAndVersions(
          keysToDelete.toArray(new KeyAndVersion[0]));
      deleteObjects(owner, deleteObjectsRequest);
    } else {
      // Multi batches.
      List<KeyAndVersion> keys = new ArrayList<>(
          owner.getMaxEntriesToDelete());
      for (KeyAndVersion key : keysToDelete) {
        keys.add(key);
        if (keys.size() == owner.getMaxEntriesToDelete()) {
          // Delete one batch.
          removeKeys(owner, keys, true, false);
        }
      }
      // Delete the last batch
      removeKeys(owner, keys, true, false);
    }

    if (clearKeys) {
      keysToDelete.clear();
    }
  }

  /**
   * Translate an exception raised in an operation into an IOException. The
   * specific type of IOException depends on the class of {@link ObsException}
   * passed in, and any status codes included in the operation. That is: HTTP
   * error codes are examined and can be used to build a more specific
   * response.
   *
   * @param operation operation
   * @param path      path operated on (must not be null)
   * @param exception obs exception raised
   * @return an IOE which wraps the caught exception.
   */
  static IOException translateException(final String operation,
      final Path path, final ObsException exception) {
    return translateException(operation, path.toString(), exception);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param owner     the owner OBSFileSystem instance
   * @param f         given path
   * @param recursive flag indicating if list is recursive
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException           due to an IO problem.
   * @throws ObsException          on failures inside the OBS SDK
   */
  static FileStatus[] innerListStatus(final OBSFileSystem owner, final Path f,
      final boolean recursive)
      throws FileNotFoundException, IOException, ObsException {
    Path path = qualify(owner, f);
    String key = pathToKey(owner, path);

    List<FileStatus> result;
    final FileStatus fileStatus = owner.getFileStatus(path);

    if (fileStatus.isDirectory()) {
      key = maybeAddTrailingSlash(key);
      String delimiter = recursive ? null : "/";
      ListObjectsRequest request = createListObjectsRequest(owner, key,
          delimiter);
      LOG.debug(
          "listStatus: doing listObjects for directory {} - recursive {}",
          f, recursive);

      OBSListing.FileStatusListingIterator files = owner.getObsListing()
          .createFileStatusListingIterator(
              path, request, OBSListing.ACCEPT_ALL,
              new OBSListing.AcceptAllButSelfAndS3nDirs(path));
      result = new ArrayList<>(files.getBatchSize());
      while (files.hasNext()) {
        result.add(files.next());
      }

      return result.toArray(new FileStatus[0]);
    } else {
      LOG.debug("Adding: rd (not a dir): {}", path);
      FileStatus[] stats = new FileStatus[1];
      stats[0] = fileStatus;
      return stats;
    }
  }

  /**
   * Create a {@code ListObjectsRequest} request against this bucket.
   *
   * @param owner     the owner OBSFileSystem instance
   * @param key       key for request
   * @param delimiter any delimiter
   * @return the request
   */
  static ListObjectsRequest createListObjectsRequest(
      final OBSFileSystem owner, final String key, final String delimiter) {
    return createListObjectsRequest(owner, key, delimiter, -1);
  }

  static ListObjectsRequest createListObjectsRequest(
      final OBSFileSystem owner, final String key, final String delimiter,
      final int maxKeyNum) {
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(owner.getBucket());
    if (maxKeyNum > 0 && maxKeyNum < owner.getMaxKeys()) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(owner.getMaxKeys());
    }
    request.setPrefix(key);
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return request;
  }

  /**
   * Implements the specific logic to reject root directory deletion. The caller
   * must return the result of this call, rather than attempt to continue with
   * the delete operation: deleting root directories is never allowed. This
   * method simply implements the policy of when to return an exit code versus
   * raise an exception.
   *
   * @param bucket     bucket name
   * @param isEmptyDir flag indicating if the directory is empty
   * @param recursive  recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was explicitly rejected.
   */
  static boolean rejectRootDirectoryDelete(final String bucket,
      final boolean isEmptyDir,
      final boolean recursive)
      throws IOException {
    LOG.info("obs delete the {} root directory of {}", bucket, recursive);
    if (isEmptyDir) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      // reject
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  /**
   * Make the given path and all non-existent parents into directories.
   *
   * @param owner the owner OBSFileSystem instance
   * @param path  path to create
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException                other IO problems
   * @throws ObsException               on failures inside the OBS SDK
   */
  static boolean innerMkdirs(final OBSFileSystem owner, final Path path)
      throws IOException, FileAlreadyExistsException, ObsException {
    LOG.debug("Making directory: {}", path);
    FileStatus fileStatus;
    try {
      fileStatus = owner.getFileStatus(path);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + path);
      }
    } catch (FileNotFoundException e) {
      Path fPart = path.getParent();
      do {
        try {
          fileStatus = owner.getFileStatus(fPart);
          if (fileStatus.isDirectory()) {
            break;
          }
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(
                String.format("Can't make directory for path '%s'"
                    + " since it is a file.", fPart));
          }
        } catch (FileNotFoundException fnfe) {
          LOG.debug("file {} not fount, but ignore.", path);
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = pathToKey(owner, path);
      if (owner.isFsBucket()) {
        OBSPosixBucketUtils.fsCreateFolder(owner, key);
      } else {
        OBSObjectBucketUtils.createFakeDirectory(owner, key);
      }
      return true;
    }
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics in the
   * process.
   *
   * @param owner   the owner OBSFileSystem instance
   * @param request request to initiate
   * @return the results
   * @throws IOException on any failure to list objects
   */
  static ObjectListing listObjects(final OBSFileSystem owner,
      final ListObjectsRequest request) throws IOException {
    if (request.getDelimiter() == null && request.getMarker() == null
        && owner.isFsBucket() && owner.isObsClientDFSListEnable()) {
      return OBSFsDFSListing.fsDFSListObjects(owner, request);
    }

    return commonListObjects(owner, request);
  }

  static ObjectListing commonListObjects(final OBSFileSystem owner,
      final ListObjectsRequest request) {
    for (int retryTime = 1; retryTime < MAX_RETRY_TIME; retryTime++) {
      try {
        owner.getSchemeStatistics().incrementReadOps(1);
        return owner.getObsClient().listObjects(request);
      } catch (ObsException e) {
        LOG.warn("Failed to commonListObjects for request[{}], retry "
                + "time [{}], due to exception[{}]",
            request, retryTime, e);
        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          LOG.error("Failed to commonListObjects for request[{}], "
                  + "retry time [{}], due to exception[{}]",
              request, retryTime, e);
          throw e;
        }
      }
    }

    owner.getSchemeStatistics().incrementReadOps(1);
    return owner.getObsClient().listObjects(request);
  }

  /**
   * List the next set of objects.
   *
   * @param owner   the owner OBSFileSystem instance
   * @param objects paged result
   * @return the next result object
   * @throws IOException on any failure to list the next set of objects
   */
  static ObjectListing continueListObjects(final OBSFileSystem owner,
      final ObjectListing objects) throws IOException {
    if (objects.getDelimiter() == null && owner.isFsBucket()
        && owner.isObsClientDFSListEnable()) {
      return OBSFsDFSListing.fsDFSContinueListObjects(owner,
          (OBSFsDFSListing) objects);
    }

    return commonContinueListObjects(owner, objects);
  }

  private static ObjectListing commonContinueListObjects(
      final OBSFileSystem owner, final ObjectListing objects) {
    String delimiter = objects.getDelimiter();
    int maxKeyNum = objects.getMaxKeys();
    // LOG.debug("delimiters: "+objects.getDelimiter());
    ListObjectsRequest request = new ListObjectsRequest();
    request.setMarker(objects.getNextMarker());
    request.setBucketName(owner.getBucket());
    request.setPrefix(objects.getPrefix());
    if (maxKeyNum > 0 && maxKeyNum < owner.getMaxKeys()) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(owner.getMaxKeys());
    }
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return commonContinueListObjects(owner, request);
  }

  static ObjectListing commonContinueListObjects(final OBSFileSystem owner,
      final ListObjectsRequest request) {
    for (int retryTime = 1; retryTime < MAX_RETRY_TIME; retryTime++) {
      try {
        owner.getSchemeStatistics().incrementReadOps(1);
        return owner.getObsClient().listObjects(request);
      } catch (ObsException e) {
        LOG.warn("Continue list objects failed for request[{}], retry"
                + " time[{}], due to exception[{}]",
            request, retryTime, e);
        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          LOG.error("Continue list objects failed for request[{}], "
                  + "retry time[{}], due to exception[{}]",
              request, retryTime, e);
          throw e;
        }
      }
    }

    owner.getSchemeStatistics().incrementReadOps(1);
    return owner.getObsClient().listObjects(request);
  }

  /**
   * Predicate: does the object represent a directory?.
   *
   * @param name object name
   * @param size object size
   * @return true if it meets the criteria for being an object
   */
  public static boolean objectRepresentsDirectory(final String name,
      final long size) {
    return !name.isEmpty() && name.charAt(name.length() - 1) == '/'
        && size == 0L;
  }

  /**
   * Date to long conversion. Handles null Dates that can be returned by OBS by
   * returning 0
   *
   * @param date date from OBS query
   * @return timestamp of the object
   */
  public static long dateToLong(final Date date) {
    if (date == null) {
      return 0L;
    }

    return date.getTime() / OBSConstants.SEC2MILLISEC_FACTOR
        * OBSConstants.SEC2MILLISEC_FACTOR;
  }

  // Used to check if a folder is empty or not.
  static boolean isFolderEmpty(final OBSFileSystem owner, final String key)
      throws FileNotFoundException, ObsException {
    for (int retryTime = 1; retryTime < MAX_RETRY_TIME; retryTime++) {
      try {
        return innerIsFolderEmpty(owner, key);
      } catch (ObsException e) {
        LOG.warn(
            "Failed to check empty folder for [{}], retry time [{}], "
                + "exception [{}]", key, retryTime, e);

        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }

    return innerIsFolderEmpty(owner, key);
  }

  // Used to check if a folder is empty or not by counting the number of
  // sub objects in list.
  private static boolean isFolderEmpty(final String key,
      final ObjectListing objects) {
    int count = objects.getObjects().size();
    if (count >= 2) {
      // There is a sub file at least.
      return false;
    } else if (count == 1 && !objects.getObjects()
        .get(0)
        .getObjectKey()
        .equals(key)) {
      // There is a sub file at least.
      return false;
    }

    count = objects.getCommonPrefixes().size();
    // There is a sub file at least.
    // There is no sub object.
    if (count >= 2) {
      // There is a sub file at least.
      return false;
    } else {
      return count != 1 || objects.getCommonPrefixes().get(0).equals(key);
    }
  }

  // Used to check if a folder is empty or not.
  static boolean innerIsFolderEmpty(final OBSFileSystem owner,
      final String key)
      throws FileNotFoundException, ObsException {
    String obsKey = maybeAddTrailingSlash(key);
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(owner.getBucket());
    request.setPrefix(obsKey);
    request.setDelimiter("/");
    request.setMaxKeys(MAX_KEYS_FOR_CHECK_FOLDER_EMPTY);
    owner.getSchemeStatistics().incrementReadOps(1);
    ObjectListing objects = owner.getObsClient().listObjects(request);

    if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjects()
        .isEmpty()) {
      if (isFolderEmpty(obsKey, objects)) {
        LOG.debug("Found empty directory {}", obsKey);
        return true;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found path as directory (with /): {}/{}",
            objects.getCommonPrefixes().size(),
            objects.getObjects().size());

        for (ObsObject summary : objects.getObjects()) {
          LOG.debug("Summary: {} {}", summary.getObjectKey(),
              summary.getMetadata().getContentLength());
        }
        for (String prefix : objects.getCommonPrefixes()) {
          LOG.debug("Prefix: {}", prefix);
        }
      }
      LOG.debug("Found non-empty directory {}", obsKey);
      return false;
    } else if (obsKey.isEmpty()) {
      LOG.debug("Found root directory");
      return true;
    } else if (owner.isFsBucket()) {
      LOG.debug("Found empty directory {}", obsKey);
      return true;
    }

    LOG.debug("Not Found: {}", obsKey);
    throw new FileNotFoundException("No such file or directory: " + obsKey);
  }

  /**
   * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
   *
   * @param owner  the owner OBSFileSystem instance
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  static LocatedFileStatus toLocatedFileStatus(final OBSFileSystem owner,
      final FileStatus status) throws IOException {
    return new LocatedFileStatus(
        status, status.isFile() ? owner.getFileBlockLocations(status, 0,
        status.getLen()) : null);
  }

  /**
   * Create a appendFile request. Adds the ACL and metadata
   *
   * @param owner          the owner OBSFileSystem instance
   * @param key            key of object
   * @param tmpFile        temp file or input stream
   * @param recordPosition client record next append position
   * @return the request
   * @throws IOException any problem
   */
  static WriteFileRequest newAppendFileRequest(final OBSFileSystem owner,
      final String key, final long recordPosition, final File tmpFile)
      throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(tmpFile);
    ObsFSAttribute obsFsAttribute;
    try {
      GetAttributeRequest getAttributeReq = new GetAttributeRequest(
          owner.getBucket(), key);
      obsFsAttribute = owner.getObsClient().getAttribute(getAttributeReq);
    } catch (ObsException e) {
      throw translateException("GetAttributeRequest", key, e);
    }

    long appendPosition = Math.max(recordPosition,
        obsFsAttribute.getContentLength());
    if (recordPosition != obsFsAttribute.getContentLength()) {
      LOG.warn("append url[{}] position[{}], file contentLength[{}] not"
              + " equal to recordPosition[{}].", key, appendPosition,
          obsFsAttribute.getContentLength(), recordPosition);
    }
    WriteFileRequest writeFileReq = new WriteFileRequest(owner.getBucket(),
        key, tmpFile, appendPosition);
    writeFileReq.setAcl(owner.getCannedACL());
    return writeFileReq;
  }

  /**
   * Create a appendFile request. Adds the ACL and metadata
   *
   * @param owner          the owner OBSFileSystem instance
   * @param key            key of object
   * @param inputStream    temp file or input stream
   * @param recordPosition client record next append position
   * @return the request
   * @throws IOException any problem
   */
  static WriteFileRequest newAppendFileRequest(final OBSFileSystem owner,
      final String key, final long recordPosition,
      final InputStream inputStream) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(inputStream);
    ObsFSAttribute obsFsAttribute;
    try {
      GetAttributeRequest getAttributeReq = new GetAttributeRequest(
          owner.getBucket(), key);
      obsFsAttribute = owner.getObsClient().getAttribute(getAttributeReq);
    } catch (ObsException e) {
      throw translateException("GetAttributeRequest", key, e);
    }

    long appendPosition = Math.max(recordPosition,
        obsFsAttribute.getContentLength());
    if (recordPosition != obsFsAttribute.getContentLength()) {
      LOG.warn("append url[{}] position[{}], file contentLength[{}] not"
              + " equal to recordPosition[{}].", key, appendPosition,
          obsFsAttribute.getContentLength(), recordPosition);
    }
    WriteFileRequest writeFileReq = new WriteFileRequest(owner.getBucket(),
        key, inputStream, appendPosition);
    writeFileReq.setAcl(owner.getCannedACL());
    return writeFileReq;
  }

  /**
   * Append File.
   *
   * @param owner             the owner OBSFileSystem instance
   * @param appendFileRequest append object request
   * @throws IOException on any failure to append file
   */
  static void appendFile(final OBSFileSystem owner,
      final WriteFileRequest appendFileRequest) throws IOException {
    long len = 0;
    if (appendFileRequest.getFile() != null) {
      len = appendFileRequest.getFile().length();
    }

    try {
      LOG.debug("Append file, key {} position {} size {}",
          appendFileRequest.getObjectKey(),
          appendFileRequest.getPosition(),
          len);
      owner.getObsClient().writeFile(appendFileRequest);
      owner.getSchemeStatistics().incrementWriteOps(1);
      owner.getSchemeStatistics().incrementBytesWritten(len);
    } catch (ObsException e) {
      throw translateException("AppendFile",
          appendFileRequest.getObjectKey(), e);
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any Exception or null
   * pointers. (This is the SLF4J equivalent of that in {@code IOUtils}).
   *
   * @param closeables the objects to close
   */
  static void closeAll(final java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          if (LOG != null) {
            LOG.debug("Closing {}", c);
          }
          c.close();
        } catch (Exception e) {
          if (LOG != null && LOG.isDebugEnabled()) {
            LOG.debug("Exception in closing {}", c, e);
          }
        }
      }
    }
  }

  /**
   * Extract an exception from a failed future, and convert to an IOE.
   *
   * @param operation operation which failed
   * @param path      path operated on (may be null)
   * @param ee        execution exception
   * @return an IOE which can be thrown
   */
  static IOException extractException(final String operation,
      final String path, final ExecutionException ee) {
    IOException ioe;
    Throwable cause = ee.getCause();
    if (cause instanceof ObsException) {
      ioe = translateException(operation, path, (ObsException) cause);
    } else if (cause instanceof IOException) {
      ioe = (IOException) cause;
    } else {
      ioe = new IOException(operation + " failed: " + cause, cause);
    }
    return ioe;
  }

  /**
   * Create a files status instance from a listing.
   *
   * @param keyPath   path to entry
   * @param summary   summary from OBS
   * @param blockSize block size to declare.
   * @param owner     owner of the file
   * @return a status entry
   */
  static OBSFileStatus createFileStatus(
      final Path keyPath, final ObsObject summary, final long blockSize,
      final String owner) {
    if (objectRepresentsDirectory(
        summary.getObjectKey(), summary.getMetadata().getContentLength())) {
      return new OBSFileStatus(keyPath, owner);
    } else {
      return new OBSFileStatus(
          summary.getMetadata().getContentLength(),
          dateToLong(summary.getMetadata().getLastModified()),
          keyPath,
          blockSize,
          owner);
    }
  }

  /**
   * Return the access key and secret for OBS API use. Credentials may exist in
   * configuration, within credential providers or indicated in the UserInfo of
   * the name URI param.
   *
   * @param name the URI for which we need the access keys.
   * @param conf the Configuration object to interrogate for keys.
   * @return OBSAccessKeys
   * @throws IOException problems retrieving passwords from KMS.
   */
  static OBSLoginHelper.Login getOBSAccessKeys(final URI name,
      final Configuration conf)
      throws IOException {
    OBSLoginHelper.Login login
        = OBSLoginHelper.extractLoginDetailsWithWarnings(name);
    Configuration c =
        ProviderUtils.excludeIncompatibleCredentialProviders(conf,
            OBSFileSystem.class);
    String accessKey = getPassword(c, OBSConstants.ACCESS_KEY,
        login.getUser());
    String secretKey = getPassword(c, OBSConstants.SECRET_KEY,
        login.getPassword());
    String sessionToken = getPassword(c, OBSConstants.SESSION_TOKEN,
        login.getToken());
    return new OBSLoginHelper.Login(accessKey, secretKey, sessionToken);
  }

  /**
   * Get a password from a configuration, or, if a value is passed in, pick that
   * up instead.
   *
   * @param conf configuration
   * @param key  key to look up
   * @param val  current value: if non empty this is used instead of querying
   *             the configuration.
   * @return a password or "".
   * @throws IOException on any problem
   */
  private static String getPassword(final Configuration conf,
      final String key, final String val) throws IOException {
    return StringUtils.isEmpty(val) ? lookupPassword(conf, key) : val;
  }

  /**
   * Get a password from a configuration/configured credential providers.
   *
   * @param conf configuration
   * @param key  key to look up
   * @return a password or the value in {@code defVal}
   * @throws IOException on any problem
   */
  private static String lookupPassword(final Configuration conf,
      final String key) throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ? new String(pass).trim() : "";
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }

  /**
   * String information about a summary entry for debug messages.
   *
   * @param summary summary object
   * @return string value
   */
  static String stringify(final ObsObject summary) {
    return summary.getObjectKey() + " size=" + summary.getMetadata()
        .getContentLength();
  }

  /**
   * Get a integer option not smaller than the minimum allowed value.
   *
   * @param conf   configuration
   * @param key    key to look up
   * @param defVal default value
   * @param min    minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static int intOption(final Configuration conf, final String key,
      final int defVal,
      final int min) {
    int v = conf.getInt(key, defVal);
    Preconditions.checkArgument(
        v >= min,
        String.format("Value of %s: %d is below the minimum value %d", key,
            v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option not smaller than the minimum allowed value.
   *
   * @param conf   configuration
   * @param key    key to look up
   * @param defVal default value
   * @param min    minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longOption(final Configuration conf, final String key,
      final long defVal,
      final long min) {
    long v = conf.getLong(key, defVal);
    Preconditions.checkArgument(
        v >= min,
        String.format("Value of %s: %d is below the minimum value %d", key,
            v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a long option not smaller than the minimum allowed value, supporting
   * memory prefixes K,M,G,T,P.
   *
   * @param conf   configuration
   * @param key    key to look up
   * @param defVal default value
   * @param min    minimum value
   * @return the value
   * @throws IllegalArgumentException if the value is below the minimum
   */
  static long longBytesOption(final Configuration conf, final String key,
      final long defVal,
      final long min) {
    long v = conf.getLongBytes(key, defVal);
    Preconditions.checkArgument(
        v >= min,
        String.format("Value of %s: %d is below the minimum value %d", key,
            v, min));
    LOG.debug("Value of {} is {}", key, v);
    return v;
  }

  /**
   * Get a size property from the configuration: this property must be at least
   * equal to {@link OBSConstants#MULTIPART_MIN_SIZE}. If it is too small, it is
   * rounded up to that minimum, and a warning printed.
   *
   * @param conf     configuration
   * @param property property name
   * @param defVal   default value
   * @return the value, guaranteed to be above the minimum size
   */
  public static long getMultipartSizeProperty(final Configuration conf,
      final String property, final long defVal) {
    long partSize = conf.getLongBytes(property, defVal);
    if (partSize < OBSConstants.MULTIPART_MIN_SIZE) {
      LOG.warn("{} must be at least 5 MB; configured value is {}",
          property, partSize);
      partSize = OBSConstants.MULTIPART_MIN_SIZE;
    }
    return partSize;
  }

  /**
   * Ensure that the long value is in the range of an integer.
   *
   * @param name property name for error messages
   * @param size original size
   * @return the size, guaranteed to be less than or equal to the max value of
   * an integer.
   */
  static int ensureOutputParameterInRange(final String name,
      final long size) {
    if (size > Integer.MAX_VALUE) {
      LOG.warn(
          "obs: {} capped to ~2.14GB"
              + " (maximum allowed size with current output mechanism)",
          name);
      return Integer.MAX_VALUE;
    } else {
      return (int) size;
    }
  }

  /**
   * Propagates bucket-specific settings into generic OBS configuration keys.
   * This is done by propagating the values of the form {@code
   * fs.obs.bucket.${bucket}.key} to {@code fs.obs.key}, for all values of "key"
   * other than a small set of unmodifiable values.
   *
   * <p>The source of the updated property is set to the key name of the
   * bucket property, to aid in diagnostics of where things came from.
   *
   * <p>Returns a new configuration. Why the clone? You can use the same conf
   * for different filesystems, and the original values are not updated.
   *
   * <p>The {@code fs.obs.impl} property cannot be set, nor can any with the
   * prefix {@code fs.obs.bucket}.
   *
   * <p>This method does not propagate security provider path information
   * from the OBS property into the Hadoop common provider: callers must call
   * {@link #patchSecurityCredentialProviders(Configuration)} explicitly.
   *
   * @param source Source Configuration object.
   * @param bucket bucket name. Must not be empty.
   * @return a (potentially) patched clone of the original.
   */
  static Configuration propagateBucketOptions(final Configuration source,
      final String bucket) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "bucket");
    final String bucketPrefix = OBSConstants.FS_OBS_BUCKET_PREFIX + bucket
        + '.';
    LOG.debug("Propagating entries under {}", bucketPrefix);
    final Configuration dest = new Configuration(source);
    for (Map.Entry<String, String> entry : source) {
      final String key = entry.getKey();
      // get the (unexpanded) value.
      final String value = entry.getValue();
      if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
        continue;
      }
      // there's a bucket prefix, so strip it
      final String stripped = key.substring(bucketPrefix.length());
      if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
        // tell user off
        LOG.debug("Ignoring bucket option {}", key);
      } else {
        // propagate the value, building a new origin field.
        // to track overwrites, the generic key is overwritten even if
        // already matches the new one.
        final String generic = OBSConstants.FS_OBS_PREFIX + stripped;
        LOG.debug("Updating {}", generic);
        dest.set(generic, value, key);
      }
    }
    return dest;
  }

  /**
   * Patch the security credential provider information in {@link
   * #CREDENTIAL_PROVIDER_PATH} with the providers listed in {@link
   * OBSConstants#OBS_SECURITY_CREDENTIAL_PROVIDER_PATH}.
   *
   * <p>This allows different buckets to use different credential files.
   *
   * @param conf configuration to patch
   */
  static void patchSecurityCredentialProviders(final Configuration conf) {
    Collection<String> customCredentials =
        conf.getStringCollection(
            OBSConstants.OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
    Collection<String> hadoopCredentials = conf.getStringCollection(
        CREDENTIAL_PROVIDER_PATH);
    if (!customCredentials.isEmpty()) {
      List<String> all = Lists.newArrayList(customCredentials);
      all.addAll(hadoopCredentials);
      String joined = StringUtils.join(all, ',');
      LOG.debug("Setting {} to {}", CREDENTIAL_PROVIDER_PATH, joined);
      conf.set(CREDENTIAL_PROVIDER_PATH, joined, "patch of "
          + OBSConstants.OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
    }
  }

  /**
   * Verify that the bucket exists. This does not check permissions, not even
   * read access.
   *
   * @param owner the owner OBSFileSystem instance
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException           any other problem talking to OBS
   */
  static void verifyBucketExists(final OBSFileSystem owner)
      throws FileNotFoundException, IOException {
    int retryTime = 1;
    while (true) {
      try {
        if (!owner.getObsClient().headBucket(owner.getBucket())) {
          throw new FileNotFoundException(
              "Bucket " + owner.getBucket() + " does not exist");
        }
        return;
      } catch (ObsException e) {
        LOG.warn("Failed to head bucket for [{}], retry time [{}], "
                + "exception [{}]", owner.getBucket(), retryTime,
            translateException("doesBucketExist", owner.getBucket(),
                e));

        if (MAX_RETRY_TIME == retryTime) {
          throw translateException("doesBucketExist",
              owner.getBucket(), e);
        }

        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
      retryTime++;
    }
  }

  /**
   * initialize multi-part upload, purge larger than the value of
   * PURGE_EXISTING_MULTIPART_AGE.
   *
   * @param owner the owner OBSFileSystem instance
   * @param conf  the configuration to use for the FS
   * @throws IOException on any failure to initialize multipart upload
   */
  static void initMultipartUploads(final OBSFileSystem owner,
      final Configuration conf)
      throws IOException {
    boolean purgeExistingMultipart =
        conf.getBoolean(OBSConstants.PURGE_EXISTING_MULTIPART,
            OBSConstants.DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge =
        longOption(conf, OBSConstants.PURGE_EXISTING_MULTIPART_AGE,
            OBSConstants.DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (!purgeExistingMultipart) {
      return;
    }

    final Date purgeBefore = new Date(
        new Date().getTime() - purgeExistingMultipartAge * 1000);

    try {
      ListMultipartUploadsRequest request
          = new ListMultipartUploadsRequest(owner.getBucket());
      while (true) {
        // List + purge
        MultipartUploadListing uploadListing = owner.getObsClient()
            .listMultipartUploads(request);
        for (MultipartUpload upload
            : uploadListing.getMultipartTaskList()) {
          if (upload.getInitiatedDate().compareTo(purgeBefore) < 0) {
            owner.getObsClient().abortMultipartUpload(
                new AbortMultipartUploadRequest(
                    owner.getBucket(), upload.getObjectKey(),
                    upload.getUploadId()));
          }
        }
        if (!uploadListing.isTruncated()) {
          break;
        }
        request.setUploadIdMarker(
            uploadListing.getNextUploadIdMarker());
        request.setKeyMarker(uploadListing.getNextKeyMarker());
      }
    } catch (ObsException e) {
      if (e.getResponseCode() == FORBIDDEN_CODE) {
        LOG.debug("Failed to purging multipart uploads against {},"
                + " FS may be read only", owner.getBucket(),
            e);
      } else {
        throw translateException("purging multipart uploads",
            owner.getBucket(), e);
      }
    }
  }

  static void shutdownAll(final ExecutorService... executors) {
    for (ExecutorService exe : executors) {
      if (exe != null) {
        try {
          if (LOG != null) {
            LOG.debug("Shutdown {}", exe);
          }
          exe.shutdown();
        } catch (Exception e) {
          if (LOG != null && LOG.isDebugEnabled()) {
            LOG.debug("Exception in shutdown {}", exe, e);
          }
        }
      }
    }
  }
}
