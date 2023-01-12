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
import com.obs.services.model.CompleteMultipartUploadRequest;
import com.obs.services.model.CompleteMultipartUploadResult;
import com.obs.services.model.InitiateMultipartUploadRequest;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.PutObjectResult;
import com.obs.services.model.UploadPartRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for an ongoing write operation.
 *
 * <p>It hides direct access to the OBS API from the output stream, and is a
 * location where the object upload process can be evolved/enhanced.
 *
 * <p>Features
 *
 * <ul>
 * <li>Methods to create and submit requests to OBS, so avoiding all direct
 * interaction with the OBS APIs.
 * <li>Some extra preflight checks of arguments, so failing fast on errors.
 * <li>Callbacks to let the FS know of events in the output stream upload
 * process.
 * </ul>
 * <p>
 * Each instance of this state is unique to a single output stream.
 */
class OBSWriteOperationHelper {
  /**
   * Class logger.
   */
  public static final Logger LOG = LoggerFactory.getLogger(
      OBSWriteOperationHelper.class);

  /**
   * Part number of the multipart task.
   */
  static final int PART_NUMBER = 10000;

  /**
   * Owning filesystem.
   */
  private final OBSFileSystem owner;

  /**
   * Bucket of the owner FS.
   */
  private final String bucket;

  /**
   * Define obs client.
   */
  private final ObsClient obs;

  protected OBSWriteOperationHelper(final OBSFileSystem fs) {
    this.owner = fs;
    this.bucket = fs.getBucket();
    this.obs = fs.getObsClient();
  }

  /**
   * Create a {@link PutObjectRequest} request. If {@code length} is set, the
   * metadata is configured with the size of the upload.
   *
   * @param destKey     key of object
   * @param inputStream source data
   * @param length      size, if known. Use -1 for not known
   * @return the request
   */
  PutObjectRequest newPutRequest(final String destKey,
      final InputStream inputStream,
      final long length) {
    return OBSCommonUtils.newPutObjectRequest(owner, destKey,
        newObjectMetadata(length), inputStream);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   *
   * @param destKey    object key for request
   * @param sourceFile source file
   * @return the request
   */
  PutObjectRequest newPutRequest(final String destKey,
      final File sourceFile) {
    int length = (int) sourceFile.length();
    return OBSCommonUtils.newPutObjectRequest(owner, destKey,
        newObjectMetadata(length), sourceFile);
  }

  /**
   * Callback on a successful write.
   *
   * @param destKey object key
   */
  void writeSuccessful(final String destKey) {
    LOG.debug("Finished write to {}", destKey);
  }

  /**
   * Create a new object metadata instance. Any standard metadata headers are
   * added here, for example: encryption.
   *
   * @param length size, if known. Use -1 for not known
   * @return a new metadata instance
   */
  public ObjectMetadata newObjectMetadata(final long length) {
    return OBSObjectBucketUtils.newObjectMetadata(length);
  }

  /**
   * Start the multipart upload process.
   *
   * @param destKey object key
   * @return the upload result containing the ID
   * @throws IOException IO problem
   */
  String initiateMultiPartUpload(final String destKey) throws IOException {
    LOG.debug("Initiating Multipart upload");
    final InitiateMultipartUploadRequest initiateMPURequest =
        new InitiateMultipartUploadRequest(bucket, destKey);
    initiateMPURequest.setAcl(owner.getCannedACL());
    initiateMPURequest.setMetadata(newObjectMetadata(-1));
    if (owner.getSse().isSseCEnable()) {
      initiateMPURequest.setSseCHeader(owner.getSse().getSseCHeader());
    } else if (owner.getSse().isSseKmsEnable()) {
      initiateMPURequest.setSseKmsHeader(
          owner.getSse().getSseKmsHeader());
    }
    try {
      return obs.initiateMultipartUpload(initiateMPURequest)
          .getUploadId();
    } catch (ObsException ace) {
      throw OBSCommonUtils.translateException("Initiate MultiPartUpload",
          destKey, ace);
    }
  }

  /**
   * Complete a multipart upload operation.
   *
   * @param destKey   Object key
   * @param uploadId  multipart operation Id
   * @param partETags list of partial uploads
   * @return the result
   * @throws ObsException on problems.
   */
  CompleteMultipartUploadResult completeMultipartUpload(
      final String destKey, final String uploadId,
      final List<PartEtag> partETags)
      throws ObsException {
    Preconditions.checkNotNull(uploadId);
    Preconditions.checkNotNull(partETags);
    Preconditions.checkArgument(!partETags.isEmpty(),
        "No partitions have been uploaded");
    LOG.debug("Completing multipart upload {} with {} parts", uploadId,
        partETags.size());
    // a copy of the list is required, so that the OBS SDK doesn't
    // attempt to sort an unmodifiable list.
    return obs.completeMultipartUpload(
        new CompleteMultipartUploadRequest(bucket, destKey, uploadId,
            new ArrayList<>(partETags)));
  }

  /**
   * Abort a multipart upload operation.
   *
   * @param destKey  object key
   * @param uploadId multipart operation Id
   * @throws ObsException on problems. Immediately execute
   */
  void abortMultipartUpload(final String destKey, final String uploadId)
      throws ObsException {
    LOG.debug("Aborting multipart upload {}", uploadId);
    obs.abortMultipartUpload(
        new AbortMultipartUploadRequest(bucket, destKey, uploadId));
  }

  /**
   * Create request for uploading one part of a multipart task.
   *
   * @param destKey    destination object key
   * @param uploadId   upload id
   * @param partNumber part number
   * @param size       data size
   * @param sourceFile source file to be uploaded
   * @return part upload request
   */
  UploadPartRequest newUploadPartRequest(
      final String destKey,
      final String uploadId,
      final int partNumber,
      final int size,
      final File sourceFile) {
    Preconditions.checkNotNull(uploadId);

    Preconditions.checkArgument(sourceFile != null, "Data source");
    Preconditions.checkArgument(size > 0, "Invalid partition size %s",
        size);
    Preconditions.checkArgument(
        partNumber > 0 && partNumber <= PART_NUMBER);

    LOG.debug("Creating part upload request for {} #{} size {}", uploadId,
        partNumber, size);
    UploadPartRequest request = new UploadPartRequest();
    request.setUploadId(uploadId);
    request.setBucketName(bucket);
    request.setObjectKey(destKey);
    request.setPartSize((long) size);
    request.setPartNumber(partNumber);
    request.setFile(sourceFile);
    if (owner.getSse().isSseCEnable()) {
      request.setSseCHeader(owner.getSse().getSseCHeader());
    }
    return request;
  }

  /**
   * Create request for uploading one part of a multipart task.
   *
   * @param destKey      destination object key
   * @param uploadId     upload id
   * @param partNumber   part number
   * @param size         data size
   * @param uploadStream upload stream for the part
   * @return part upload request
   */
  UploadPartRequest newUploadPartRequest(
      final String destKey,
      final String uploadId,
      final int partNumber,
      final int size,
      final InputStream uploadStream) {
    Preconditions.checkNotNull(uploadId);

    Preconditions.checkArgument(uploadStream != null, "Data source");
    Preconditions.checkArgument(size > 0, "Invalid partition size %s",
        size);
    Preconditions.checkArgument(
        partNumber > 0 && partNumber <= PART_NUMBER);

    LOG.debug("Creating part upload request for {} #{} size {}", uploadId,
        partNumber, size);
    UploadPartRequest request = new UploadPartRequest();
    request.setUploadId(uploadId);
    request.setBucketName(bucket);
    request.setObjectKey(destKey);
    request.setPartSize((long) size);
    request.setPartNumber(partNumber);
    request.setInput(uploadStream);
    if (owner.getSse().isSseCEnable()) {
      request.setSseCHeader(owner.getSse().getSseCHeader());
    }
    return request;
  }

  public String toString(final String destKey) {
    return "{bucket=" + bucket + ", key='" + destKey + '\'' + '}';
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   *
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws IOException on problems
   */
  PutObjectResult putObject(final PutObjectRequest putObjectRequest)
      throws IOException {
    try {
      return OBSCommonUtils.putObjectDirect(owner, putObjectRequest);
    } catch (ObsException e) {
      throw OBSCommonUtils.translateException("put",
          putObjectRequest.getObjectKey(), e);
    }
  }
}
