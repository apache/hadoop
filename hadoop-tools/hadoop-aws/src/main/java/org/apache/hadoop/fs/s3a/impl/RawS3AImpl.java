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

package org.apache.hadoop.fs.s3a.impl;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.MultipartUtils;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.fs.s3a.auth.SignerManager;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAX_PAGING_KEYS;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PURGE_EXISTING_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_PURGE_EXISTING_MULTIPART_AGE;
import static org.apache.hadoop.fs.s3a.Constants.MAX_PAGING_KEYS;
import static org.apache.hadoop.fs.s3a.Constants.PURGE_EXISTING_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.PURGE_EXISTING_MULTIPART_AGE;
import static org.apache.hadoop.fs.s3a.S3AUtils.intOption;
import static org.apache.hadoop.fs.s3a.S3AUtils.longOption;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUESTS;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DELETE_CONSIDERED_IDEMPOTENT;

public class RawS3AImpl extends AbstractS3AService implements RawS3A {

  public static final Logger LOG = LoggerFactory.getLogger(RawS3AImpl.class);

  private AWSCredentialProviderList credentials;

  private SignerManager signerManager;

  private TransferManager transfers;

  private RequestFactory factory;

  private AmazonS3 s3;

  private int maxKeys;


  public RawS3AImpl(final String name) {
    super(name);
  }

  public RawS3AImpl() {
    this("raws3a");
  }

  @Override
  public void bind(
      final StoreContext storeContext,
      final AWSCredentialProviderList credentials,
      final SignerManager signerManager,
      final TransferManager transfers,
      final AmazonS3 s3client,
      final RequestFactory requestFactory) {

    super.bind(storeContext);
    this.credentials = credentials;
    this.signerManager = signerManager;
    this.transfers = transfers;
    this.s3 = s3client;
  }


  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    maxKeys = intOption(getConfig(), MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS,
        1);
    initMultipartUploads();
  }

  /**
   * Returns the S3 client used by this filesystem.
   * This is for internal use within the S3A code itself.
   * @return AmazonS3Client
   */
  AmazonS3 getAmazonS3Client() {
    return s3;
  }


  /**
   * Request object metadata; increments counters in the process.
   * Retry policy: retry untranslated.
   * Uses changeTracker to detect an unexpected file version (eTag or versionId)
   * @param key key
   * @param changeTracker the change tracker to detect unexpected object version
   * @param changeInvoker the invoker providing the retry policy
   * @param operation the operation (e.g. "read" or "copy") triggering this call
   * @return the metadata
   * @throws IOException if the retry invocation raises one (it shouldn't).
   * @throws RemoteFileChangedException if an unexpected version is detected
   */
  @Override
  @Retries.RetryRaw
  public ObjectMetadata getObjectMetadata(String key,
      ChangeTracker changeTracker,
      Invoker changeInvoker,
      String operation) throws IOException {
    GetObjectMetadataRequest request = factory.newGetObjectMetadataRequest(key);
    ObjectMetadata meta = changeInvoker.retryUntranslated("GET " + key, true,
        () -> {
          incrementStatistic(OBJECT_METADATA_REQUESTS);
          LOG.debug("HEAD {} with change tracker {}", key, changeTracker);
          if (changeTracker != null) {
            changeTracker.maybeApplyConstraint(request);
          }
          ObjectMetadata objectMetadata = s3.getObjectMetadata(request);
          if (changeTracker != null) {
            changeTracker.processMetadata(objectMetadata, operation);
          }
          return objectMetadata;
        });
    incrementReadOperations();
    return meta;
  }

  /**
   * Reject any request to delete an object where the key is root.
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket " + getBucket()
          + " cannot be deleted");
    }
  }

  /**
   * Delete an object. This is the low-level internal call which
   * <i>does not</i> update the metastore.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * This call does <i>not</i> create any mock parent entries.
   *
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param key key to blob to delete.
   * @throws AmazonClientException problems working with S3
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  @Override
  @VisibleForTesting
  @Retries.RetryRaw
  public void deleteObject(String key)
      throws AmazonClientException, IOException {
    blockRootDelete(key);
    incrementWriteOperations();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false,
                 "deleting %s", key)) {
      getInvoker().retryUntranslated(
          String.format("Delete %s:/%s", getBucket(), key),
          DELETE_CONSIDERED_IDEMPOTENT,
          () -> {
            incrementStatistic(OBJECT_DELETE_REQUESTS);
            s3.deleteObject(getBucket(), key);
            return null;
          });
    }
  }

  /**
   * Perform a bulk object delete operation.
   * Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   * Retry policy: retry untranslated; delete considered idempotent.
   * If the request is throttled, this is logged in the throttle statistics,
   * with the counter set to the number of keys, rather than the number
   * of invocations of the delete operation.
   * This is because S3 considers each key as one mutating operation on
   * the store when updating its load counters on a specific partition
   * of an S3 bucket.
   * If only the request was measured, this operation would under-report.
   * @param deleteRequest keys to delete on the s3-backend
   * @return the AWS response
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted.
   * @throws AmazonClientException amazon-layer failure.
   */
  @Override
  @Retries.RetryRaw
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteRequest)
      throws MultiObjectDeleteException, AmazonClientException, IOException {
    BulkDeleteRetryHandler retryHandler =
        new BulkDeleteRetryHandler(getStoreContext());
    incrementWriteOperations();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "DELETE %d keys",
                 deleteRequest.getKeys().size())) {
      return getInvoker().retryUntranslated("delete",
          InternalConstants.DELETE_CONSIDERED_IDEMPOTENT,
          (text, e, r, i) -> {
            // handle the failure
            retryHandler.bulkDeleteRetried(deleteRequest, e);
          },
          () -> {
            incrementStatistic(OBJECT_DELETE_REQUESTS, 1);
            return s3.deleteObjects(deleteRequest);
          });
    } catch (MultiObjectDeleteException e) {
      // one or more of the keys could not be deleted.
      // log and rethrow
      List<MultiObjectDeleteException.DeleteError> errors = e.getErrors();
      LOG.debug("Partial failure of delete, {} errors", errors.size(), e);
      for (MultiObjectDeleteException.DeleteError error : errors) {
        LOG.debug("{}: \"{}\" - {}",
            error.getKey(), error.getCode(), error.getMessage());
      }
      throw e;
    }
  }

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the request.</i>
   *
   * Retry Policy: none.
   * @param request request
   * @return the result of the operation.
   * @throws AmazonClientException on problems
   */
  @Override
  @Retries.OnceRaw
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws AmazonClientException {
    long len = request.getPartSize();
    factory.setOptionalUploadPartRequestParameters(request);
    incrementPutStartStatistics(len);
    try {
      UploadPartResult uploadPartResult = s3.uploadPart(request);
      incrementPutCompletedStatistics(true, len);
      return uploadPartResult;
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Get the length of the PUT, verifying that the length is known.
   * @param putObjectRequest a request bound to a file or a stream.
   * @return the request length
   * @throws IllegalArgumentException if the length is negative
   */
  @Override
  public long getPutRequestLength(PutObjectRequest putObjectRequest) {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    Preconditions.checkState(len >= 0, "Cannot PUT object of unknown length");
    return len;
  }


  /**
   * At the start of a put/multipart upload operation, update the
   * relevant counters.
   *
   * @param bytes bytes in the request.
   */
  public void incrementPutStartStatistics(long bytes) {
/*    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    incrementStatistic(OBJECT_PUT_REQUESTS);
    incrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);
    if (bytes > 0) {
      incrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }*/
  }

  /**
   * At the end of a put/multipart upload operation, update the
   * relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  public void incrementPutCompletedStatistics(boolean success, long bytes) {
/*    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      incrementStatistic(OBJECT_PUT_BYTES, bytes);
      decrementGauge(OBJECT_PUT_BYTES_PENDING, bytes);
    }
    incrementStatistic(OBJECT_PUT_REQUESTS_COMPLETED);
    decrementGauge(OBJECT_PUT_REQUESTS_ACTIVE, 1);*/
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events.
   * Increments those statistics which are expected to be updated during
   * the ongoing upload operation.
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  public void incrementPutProgressStatistics(String key, long bytes) {
/*    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      statistics.incrementBytesWritten(bytes);
    }*/
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   *
   * Retry Policy: none.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws AmazonClientException on problems
   */
  @Override
  @Retries.OnceRaw
  public PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest)
      throws AmazonClientException {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {}", len, putObjectRequest.getKey());
    incrementPutStartStatistics(len);
    try {
      PutObjectResult result = s3.putObject(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      // update metadata
      return result;
    } catch (AmazonClientException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }


  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * Retry policy: N/A: the transfer manager is performing the upload.
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  @Override
  @Retries.OnceRaw
  public UploadInfo putObject(PutObjectRequest putObjectRequest) {
    long len = getPutRequestLength(putObjectRequest);
    LOG.debug("PUT {} bytes to {} via transfer manager ",
        len, putObjectRequest.getKey());
    incrementPutStartStatistics(len);
    Upload upload = transfers.upload(putObjectRequest);
    return new UploadInfo(upload, len);
  }

  @Retries.RetryTranslated
  private void initMultipartUploads() throws IOException {
    Configuration conf = getConfig();
    boolean purgeExistingMultipart = conf.getBoolean(PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = longOption(conf,
        PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (purgeExistingMultipart) {
      try {
        abortOutstandingMultipartUploads(purgeExistingMultipartAge);
      } catch (AccessDeniedException e) {
//        instrumentation.errorIgnored();
        LOG.debug("Failed to purge multipart uploads against {}," +
            " FS may be read only", getBucket());
      }
    }
  }

  /**
   * Abort all outstanding MPUs older than a given age.
   * @param seconds time in seconds
   * @throws IOException on any failure, other than 403 "permission denied"
   */
  @Override
  @Retries.RetryTranslated
  public void abortOutstandingMultipartUploads(long seconds)
      throws IOException {
    Preconditions.checkArgument(seconds >= 0);
    Date purgeBefore =
        new Date(new Date().getTime() - seconds * 1000);
    LOG.debug("Purging outstanding multipart uploads older than {}",
        purgeBefore);
    getInvoker().retry("Purging multipart uploads", getBucket(), true,
        () -> transfers.abortMultipartUploads(getBucket(), purgeBefore));
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param destKey destination key
   * @param uploadId Upload ID
   */
  @Override
  @Retries.OnceRaw
  public void abortMultipartUpload(String destKey, String uploadId) {
    LOG.debug("Aborting multipart upload {} to {}", uploadId, destKey);
    getAmazonS3Client().abortMultipartUpload(
        factory.newAbortMultipartUploadRequest(destKey, uploadId)
    );
  }

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param upload the listed upload to abort.
   */
  @Override
  @Retries.OnceRaw
  public void abortMultipartUpload(MultipartUpload upload) {
    String destKey;
    String uploadId;
    destKey = upload.getKey();
    uploadId = upload.getUploadId();
    if (LOG.isInfoEnabled()) {
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      LOG.debug("Aborting multipart upload {} to {} initiated by {} on {}",
          uploadId, destKey, upload.getInitiator(),
          df.format(upload.getInitiated()));
    }
    getAmazonS3Client().abortMultipartUpload(
        new AbortMultipartUploadRequest(getBucket(),
            destKey,
            uploadId));
  }

  /**
   * Listing all multipart uploads; limited to the first few hundred.
   * See {@link #listUploads(String)} for an iterator-based version that does
   * not limit the number of entries returned.
   * Retry policy: retry, translated.
   * @return a listing of multipart uploads.
   * @param prefix prefix to scan for, "" for none
   * @throws IOException IO failure, including any uprated AmazonClientException
   */
  @Override
  @InterfaceAudience.Private
  @Retries.RetryTranslated
  public List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException {
    ListMultipartUploadsRequest request = factory.newListMultipartUploadsRequest(
        prefix);
    return getInvoker().retry("listMultipartUploads", prefix, true,
        () -> s3.listMultipartUploads(request).getMultipartUploads());
  }

  /**
   * List any pending multipart uploads whose keys begin with prefix, using
   * an iterator that can handle an unlimited number of entries.
   * See {@link #listMultipartUploads(String)} for a non-iterator version of
   * this.
   *
   * @param prefix optional key prefix to search
   * @return Iterator over multipart uploads.
   * @throws IOException on failure
   */
  @Override
  public MultipartUtils.UploadIterator listUploads(@Nullable String prefix)
      throws IOException {
    return MultipartUtils.listMultipartUploads(s3, getInvoker(), getBucket(), maxKeys,
        prefix);
  }

}
