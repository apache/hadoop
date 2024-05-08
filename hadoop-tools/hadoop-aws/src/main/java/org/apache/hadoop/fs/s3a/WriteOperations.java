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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/**
 * Operations to update the store.
 * This is effectively a private internal API for classes used as part
 * of the S3A implementation.
 * New extension points SHOULD use this interface -provided there
 * is no plan to backport to previous versions. In those situations,
 * use `WriteOperationHelper` directly.
 * @since Hadoop 3.3.0
 */
public interface WriteOperations extends AuditSpanSource, Closeable {

  /**
   * Execute a function with retry processing.
   * @param <T> type of return value
   * @param action action to execute (used in error messages)
   * @param path path of work (used in error messages)
   * @param idempotent does the operation have semantics
   * which mean that it can be retried even if was already executed?
   * @param operation operation to execute
   * @return the result of the call
   * @throws IOException any IOE raised, or translated exception
   */
  <T> T retry(String action,
      String path,
      boolean idempotent,
      CallableRaisingIOE<T> operation)
      throws IOException;

  /**
   * Create a {@link PutObjectRequest} request against the specific key.
   * @param destKey destination key
   * @param length size, if known. Use -1 for not known
   * @param options options for the request
   * @param isFile is data to be uploaded a file
   * @return the request
   */
  PutObjectRequest createPutObjectRequest(String destKey,
      long length,
      @Nullable PutObjectOptions options,
      boolean isFile);

  /**
   * Callback on a successful write.
   * @param length length of the write
   */
  void writeSuccessful(long length);

  /**
   * Callback on a write failure.
   * @param ex Any exception raised which triggered the failure.
   */
  void writeFailed(Exception ex);

  /**
   * Start the multipart upload process.
   * Retry policy: retrying, translated.
   * @param destKey destination of upload
   * @param options options for the put request
   * @return the upload result containing the ID
   * @throws IOException IO problem
   */
  @Retries.RetryTranslated
  String initiateMultiPartUpload(String destKey, PutObjectOptions options) throws IOException;

  /**
   * This completes a multipart upload to the destination key via
   * {@code finalizeMultipartUpload()}.
   * Retry policy: retrying, translated.
   * Retries increment the {@code errorCount} counter.
   * @param destKey destination
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @param errorCount a counter incremented by 1 on every error; for
   * use in statistics
   * @param putOptions put object options
   * @return the result of the operation.
   * @throws IOException if problems arose which could not be retried, or
   * the retry count was exceeded
   */
  @Retries.RetryTranslated
  CompleteMultipartUploadResponse completeMPUwithRetries(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags,
      long length,
      AtomicInteger errorCount,
      PutObjectOptions putOptions)
      throws IOException;

  /**
   * Abort a multipart upload operation.
   * @param destKey destination key of the upload
   * @param uploadId multipart operation Id
   * @param shouldRetry should failures trigger a retry?
   * @param retrying callback invoked on every retry
   * @throws IOException failure to abort
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Retries.RetryTranslated
  void abortMultipartUpload(String destKey, String uploadId,
      boolean shouldRetry, Invoker.Retried retrying)
      throws IOException;

  /**
   * Abort a multipart commit operation.
   * @param upload upload to abort.
   * @throws IOException on problems.
   */
  @Retries.RetryTranslated
  void abortMultipartUpload(MultipartUpload upload)
      throws IOException;

  /**
   * Abort multipart uploads under a path: limited to the first
   * few hundred.
   * @param prefix prefix for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble; FileNotFoundExceptions are swallowed.
   */
  @Retries.RetryTranslated
  int abortMultipartUploadsUnderPath(String prefix)
      throws IOException;

  /**
   * List in-progress multipart uploads under a path: limited to the first
   * few hundred.
   * @param prefix prefix for uploads to list
   * @return a list of in-progress multipart uploads
   * @throws IOException on problems
   */
  List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException;

  /**
   * Abort a multipart commit operation.
   * @param destKey destination key of ongoing operation
   * @param uploadId multipart operation Id
   * @throws IOException on problems.
   * @throws FileNotFoundException if the abort ID is unknown
   */
  @Retries.RetryTranslated
  void abortMultipartCommit(String destKey, String uploadId)
      throws IOException;

  /**
   * Create and initialize a part request builder of a multipart upload.
   * @param destKey destination key of ongoing operation
   * @param uploadId ID of ongoing upload
   * @param partNumber current part number of the upload
   * @param size amount of data
   * @return the request builder.
   * @throws IllegalArgumentException if the parameters are invalid
   * @throws PathIOException if the part number is out of range.
   */
  UploadPartRequest.Builder newUploadPartRequestBuilder(
      String destKey,
      String uploadId,
      int partNumber,
      long size) throws IOException;

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * @param putObjectRequest the request
   * @param putOptions put object options
   * @param durationTrackerFactory factory for duration tracking
   * @param uploadData data to be uploaded
   * @param isFile is data to be uploaded a file
   * @return the upload initiated
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  PutObjectResponse putObject(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions, S3ADataBlocks.BlockUploadData uploadData, boolean isFile,
      DurationTrackerFactory durationTrackerFactory)
      throws IOException;

  /**
   * Revert a commit by deleting the file.
   * No attempt is made to probe for/recreate a parent dir marker
   * Relies on retry code in filesystem.
   * @throws IOException on problems
   * @param destKey destination key
   */
  @Retries.OnceTranslated
  void revertCommit(String destKey) throws IOException;

  /**
   * This completes a multipart upload to the destination key via
   * {@code finalizeMultipartUpload()}.
   * Retry policy: retrying, translated.
   * Retries increment the {@code errorCount} counter.
   * @param destKey destination
   * @param uploadId multipart operation Id
   * @param partETags list of partial uploads
   * @param length length of the upload
   * @return the result of the operation.
   * @throws IOException if problems arose which could not be retried, or
   * the retry count was exceeded
   */
  @Retries.RetryTranslated
  CompleteMultipartUploadResponse commitUpload(
      String destKey,
      String uploadId,
      List<CompletedPart> partETags,
      long length)
      throws IOException;

  /**
   * Upload part of a multi-partition file.
   * @param request the upload part request.
   * @param body the request body.
   * @param durationTrackerFactory factory for duration tracking
   * @return the result of the operation.
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  UploadPartResponse uploadPart(UploadPartRequest request, RequestBody body,
      DurationTrackerFactory durationTrackerFactory)
      throws IOException;

  /**
   * Get the configuration of this instance; essentially the owning
   * filesystem configuration.
   * @return the configuration.
   */
  Configuration getConf();

  /**
   * Increment the write operation counter
   * of the filesystem.
   */
  void incrementWriteOperations();
}
