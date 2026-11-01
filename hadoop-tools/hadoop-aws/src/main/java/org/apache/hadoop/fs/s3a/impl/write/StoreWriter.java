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

package org.apache.hadoop.fs.s3a.impl.write;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import javax.annotation.Nullable;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.ProgressableProgressListener;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.service.Service;

/**
 * Interface for store writing and multipart IO operations:
 * put, create, upload part, complete, list, abort.
 */
public interface StoreWriter extends Service {

  /**
   * Registered service name: {@value}.
   */
  String STORE_WRITER = "StoreWriter";

  /**
   * Start a transfer-manager managed async PUT of an object,
   * incrementing the put requests and put bytes
   * counters.
   * <p>
   * It does not update the other counters,
   * as existing code does that as progress callbacks come in.
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <p>
   * Because the operation is async, any stream supplied in the request
   * must reference data (files, buffers) which stay valid until the upload
   * completes.
   * Retry policy: N/A: the transfer manager is performing the upload.
   * Auditing: must be inside an audit span.
   * @param putObjectRequest the request
   * @param file the file to be uploaded
   * @param listener the progress listener for the request
   * @return the upload initiated
   * @throws IOException if transfer manager creation failed.
   */
  @Retries.OnceRaw
  UploadInfo putObject(
      PutObjectRequest putObjectRequest,
      File file,
      ProgressableProgressListener listener) throws IOException;

  /**
   * PUT an object directly (i.e. not via the transfer manager).
   * Byte length is calculated from the file length, or, if there is no
   * file, from the content length of the header.
   * <p>
   * Retry Policy: none.
   * Auditing: must be inside an audit span.
   * <i>Important: this call will close any input stream in the request.</i>
   * @param putObjectRequest the request
   * @param putOptions put object options
   * @param uploadData data to be uploaded
   * @param durationTrackerFactory factory for duration tracking
   * @return the upload initiated
   * @throws SdkException on problems
   */
  @Retries.OnceRaw
  PutObjectResponse putObjectDirect(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory durationTrackerFactory)
      throws SdkException;

  /**
   * Wait for an upload to complete.
   * If the upload (or its result collection) failed, this is where
   * the failure is raised as an AWS exception.
   * Calls {@link S3AStore#incrementPutCompletedStatistics(boolean, long)}
   * to update the statistics.
   * @param key destination key
   * @param uploadInfo upload to wait for
   * @return the upload result
   * @throws IOException IO failure
   * @throws CancellationException if the wait() was cancelled
   */
  @Retries.OnceTranslated
  CompletedFileUpload waitForUploadCompletion(String key, UploadInfo uploadInfo)
      throws IOException;

  /**
   * Initiate an MPU.
   * @param request request.
   * @return the result of the operation.
   * @throws AwsServiceException on problems
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Retries.OnceRaw
  CreateMultipartUploadResponse initiateMultipartUpload(
      CreateMultipartUploadRequest request)
      throws AwsServiceException, UncheckedIOException;

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the body.</i>
   * <p>
   * Retry Policy: none.
   * @param trackerFactory duration tracker factory for operation
   * @param request the upload part request.
   * @param body the request body.
   * @return the result of the operation.
   * @throws AwsServiceException on problems
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Retries.OnceRaw
  UploadPartResponse uploadPart(
      UploadPartRequest request,
      RequestBody body,
      @Nullable DurationTrackerFactory trackerFactory)
      throws AwsServiceException, UncheckedIOException;

  /**
   * Complete a multipart upload.
   * @param request request
   * @return the response
   * @throws AwsServiceException on problems
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Retries.OnceRaw
  CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request)
      throws AwsServiceException, UncheckedIOException;

  /**
   * Abort a multipart upload.
   * @param upload upload
   * @return the response from the request
   * @throws AwsServiceException on problems
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Retries.OnceRaw
  AbortMultipartUploadResponse abortMultipartUpload(MultipartUpload upload)
      throws AwsServiceException, UncheckedIOException;

  /**
   * Abort a multipart upload.
   * @param destKey key of destination path.
   * @param uploadId upload operation ID
   * @return the response
   * @throws UncheckedIOException failure to instantiate the s3 client
   */
  @Retries.OnceRaw
  AbortMultipartUploadResponse abortMultipartUpload(String destKey, String uploadId)
      throws AwsServiceException, UncheckedIOException;

  /**
   * List and abort all multipart uploads older than a specified age.
   * @param seconds age of multiparts to abort.
   * @param prefix prefix to scan for, "" for none
   * @param maxKeys maximum number of keys to list and abort
   * @param context store context to use
   * @throws IOException IO failure, including any uprated SdkException
   */
  @Retries.RetryTranslated
  void abortOutstandingMultipartUploads(long seconds, @Nullable String prefix, int maxKeys,
      StoreContext context)
      throws IOException;

  /**
   * Listing all multipart uploads; limited to the first few hundred.
   * Retry policy: retry, translated.
   * @param prefix prefix to scan for, "" for none
   * @return a listing of multipart uploads.
   * @throws IOException IO failure, including any uprated SdkException
   */
  @Retries.RetryTranslated
  List<MultipartUpload> listMultipartUploads(@Nullable String prefix)
      throws IOException;


  /**
   * List multipart uploads under a path.
   * @param storeContext store context.
   * @param prefix prefix, may be null or empty.
   * @param maxKeys maximum number of keys.
   * @return an iterator.
   * @throws IOException failure to initiate the listing operation.
   */
  @Retries.RetryTranslated
  RemoteIterator<MultipartUpload> listMultipartUploads(
      StoreContext storeContext,
      @Nullable String prefix,
      int maxKeys)
      throws IOException;
}
