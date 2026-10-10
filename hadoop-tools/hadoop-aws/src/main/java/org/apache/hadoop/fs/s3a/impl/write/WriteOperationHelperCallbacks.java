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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/***
 * Callbacks for writeOperationHelper.
 */
public interface WriteOperationHelperCallbacks {

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
  @Retries.OnceRaw()
  PutObjectResponse putObjectDirect(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory durationTrackerFactory)
      throws SdkException;

  /**
   * Initiates a complete multi-part upload request.
   * @param request Complete multi-part upload request
   * @return completeMultipartUploadResult
   */
  @Retries.OnceRaw
  CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request);

  /**
   * Upload part of a multi-partition file.
   * Increments the write and put counters.
   * <i>Important: this call does not close any input stream in the body.</i>
   * <p>
   * Retry Policy: none.
   * @param durationTrackerFactory duration tracker factory for operation
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
      DurationTrackerFactory durationTrackerFactory)
      throws AwsServiceException, UncheckedIOException;

  /**
   * Callback from {@link Invoker} when an operation is retried.
   * @param text text of the operation
   * @param ex exception
   * @param retries number of retries
   * @param idempotent is the method idempotent
   */
  void operationRetried(
      String text,
      Exception ex,
      int retries,
      boolean idempotent);

  /**
   * Initiate a multipart upload from the preconfigured request.
   * Retry policy: none + untranslated.
   * @param request request to initiate
   * @return the result of the call
   * @throws SdkException on failures inside the AWS SDK
   * @throws IOException Other IO problems
   */
  @Retries.OnceRaw
  CreateMultipartUploadResponse initiateMultipartUpload(
      CreateMultipartUploadRequest request) throws IOException;

  /**
   * Abort a multipart upload.
   * Retry policy: none.
   * @param upload the listed upload to abort.
   * @throws IOException IO failure, including any uprated SdkException
   */
  @Retries.OnceTranslated
  void abortMultipartUpload(MultipartUpload upload) throws IOException;

  @Retries.OnceTranslated
  void abortMultipartUpload(String destKey, String uploadId) throws IOException;

  /**
   * List in-progress multipart uploads under a path: limited to the first
   * few hundred.
   * @param prefix prefix for uploads to list
   * @return a list of in-progress multipart uploads
   * @throws IOException on problems
   */
  @Retries.RetryTranslated
  List<MultipartUpload> listMultipartUploads(String prefix)
      throws IOException;

  /**
   * Delete an object after acquiring write capacity.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @throws SdkException problems working with S3
   * @throws UncheckedIOException from invoker signature only -should not be raised.
   */
  @Retries.RetryRaw
  void deleteObjectAtPath(
      String key,
      boolean isFile)
      throws SdkException, UncheckedIOException;

  /**
   * Increment the write operation counter.
   * This is somewhat inaccurate, as it appears to be invoked more
   * often than needed in progress callbacks.
   */
  void incrementWriteOperations();

  /**
   * Get the name of the bucket this store is bound to.
   * @return a non-empty string
   */
  String getBucket();

  /**
   * Accessor for the store request factory..
   * @return request factory
   */
  RequestFactory getRequestFactory();

  /**
   * Get the configuration.
   * @return configuration of the store
   */
  Configuration getConf();
}
