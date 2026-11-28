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

package org.apache.hadoop.fs.s3a.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
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
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.write.WriteOperationHelperCallbacks;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * Minimal implementation of writeOperationHelper callbacks.
 * Callbacks which need to talk to S3 use the s3 client resolved
 * on demand from {@link #s3clientSupplier}.
 * if this returns null, the operations raise NPEs.
 */
public class MinimalWriteOperationHelperCallbacks
    implements WriteOperationHelperCallbacks {

  /**
   * Supplier of the s3 client.
   */
  private final Supplier<S3Client> s3clientSupplier;

  private final String bucket;

  private final RequestFactory requestFactory;

  /**
   * Constructor.
   * @param bucket bucket name
   * @param s3clientSupplier supplier of the S3 client.
   */
  public MinimalWriteOperationHelperCallbacks(
      final String bucket,
      final Supplier<S3Client> s3clientSupplier,
      final RequestFactory requestFactory) {
    this.s3clientSupplier = s3clientSupplier;
    this.requestFactory = requestFactory;
    this.bucket = bucket;
  }

  @Override
  public CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return s3clientSupplier.get().completeMultipartUpload(request);
  }

  @Override
  public UploadPartResponse uploadPart(final UploadPartRequest request,
      final RequestBody body,
      final DurationTrackerFactory durationTrackerFactory)
      throws AwsServiceException, UncheckedIOException {
    return s3clientSupplier.get().uploadPart(request, body);
  }

  @Override
  public PutObjectResponse putObjectDirect(final PutObjectRequest putObjectRequest,
      final PutObjectOptions putOptions,
      final S3ADataBlocks.BlockUploadData uploadData,
      final DurationTrackerFactory durationTrackerFactory) throws SdkException {
    return null;
  }

  @Override
  public void operationRetried(final String text,
      final Exception ex,
      final int retries,
      final boolean idempotent) {

  }

  @Override
  public CreateMultipartUploadResponse initiateMultipartUpload(
      final CreateMultipartUploadRequest request)
      throws IOException {
    return s3clientSupplier.get().createMultipartUpload(request);
  }

  @Override
  public void abortMultipartUpload(final MultipartUpload upload) throws IOException {
    abortMultipartUpload(upload.key(), upload.uploadId());
  }

  @Override
  public void abortMultipartUpload(final String destKey, final String uploadId) throws IOException {
    s3clientSupplier.get().abortMultipartUpload(
        getRequestFactory().newAbortMultipartUploadRequestBuilder(destKey, uploadId)
            .build());
  }

  @Override
  public List<MultipartUpload> listMultipartUploads(final String prefix) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public void deleteObjectAtPath(final String key, final boolean isFile)
      throws SdkException, UncheckedIOException {
    s3clientSupplier.get()
        .deleteObject(getRequestFactory().newDeleteObjectRequestBuilder(key).build());
  }

  @Override
  public void incrementWriteOperations() {

  }

  @Override
  public String getBucket() {
    return bucket;
  }

  @Override
  public RequestFactory getRequestFactory() {
    return requestFactory;
  }

  @Override
  public Configuration getConf() {
    return new Configuration();
  }
}

