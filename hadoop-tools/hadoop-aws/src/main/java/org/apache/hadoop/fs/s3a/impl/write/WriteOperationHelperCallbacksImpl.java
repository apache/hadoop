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
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * Callbacks for WriteOperationHelper.
 */
public final class WriteOperationHelperCallbacksImpl
    implements WriteOperationHelperCallbacks {

  /**
   * Store through which data is written.
   */
  private final S3AStore store;

  /**
   * Write operations.
   */
  private StoreWriter storeWriter;

  /**
   * Constructor.
   * @param store store for interaction with.
   */
  public WriteOperationHelperCallbacksImpl(final S3AStore store) {
    this.store = store;
    this.storeWriter = store.getStoreWriter();
  }

  @Retries.OnceRaw
  @Override
  public PutObjectResponse putObjectDirect(PutObjectRequest putObjectRequest,
      PutObjectOptions putOptions,
      S3ADataBlocks.BlockUploadData uploadData,
      DurationTrackerFactory durationTrackerFactory)
      throws SdkException {
    return storeWriter.putObjectDirect(putObjectRequest, putOptions, uploadData,
        durationTrackerFactory);
  }

  @Override
  @Retries.OnceRaw
  public CompleteMultipartUploadResponse completeMultipartUpload(
      CompleteMultipartUploadRequest request) {
    return storeWriter.completeMultipartUpload(request);
  }

  @Override
  @Retries.OnceRaw
  public UploadPartResponse uploadPart(
      final UploadPartRequest request,
      final RequestBody body,
      final DurationTrackerFactory durationTrackerFactory)
      throws AwsServiceException, UncheckedIOException {
    return storeWriter.uploadPart(request, body, durationTrackerFactory);
  }

  @Override
  public void operationRetried(
      String text,
      Exception ex,
      int retries,
      boolean idempotent) {
    store.operationRetried(ex);
  }


  @Retries.OnceRaw
  @Override
  public CreateMultipartUploadResponse initiateMultipartUpload(
      CreateMultipartUploadRequest request) throws IOException {
    return storeWriter.initiateMultipartUpload(request);
  }

  @Override
  public void abortMultipartUpload(final MultipartUpload upload) throws IOException {
    storeWriter.abortMultipartUpload(upload);
  }

  @Retries.OnceTranslated
  @Override
  public void abortMultipartUpload(String destKey, String uploadId) throws IOException {
    storeWriter.abortMultipartUpload(destKey, uploadId);
  }

  @Retries.RetryTranslated
  @Override
  public List<MultipartUpload> listMultipartUploads(final String prefix)
      throws IOException {
    return storeWriter.listMultipartUploads(prefix);
  }

  @Retries.RetryRaw
  @Override
  public void deleteObjectAtPath(
      String key,
      boolean isFile)
      throws SdkException, UncheckedIOException {
    store.deleteObjectAtPath(key, isFile);
  }

  @Override
  public void incrementWriteOperations() {
    store.incrementWriteOperations();
  }

  @Override
  public String getBucket() {
    return store.getStoreContext().getBucket();
  }

  @Override
  public RequestFactory getRequestFactory() {
    return store.getRequestFactory();
  }

  @Override
  public Configuration getConf() {
    return store.getConfig();
  }
}
