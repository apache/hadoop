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

import java.io.UncheckedIOException;
import java.util.function.Supplier;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * Minimal implementation of writeOperationHelper callbacks.
 * Callbacks which need to talk to S3 use the s3 client resolved
 * on demand from {@link #s3clientSupplier}.
 * if this returns null, the operations raise NPEs.
 */
public class MinimalWriteOperationHelperCallbacks
    implements WriteOperationHelper.WriteOperationHelperCallbacks {

  /**
   * Supplier of the s3 client.
   */
  private final Supplier<S3Client> s3clientSupplier;

  /**
   * Constructor.
   * @param s3clientSupplier supplier of the S3 client.
   */
  public MinimalWriteOperationHelperCallbacks(
      final Supplier<S3Client> s3clientSupplier) {
    this.s3clientSupplier = s3clientSupplier;
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
  public void finishedWrite(final String key,
      final long length,
      final PutObjectOptions putOptions) {

  }
}

