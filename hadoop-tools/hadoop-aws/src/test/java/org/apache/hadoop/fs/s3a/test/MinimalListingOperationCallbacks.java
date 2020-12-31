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
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.s3a.S3ListResult;
import org.apache.hadoop.fs.s3a.impl.ListingOperationCallbacks;
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

/**
 * Stub implementation of {@link ListingOperationCallbacks}.
 */
public class MinimalListingOperationCallbacks
    implements ListingOperationCallbacks {

  @Override
  public CompletableFuture<S3ListResult> listObjectsAsync(
      final S3ListRequest request,
      final DurationTrackerFactory trackerFactory) throws IOException {
    return null;
  }

  @Override
  public CompletableFuture<S3ListResult> continueListObjectsAsync(
      final S3ListRequest request,
      final S3ListResult prevResult,
      final DurationTrackerFactory trackerFactory) throws IOException {
    return null;
  }

  @Override
  public S3ALocatedFileStatus toLocatedFileStatus(
      S3AFileStatus status) throws IOException {
    return null;
  }

  @Override
  public S3ListRequest createListObjectsRequest(
      String key,
      String delimiter) {
    return null;
  }

  @Override
  public long getDefaultBlockSize(Path path) {
    return 0;
  }

  @Override
  public int getMaxKeys() {
    return 0;
  }

  @Override
  public ITtlTimeProvider getUpdatedTtlTimeProvider() {
    return null;
  }

  @Override
  public boolean allowAuthoritative(Path p) {
    return false;
  }

}
