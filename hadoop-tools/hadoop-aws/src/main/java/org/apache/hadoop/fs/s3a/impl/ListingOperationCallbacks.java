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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.s3a.S3ListResult;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;

/**
 * These are all the callbacks which
 * {@link org.apache.hadoop.fs.s3a.Listing} operations
 * need, derived from the actual appropriate S3AFileSystem
 * methods.
 */
public interface ListingOperationCallbacks {

  /**
   * Initiate a {@code listObjectsAsync} operation, incrementing metrics
   * in the process.
   *
   * Retry policy: failures will come from the future.
   * @param request request to initiate
   * @param trackerFactory tracker with statistics to update
   * @param span audit span for this operation
   * @return the results
   */
  @Retries.RetryRaw
  CompletableFuture<S3ListResult> listObjectsAsync(
      S3ListRequest request,
      DurationTrackerFactory trackerFactory,
      AuditSpan span);

  /**
   * List the next set of objects.
   * Retry policy: failures will come from the future.
   * @param request last list objects request to continue
   * @param prevResult last paged result to continue from
   * @param trackerFactory tracker with statistics to update
   * @param span audit span for the IO
   * @return the next result object
   */
  @Retries.RetryRaw
  CompletableFuture<S3ListResult> continueListObjectsAsync(
      S3ListRequest request,
      S3ListResult prevResult,
      DurationTrackerFactory trackerFactory,
      AuditSpan span);

  /**
   * Build a {@link S3ALocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  S3ALocatedFileStatus toLocatedFileStatus(
          S3AFileStatus status)
          throws IOException;
  /**
   * Create a {@code ListObjectsRequest} request against this bucket,
   * with the maximum keys returned in a query set in the FS config.
   * The active span for the FS is handed the request to prepare it
   * before this method returns.
   * {@link #getMaxKeys()}.
   * @param key key for request
   * @param delimiter any delimiter
   * @param span span within which the request takes place.
   * @return the request
   */
  S3ListRequest createListObjectsRequest(
      String key,
      String delimiter,
      AuditSpan span);

  /**
   * Return the number of bytes that large input files should be optimally
   * be split into to minimize I/O time.  The given path will be used to
   * locate the actual filesystem.  The full path does not have to exist.
   * @param path path of file
   * @return the default block size for the path's filesystem
   */
  long getDefaultBlockSize(Path path);

  /**
   * Get the maximum key count.
   * @return a value, valid after initialization
   */
  int getMaxKeys();

}
