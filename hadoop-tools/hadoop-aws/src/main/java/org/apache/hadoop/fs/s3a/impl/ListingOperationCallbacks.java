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
import org.apache.hadoop.fs.s3a.s3guard.ITtlTimeProvider;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;

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
   * Retry policy: retry untranslated.
   * @param request request to initiate
   * @param trackerFactory tracker with statistics to update
   * @return the results
   * @throws IOException if the retry invocation raises one (it shouldn't).
   */
  @Retries.RetryRaw
  CompletableFuture<S3ListResult> listObjectsAsync(
          S3ListRequest request,
          DurationTrackerFactory trackerFactory)
          throws IOException;

  /**
   * List the next set of objects.
   * Retry policy: retry untranslated.
   * @param request last list objects request to continue
   * @param prevResult last paged result to continue from
   * @param trackerFactory tracker with statistics to update
   * @return the next result object
   * @throws IOException none, just there for retryUntranslated.
   */
  @Retries.RetryRaw
  CompletableFuture<S3ListResult> continueListObjectsAsync(
          S3ListRequest request,
          S3ListResult prevResult,
          DurationTrackerFactory trackerFactory)
          throws IOException;

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
   * with the maximum keys returned in a query set by
   * {@link #getMaxKeys()}.
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  S3ListRequest createListObjectsRequest(
          String key,
          String delimiter);


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

  /**
   * Get the updated time provider for the current fs instance.
   * @return implementation of {@link ITtlTimeProvider}
   */
  ITtlTimeProvider getUpdatedTtlTimeProvider();

  /**
   * Is the path for this instance considered authoritative on the client,
   * that is: will listing/status operations only be handled by the metastore,
   * with no fallback to S3.
   * @param p path
   * @return true iff the path is authoritative on the client.
   */
  boolean allowAuthoritative(Path p);

}
