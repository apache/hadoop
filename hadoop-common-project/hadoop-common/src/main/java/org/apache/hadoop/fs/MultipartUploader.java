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

package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * MultipartUploader is an interface for copying files multipart and across
 * multiple nodes.
 * <p></p>
 * The interface extends {@link IOStatisticsSource} so that there is no
 * need to cast an instance to see if is a source of statistics.
 * However, implementations MAY return null for their actual statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface MultipartUploader extends Closeable,
    IOStatisticsSource {


  /**
   * Initialize a multipart upload.
   * @param filePath Target path for upload.
   * @return unique identifier associating part uploads.
   * @throws IOException IO failure
   */
  CompletableFuture<UploadHandle> startUpload(Path filePath)
      throws IOException;

  /**
   * Put part as part of a multipart upload.
   * It is possible to have parts uploaded in any order (or in parallel).
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param partNumber Index of the part relative to others.
   * @param filePath Target path for upload (as {@link #startUpload(Path)}).
   * @param inputStream Data for this part. Implementations MUST close this
   * stream after reading in the data.
   * @param lengthInBytes Target length to read from the stream.
   * @return unique PartHandle identifier for the uploaded part.
   * @throws IOException IO failure
   */
  CompletableFuture<PartHandle> putPart(
      UploadHandle uploadId,
      int partNumber,
      Path filePath,
      InputStream inputStream,
      long lengthInBytes)
      throws IOException;

  /**
   * Complete a multipart upload.
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param filePath Target path for upload (as {@link #startUpload(Path)}.
   * @param handles non-empty map of part number to part handle.
   *          from {@link #putPart(UploadHandle, int, Path, InputStream, long)}.
   * @return unique PathHandle identifier for the uploaded file.
   * @throws IOException IO failure
   */
  CompletableFuture<PathHandle> complete(
      UploadHandle uploadId,
      Path filePath,
      Map<Integer, PartHandle> handles)
      throws IOException;

  /**
   * Aborts a multipart upload.
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param filePath Target path for upload (same as {@link #startUpload(Path)}.
   * @throws IOException IO failure
   * @return a future; the operation will have completed
   */
  CompletableFuture<Void> abort(UploadHandle uploadId, Path filePath)
      throws IOException;

  /**
   * Best effort attempt to aborts multipart uploads under a path.
   * Not all implementations support this, and those which do may
   * be vulnerable to eventually consistent listings of current uploads
   * -some may be missed.
   * @param path path to abort uploads under.
   * @return a future to the number of entries aborted;
   * -1 if aborting is unsupported
   * @throws IOException IO failure
   */
  CompletableFuture<Integer> abortUploadsUnderPath(Path path) throws IOException;

}
