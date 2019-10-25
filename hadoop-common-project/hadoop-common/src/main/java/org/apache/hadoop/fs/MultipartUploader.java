/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MultipartUploader is an interface for copying files multipart and across
 * multiple nodes. Users should:
 * <ol>
 *   <li>Initialize an upload.</li>
 *   <li>Upload parts in any order.</li>
 *   <li>Complete the upload in order to have it materialize in the destination
 *   FS.</li>
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface MultipartUploader extends Closeable {


  /**
   * Initialize a multipart upload.
   * @param filePath Target path for upload.
   * @return unique identifier associating part uploads.
   * @throws IOException IO failure
   */
  CompletableFuture<UploadHandle> initialize(Path filePath)
      throws IOException;

  /**
   * Put part as part of a multipart upload.
   * It is possible to have parts uploaded in any order (or in parallel).
   * @param filePath Target path for upload (same as {@link #initialize(Path)}).
   * @param inputStream Data for this part. Implementations MUST close this
   * stream after reading in the data.
   * @param partNumber Index of the part relative to others.
   * @param uploadId Identifier from {@link #initialize(Path)}.
   * @param lengthInBytes Target length to read from the stream.
   * @return unique PartHandle identifier for the uploaded part.
   * @throws IOException IO failure
   */
  CompletableFuture<PartHandle> putPart(Path filePath,
      InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes)
      throws IOException;

  /**
   * Complete a multipart upload.
   * @param filePath Target path for upload (same as {@link #initialize(Path)}.
   * @param handles non-empty map of part number to part handle.
   *          from {@link #putPart(Path, InputStream, int, UploadHandle, long)}.
   * @param multipartUploadId Identifier from {@link #initialize(Path)}.
   * @return unique PathHandle identifier for the uploaded file.
   * @throws IOException IO failure
   */
  CompletableFuture<PathHandle> complete(Path filePath,
      Map<Integer, PartHandle> handles,
      UploadHandle multipartUploadId)
      throws IOException;

  /**
   * Aborts a multipart upload.
   * @param filePath Target path for upload (same as {@link #initialize(Path)}.
   * @param multipartUploadId Identifier from {@link #initialize(Path)}.
   * @throws IOException IO failure
   * @return
   */
  CompletableFuture<Void> abort(Path filePath,
      UploadHandle multipartUploadId)
      throws IOException;

}
