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

package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UploadHandle;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;

/**
 * Standard base class for Multipart Uploaders.
 */
public abstract class AbstractMultipartUploader implements MultipartUploader {

  /**
   * Base path of upload.
   */
  private final Path basePath;

  /**
   * Instantiate.
   * @param basePath base path
   */
  protected AbstractMultipartUploader(final Path basePath) {
    this.basePath = Objects.requireNonNull(basePath, "null path");
  }

  /**
   * Perform any cleanup.
   * The upload is not required to support any operations after this.
   * @throws IOException problems on close.
   */
  @Override
  public void close() throws IOException {
  }

  protected Path getBasePath() {
    return basePath;
  }

  /**
   * Validate a path.
   * @param path path to check.
   */
  protected void checkPath(Path path) {
    Objects.requireNonNull(path, "null path");
    Preconditions.checkArgument(path.toString().startsWith(basePath.toString()),
        "Path %s is not under %s", path, basePath);
  }

  /**
   * Utility method to validate uploadIDs.
   * @param uploadId Upload ID
   * @throws IllegalArgumentException invalid ID
   */
  protected void checkUploadId(byte[] uploadId)
      throws IllegalArgumentException {
    checkArgument(uploadId != null, "null uploadId");
    checkArgument(uploadId.length > 0,
        "Empty UploadId is not valid");
  }

  /**
   * Utility method to validate partHandles.
   * @param partHandles handles
   * @throws IllegalArgumentException if the parts are invalid
   */
  protected void checkPartHandles(Map<Integer, PartHandle> partHandles) {
    checkArgument(!partHandles.isEmpty(),
        "Empty upload");
    partHandles.keySet()
        .stream()
        .forEach(key ->
            checkArgument(key > 0,
                "Invalid part handle index %s", key));
  }

  /**
   * Check all the arguments to the
   * {@link MultipartUploader#putPart(UploadHandle, int, Path, InputStream, long)}
   * operation.
   * @param filePath Target path for upload (as {@link #startUpload(Path)}).
   * @param inputStream Data for this part. Implementations MUST close this
   * stream after reading in the data.
   * @param partNumber Index of the part relative to others.
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param lengthInBytes Target length to read from the stream.
   * @throws IllegalArgumentException invalid argument
   */
  protected void checkPutArguments(Path filePath,
      InputStream inputStream,
      int partNumber,
      UploadHandle uploadId,
      long lengthInBytes) throws IllegalArgumentException {
    checkPath(filePath);
    checkArgument(inputStream != null, "null inputStream");
    checkArgument(partNumber > 0, "Invalid part number: %d", partNumber);
    checkArgument(uploadId != null, "null uploadId");
    checkArgument(lengthInBytes >= 0, "Invalid part length: %d", lengthInBytes);
  }

  /**
   * {@inheritDoc}.
   * @param path path to abort uploads under.
   * @return a future to -1.
   * @throws IOException
   */
  public CompletableFuture<Integer> abortUploadsUnderPath(Path path)
      throws IOException {
    checkPath(path);
    CompletableFuture<Integer> f = new CompletableFuture<>();
    f.complete(-1);
    return f;
  }

}
