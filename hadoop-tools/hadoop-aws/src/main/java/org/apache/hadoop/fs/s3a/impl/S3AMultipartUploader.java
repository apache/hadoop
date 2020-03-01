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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.impl.AbstractMultipartUploader;
import org.apache.hadoop.fs.s3a.WriteOperations;
import org.apache.hadoop.fs.s3a.impl.statistics.S3AMultipartUploaderStatistics;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;

/**
 * MultipartUploader for S3AFileSystem. This uses the S3 multipart
 * upload mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3AMultipartUploader extends AbstractMultipartUploader {

  private final S3AMultipartUploaderBuilder builder;

  /** Header for serialized Parts: {@value}. */

  public static final String HEADER = "S3A-part01";

  private final WriteOperations writeOperations;

  private final StoreContext context;

  private final S3AMultipartUploaderStatistics statistics;

  /**
   * Bulk state; demand created and then retained.
   */
  private BulkOperationState operationState;

  /**
   * Was an operation state requested but not returned?
   */
  private boolean noOperationState;

  /**
   * Instatiate; this is called by the builder.
   * @param builder builder
   * @param writeOperations writeOperations
   * @param context s3a context
   * @param statistics statistics callbacks
   */
  S3AMultipartUploader(
      final S3AMultipartUploaderBuilder builder,
      final WriteOperations writeOperations,
      final StoreContext context,
      final S3AMultipartUploaderStatistics statistics) {
    super(builder.getPath());
    this.builder = builder;
    this.writeOperations = writeOperations;
    this.context = context;
    this.statistics = statistics;
  }

  @Override
  public void close() throws IOException {
    if (operationState != null) {
      operationState.close();
    }
    super.close();
  }

  /**
   * Retrieve the operation state; create one on demand if needed
   * <i>and there has been no unsuccessful attempt to create one.</i>
   * @return an active operation state.
   * @throws IOException failure
   */
  private synchronized BulkOperationState retrieveOperationState()
      throws IOException {
    if (operationState == null && !noOperationState) {
      operationState = writeOperations.initiateOperation(getBasePath(),
          BulkOperationState.OperationType.Upload);
      noOperationState = operationState != null;
    }
    return operationState;
  }

  @Override
  public CompletableFuture<UploadHandle> startUpload(Path filePath)
      throws IOException {
    checkPath(filePath);
    String key = context.pathToKey(filePath);
    return context.submit(new CompletableFuture<>(),
        () -> {
          String uploadId = writeOperations.initiateMultiPartUpload(key);
          statistics.uploadStarted();
          return BBUploadHandle.from(ByteBuffer.wrap(
              uploadId.getBytes(Charsets.UTF_8)));
        });
  }

  @Override
  public CompletableFuture<PartHandle> putPart(UploadHandle uploadId,
      int partNumber,
      Path filePath,
      InputStream inputStream,
      long lengthInBytes)
      throws IOException {
    checkPutArguments(filePath, inputStream, partNumber, uploadId,
        lengthInBytes);
    byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String key = context.pathToKey(filePath);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    return context.submit(new CompletableFuture<>(),
        () -> {
          UploadPartRequest request = writeOperations.newUploadPartRequest(key,
              uploadIdString, partNumber, (int) lengthInBytes, inputStream,
              null, 0L);
          UploadPartResult result = writeOperations.uploadPart(request);
          statistics.partPut();
          String eTag = result.getETag();
          return BBPartHandle.from(
              ByteBuffer.wrap(
                  buildPartHandlePayload(
                      result.getPartNumber(),
                      eTag,
                      lengthInBytes)));
        });
  }

  @Override
  public CompletableFuture<PathHandle> complete(
      UploadHandle uploadHandle,
      Path filePath,
      Map<Integer, PartHandle> handleMap)
      throws IOException {
    checkPath(filePath);
    byte[] uploadIdBytes = uploadHandle.toByteArray();
    checkUploadId(uploadIdBytes);
    checkPartHandles(handleMap);
    List<Map.Entry<Integer, PartHandle>> handles =
        new ArrayList<>(handleMap.entrySet());
    handles.sort(Comparator.comparingInt(Map.Entry::getKey));
    int count = handles.size();
    String key = context.pathToKey(filePath);

    String uploadIdStr = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    ArrayList<PartETag> eTags = new ArrayList<>();
    eTags.ensureCapacity(handles.size());
    long totalLength = 0;
    // built up to identify duplicates -if the size of this set is
    // below that of the number of parts, then there's a duplicate entry.
    Set<Integer> ids = new HashSet<>(count);

    for (Map.Entry<Integer, PartHandle> handle : handles) {
      byte[] payload = handle.getValue().toByteArray();
      Triple<Integer, Long, String> result = parsePartHandlePayload(payload);
      ids.add(result.getLeft());
      totalLength += result.getMiddle();
      eTags.add(new PartETag(handle.getKey(), result.getRight()));
    }
    Preconditions.checkArgument(ids.size() == count,
        "Duplicate PartHandles");

    // retrieve/create operation state for scalability of completion.
    final BulkOperationState state = retrieveOperationState();
    long finalLen = totalLength;
    return context.submit(new CompletableFuture<>(),
        () -> {
          CompleteMultipartUploadResult result =
              writeOperations.commitUpload(
                  key,
                  uploadIdStr,
                  eTags,
                  finalLen,
                  state);

          byte[] eTag = result.getETag().getBytes(Charsets.UTF_8);
          statistics.uploadCompleted();
          return (PathHandle) () -> ByteBuffer.wrap(eTag);
        });
  }

  @Override
  public CompletableFuture<Void> abort(
      UploadHandle uploadId,
      Path filePath)
      throws IOException {
    checkPath(filePath);
    final byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    return context.submit(new CompletableFuture<>(),
        () -> {
          writeOperations.abortMultipartCommit(
              context.pathToKey(filePath),
              uploadIdString);
          statistics.uploadAborted();
          return null;
        });
  }

  /**
   * Upload all MPUs under the path.
   * @param path path to abort uploads under.
   * @return a future which eventually returns the number of entries found
   * @throws IOException submission failure
   */
  @Override
  public CompletableFuture<Integer> abortUploadsUnderPath(final Path path)
      throws IOException {
    statistics.abortUploadsUnderPathInvoked();
    return context.submit(new CompletableFuture<>(),
        () ->
            writeOperations.abortMultipartUploadsUnderPath(
                context.pathToKey(path)));
  }

  /**
   * Build the payload for marshalling.
   *
   * @param partNumber part number from response
   * @param eTag upload etag
   * @param len length
   * @return a byte array to marshall.
   * @throws IOException error writing the payload
   */
  @VisibleForTesting
  static byte[] buildPartHandlePayload(
      int partNumber,
      String eTag,
      long len)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(eTag),
        "Empty etag");
    Preconditions.checkArgument(len >= 0,
        "Invalid length");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeUTF(HEADER);
      output.writeInt(partNumber);
      output.writeLong(len);
      output.writeUTF(eTag);
    }
    return bytes.toByteArray();
  }

  /**
   * Parse the payload marshalled as a part handle.
   * @param data handle data
   * @return the length and etag
   * @throws IOException error reading the payload
   */
  @VisibleForTesting
  static Triple<Integer, Long, String> parsePartHandlePayload(byte[] data)
      throws IOException {

    try (DataInputStream input =
             new DataInputStream(new ByteArrayInputStream(data))) {
      final String header = input.readUTF();
      if (!HEADER.equals(header)) {
        throw new IOException("Wrong header string: \"" + header + "\"");
      }
      final int partNumber = input.readInt();
      final long len = input.readLong();
      final String etag = input.readUTF();
      if (len < 0) {
        throw new IOException("Negative length");
      }
      return Triple.of(partNumber, len, etag);
    }
  }

}
