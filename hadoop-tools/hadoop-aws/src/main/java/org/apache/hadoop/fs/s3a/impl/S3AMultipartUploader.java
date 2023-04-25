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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.impl.AbstractMultipartUploader;
import org.apache.hadoop.fs.s3a.WriteOperations;
import org.apache.hadoop.fs.s3a.statistics.S3AMultipartUploaderStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.s3a.Statistic.MULTIPART_UPLOAD_COMPLETED;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_INITIATED;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfCallable;

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
    super(context.makeQualified(builder.getPath()));
    this.builder = builder;
    this.writeOperations = writeOperations;
    this.context = context;
    this.statistics = Objects.requireNonNull(statistics);
  }

  @Override
  public IOStatistics getIOStatistics() {
    return statistics.getIOStatistics();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "S3AMultipartUploader{");
    sb.append("base=").append(getBasePath());
    sb.append("; statistics=").append(
        ioStatisticsToString(statistics.getIOStatistics()));
    sb.append('}');
    return sb.toString();
  }

  @Override
  public CompletableFuture<UploadHandle> startUpload(
      final Path filePath)
      throws IOException {
    Path dest = context.makeQualified(filePath);
    checkPath(dest);
    String key = context.pathToKey(dest);
    return context.submit(new CompletableFuture<>(),
        trackDurationOfCallable(statistics, OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(), () -> {
          String uploadId = writeOperations.initiateMultiPartUpload(key,
              PutObjectOptions.keepingDirs());
          statistics.uploadStarted();
          return BBUploadHandle.from(ByteBuffer.wrap(
              uploadId.getBytes(Charsets.UTF_8)));
        }));
  }

  @Override
  public CompletableFuture<PartHandle> putPart(
      final UploadHandle uploadId,
      final int partNumber,
      final Path filePath,
      final InputStream inputStream,
      final long lengthInBytes)
      throws IOException {
    Path dest = context.makeQualified(filePath);
    checkPutArguments(dest, inputStream, partNumber, uploadId,
        lengthInBytes);
    byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String key = context.pathToKey(dest);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    return context.submit(new CompletableFuture<>(),
        () -> {
          UploadPartRequest request = writeOperations.newUploadPartRequest(key,
              uploadIdString, partNumber, (int) lengthInBytes, inputStream,
              null, 0L);
          UploadPartResult result = writeOperations.uploadPart(request, statistics);
          statistics.partPut(lengthInBytes);
          String eTag = result.getETag();
          return BBPartHandle.from(
              ByteBuffer.wrap(
                  buildPartHandlePayload(
                      filePath.toUri().toString(),
                      uploadIdString,
                      result.getPartNumber(),
                      eTag,
                      lengthInBytes)));
        });
  }

  @Override
  public CompletableFuture<PathHandle> complete(
      final UploadHandle uploadHandle,
      final Path filePath,
      final Map<Integer, PartHandle> handleMap)
      throws IOException {
    Path dest = context.makeQualified(filePath);
    checkPath(dest);
    byte[] uploadIdBytes = uploadHandle.toByteArray();
    checkUploadId(uploadIdBytes);
    checkPartHandles(handleMap);
    List<Map.Entry<Integer, PartHandle>> handles =
        new ArrayList<>(handleMap.entrySet());
    handles.sort(Comparator.comparingInt(Map.Entry::getKey));
    int count = handles.size();
    String key = context.pathToKey(dest);

    String uploadIdStr = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    ArrayList<PartETag> eTags = new ArrayList<>();
    eTags.ensureCapacity(handles.size());
    long totalLength = 0;
    // built up to identify duplicates -if the size of this set is
    // below that of the number of parts, then there's a duplicate entry.
    Set<Integer> ids = new HashSet<>(count);

    for (Map.Entry<Integer, PartHandle> handle : handles) {
      PartHandlePayload payload = parsePartHandlePayload(
          handle.getValue().toByteArray());
      payload.validate(uploadIdStr, filePath);
      ids.add(payload.getPartNumber());
      totalLength += payload.getLen();
      eTags.add(new PartETag(handle.getKey(), payload.getEtag()));
    }
    Preconditions.checkArgument(ids.size() == count,
        "Duplicate PartHandles");

    // retrieve/create operation state for scalability of completion.
    long finalLen = totalLength;
    return context.submit(new CompletableFuture<>(),
        trackDurationOfCallable(statistics, MULTIPART_UPLOAD_COMPLETED.getSymbol(), () -> {
          CompleteMultipartUploadResult result =
              writeOperations.commitUpload(
                  key,
                  uploadIdStr,
                  eTags,
                  finalLen
              );

          byte[] eTag = result.getETag().getBytes(Charsets.UTF_8);
          statistics.uploadCompleted();
          return (PathHandle) () -> ByteBuffer.wrap(eTag);
        }));
  }

  @Override
  public CompletableFuture<Void> abort(
      final UploadHandle uploadId,
      final Path filePath)
      throws IOException {
    Path dest = context.makeQualified(filePath);
    checkPath(dest);
    final byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    return context.submit(new CompletableFuture<>(),
        () -> {
          writeOperations.abortMultipartCommit(
              context.pathToKey(dest),
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
   * @param etag upload etag
   * @param len length
   * @return a byte array to marshall.
   * @throws IOException error writing the payload
   */
  @VisibleForTesting
  static byte[] buildPartHandlePayload(
      final String path,
      final String uploadId,
      final int partNumber,
      final String etag,
      final long len)
      throws IOException {

    return new PartHandlePayload(path, uploadId, partNumber, len, etag)
        .toBytes();
  }

  /**
   * Parse the payload marshalled as a part handle.
   * @param data handle data
   * @return the length and etag
   * @throws IOException error reading the payload
   */
  @VisibleForTesting
  static PartHandlePayload parsePartHandlePayload(
      final byte[] data)
      throws IOException {

    try (DataInputStream input =
             new DataInputStream(new ByteArrayInputStream(data))) {
      final String header = input.readUTF();
      if (!HEADER.equals(header)) {
        throw new IOException("Wrong header string: \"" + header + "\"");
      }
      final String path = input.readUTF();
      final String uploadId = input.readUTF();
      final int partNumber = input.readInt();
      final long len = input.readLong();
      final String etag = input.readUTF();
      if (len < 0) {
        throw new IOException("Negative length");
      }
      return new PartHandlePayload(path, uploadId, partNumber, len, etag);
    }
  }

  /**
   * Payload of a part handle; serializes
   * the fields using DataInputStream and DataOutputStream.
   */
  @VisibleForTesting
  static final class PartHandlePayload {

    private final String path;

    private final String uploadId;

    private final int partNumber;

    private final long len;

    private final String etag;

    private PartHandlePayload(
        final String path,
        final String uploadId,
        final int partNumber,
        final long len,
        final String etag) {
      Preconditions.checkArgument(StringUtils.isNotEmpty(etag),
          "Empty etag");
      Preconditions.checkArgument(StringUtils.isNotEmpty(path),
          "Empty path");
      Preconditions.checkArgument(StringUtils.isNotEmpty(uploadId),
          "Empty uploadId");
      Preconditions.checkArgument(len >= 0,
          "Invalid length");

      this.path = path;
      this.uploadId = uploadId;
      this.partNumber = partNumber;
      this.len = len;
      this.etag = etag;
    }

    public String getPath() {
      return path;
    }

    public int getPartNumber() {
      return partNumber;
    }

    public long getLen() {
      return len;
    }

    public String getEtag() {
      return etag;
    }

    public String getUploadId() {
      return uploadId;
    }

    public byte[] toBytes()
        throws IOException {
      Preconditions.checkArgument(StringUtils.isNotEmpty(etag),
          "Empty etag");
      Preconditions.checkArgument(len >= 0,
          "Invalid length");

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream output = new DataOutputStream(bytes)) {
        output.writeUTF(HEADER);
        output.writeUTF(path);
        output.writeUTF(uploadId);
        output.writeInt(partNumber);
        output.writeLong(len);
        output.writeUTF(etag);
      }
      return bytes.toByteArray();
    }

    public void validate(String uploadIdStr, Path filePath)
        throws PathIOException {
      String destUri = filePath.toUri().toString();
      if (!destUri.equals(path)) {
        throw new PathIOException(destUri,
            "Multipart part path mismatch: " + path);
      }
      if (!uploadIdStr.equals(uploadId)) {
        throw new PathIOException(destUri,
            "Multipart part ID mismatch: " + uploadId);
      }
    }
  }


}
