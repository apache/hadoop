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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.fs.impl.AbstractMultipartUploader;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

/**
 * MultipartUploader for S3AFileSystem. This uses the S3 multipart
 * upload mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3AMultipartUploader extends AbstractMultipartUploader {

  private final S3AMultipartUploaderBuilder builder;

  /** Header for Parts: {@value}. */

  public static final String HEADER = "S3A-part01";

  private final WriteOperationHelper writeHelper;

  private final StoreContext context;

  public S3AMultipartUploader(final S3AMultipartUploaderBuilder builder,
      final WriteOperationHelper writeHelper,
      final StoreContext context) {
    this.builder = builder;
    this.writeHelper = writeHelper;

    this.context = context;
  }

  @Override
  public CompletableFuture<UploadHandle> initialize(Path filePath)
      throws IOException {
    String key = context.pathToKey(filePath);
    return context.submit(new CompletableFuture<>(),
        () -> {
          String uploadId = writeHelper.initiateMultiPartUpload(key);
          return BBUploadHandle.from(ByteBuffer.wrap(
              uploadId.getBytes(Charsets.UTF_8)));
        });
  }

  @Override
  public CompletableFuture<PartHandle> putPart(Path filePath,
      InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes)
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
          UploadPartRequest request = writeHelper.newUploadPartRequest(key,
              uploadIdString, partNumber, (int) lengthInBytes, inputStream,
              null, 0L);
          UploadPartResult result = writeHelper.uploadPart(request);
          String eTag = result.getETag();
          return BBPartHandle.from(
              ByteBuffer.wrap(
                  buildPartHandlePayload(eTag, lengthInBytes)));
        });
  }

  @Override
  public CompletableFuture<PathHandle> complete(Path filePath,
      Map<Integer, PartHandle> handleMap,
      UploadHandle uploadId)
      throws IOException {
    byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);

    checkPartHandles(handleMap);
    List<Map.Entry<Integer, PartHandle>> handles =
        new ArrayList<>(handleMap.entrySet());
    handles.sort(Comparator.comparingInt(Map.Entry::getKey));
    String key = context.pathToKey(filePath);

    String uploadIdStr = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    ArrayList<PartETag> eTags = new ArrayList<>();
    eTags.ensureCapacity(handles.size());
    long totalLength = 0;
    for (Map.Entry<Integer, PartHandle> handle : handles) {
      byte[] payload = handle.getValue().toByteArray();
      Pair<Long, String> result = parsePartHandlePayload(payload);
      totalLength += result.getLeft();
      eTags.add(new PartETag(handle.getKey(), result.getRight()));
    }
    AtomicInteger errorCount = new AtomicInteger(0);
    long finalLen = totalLength;
    return context.submit(new CompletableFuture<>(),
        () -> {
          CompleteMultipartUploadResult result
              = writeHelper.completeMPUwithRetries(
              key, uploadIdStr, eTags, finalLen, errorCount);

          byte[] eTag = result.getETag().getBytes(Charsets.UTF_8);
          return (PathHandle) () -> ByteBuffer.wrap(eTag);
        });
  }

  @Override
  public CompletableFuture<Void> abort(Path filePath, UploadHandle uploadId)
      throws IOException {
    final byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    return context.submit(new CompletableFuture<>(),
        () -> {
          writeHelper.abortMultipartCommit(context.pathToKey(filePath),
              uploadIdString);
          return null;
        });
  }


  /**
   * Build the payload for marshalling.
   * @param eTag upload etag
   * @param len length
   * @return a byte array to marshall.
   * @throws IOException error writing the payload
   */
  @VisibleForTesting
  static byte[] buildPartHandlePayload(String eTag, long len)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(eTag),
        "Empty etag");
    Preconditions.checkArgument(len >= 0,
        "Invalid length");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeUTF(HEADER);
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
  static Pair<Long, String> parsePartHandlePayload(byte[] data)
      throws IOException {

    try (DataInputStream input =
             new DataInputStream(new ByteArrayInputStream(data))) {
      final String header = input.readUTF();
      if (!HEADER.equals(header)) {
        throw new IOException("Wrong header string: \"" + header + "\"");
      }
      final long len = input.readLong();
      final String etag = input.readUTF();
      if (len < 0) {
        throw new IOException("Negative length");
      }
      return Pair.of(len, etag);
    }
  }

}
