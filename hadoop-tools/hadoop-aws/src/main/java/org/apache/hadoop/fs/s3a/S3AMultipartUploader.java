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
package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Charsets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;
import org.apache.hadoop.hdfs.DFSUtilClient;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MultipartUploader for S3AFileSystem. This uses the S3 multipart
 * upload mechanism.
 */
public class S3AMultipartUploader extends MultipartUploader {

  private final S3AFileSystem s3a;

  public S3AMultipartUploader(FileSystem fs, Configuration conf) {
    if (!(fs instanceof S3AFileSystem)) {
      throw new IllegalArgumentException(
          "S3A MultipartUploads must use S3AFileSystem");
    }
    s3a = (S3AFileSystem) fs;
  }

  @Override
  public UploadHandle initialize(Path filePath) throws IOException {
    String key = s3a.pathToKey(filePath);
    InitiateMultipartUploadRequest request =
        new InitiateMultipartUploadRequest(s3a.getBucket(), key);
    LOG.debug("initialize request: {}", request);
    InitiateMultipartUploadResult result = s3a.initiateMultipartUpload(request);
    String uploadId = result.getUploadId();
    return BBUploadHandle.from(ByteBuffer.wrap(
        uploadId.getBytes(Charsets.UTF_8)));
  }

  @Override
  public PartHandle putPart(Path filePath, InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes) {
    String key = s3a.pathToKey(filePath);
    UploadPartRequest request = new UploadPartRequest();
    byte[] uploadIdBytes = uploadId.toByteArray();
    request.setUploadId(new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8));
    request.setInputStream(inputStream);
    request.setPartSize(lengthInBytes);
    request.setPartNumber(partNumber);
    request.setBucketName(s3a.getBucket());
    request.setKey(key);
    LOG.debug("putPart request: {}", request);
    UploadPartResult result = s3a.uploadPart(request);
    String eTag = result.getETag();
    return BBPartHandle.from(ByteBuffer.wrap(eTag.getBytes(Charsets.UTF_8)));
  }

  @Override
  public PathHandle complete(Path filePath,
      List<Pair<Integer, PartHandle>> handles, UploadHandle uploadId) {
    String key = s3a.pathToKey(filePath);
    CompleteMultipartUploadRequest request =
        new CompleteMultipartUploadRequest();
    request.setBucketName(s3a.getBucket());
    request.setKey(key);
    byte[] uploadIdBytes = uploadId.toByteArray();
    request.setUploadId(new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8));
    List<PartETag> eTags = handles
        .stream()
        .map(handle -> {
          byte[] partEtagBytes = handle.getRight().toByteArray();
          return new PartETag(handle.getLeft(),
              new String(partEtagBytes, 0, partEtagBytes.length,
                  Charsets.UTF_8));
        })
        .collect(Collectors.toList());
    request.setPartETags(eTags);
    LOG.debug("Complete request: {}", request);
    CompleteMultipartUploadResult completeMultipartUploadResult =
        s3a.getAmazonS3Client().completeMultipartUpload(request);

    byte[] eTag = DFSUtilClient.string2Bytes(
        completeMultipartUploadResult.getETag());
    return (PathHandle) () -> ByteBuffer.wrap(eTag);
  }

  @Override
  public void abort(Path filePath, UploadHandle uploadId) {
    String key = s3a.pathToKey(filePath);
    byte[] uploadIdBytes = uploadId.toByteArray();
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(s3a
        .getBucket(), key, uploadIdString);
    LOG.debug("Abort request: {}", request);
    s3a.getAmazonS3Client().abortMultipartUpload(request);
  }

  /**
   * Factory for creating MultipartUploader objects for s3a:// FileSystems.
   */
  public static class Factory extends MultipartUploaderFactory {
    @Override
    protected MultipartUploader createMultipartUploader(FileSystem fs,
        Configuration conf) {
      if (fs.getScheme().equals("s3a")) {
        return new S3AMultipartUploader(fs, conf);
      }
      return null;
    }
  }
}
