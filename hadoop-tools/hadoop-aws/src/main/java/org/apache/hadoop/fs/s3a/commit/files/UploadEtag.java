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

package org.apache.hadoop.fs.s3a.commit.files;

import java.io.Serializable;
import java.util.StringJoiner;

import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * Stores ETag and checksum values from {@link  CompletedPart} responses from S3.
 * These values need to be stored to be later passed to the
 * {@link software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
 * CompleteMultipartUploadRequest}
 */
public class UploadEtag implements Serializable {

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 1L;

  private String etag;
  private String checksumAlgorithm;
  private String checksum;

  public UploadEtag() {
  }

  public UploadEtag(String etag, String checksumAlgorithm, String checksum) {
    this.etag = etag;
    this.checksumAlgorithm = checksumAlgorithm;
    this.checksum = checksum;
  }

  public String getEtag() {
    return etag;
  }

  public void setEtag(String etag) {
    this.etag = etag;
  }

  public String getChecksumAlgorithm() {
    return checksumAlgorithm;
  }

  public void setChecksumAlgorithm(String checksumAlgorithm) {
    this.checksumAlgorithm = checksumAlgorithm;
  }

  public String getChecksum() {
    return checksum;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public static UploadEtag fromCompletedPart(CompletedPart completedPart) {
    UploadEtag uploadEtag = new UploadEtag();
    uploadEtag.setEtag(completedPart.eTag());
    if (completedPart.checksumCRC32() != null) {
      uploadEtag.setChecksumAlgorithm(ChecksumAlgorithm.CRC32.toString());
      uploadEtag.setChecksum(completedPart.checksumCRC32());
    }
    if (completedPart.checksumCRC32C() != null) {
      uploadEtag.setChecksumAlgorithm(ChecksumAlgorithm.CRC32_C.toString());
      uploadEtag.setChecksum(completedPart.checksumCRC32C());
    }
    if (completedPart.checksumSHA1() != null) {
      uploadEtag.setChecksumAlgorithm(ChecksumAlgorithm.SHA1.toString());
      uploadEtag.setChecksum(completedPart.checksumSHA1());
    }
    if (completedPart.checksumSHA256() != null) {
      uploadEtag.setChecksumAlgorithm(ChecksumAlgorithm.SHA256.toString());
      uploadEtag.setChecksum(completedPart.checksumSHA256());
    }
    return uploadEtag;
  }

  public static CompletedPart toCompletedPart(UploadEtag uploadEtag, int partNumber) {
    final CompletedPart.Builder builder = CompletedPart.builder()
        .partNumber(partNumber)
        .eTag(uploadEtag.etag);
    if (uploadEtag.checksumAlgorithm == null) {
      return builder.build();
    }
    final ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.fromValue(
        uploadEtag.checksumAlgorithm);
    switch (checksumAlgorithm) {
    case CRC32:
      builder.checksumCRC32(uploadEtag.checksum);
      break;
    case CRC32_C:
      builder.checksumCRC32C(uploadEtag.checksum);
      break;
    case SHA1:
      builder.checksumSHA1(uploadEtag.checksum);
      break;
    case SHA256:
      builder.checksumSHA256(uploadEtag.checksum);
      break;
    default:
      // do nothing
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", UploadEtag.class.getSimpleName() + "[", "]")
        .add("serialVersionUID='" + serialVersionUID + "'")
        .add("etag='" + etag + "'")
        .add("checksumAlgorithm='" + checksumAlgorithm + "'")
        .add("checksum='" + checksum + "'")
        .toString();
  }
}
