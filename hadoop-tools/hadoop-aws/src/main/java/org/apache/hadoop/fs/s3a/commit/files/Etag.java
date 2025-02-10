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

import java.util.StringJoiner;

import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * Stores ETag and checksum values from {@link  CompletedPart} responses from S3.
 * These values need to be stored to be later passed to the
 * {@link software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
 * CompleteMultipartUploadRequest}
 */
public class Etag {
  private String etag;
  private String checksumAlgorithm;
  private String checksum;

  public Etag() {
  }

  public Etag(String etag, String checksumAlgorithm, String checksum) {
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

  public static Etag fromCompletedPart(CompletedPart completedPart) {
    Etag etag = new Etag();
    etag.setEtag(completedPart.eTag());
    if (completedPart.checksumCRC32() != null) {
      etag.setChecksumAlgorithm(ChecksumAlgorithm.CRC32.toString());
      etag.setChecksum(completedPart.checksumCRC32());
    }
    if (completedPart.checksumCRC32C() != null) {
      etag.setChecksumAlgorithm(ChecksumAlgorithm.CRC32_C.toString());
      etag.setChecksum(completedPart.checksumCRC32C());
    }
    if (completedPart.checksumSHA1() != null) {
      etag.setChecksumAlgorithm(ChecksumAlgorithm.SHA1.toString());
      etag.setChecksum(completedPart.checksumSHA1());
    }
    if (completedPart.checksumSHA256() != null) {
      etag.setChecksumAlgorithm(ChecksumAlgorithm.SHA256.toString());
      etag.setChecksum(completedPart.checksumSHA256());
    }
    return etag;
  }

  public static CompletedPart toCompletedPart(Etag etag, int partNumber) {
    final CompletedPart.Builder builder = CompletedPart.builder()
        .partNumber(partNumber)
        .eTag(etag.etag);
    if (etag.checksumAlgorithm == null) {
      return builder.build();
    }
    final ChecksumAlgorithm checksumAlgorithm = ChecksumAlgorithm.fromValue(etag.checksumAlgorithm);
    switch (checksumAlgorithm) {
    case CRC32:
      builder.checksumCRC32(etag.checksum);
      break;
    case CRC32_C:
      builder.checksumCRC32C(etag.checksum);
      break;
    case SHA1:
      builder.checksumSHA1(etag.checksum);
      break;
    case SHA256:
      builder.checksumSHA256(etag.checksum);
      break;
    default:
      // do nothing
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Etag.class.getSimpleName() + "[", "]")
        .add("etag='" + etag + "'")
        .add("checksumAlgorithm='" + checksumAlgorithm + "'")
        .add("checksum='" + checksum + "'")
        .toString();
  }
}
