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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CompletedPart;

public class TestUploadEtag {

  @Test
  public void testFromCompletedPartCRC32() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumCRC32("checksum")
        .build();
    final UploadEtag uploadEtag = UploadEtag.fromCompletedPart(completedPart);
    Assertions.assertThat(uploadEtag.getEtag())
        .describedAs("Etag mismatch")
        .isEqualTo("tag");
    Assertions.assertThat(uploadEtag.getChecksumAlgorithm())
        .describedAs("Checksum algorithm should be CRC32")
        .isEqualTo("CRC32");
    Assertions.assertThat(uploadEtag.getChecksum())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testFromCompletedPartCRC32C() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumCRC32C("checksum")
        .build();
    final UploadEtag uploadEtag = UploadEtag.fromCompletedPart(completedPart);
    Assertions.assertThat(uploadEtag.getEtag())
        .describedAs("Etag mismatch")
        .isEqualTo("tag");
    Assertions.assertThat(uploadEtag.getChecksumAlgorithm())
        .describedAs("Checksum algorithm should be CRC32C")
        .isEqualTo("CRC32C");
    Assertions.assertThat(uploadEtag.getChecksum())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testFromCompletedPartSHA1() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumSHA1("checksum")
        .build();
    final UploadEtag uploadEtag = UploadEtag.fromCompletedPart(completedPart);
    Assertions.assertThat(uploadEtag.getEtag())
        .describedAs("Etag mismatch")
        .isEqualTo("tag");
    Assertions.assertThat(uploadEtag.getChecksumAlgorithm())
        .describedAs("Checksum algorithm should be SHA1")
        .isEqualTo("SHA1");
    Assertions.assertThat(uploadEtag.getChecksum())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testFromCompletedPartSHA256() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumSHA256("checksum")
        .build();
    final UploadEtag uploadEtag = UploadEtag.fromCompletedPart(completedPart);
    Assertions.assertThat(uploadEtag.getEtag())
        .describedAs("Etag mismatch")
        .isEqualTo("tag");
    Assertions.assertThat(uploadEtag.getChecksumAlgorithm())
        .describedAs("Checksum algorithm should be SHA256")
        .isEqualTo("SHA256");
    Assertions.assertThat(uploadEtag.getChecksum())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testFromCompletedPartNoChecksum() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .build();
    final UploadEtag uploadEtag = UploadEtag.fromCompletedPart(completedPart);
    Assertions.assertThat(uploadEtag.getEtag())
        .describedAs("Etag mismatch")
        .isEqualTo("tag");
    Assertions.assertThat(uploadEtag.getChecksumAlgorithm())
        .describedAs("uploadEtag.getChecksumAlgorithm()")
        .isNull();
    Assertions.assertThat(uploadEtag.getChecksum())
        .describedAs("uploadEtag.getChecksum()")
        .isNull();
  }

  @Test
  public void testToCompletedPartCRC32() {
    final UploadEtag uploadEtag = new UploadEtag("tag", "CRC32", "checksum");
    final CompletedPart completedPart = UploadEtag.toCompletedPart(uploadEtag, 1);
    Assertions.assertThat(completedPart.checksumCRC32())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testToCompletedPartCRC32C() {
    final UploadEtag uploadEtag = new UploadEtag("tag", "CRC32C", "checksum");
    final CompletedPart completedPart = UploadEtag.toCompletedPart(uploadEtag, 1);
    Assertions.assertThat(completedPart.checksumCRC32C())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testToCompletedPartSHA1() {
    final UploadEtag uploadEtag = new UploadEtag("tag", "SHA1", "checksum");
    final CompletedPart completedPart = UploadEtag.toCompletedPart(uploadEtag, 1);
    Assertions.assertThat(completedPart.checksumSHA1())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testToCompletedPartSHA256() {
    final UploadEtag uploadEtag = new UploadEtag("tag", "SHA256", "checksum");
    final CompletedPart completedPart = UploadEtag.toCompletedPart(uploadEtag, 1);
    Assertions.assertThat(completedPart.checksumSHA256())
        .describedAs("Checksum mismatch")
        .isEqualTo("checksum");
  }

  @Test
  public void testToCompletedPartNoChecksum() {
    final UploadEtag uploadEtag = new UploadEtag("tag", null, null);
    final CompletedPart completedPart = UploadEtag.toCompletedPart(uploadEtag, 1);
    Assertions.assertThat(completedPart.checksumCRC32())
        .describedAs("completedPart.checksumCRC32()")
        .isNull();
    Assertions.assertThat(completedPart.checksumCRC32C())
        .describedAs("completedPart.checksumCRC32C()")
        .isNull();
    Assertions.assertThat(completedPart.checksumSHA1())
        .describedAs("completedPart.checksumSHA1()")
        .isNull();
    Assertions.assertThat(completedPart.checksumSHA256())
        .describedAs("completedPart.checksumSHA256()")
        .isNull();
  }
}
