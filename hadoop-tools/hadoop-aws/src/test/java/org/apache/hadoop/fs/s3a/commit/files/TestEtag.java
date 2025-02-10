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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestEtag {

  @Test
  public void testFromCompletedPartCRC32() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumCRC32("checksum")
        .build();
    final Etag etag = Etag.fromCompletedPart(completedPart);
    assertEquals("tag", etag.getEtag());
    assertEquals("CRC32", etag.getChecksumAlgorithm());
    assertEquals("checksum", etag.getChecksum());
  }

  @Test
  public void testFromCompletedPartCRC32C() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumCRC32C("checksum")
        .build();
    final Etag etag = Etag.fromCompletedPart(completedPart);
    assertEquals("tag", etag.getEtag());
    assertEquals("CRC32C", etag.getChecksumAlgorithm());
    assertEquals("checksum", etag.getChecksum());
  }

  @Test
  public void testFromCompletedPartSHA1() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumSHA1("checksum")
        .build();
    final Etag etag = Etag.fromCompletedPart(completedPart);
    assertEquals("tag", etag.getEtag());
    assertEquals("SHA1", etag.getChecksumAlgorithm());
    assertEquals("checksum", etag.getChecksum());
  }

  @Test
  public void testFromCompletedPartSHA256() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .checksumSHA256("checksum")
        .build();
    final Etag etag = Etag.fromCompletedPart(completedPart);
    assertEquals("tag", etag.getEtag());
    assertEquals("SHA256", etag.getChecksumAlgorithm());
    assertEquals("checksum", etag.getChecksum());
  }

  @Test
  public void testFromCompletedPartNoChecksum() {
    final CompletedPart completedPart = CompletedPart.builder()
        .eTag("tag")
        .build();
    final Etag etag = Etag.fromCompletedPart(completedPart);
    assertEquals("tag", etag.getEtag());
    assertNull(etag.getChecksumAlgorithm());
    assertNull(etag.getChecksum());
  }

  @Test
  public void testToCompletedPartCRC32() {
    final Etag etag = new Etag("tag", "CRC32", "checksum");
    final CompletedPart completedPart = Etag.toCompletedPart(etag, 1);
    assertEquals("checksum", completedPart.checksumCRC32());
  }

  @Test
  public void testToCompletedPartCRC32C() {
    final Etag etag = new Etag("tag", "CRC32C", "checksum");
    final CompletedPart completedPart = Etag.toCompletedPart(etag, 1);
    assertEquals("checksum", completedPart.checksumCRC32C());
  }

  @Test
  public void testToCompletedPartSHA1() {
    final Etag etag = new Etag("tag", "SHA1", "checksum");
    final CompletedPart completedPart = Etag.toCompletedPart(etag, 1);
    assertEquals("checksum", completedPart.checksumSHA1());
  }

  @Test
  public void testToCompletedPartSHA256() {
    final Etag etag = new Etag("tag", "SHA256", "checksum");
    final CompletedPart completedPart = Etag.toCompletedPart(etag, 1);
    assertEquals("checksum", completedPart.checksumSHA256());
  }

  @Test
  public void testToCompletedPartNoChecksum() {
    final Etag etag = new Etag("tag", null, null);
    final CompletedPart completedPart = Etag.toCompletedPart(etag, 1);
    assertNull(completedPart.checksumCRC32());
    assertNull(completedPart.checksumCRC32C());
    assertNull(completedPart.checksumSHA1());
    assertNull(completedPart.checksumSHA256());
  }
}
