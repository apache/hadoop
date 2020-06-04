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

import java.io.EOFException;
import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.S3AMultipartUploader.PartHandlePayload;
import static org.apache.hadoop.fs.s3a.impl.S3AMultipartUploader.buildPartHandlePayload;
import static org.apache.hadoop.fs.s3a.impl.S3AMultipartUploader.parsePartHandlePayload;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test of multipart upload support methods and classes.
 */
public class TestS3AMultipartUploaderSupport extends HadoopTestBase {

  public static final String PATH = "s3a://bucket/path";

  public static final String UPLOAD = "01";

  @Test
  public void testRoundTrip() throws Throwable {
    PartHandlePayload result = roundTrip(999, "tag", 1);
    assertEquals(PATH, result.getPath());
    assertEquals(UPLOAD, result.getUploadId());
    assertEquals(999, result.getPartNumber());
    assertEquals("tag", result.getEtag());
    assertEquals(1, result.getLen());
  }

  @Test
  public void testRoundTrip2() throws Throwable {
    long len = 1L + Integer.MAX_VALUE;
    PartHandlePayload result =
        roundTrip(1, "11223344", len);
    assertEquals(1, result.getPartNumber());
    assertEquals("11223344", result.getEtag());
    assertEquals(len, result.getLen());
  }

  @Test
  public void testNoEtag() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> buildPartHandlePayload(PATH, UPLOAD,
            0, "", 1));
  }

  @Test
  public void testNoLen() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> buildPartHandlePayload(PATH, UPLOAD, 0, "tag", -1));
  }

  @Test
  public void testBadPayload() throws Throwable {
    intercept(EOFException.class,
        () -> parsePartHandlePayload(new byte[0]));
  }

  @Test
  public void testBadHeader() throws Throwable {
    byte[] bytes = buildPartHandlePayload(PATH, UPLOAD, 0, "tag", 1);
    bytes[2] = 'f';
    intercept(IOException.class, "header",
        () -> parsePartHandlePayload(bytes));
  }

  private PartHandlePayload roundTrip(
      int partNumber,
      String tag,
      long len) throws IOException {
    byte[] bytes = buildPartHandlePayload(PATH, UPLOAD, partNumber, tag, len);
    return parsePartHandlePayload(bytes);
  }
}
