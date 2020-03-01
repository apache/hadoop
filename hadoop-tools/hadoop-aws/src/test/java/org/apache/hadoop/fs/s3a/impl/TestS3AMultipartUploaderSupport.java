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

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.S3AMultipartUploader.*;
import static org.apache.hadoop.fs.s3a.impl.S3AMultipartUploader.parsePartHandlePayload;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test multipart upload support methods and classes.
 */
public class TestS3AMultipartUploaderSupport extends HadoopTestBase {

  @Test
  public void testRoundTrip() throws Throwable {
    Triple<Integer, Long, String>result = roundTrip(999, "tag", 1);
    assertEquals(999, (int)result.getLeft());
    assertEquals("tag", result.getRight());
    assertEquals(1, result.getMiddle().longValue());
  }

  @Test
  public void testRoundTrip2() throws Throwable {
    long len = 1L + Integer.MAX_VALUE;
    Triple<Integer, Long, String>result =
        roundTrip(1, "11223344", len);
    assertEquals(1, (int) result.getLeft());
    assertEquals("11223344", result.getRight());
    assertEquals(len, result.getMiddle().longValue());
  }

  @Test
  public void testNoEtag() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> buildPartHandlePayload(0, "", 1));
  }

  @Test
  public void testNoLen() throws Throwable {
    intercept(IllegalArgumentException.class,
        () -> buildPartHandlePayload(0, "tag", -1));
  }

  @Test
  public void testBadPayload() throws Throwable {
    intercept(EOFException.class,
        () -> parsePartHandlePayload(new byte[0]));
  }

  @Test
  public void testBadHeader() throws Throwable {
    byte[] bytes = buildPartHandlePayload(0, "tag", 1);
    bytes[2]='f';
    intercept(IOException.class, "header",
        () -> parsePartHandlePayload(bytes));
  }

  private Triple<Integer, Long, String> roundTrip(
      int partNumber,
      String tag,
      long len) throws IOException {
    byte[] bytes = buildPartHandlePayload(partNumber, tag, len);
    return parsePartHandlePayload(bytes);
  }
}
