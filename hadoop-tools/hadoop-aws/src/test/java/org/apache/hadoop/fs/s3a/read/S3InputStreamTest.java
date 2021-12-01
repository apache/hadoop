/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import static org.junit.Assert.*;

import com.amazonaws.services.s3.AmazonS3;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Applies the same set of tests to both S3CachingInputStream and S3InMemoryInputStream.
 */
public class S3InputStreamTest {

  private static final int FILE_SIZE = 10;

  private final ExecutorService threadPool = Executors.newFixedThreadPool(4);
  private final FuturePool futurePool = new ExecutorServiceFuturePool(threadPool);
  private final AmazonS3 client = TestS3File.createClient("bucket");

  @Test
  public void testRead0SizedFile() throws Exception {
    S3InputStream inputStream =
        new Fakes.TestS3InMemoryInputStream(futurePool, "bucket", "key", 0, client);
    testRead0SizedFileHelper(inputStream, 9);

    inputStream = new Fakes.TestS3CachingInputStream(futurePool, 5, 2, "bucket", "key", 0, client);
    testRead0SizedFileHelper(inputStream, 5);
  }

  private void testRead0SizedFileHelper(S3InputStream inputStream, int bufferSize)
      throws Exception {
    assertEquals(0, inputStream.available());
    assertEquals(-1, inputStream.read());
    assertEquals(-1, inputStream.read());

    byte[] buffer = new byte[2];
    assertEquals(-1, inputStream.read(buffer));
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void testRead() throws Exception {
    S3InputStream inputStream =
        new Fakes.TestS3InMemoryInputStream(futurePool, "bucket", "key", FILE_SIZE, client);
    testReadHelper(inputStream, FILE_SIZE);

    inputStream =
        new Fakes.TestS3CachingInputStream(futurePool, 5, 2, "bucket", "key", FILE_SIZE, client);
    testReadHelper(inputStream, 5);
  }

  private void testReadHelper(S3InputStream inputStream, int bufferSize) throws Exception {
    assertEquals(bufferSize, inputStream.available());
    assertEquals(0, inputStream.read());
    assertEquals(1, inputStream.read());

    byte[] buffer = new byte[2];
    assertEquals(2, inputStream.read(buffer));
    assertEquals(2, buffer[0]);
    assertEquals(3, buffer[1]);

    assertEquals(4, inputStream.read());

    buffer = new byte[10];
    int curPos = (int) inputStream.getPos();
    int expectedRemainingBytes = (int) (FILE_SIZE - curPos);
    int readStartOffset = 2;
    assertEquals(
        expectedRemainingBytes,
        inputStream.read(buffer, readStartOffset, expectedRemainingBytes));

    for (int i = 0; i < expectedRemainingBytes; i++) {
      assertEquals(curPos + i, buffer[readStartOffset + i]);
    }

    assertEquals(-1, inputStream.read());
    Thread.sleep(100);
    assertEquals(-1, inputStream.read());
    assertEquals(-1, inputStream.read());
    assertEquals(-1, inputStream.read(buffer));
    assertEquals(-1, inputStream.read(buffer, 1, 3));
  }

  @Test
  public void testSeek() throws Exception {
    S3InputStream inputStream;
    inputStream = new Fakes.TestS3InMemoryInputStream(futurePool, "bucket", "key", 9, client);
    testSeekHelper(inputStream, 9, 9);

    inputStream = new Fakes.TestS3CachingInputStream(futurePool, 5, 1, "bucket", "key", 9, client);
    testSeekHelper(inputStream, 5, 9);
  }

  private void testSeekHelper(S3InputStream inputStream, int bufferSize, int fileSize)
      throws Exception {
    assertEquals(0, inputStream.getPos());
    inputStream.seek(7);
    assertEquals(7, inputStream.getPos());
    inputStream.seek(0);

    assertEquals(bufferSize, inputStream.available());
    for (int i = 0; i < fileSize; i++) {
      assertEquals(i, inputStream.read());
    }

    for (int i = 0; i < fileSize; i++) {
      inputStream.seek(i);
      for (int j = i; j < fileSize; j++) {
        assertEquals(j, inputStream.read());
      }
    }
  }

  @Test
  public void testRandomSeek() throws Exception {
    S3InputStream inputStream;
    inputStream = new Fakes.TestS3InMemoryInputStream(futurePool, "bucket", "key", 9, client);
    testRandomSeekHelper(inputStream, 9, 9);

    inputStream = new Fakes.TestS3CachingInputStream(futurePool, 5, 1, "bucket", "key", 9, client);
    testRandomSeekHelper(inputStream, 5, 9);
  }

  private void testRandomSeekHelper(S3InputStream inputStream, int bufferSize, int fileSize)
      throws Exception {
    assertEquals(0, inputStream.getPos());
    inputStream.seek(7);
    assertEquals(7, inputStream.getPos());
    inputStream.seek(0);

    assertEquals(bufferSize, inputStream.available());
    for (int i = 0; i < fileSize; i++) {
      assertEquals(i, inputStream.read());
    }

    for (int i = 0; i < fileSize; i++) {
      inputStream.seek(i);
      for (int j = i; j < fileSize; j++) {
        assertEquals(j, inputStream.read());
      }

      int seekFromEndPos = fileSize - i - 1;
      inputStream.seek(seekFromEndPos);
      for (int j = seekFromEndPos; j < fileSize; j++) {
        assertEquals(j, inputStream.read());
      }
    }
  }

  @Test
  public void testClose() throws Exception {
    S3InputStream inputStream =
        new Fakes.TestS3InMemoryInputStream(futurePool, "bucket", "key", 9, client);
    testCloseHelper(inputStream, 9);

    inputStream =
        new Fakes.TestS3CachingInputStream(futurePool, 5, 3, "bucket", "key", 9, client);
    testCloseHelper(inputStream, 5);
  }

  private void testCloseHelper(S3InputStream inputStream, int bufferSize) throws Exception {
    assertEquals(bufferSize, inputStream.available());
    assertEquals(0, inputStream.read());
    assertEquals(1, inputStream.read());

    inputStream.close();

    assertEquals(0, inputStream.available());
    assertEquals(-1, inputStream.read());

    byte[] buffer = new byte[10];
    assertEquals(-1, inputStream.read(buffer));

    // Verify a second close() does not throw.
    inputStream.close();
  }
}
