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

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.impl.prefetch.ExceptionAsserts;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Applies the same set of tests to both S3ACachingInputStream and S3AInMemoryInputStream.
 */
public class TestS3ARemoteInputStream extends AbstractHadoopTestBase {

  private static final int FILE_SIZE = 10;

  private final ExecutorService threadPool = Executors.newFixedThreadPool(4);

  private final ExecutorServiceFuturePool futurePool =
      new ExecutorServiceFuturePool(threadPool);

  private final S3AInputStream.InputStreamCallbacks client =
      MockS3ARemoteObject.createClient("bucket");

  @Test
  public void testArgChecks() throws Exception {
    S3AReadOpContext readContext =
        S3APrefetchFakes.createReadContext(futurePool, "key", 10, 10, 1);
    S3ObjectAttributes attrs =
        S3APrefetchFakes.createObjectAttributes("bucket", "key", 10);
    S3AInputStreamStatistics stats =
        readContext.getS3AStatisticsContext().newInputStreamStatistics();

    Configuration conf = S3ATestUtils.prepareTestConfiguration(new Configuration());
    // Should not throw.
    new S3ACachingInputStream(readContext, attrs, client, stats, conf, null);

    ExceptionAsserts.assertThrows(
        NullPointerException.class,
        () -> new S3ACachingInputStream(null, attrs, client, stats, conf, null));

    ExceptionAsserts.assertThrows(
        NullPointerException.class,
        () -> new S3ACachingInputStream(readContext, null, client, stats, conf, null));

    ExceptionAsserts.assertThrows(
        NullPointerException.class,
        () -> new S3ACachingInputStream(readContext, attrs, null, stats, conf, null));

    ExceptionAsserts.assertThrows(
        NullPointerException.class,
        () -> new S3ACachingInputStream(readContext, attrs, client, null, conf, null));
  }

  @Test
  public void testRead0SizedFile() throws Exception {
    S3ARemoteInputStream inputStream =
        S3APrefetchFakes.createS3InMemoryInputStream(futurePool, "bucket",
            "key", 0);
    testRead0SizedFileHelper(inputStream, 9);

    inputStream =
        S3APrefetchFakes.createS3CachingInputStream(futurePool, "bucket", "key",
            0, 5, 2);
    testRead0SizedFileHelper(inputStream, 5);
  }

  private void testRead0SizedFileHelper(S3ARemoteInputStream inputStream,
      int bufferSize)
      throws Exception {
    assertAvailable(0, inputStream);
    assertEquals(-1, inputStream.read());
    assertEquals(-1, inputStream.read());

    byte[] buffer = new byte[2];
    assertEquals(-1, inputStream.read(buffer));
    assertEquals(-1, inputStream.read());
  }

  @Test
  public void testRead() throws Exception {
    S3ARemoteInputStream inputStream =
        S3APrefetchFakes.createS3InMemoryInputStream(futurePool, "bucket",
            "key", FILE_SIZE);
    testReadHelper(inputStream, FILE_SIZE);

    inputStream =
        S3APrefetchFakes.createS3CachingInputStream(futurePool, "bucket", "key",
            FILE_SIZE, 5, 2);
    testReadHelper(inputStream, 5);
  }

  private void testReadHelper(S3ARemoteInputStream inputStream, int bufferSize)
      throws Exception {
    assertEquals(0, inputStream.read());
    assertAvailable(bufferSize - 1, inputStream);
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
    S3ARemoteInputStream inputStream;
    inputStream =
        S3APrefetchFakes.createS3InMemoryInputStream(futurePool, "bucket",
            "key", 9);
    testSeekHelper(inputStream, 9, 9);

    inputStream =
        S3APrefetchFakes.createS3CachingInputStream(futurePool, "bucket", "key",
            9, 5, 1);
    testSeekHelper(inputStream, 5, 9);
  }

  private void testSeekHelper(S3ARemoteInputStream inputStream,
      int bufferSize,
      int fileSize)
      throws Exception {
    assertAvailable(0, inputStream);
    assertEquals(0, inputStream.getPos());
    inputStream.seek(bufferSize);
    assertAvailable(0, inputStream);
    assertEquals(bufferSize, inputStream.getPos());
    inputStream.seek(0);
    assertAvailable(0, inputStream);

    for (int i = 0; i < fileSize; i++) {
      assertEquals(i, inputStream.read());
    }

    for (int i = 0; i < fileSize; i++) {
      inputStream.seek(i);
      for (int j = i; j < fileSize; j++) {
        assertEquals(j, inputStream.read());
      }
    }

    // Can seek to the EOF: read() will then return -1.
    inputStream.seek(fileSize);
    assertEquals(-1, inputStream.read());

    // Test invalid seeks.
    ExceptionAsserts.assertThrows(
        EOFException.class,
        FSExceptionMessages.NEGATIVE_SEEK,
        () -> inputStream.seek(-1));

    ExceptionAsserts.assertThrows(
        EOFException.class,
        FSExceptionMessages.CANNOT_SEEK_PAST_EOF,
        () -> inputStream.seek(fileSize + 1));
  }

  @Test
  public void testRandomSeek() throws Exception {
    S3ARemoteInputStream inputStream;
    inputStream =
        S3APrefetchFakes.createS3InMemoryInputStream(futurePool, "bucket",
            "key", 9);
    testRandomSeekHelper(inputStream, 9, 9);

    inputStream =
        S3APrefetchFakes.createS3CachingInputStream(futurePool, "bucket", "key",
            9, 5, 1);
    testRandomSeekHelper(inputStream, 5, 9);
  }

  private void testRandomSeekHelper(S3ARemoteInputStream inputStream,
      int bufferSize,
      int fileSize)
      throws Exception {
    assertEquals(0, inputStream.getPos());
    inputStream.seek(7);
    assertEquals(7, inputStream.getPos());
    inputStream.seek(0);

    assertAvailable(0, inputStream);
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
    S3ARemoteInputStream inputStream =
        S3APrefetchFakes.createS3InMemoryInputStream(futurePool, "bucket",
            "key", 9);
    testCloseHelper(inputStream, 9);

    inputStream =
        S3APrefetchFakes.createS3CachingInputStream(futurePool, "bucket", "key",
            9, 5, 3);
    testCloseHelper(inputStream, 5);
  }

  private void testCloseHelper(S3ARemoteInputStream inputStream, int bufferSize)
      throws Exception {
    assertAvailable(0, inputStream);
    assertEquals(0, inputStream.read());
    assertEquals(1, inputStream.read());
    assertAvailable(bufferSize - 2, inputStream);

    inputStream.close();

    ExceptionAsserts.assertThrows(
        IOException.class,
        FSExceptionMessages.STREAM_IS_CLOSED,
        () -> inputStream.available());

    ExceptionAsserts.assertThrows(
        IOException.class,
        FSExceptionMessages.STREAM_IS_CLOSED,
        () -> inputStream.read());

    byte[] buffer = new byte[10];
    ExceptionAsserts.assertThrows(
        IOException.class,
        FSExceptionMessages.STREAM_IS_CLOSED,
        () -> inputStream.read(buffer));

    // Verify a second close() does not throw.
    inputStream.close();
  }

  private static void assertAvailable(int expected, InputStream inputStream)
      throws IOException {
    assertThat(inputStream.available())
        .describedAs("Check available bytes on stream %s", inputStream)
        .isEqualTo(expected);
  }
}
