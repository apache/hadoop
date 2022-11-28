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

import java.io.IOException;
import java.io.InputStream;

import software.amazon.awssdk.http.Abortable;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;


import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DRAIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext.EMPTY_INPUT_STREAM_STATISTICS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for stream draining.
 */
public class TestSDKStreamDrainer extends HadoopTestBase {

  public static final int BYTES = 100;

  /**
   * Aborting does as asked.
   */
  @Test
  public void testDrainerAborted() throws Throwable {
    assertAborted(drainer(BYTES, true, stream()));
  }

  /**
   * Create a stream of the default length.
   * @return a stream.
   */
  private static FakeSDKInputStream stream() {
    return new FakeSDKInputStream(BYTES);
  }

  /**
   * a normal drain; all bytes are read. No abort.
   */
  @Test
  public void testDrainerDrained() throws Throwable {
    assertBytesReadNotAborted(
        drainer(BYTES, false, stream()),
        BYTES);
  }

  /**
   * Empty streams are fine.
   */
  @Test
  public void testEmptyStream() throws Throwable {
    int size = 0;
    assertBytesReadNotAborted(
        drainer(size, false, new FakeSDKInputStream(size)),
        size);
  }

  /**
   * Single char read; just a safety check on the test stream more than
   * the production code.
   */
  @Test
  public void testSingleChar() throws Throwable {
    int size = 1;
    assertBytesReadNotAborted(
        drainer(size, false, new FakeSDKInputStream(size)),
        size);
  }

  /**
   * a read spanning multiple buffers.
   */
  @Test
  public void testMultipleBuffers() throws Throwable {
    int size = DRAIN_BUFFER_SIZE + 1;
    assertBytesReadNotAborted(
        drainer(size, false, new FakeSDKInputStream(size)),
        size);
  }

  /**
   * Read of exactly one buffer.
   */
  @Test
  public void testExactlyOneBuffer() throws Throwable {
    int size = DRAIN_BUFFER_SIZE;
    assertBytesReadNotAborted(
        drainer(size, false, new FakeSDKInputStream(size)),
        size);
  }

  /**
   * Less data than expected came back. not escalated.
   */
  @Test
  public void testStreamUnderflow() throws Throwable {
    int size = 50;
    assertBytesReadNotAborted(
        drainer(BYTES, false, new FakeSDKInputStream(size)),
        size);
  }

  /**
   * Test a drain where a read triggers an IOE; this must escalate
   * to an abort.
   */
  @Test
  public void testReadFailure() throws Throwable {
    int threshold = 50;
    SDKStreamDrainer drainer = new SDKStreamDrainer("s3://example/",
        new FakeSDKInputStream(BYTES, threshold),
        false,
        BYTES,
        EMPTY_INPUT_STREAM_STATISTICS, "test");
    intercept(IOException.class, "", () ->
        drainer.applyRaisingException());

    assertAborted(drainer);
  }

  /**
   * abort does not read(), so the exception will not surface.
   */
  @Test
  public void testReadFailureDoesNotSurfaceInAbort() throws Throwable {
    int threshold = 50;
    SDKStreamDrainer drainer = new SDKStreamDrainer("s3://example/",
        new FakeSDKInputStream(BYTES, threshold),
        true,
        BYTES,
        EMPTY_INPUT_STREAM_STATISTICS, "test");
    drainer.applyRaisingException();

    assertAborted(drainer);
  }

  /**
   * make sure the underlying stream read code works.
   */
  @Test
  public void testFakeStreamRead() throws Throwable {
    FakeSDKInputStream stream = stream();
    int count = 0;
    while (stream.read() > 0) {
      count++;
    }
    Assertions.assertThat(count)
        .describedAs("bytes read from %s", stream)
        .isEqualTo(BYTES);
  }

  /**
   * Create a drainer and invoke it, rethrowing any exception
   * which occurred during the draining.
   * @param remaining bytes remaining in the stream
   * @param shouldAbort should we abort?
   * @param in input stream.
   * @return the drainer
   * @throws Throwable something went wrong
   */
  private SDKStreamDrainer drainer(int remaining,
      boolean shouldAbort,
      FakeSDKInputStream in) throws Throwable {
    SDKStreamDrainer drainer = new SDKStreamDrainer("s3://example/",
        in,
        shouldAbort,
        remaining,
        EMPTY_INPUT_STREAM_STATISTICS, "test");
    drainer.applyRaisingException();
    return drainer;
  }


  /**
   * The draining aborted.
   * @param drainer drainer to assert on.
   * @return the drainer.
   */
  private SDKStreamDrainer assertAborted(SDKStreamDrainer drainer) {
    Assertions.assertThat(drainer)
        .matches(SDKStreamDrainer::aborted, "aborted");
    return drainer;
  }

  /**
   * The draining was not aborted.
   * @param drainer drainer to assert on.
   * @return the drainer.
   */
  private SDKStreamDrainer assertNotAborted(SDKStreamDrainer drainer) {
    Assertions.assertThat(drainer)
        .matches(d -> !d.aborted(), "is not aborted");
    return drainer;
  }

  /**
   * The draining was not aborted and {@code bytes} were read.
   * @param drainer drainer to assert on.
   * @param bytes expected byte count
   * @return the drainer.
   */
  private SDKStreamDrainer assertBytesReadNotAborted(SDKStreamDrainer drainer,
      int bytes) {
    return assertBytesRead(assertNotAborted(drainer), bytes);
  }

  /**
   * Assert {@code bytes} were read.
   * @param drainer drainer to assert on.
   * @param bytes expected byte count
   * @return the drainer.
   */
  private static SDKStreamDrainer assertBytesRead(final SDKStreamDrainer drainer,
      final int bytes) {
    Assertions.assertThat(drainer)
        .describedAs("bytes read by %s", drainer)
        .extracting(SDKStreamDrainer::getDrained)
        .isEqualTo(bytes);
    return drainer;
  }


  /**
   * Fake stream; generates data dynamically.
   * Only overrides the methods used in stream draining.
   */
  private static final class FakeSDKInputStream extends InputStream
      implements Abortable {

    private final int capacity;

    private final int readToRaiseIOE;

    private int bytesRead;

    private boolean closed;

    private boolean aborted;

    /**
     * read up to the capacity; optionally trigger an IOE.
     * @param capacity total capacity.
     * @param readToRaiseIOE position to raise an IOE, or -1
     */
    private FakeSDKInputStream(final int capacity, final int readToRaiseIOE) {
      this.capacity = capacity;
      this.readToRaiseIOE = readToRaiseIOE;
    }

    /**
     * read up to the capacity.
     * @param capacity total capacity.
     */
    private FakeSDKInputStream(final int capacity) {
      this(capacity, -1);
    }

    @Override
    public void abort() {
      aborted = true;
    }

    @Override
    public int read() throws IOException {
      if (bytesRead >= capacity) {
        // EOF
        return -1;
      }
      bytesRead++;
      if (readToRaiseIOE > 0 && bytesRead >= readToRaiseIOE) {
        throw new IOException("IOE triggered on reading byte " + bytesRead);
      }
      return (int) '0' + (bytesRead % 10);
    }

    @Override
    public int read(final byte[] bytes, final int off, final int len)
        throws IOException {
      int count = 0;

      try {
        while (count < len) {
          int r = read();
          if (r < 0) {
            break;
          }
          bytes[off + count] = (byte) r;
          count++;
        }
      } catch (IOException e) {
        if (count == 0) {
          // first byte
          throw e;
        }
        // otherwise break loop
      }
      return count;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public String toString() {
      return "FakeSDKInputStream{" +
          "capacity=" + capacity +
          ", readToRaiseIOE=" + readToRaiseIOE +
          ", bytesRead=" + bytesRead +
          ", closed=" + closed +
          ", aborted=" + aborted +
          "} " + super.toString();
    }
  }
}
