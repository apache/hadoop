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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static org.apache.hadoop.fs.azurebfs.services.ReadBufferManager.NUM_BUFFERS;
import static org.apache.hadoop.fs.azurebfs.services.ReadBufferManager.getBufferManager;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_ACTIVE_PREFETCH_OPERATIONS;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BLOCKS_DISCARDED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BLOCKS_EVICTED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BLOCKS_USED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BYTES_DISCARDED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BYTES_USED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_OPERATIONS;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for ReadBufferManager; uses stub ReadBufferStreamOperations
 * to simulate failures, count statistics etc.
 */
public class TestReadBufferManager extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestReadBufferManager.class);

  private ReadBufferManager bufferManager;

  @Before
  public void setup() throws Exception {
    bufferManager = getBufferManager();
  }

  /**
   * doneReading of a not in-progress ReadBuffer is an error.
   */
  @Test
  public void testBufferNotInProgressList() throws Throwable {
    ReadBuffer buffer = readBuffer(new StubReadBufferOperations());
    intercept(IllegalStateException.class, () ->
        bufferManager.doneReading(buffer, ReadBufferStatus.READ_FAILED, 0));
  }

  /**
   * Create a read buffer.
   * @param operations callbacks to use
   * @return a buffer bonded to the operations, and with a countdown latch.
   */
  private static ReadBuffer readBuffer(final ReadBufferStreamOperations operations) {
    final ReadBuffer buffer = new ReadBuffer();
    buffer.setStream(operations);
    buffer.setLatch(new CountDownLatch(1));
    return buffer;
  }

  /**
   * zero byte results are not considered successes, but not failures either.
   */
  @Test
  public void testZeroByteRead() throws Throwable {
    final StubReadBufferOperations operations = new StubReadBufferOperations();
    operations.setBytesToRead(0);

    ReadBuffer buffer = prefetch(operations, 0);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.AVAILABLE, "buffer is available")
        .matches(b -> !b.hasIndexedBuffer(), "no indexed buffer")
        .matches(b -> b.getLength() == 0, "buffer is empty");

    // even though it succeeded, it didn't return any data, so
    // not placed on the completed list.
    assertNotInCompletedList(buffer);
  }

  /**
   * Returning a negative number is the same as a zero byte response.
   */
  @Test
  public void testNegativeByteRead() throws Throwable {
    final StubReadBufferOperations operations = new StubReadBufferOperations();
    operations.setBytesToRead(-1);
    ReadBuffer buffer = prefetch(operations, 0);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.AVAILABLE, "buffer is available")
        .matches(b -> !b.hasIndexedBuffer(), "no indexed buffer")
        .matches(b -> b.getLength() == 0, "buffer is empty");

    // even though it succeeded, it didn't return any data, so
    // not placed on the completed list.
    assertNotInCompletedList(buffer);
  }

  /**
   * Queue a prefetch against the passed in stream operations, then block
   * awaiting its actual execution.
   * @param operations operations to use
   * @param offset offset of the read.
   * @return the completed buffer.
   * @throws InterruptedException await was interrupted
   */
  private ReadBuffer prefetch(final ReadBufferStreamOperations operations, final int offset)
      throws InterruptedException {
    return awaitDone(bufferManager.queueReadAhead(operations, offset, 1, null));
  }

  /**
   * read raises an IOE; must be caught, saved without wrapping to the ReadBuffer,
   * which is declared {@code READ_FAILED}.
   * The buffer is freed but it is still placed in the completed list.
   * Evictions don't find it (no buffer to return) but will silently
   * evict() it when it is out of date.
   */
  @Test
  public void testIOEInRead() throws Throwable {

    final StubReadBufferOperations operations = new StubReadBufferOperations();
    final EOFException ioe = new EOFException("eof");
    operations.exceptionToRaise = ioe;
    ReadBuffer buffer = prefetch(operations, 0);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.READ_FAILED, "buffer is failed")
        .matches(b -> !b.hasIndexedBuffer(), "no indexed buffer")
        .extracting(ReadBuffer::getErrException)
        .isEqualTo(ioe);

    // it is on the completed list for followup reads
    assertInCompletedList(buffer);

    // evict doesn't find it, as there is no useful buffer
    Assertions.assertThat(bufferManager.callTryEvict())
        .describedAs("evict() should not find failed buffer")
        .isFalse();
    assertInCompletedList(buffer);

    // set the timestamp to be some time ago
    buffer.setTimeStamp(1000);

    // evict still doesn't find it
    Assertions.assertThat(bufferManager.callTryEvict())
        .describedAs("evict() should not find failed buffer")
        .isFalse();

    // but now it was evicted due to its age
    assertNotInCompletedList(buffer);
    operations.assertCounter(STREAM_READ_PREFETCH_BLOCKS_EVICTED)
        .isEqualTo(1);
  }

  /**
   * Stream is closed before the read; this should trigger failure before
   * {@code readRemote()} is invoked.
   * The stream state change does not invoke
   * {@link ReadBufferManager#purgeBuffersForStream(ReadBufferStreamOperations)};
   * success relies on ReadBufferWorker.
   */
  @Test
  public void testClosedBeforeRead() throws Throwable {

    final StubReadBufferOperations operations = new StubReadBufferOperations();
    operations.setClosed(true);
    // this exception won't get raised because the read was cancelled first
    operations.exceptionToRaise = new EOFException("eof");

    ReadBuffer buffer = prefetch(operations, 0);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.READ_FAILED, "buffer is failed")
        .matches(b -> !b.hasIndexedBuffer(), "no indexed buffer")
        .extracting(ReadBuffer::getErrException)
        .isNull();

    // its on the completed list for followup reads
    assertNotInCompletedList(buffer);
  }

  /**
   * Stream is closed during the read; this should trigger failure.
   * The stream state change does not invoke
   * {@link ReadBufferManager#purgeBuffersForStream(ReadBufferStreamOperations)};
   * success relies on ReadBufferWorker.
   */
  @Test
  public void testClosedDuringRead() throws Throwable {

    //  have a different handler for the read call.
    final StubReadBufferOperations operations =
        new StubReadBufferOperations((self) -> {
          self.setClosed(true);
          return 1;
        });

    ReadBuffer buffer = prefetch(operations, 0);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.READ_FAILED, "buffer is failed")
        .matches(b -> !b.hasIndexedBuffer(), "no indexed buffer")
        .extracting(ReadBuffer::getErrException)
        .isNull();

    // its on the completed list for followup reads
    assertNotInCompletedList(buffer);
    operations.assertCounter(STREAM_READ_PREFETCH_BLOCKS_DISCARDED)
        .isEqualTo(1);
  }

  /**
   * When a stream is closed, all entries in the completed list are closed.
   */
  @Test
  public void testStreamCloseEvictsCompleted() throws Throwable {
    final StubReadBufferOperations operations = new StubReadBufferOperations();
    operations.bytesToRead = 1;
    // two successes
    ReadBuffer buffer1 = prefetch(operations, 0);
    ReadBuffer buffer2 = prefetch(operations, 1000);

    // final read is a failure
    operations.exceptionToRaise = new FileNotFoundException("/fnfe");
    ReadBuffer failed = prefetch(operations, 2000);

    List<ReadBuffer> buffers = Arrays.asList(buffer1, buffer2, failed);

    // all buffers must be in the completed list
    buffers.forEach(TestReadBufferManager::assertInCompletedList);

    // xor for an exclusivity assertion.
    // the read failed xor it succeeded with an indexed buffer.
    Assertions.assertThat(buffers)
        .allMatch(b ->
            b.getStatus() == ReadBufferStatus.READ_FAILED
            ^ b.hasIndexedBuffer(),
            "either was failure or it has an indexed buffer but not both");

    // now state that buffer 1 was read
    buffer1.dataConsumedByStream(0, 1);
    Assertions.assertThat(buffer1)
        .matches(ReadBuffer::isAnyByteConsumed, "any byte consumed")
        .matches(ReadBuffer::isFirstByteConsumed, "first byte consumed")
        .matches(ReadBuffer::isLastByteConsumed, "last byte consumed");
    operations.assertCounter(STREAM_READ_PREFETCH_BLOCKS_USED)
        .isEqualTo(1);

    operations.assertCounter(STREAM_READ_PREFETCH_BYTES_USED)
        .isEqualTo(1);

    // now tell buffer manager of the stream closure
    operations.setClosed(true);
    bufferManager.purgeBuffersForStream(operations);

    // now the buffers are out the list
    buffers.forEach(TestReadBufferManager::assertNotInCompletedList);
    Assertions.assertThat(buffers)
        .allMatch(b -> !b.hasIndexedBuffer(), "no indexed buffer");

    // all blocks declared evicted
    operations.assertCounter(STREAM_READ_PREFETCH_BLOCKS_EVICTED)
        .isEqualTo(3);
    // only one buffer was evicted unused; the error buffer
    // doesn't get included, while the first buffer was
    // used.
    operations.assertCounter(STREAM_READ_PREFETCH_BLOCKS_DISCARDED)
        .isEqualTo(1);
  }

  /**
   * Even if (somehow) the closed stream makes it to the completed list,
   * if it succeeded it will be found and returned.
   */
  @Test
  public void testEvictFindsClosedStreams() throws Throwable {
    final StubReadBufferOperations operations = new StubReadBufferOperations();
    operations.bytesToRead = 1;
    // two successes
    ReadBuffer buffer1 = prefetch(operations, 0);

    // final read is a failure
    operations.exceptionToRaise = new FileNotFoundException("/fnfe");
    ReadBuffer failed = prefetch(operations, 2000);

    List<ReadBuffer> buffers = Arrays.asList(buffer1, failed);

    // all buffers must be in the completed list
    buffers.forEach(TestReadBufferManager::assertInCompletedList);

    // xor for an exclusivity assertion.
    // the read failed xor it succeeded with an indexed buffer.
    Assertions.assertThat(buffers)
        .allMatch(b ->
            b.getStatus() == ReadBufferStatus.READ_FAILED
            ^ b.hasIndexedBuffer(),
            "either was failure or it has an indexed buffer but not both");

    // now state that buffer 1 was read
    buffer1.dataConsumedByStream(0, 1);

    // now close the stream without telling the buffer manager
    operations.setClosed(true);

    // evict finds the buffer with data
    Assertions.assertThat(bufferManager.callTryEvict())
        .describedAs("evict() should return closed buffer with data")
        .isTrue();

    // but now it was evicted due to its age
    assertNotInCompletedList(buffer1);
    assertInCompletedList(failed);

    // second still doesn't find the failed buffer, as it has no data
    Assertions.assertThat(bufferManager.callTryEvict())
        .describedAs("evict() should not find failed buffer")
        .isFalse();

    // but it was quietly evicted due to its state
    assertNotInCompletedList(failed);

  }

  /**
   * HADOOP-18521: if we remove ownership of a buffer during the remote read,
   * things blow up.
   */
  @Test
  public void testOwnershipChangedDuringRead() throws Throwable {

    final Object blockUntilReadStarted = new Object();
    // this stores the reference and is the object waited on to
    // block read operation until buffer index is patched.

    final AtomicReference<ReadBuffer> bufferRef = new AtomicReference<>();

    //  handler blocks for buffer reference update.
    final StubReadBufferOperations operations =
        new StubReadBufferOperations((self) -> {
          synchronized (blockUntilReadStarted) {
            blockUntilReadStarted.notify();
          }
          synchronized (bufferRef) {
            if (bufferRef.get() == null) {
              try {
                bufferRef.wait();
              } catch (InterruptedException e) {
                throw new InterruptedIOException("interrupted");
              }
            }
          }
          return 1;
        });

    // queue the operation
    ReadBuffer buffer = bufferManager.queueReadAhead(operations, 0, 1, null);

    synchronized (blockUntilReadStarted) {
      blockUntilReadStarted.wait();
    }

    // set the buffer index to a different value
    final int index = buffer.getBufferindex();
    final int newIndex = (index + 1) % NUM_BUFFERS;
    LOG.info("changing buffer index from {} to {}", index, newIndex);
    buffer.setBufferindex(newIndex);

    // notify waiting buffer
    synchronized (bufferRef) {
      bufferRef.set(buffer);
      bufferRef.notifyAll();
    }

    awaitDone(buffer);

    // buffer has its indexed buffer released and fields updated.
    Assertions.assertThat(buffer)
        .matches(b -> b.getStatus() == ReadBufferStatus.READ_FAILED, "buffer is failed")
        .extracting(ReadBuffer::getErrException)
        .isNotNull();

    assertExceptionContains("not owned", buffer.getErrException());

    // its on the completed list for followup reads
    assertNotInCompletedList(buffer);

    bufferManager.testResetReadBufferManager();
  }

  /**
   * Assert that a buffer is not in the completed list.
   * @param buffer buffer to validate.
   */
  private static void assertNotInCompletedList(final ReadBuffer buffer) {
    Assertions.assertThat(getBufferManager().getCompletedReadListCopy())
        .describedAs("completed read list")
        .doesNotContain(buffer);
  }

  /**
   * Assert that a buffer is in the completed list.
   * @param buffer buffer to validate.
   */
  private static void assertInCompletedList(final ReadBuffer buffer) {
    Assertions.assertThat(getBufferManager().getCompletedReadListCopy())
        .describedAs("completed read list")
        .contains(buffer);
  }

  /**
   * Awaits for a buffer to be read; times out eventually and then raises an
   * assertion.
   * @param buffer buffer to block on.
   * @return
   * @throws InterruptedException interrupted.
   */
  private static ReadBuffer awaitDone(final ReadBuffer buffer) throws InterruptedException {
    LOG.info("awaiting latch of {}", buffer);
    Assertions.assertThat(buffer.getLatch().await(1, TimeUnit.MINUTES))
        .describedAs("awaiting release of latch on %s", buffer)
        .isTrue();
    LOG.info("Prefetch completed of {}", buffer);
    return buffer;
  }

  /**
   * Stub implementation of {@link ReadBufferStreamOperations}.
   * IOStats are collected and a lambda-function can be provided
   * which is evaluated on a readRemote call. The default one is
   * {@link #raiseOrReturn(StubReadBufferOperations)}.
   */
  private static final class StubReadBufferOperations
      implements ReadBufferStreamOperations {

    private final IOStatisticsStore iostats = iostatisticsStore()
        .withCounters(
            STREAM_READ_ACTIVE_PREFETCH_OPERATIONS,
            STREAM_READ_PREFETCH_BLOCKS_DISCARDED,
            STREAM_READ_PREFETCH_BLOCKS_USED,
            STREAM_READ_PREFETCH_BYTES_DISCARDED,
            STREAM_READ_PREFETCH_BLOCKS_EVICTED,
            STREAM_READ_PREFETCH_BYTES_USED,
            STREAM_READ_PREFETCH_OPERATIONS)
        .build();

    private boolean closed;

    private int bytesToRead;

    private IOException exceptionToRaise;

    /**
     * Function which takes a ref to this object and
     * returns the number of bytes read.
     * Invoked in the {@link #readRemote(long, byte[], int, int, TracingContext)}
     * call.
     */
    private final FunctionRaisingIOE<StubReadBufferOperations, Integer> action;

    /**
     * Simple constructor.
     */
    private StubReadBufferOperations() {
      // trivia, you can't use a this(this::raiseOrReturn) call here.
      this.action = this::raiseOrReturn;
    }

    /**
     * constructor.
     * @param action lambda expression to call in readRemote()
     */
    private StubReadBufferOperations(
        FunctionRaisingIOE<StubReadBufferOperations, Integer> action) {
      this.action = action;
    }

    @Override
    public int readRemote(
        final long position,
        final byte[] b,
        final int offset,
        final int length,
        final TracingContext tracingContext) throws IOException {
      return action.apply(this);
    }

    private int raiseOrReturn(StubReadBufferOperations self) throws IOException {
      if (exceptionToRaise != null) {
        throw exceptionToRaise;
      }
      return bytesToRead;
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    private void setClosed(final boolean closed) {
      this.closed = closed;
    }

    private int getBytesToRead() {
      return bytesToRead;
    }

    private void setBytesToRead(final int bytesToRead) {
      this.bytesToRead = bytesToRead;
    }

    @Override
    public String getStreamID() {
      return "streamid";
    }

    @Override
    public IOStatisticsStore getIOStatistics() {
      return iostats;
    }

    @Override
    public String getPath() {
      return "/path";
    }

    /**
     * start asserting about a counter.
     * @param key key
     * @return assert builder
     */
    AbstractLongAssert<?> assertCounter(String key) {
      return assertThatStatisticCounter(iostats, key);
    }
  }
}
