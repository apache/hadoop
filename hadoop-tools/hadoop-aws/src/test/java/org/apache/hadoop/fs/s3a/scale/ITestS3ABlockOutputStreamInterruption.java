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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent;
import org.apache.hadoop.fs.s3a.test.SdkFaultInjector;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_OPERATIONS_PURGE_UPLOADS;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_ACTIVE_BLOCKS;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BYTEBUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_HTTP_5XX_ERRORS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyInt;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_ABORTED;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.PUT_INTERRUPTED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.PUT_STARTED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_MULTIPART_ABORTED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_MULTIPART_INITIATED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_PART_FAILED_EVENT;
import static org.apache.hadoop.fs.s3a.impl.ProgressListenerEvent.TRANSFER_PART_STARTED_EVENT;
import static org.apache.hadoop.fs.s3a.test.SdkFaultInjector.setRequestFailureConditions;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.SUFFIX_FAILURES;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Testing interrupting file writes to s3 in
 * {@link FSDataOutputStream#close()}.
 * <p>
 * This is a bit tricky as we want to verify for all the block types that we
 * can interrupt active and pending uploads and not end up with failures
 * in the close() method.
 * Ideally cleanup should take place, especially of files.
 * <p>
 * Marked as a scale test even though it tries to aggressively abort streams being written
 * and should, if working, complete fast.
 */
@RunWith(Parameterized.class)
public class ITestS3ABlockOutputStreamInterruption extends S3AScaleTestBase {

  public static final int MAX_RETRIES_IN_SDK = 2;

  /**
   * Parameterized on (buffer type, active blocks).
   * @return parameters
   */
  @Parameterized.Parameters(name = "{0}-{1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_DISK, 2},
        {FAST_UPLOAD_BUFFER_ARRAY, 1},
        {FAST_UPLOAD_BYTEBUFFER, 2}
    });
  }

  public static final int MPU_SIZE = 5 * _1MB;

  /**
   * Buffer type.
   */
  private final String bufferType;

  /**
   * How many blocks can a stream have uploading?
   */
  private final int activeBlocks;

  /**
   * Constructor.
   * @param bufferType buffer type
   * @param activeBlocks number of active blocks which can be uploaded
   */
  public ITestS3ABlockOutputStreamInterruption(final String bufferType,
      int activeBlocks) {
    this.bufferType = requireNonNull(bufferType);
    this.activeBlocks = activeBlocks;
  }

  /**
   * Get the test timeout in seconds.
   * @return the test timeout as set in system properties or the default.
   */
  protected int getTestTimeoutSeconds() {
    return getTestPropertyInt(new Configuration(),
        KEY_TEST_TIMEOUT,
        SCALE_TEST_TIMEOUT_SECONDS);
  }

  @Override
  protected Configuration createScaleConfiguration() {
    Configuration conf = super.createScaleConfiguration();

    removeBaseAndBucketOverrides(conf,
        AUDIT_EXECUTION_INTERCEPTORS,
        DIRECTORY_OPERATIONS_PURGE_UPLOADS,
        FAST_UPLOAD_BUFFER,
        MAX_ERROR_RETRIES,
        MULTIPART_SIZE,
        RETRY_HTTP_5XX_ERRORS);
    conf.set(FAST_UPLOAD_BUFFER, bufferType);
    conf.setLong(MULTIPART_SIZE, MPU_SIZE);
    // limiting block size allows for stricter ordering of block uploads:
    // only 1 should be active at a time, so when a write is cancelled
    // it should be the only one to be aborted.
    conf.setLong(FAST_UPLOAD_ACTIVE_BLOCKS, activeBlocks);

    // guarantees teardown will abort pending uploads.
    conf.setBoolean(DIRECTORY_OPERATIONS_PURGE_UPLOADS, true);
    // don't retry much
    conf.setInt(MAX_ERROR_RETRIES, MAX_RETRIES_IN_SDK);
    // use the fault injector
    SdkFaultInjector.addFaultInjection(conf);
    return conf;
  }

  /**
   * Setup MUST set up the evaluator before the FS is created.
   */
  @Override
  public void setup() throws Exception {
    SdkFaultInjector.resetFaultInjector();
    super.setup();
  }

  @Override
  public void teardown() throws Exception {
    // safety check in case the evaluation is failing any
    // request needed in cleanup.
    SdkFaultInjector.resetFaultInjector();

    super.teardown();
  }

  @Test
  public void testInterruptMultipart() throws Throwable {
    describe("Interrupt a thread performing close() on a multipart upload");

    interruptMultipartUpload(methodPath(), 6 * _1MB);
  }

  /**
   * Initiate the upload of a file of a given length, then interrupt the
   * operation in close(); assert the expected outcome including verifying
   * that it was a multipart upload which was interrupted.
   * @param path path to write
   * @param len file length
   */
  private void interruptMultipartUpload(final Path path, int len) throws Exception {
    // dataset is bigger than one block
    final byte[] dataset = dataset(len, 'a', 'z' - 'a');

    InterruptingProgressListener listener = new InterruptingProgressListener(
        Thread.currentThread(),
        TRANSFER_PART_STARTED_EVENT);
    final FSDataOutputStream out = createFile(path, listener);
    // write it twice to force a multipart upload
    out.write(dataset);
    out.write(dataset);
    expectCloseInterrupted(out);

    LOG.info("Write aborted; total bytes written = {}", listener.getBytesTransferred());
    final IOStatistics streamStats = out.getIOStatistics();
    LOG.info("stream statistics {}", ioStatisticsToPrettyString(streamStats));
    listener.assertTriggered();
    listener.assertEventCount(TRANSFER_MULTIPART_INITIATED_EVENT, 1);
    listener.assertEventCount(TRANSFER_MULTIPART_ABORTED_EVENT, 1);

    // examine the statistics
    verifyStatisticCounterValue(streamStats,
        StoreStatisticNames.OBJECT_MULTIPART_UPLOAD_ABORTED, 1);
    // expect at least one byte to be transferred
    assertBytesTransferred(listener, 1, len * 2);
  }

  /**
   * Invoke Abortable.abort() during the upload,
   * then go on to simulate an NPE in the part upload and verify
   * that this does not get escalated.
   */
  @Test
  public void testAbortDuringUpload() throws Throwable {
    describe("Abort during multipart upload");
    int len = 6 * _1MB;
    final byte[] dataset = dataset(len, 'a', 'z' - 'a');
    // the listener aborts a target
    AtomicReference<Abortable> target = new AtomicReference<>();
    Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();
    InterruptingProgressListener listener = new InterruptingProgressListener(
        TRANSFER_PART_STARTED_EVENT,
        () -> {
          final NullPointerException ex =
              new NullPointerException("simulated failure after abort");
          LOG.info("aborting target", ex);

          // abort the stream
          target.get().abort();

          // wake up any thread
          semaphore.release();

          throw ex;
        });

    final FSDataOutputStream out = createFile(methodPath(), listener);
    // the target can only be set once we have the stream reference
    target.set(out);
    // queue the write which, once the block upload begins, will trigger the abort
    out.write(dataset);
    // block until the abort is triggered
    semaphore.acquire();

    // rely on the stream having closed at this point so that the
    // failed multipart event doesn't cause any problem
    out.close();

    // abort the stream again, expect it to be already closed

    final Abortable.AbortableResult result = target.get().abort();
    Assertions.assertThat(result.alreadyClosed())
        .describedAs("already closed flag in %s", result)
        .isTrue();
    listener.assertEventCount(TRANSFER_MULTIPART_ABORTED_EVENT, 1);
    // the raised NPE should have been noted but does not escalate to any form of failure.
    // note that race conditions in the code means that it is too brittle for a strict
    // assert here
    listener.assertEventCount(TRANSFER_PART_FAILED_EVENT)
        .isBetween(0L, 1L);
  }

  /**
   * Test that a part upload failure is propagated to
   * the close() call.
   */
  @Test
  public void testPartUploadFailure() throws Throwable {
    describe("Trigger a failure during a multipart upload");
    int len = 6 * _1MB;
    final byte[] dataset = dataset(len, 'a', 'z' - 'a');
    final String text = "Simulated failure";

    // uses a semaphore to control the timing of the NPE and close() call.
    Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();
    InterruptingProgressListener listener = new InterruptingProgressListener(
        TRANSFER_PART_STARTED_EVENT,
        () -> {
          // wake up any thread
          semaphore.release();
          throw new NullPointerException(text);
        });

    final FSDataOutputStream out = createFile(methodPath(), listener);
    out.write(dataset);
    semaphore.acquire();
    // quick extra sleep to ensure the NPE is raised
    Thread.sleep(1000);

    // this will pass up the exception from the part upload
    intercept(IOException.class, text, out::close);

    listener.assertEventCount(TRANSFER_MULTIPART_ABORTED_EVENT, 1);
    listener.assertEventCount(TRANSFER_PART_FAILED_EVENT, 1);
  }

  /**
   * Assert that bytes were transferred between (inclusively) the min and max values.
   * @param listener listener
   * @param min minimum
   * @param max maximum
   */
  private static void assertBytesTransferred(
      final InterruptingProgressListener listener,
      final long min,
      final long max) {

    Assertions.assertThat(listener.getBytesTransferred())
        .describedAs("bytes transferred")
        .isBetween(min, max);
  }

  /**
   * Write a small dataset and interrupt the close() operation.
   */
  @Test
  public void testInterruptMagicWrite() throws Throwable {
    describe("Interrupt a thread performing close() on a magic upload");

    // write a smaller file to a magic path and assert multipart outcome
    Path path = new Path(methodPath(), MAGIC_PATH_PREFIX + "1/__base/file");
    interruptMultipartUpload(path, _1MB);
  }

  /**
   * Write a small dataset and interrupt the close() operation.
   */
  @Test
  public void testInterruptWhenAbortingAnUpload() throws Throwable {
    describe("Interrupt a thread performing close() on a magic upload");

    // fail more than the SDK will retry
    setRequestFailureConditions(MAX_RETRIES_IN_SDK * 2, SdkFaultInjector::isMultipartAbort);

    // write a smaller file to a magic path and assert multipart outcome
    Path path = new Path(methodPath(), MAGIC_PATH_PREFIX + "1/__base/file");
    interruptMultipartUpload(path, _1MB);

    // an abort is double counted; the outer one also includes time to cancel
    // all pending aborts so is important to measure.
    verifyStatisticCounterValue(getFileSystem().getIOStatistics(),
        OBJECT_MULTIPART_UPLOAD_ABORTED.getSymbol() + SUFFIX_FAILURES,
        2);
  }

  /**
   * Interrupt a thread performing close() on a simple PUT.
   * This is less complex than the multipart upload case
   * because the progress callback should be on the current thread.
   * <p>
   * We do expect exception translation to map the interruption to
   * a {@code InterruptedIOException} and the count of interrupted events
   * to increase.
   */
  @Test
  public void testInterruptSimplePut() throws Throwable {
    describe("Interrupt simple object PUT");

    // dataset is less than one block
    final int len = _1MB;
    final byte[] dataset = dataset(len, 'a', 'z' - 'a');
    Path path = methodPath();

    InterruptingProgressListener listener = new InterruptingProgressListener(
        Thread.currentThread(),
        PUT_STARTED_EVENT);
    final FSDataOutputStream out = createFile(path, listener);
    out.write(dataset);
    expectCloseInterrupted(out);

    LOG.info("Write aborted; total bytes written = {}", listener.getBytesTransferred());
    final IOStatistics streamStats = out.getIOStatistics();
    LOG.info("stream statistics {}", ioStatisticsToPrettyString(streamStats));
    listener.assertTriggered();
    listener.assertEventCount(PUT_INTERRUPTED_EVENT, 1);
    assertBytesTransferred(listener, 0, len);
  }

  /**
   * Expect that a close operation is interrupted the first time it
   * is invoked.
   * the second time it is invoked, it should succeed.
   * @param out output stream
   */
  private static void expectCloseInterrupted(final FSDataOutputStream out)
      throws Exception {

    // first call will be interrupted
    intercept(InterruptedIOException.class, out::close);
    // second call must be safe
    out.close();
  }

  /**
   * Create a file with a progress listener.
   * @param path path to file
   * @param listener listener
   * @return the output stream
   * @throws IOException IO failure
   */
  private FSDataOutputStream createFile(final Path path,
      final InterruptingProgressListener listener) throws IOException {
    final FSDataOutputStreamBuilder builder = getFileSystem().createFile(path);
    builder
        .overwrite(true)
        .progress(listener)
        .must(FS_S3A_CREATE_PERFORMANCE, true);
    return builder.build();
  }

  /**
   * Progress listener which interrupts the thread at any chosen callback.
   * or any other action
   */
  private static final class InterruptingProgressListener
      extends CountingProgressListener {

    /** Event to trigger action. */
    private final ProgressListenerEvent trigger;

    /** Flag set when triggered. */
    private final AtomicBoolean triggered = new AtomicBoolean(false);

    /**
     * Action to take on trigger.
     */
    private final InvocationRaisingIOE action;

    /**
     * Create.
     * @param thread thread to interrupt
     * @param trigger event to trigger on
     */
    private InterruptingProgressListener(
        final Thread thread,
        final ProgressListenerEvent trigger) {
      this(trigger, thread::interrupt);
    }

    /**
     * Create for any arbitrary action.
     * @param trigger event to trigger on
     * @param action action to take
     */
    private InterruptingProgressListener(
        final ProgressListenerEvent trigger,
        final InvocationRaisingIOE action) {
      this.trigger = trigger;
      this.action = action;
    }

    @Override
    public void progressChanged(final ProgressListenerEvent eventType,
        final long transferredBytes) {
      super.progressChanged(eventType, transferredBytes);
      if (trigger == eventType && !triggered.getAndSet(true)) {
        LOG.info("triggering action");
        try {
          action.apply();
        } catch (IOException e) {
          LOG.warn("action failed", e);
        }
      }
    }

    /**
     * Assert that the trigger took place.
     */
    private void assertTriggered() {
      assertTrue("Not triggered", triggered.get());
    }
  }



}
