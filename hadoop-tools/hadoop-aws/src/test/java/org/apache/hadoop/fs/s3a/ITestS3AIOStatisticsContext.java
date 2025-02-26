/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsContextImpl;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertCapabilities;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disablePrefetching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfAnalyticsAcceleratorEnabled;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticCounterValue;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration.enableIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration.getCurrentIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration.getThreadSpecificIOStatisticsContext;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsContextIntegration.setThreadIOStatisticsContext;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;

/**
 * Tests to verify the Thread-level IOStatistics.
 */
public class ITestS3AIOStatisticsContext extends AbstractS3ATestBase {

  private static final int SMALL_THREADS = 2;
  private static final int BYTES_BIG = 100;
  private static final int BYTES_SMALL = 50;
  private static final String[] IOSTATISTICS_CONTEXT_CAPABILITY =
      new String[] {StreamCapabilities.IOSTATISTICS_CONTEXT};
  private ExecutorService executor;

  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    disablePrefetching(configuration);
    enableIOStatisticsContext();
    return configuration;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    executor = HadoopExecutors.newFixedThreadPool(SMALL_THREADS);
    // Analytics accelerator currently does not support IOStatisticsContext, this will be added as
    // part of https://issues.apache.org/jira/browse/HADOOP-19364
    skipIfAnalyticsAcceleratorEnabled(getConfiguration(),
        "Analytics Accelerator currently does not support IOStatisticsContext");

  }

  @Override
  public void teardown() throws Exception {
    if (executor != null) {
      executor.shutdown();
    }
    super.teardown();
  }

  /**
   * Verify that S3AInputStream aggregates per thread IOStats collection
   * correctly.
   */
  @Test
  public void testS3AInputStreamIOStatisticsContext()
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName());
    byte[] data = dataset(256, 'a', 'z');
    byte[] readDataFirst = new byte[BYTES_BIG];
    byte[] readDataSecond = new byte[BYTES_SMALL];
    writeDataset(fs, path, data, data.length, 1024, true);

    CountDownLatch latch = new CountDownLatch(SMALL_THREADS);

    try {

      for (int i = 0; i < SMALL_THREADS; i++) {
        executor.submit(() -> {
          try {
            // get the thread context and reset
            IOStatisticsContext context =
                getAndResetThreadStatisticsContext();
            try (FSDataInputStream in = fs.open(path)) {
              // Assert the InputStream's stream capability to support
              // IOStatisticsContext.
              assertCapabilities(in, IOSTATISTICS_CONTEXT_CAPABILITY, null);
              in.seek(50);
              in.read(readDataFirst);
            }
            assertContextBytesRead(context, BYTES_BIG);
            // Stream is closed for a thread. Re-open and do more operations.
            try (FSDataInputStream in = fs.open(path)) {
              in.seek(100);
              in.read(readDataSecond);
            }
            assertContextBytesRead(context, BYTES_BIG + BYTES_SMALL);

            latch.countDown();
          } catch (Exception e) {
            latch.countDown();
            setFutureException(e);
            LOG.error("An error occurred while doing a task in the thread", e);
          } catch (AssertionError ase) {
            latch.countDown();
            setFutureAse(ase);
            throw ase;
          }
        });
      }
      // wait for tasks to finish.
      latch.await();
    } finally {
      executor.shutdown();
    }

    // Check if an Exception or ASE was caught while the test threads were running.
    maybeReThrowFutureException();
    maybeReThrowFutureASE();

  }

  /**
   * get the thread context and reset.
   * @return thread context
   */
  private static IOStatisticsContext getAndResetThreadStatisticsContext() {
    assertTrue("thread-level IOStatistics should be enabled by default",
        IOStatisticsContext.enabled());
    IOStatisticsContext context =
        IOStatisticsContext.getCurrentIOStatisticsContext();
    context.reset();
    return context;
  }

  /**
   * Verify that S3ABlockOutputStream aggregates per thread IOStats collection
   * correctly.
   */
  @Test
  public void testS3ABlockOutputStreamIOStatisticsContext()
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName());
    byte[] writeDataFirst = new byte[BYTES_BIG];
    byte[] writeDataSecond = new byte[BYTES_SMALL];

    final ExecutorService executorService =
        HadoopExecutors.newFixedThreadPool(SMALL_THREADS);
    CountDownLatch latch = new CountDownLatch(SMALL_THREADS);

    try {
      for (int i = 0; i < SMALL_THREADS; i++) {
        executorService.submit(() -> {
          try {
            // get the thread context and reset
            IOStatisticsContext context =
                getAndResetThreadStatisticsContext();
            try (FSDataOutputStream out = fs.create(path)) {
              // Assert the OutputStream's stream capability to support
              // IOStatisticsContext.
              assertCapabilities(out, IOSTATISTICS_CONTEXT_CAPABILITY, null);
              out.write(writeDataFirst);
            }
            assertContextBytesWrite(context, BYTES_BIG);

            // Stream is closed for a thread. Re-open and do more operations.
            try (FSDataOutputStream out = fs.create(path)) {
              out.write(writeDataSecond);
            }
            assertContextBytesWrite(context, BYTES_BIG + BYTES_SMALL);
            latch.countDown();
          } catch (Exception e) {
            latch.countDown();
            setFutureException(e);
            LOG.error("An error occurred while doing a task in the thread", e);
          } catch (AssertionError ase) {
            latch.countDown();
            setFutureAse(ase);
            throw ase;
          }
        });
      }
      // wait for tasks to finish.
      latch.await();
    } finally {
      executorService.shutdown();
    }

    // Check if an Excp or ASE was caught while the test threads were running.
    maybeReThrowFutureException();
    maybeReThrowFutureASE();
  }

  /**
   * Verify stats collection and aggregation for constructor thread, Junit
   * thread and a worker thread.
   */
  @Test
  public void testThreadIOStatisticsForDifferentThreads()
      throws IOException, InterruptedException {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName());
    byte[] data = new byte[BYTES_BIG];
    long threadIdForTest = Thread.currentThread().getId();
    IOStatisticsContext context =
        getAndResetThreadStatisticsContext();
    Assertions.assertThat(((IOStatisticsContextImpl)context).getThreadID())
        .describedAs("Thread ID of %s", context)
        .isEqualTo(threadIdForTest);
    Assertions.assertThat(((IOStatisticsContextImpl)context).getID())
        .describedAs("ID of %s", context)
        .isGreaterThan(0);

    // Write in the Junit thread.
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(data);
    }

    // Read in the Junit thread.
    try (FSDataInputStream in = fs.open(path)) {
      in.read(data);
    }

    // Worker thread work and wait for it to finish.
    TestWorkerThread workerThread = new TestWorkerThread(path, null);
    long workerThreadID = workerThread.getId();
    LOG.info("Worker thread ID: {} ", workerThreadID);
    workerThread.start();
    workerThread.join();

    assertThreadStatisticsForThread(threadIdForTest, BYTES_BIG);
    assertThreadStatisticsForThread(workerThreadID, BYTES_SMALL);
  }

  /**
   * Verify stats collection and aggregation for constructor thread, Junit
   * thread and a worker thread.
   */
  @Test
  public void testThreadSharingIOStatistics()
      throws IOException, InterruptedException {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName());
    byte[] data = new byte[BYTES_BIG];
    long threadIdForTest = Thread.currentThread().getId();
    IOStatisticsContext context =
        getAndResetThreadStatisticsContext();


    // Write in the Junit thread.
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(data);
    }

    // Read in the Junit thread.
    try (FSDataInputStream in = fs.open(path)) {
      in.read(data);
    }

    // Worker thread will share the same context.
    TestWorkerThread workerThread = new TestWorkerThread(path, context);
    long workerThreadID = workerThread.getId();
    workerThread.start();
    workerThread.join();

    assertThreadStatisticsForThread(threadIdForTest, BYTES_BIG + BYTES_SMALL);

  }

  /**
   * Test to verify if setting the current IOStatisticsContext removes the
   * current context and creates a new instance of it.
   */
  @Test
  public void testSettingNullIOStatisticsContext() {
    IOStatisticsContext ioStatisticsContextBefore =
        getCurrentIOStatisticsContext();
    // Set the current IOStatisticsContext to null, which should remove the
    // context and set a new one.
    setThreadIOStatisticsContext(null);
    // Get the context again after setting.
    IOStatisticsContext ioStatisticsContextAfter =
        getCurrentIOStatisticsContext();
    //Verify the context ID after setting to null is different than the previous
    // one.
    Assertions.assertThat(ioStatisticsContextBefore.getID())
        .describedAs("A new IOStaticsContext should be set after setting the "
            + "current to null")
        .isNotEqualTo(ioStatisticsContextAfter.getID());
  }

  /**
   * Assert bytes written by the statistics context.
   *
   * @param context statistics context.
   * @param bytes expected bytes.
   */
  private void assertContextBytesWrite(IOStatisticsContext context,
      int bytes) {
    verifyStatisticCounterValue(
        context.getIOStatistics(),
        STREAM_WRITE_BYTES,
        bytes);
  }

  /**
   * Assert bytes read by the statistics context.
   *
   * @param context statistics context.
   * @param readBytes expected bytes.
   */
  private void assertContextBytesRead(IOStatisticsContext context,
      int readBytes) {
    verifyStatisticCounterValue(
        context.getIOStatistics(),
        STREAM_READ_BYTES,
        readBytes);
  }

  /**
   * Assert fixed bytes wrote and read for a particular thread ID.
   *
   * @param testThreadId                thread ID.
   * @param expectedBytesWrittenAndRead expected bytes.
   */
  private void assertThreadStatisticsForThread(long testThreadId,
      int expectedBytesWrittenAndRead) {
    LOG.info("Thread ID to be asserted: {}", testThreadId);
    IOStatisticsContext ioStatisticsContext =
        getThreadSpecificIOStatisticsContext(testThreadId);
    Assertions.assertThat(ioStatisticsContext)
        .describedAs("IOStatisticsContext for %d", testThreadId)
        .isNotNull();


    IOStatistics ioStatistics = ioStatisticsContext.snapshot();


    assertThatStatisticCounter(ioStatistics,
            STREAM_WRITE_BYTES)
        .describedAs("Bytes written are not as expected for thread : %s",
                testThreadId)
        .isEqualTo(expectedBytesWrittenAndRead);

    assertThatStatisticCounter(ioStatistics,
        STREAM_READ_BYTES)
        .describedAs("Bytes read are not as expected for thread : %s",
                testThreadId)
        .isEqualTo(expectedBytesWrittenAndRead);
  }

  @Test
  public void testListingStatisticsContext() throws Throwable {
    describe("verify the list operations update on close()");

    S3AFileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(methodPath());

    // after all setup, get the reset context
    IOStatisticsContext context =
        getAndResetThreadStatisticsContext();
    IOStatistics ioStatistics = context.getIOStatistics();

    fs.listStatus(path);
    verifyStatisticCounterValue(ioStatistics,
        StoreStatisticNames.OBJECT_LIST_REQUEST,
        1);

    context.reset();
    foreach(fs.listStatusIterator(path), i -> {});
    verifyStatisticCounterValue(ioStatistics,
        StoreStatisticNames.OBJECT_LIST_REQUEST,
        1);

    context.reset();
    foreach(fs.listLocatedStatus(path), i -> {});
    verifyStatisticCounterValue(ioStatistics,
        StoreStatisticNames.OBJECT_LIST_REQUEST,
        1);

    context.reset();
    foreach(fs.listFiles(path, true), i -> {});
    verifyStatisticCounterValue(ioStatistics,
        StoreStatisticNames.OBJECT_LIST_REQUEST,
        1);
  }

  @Test
  public void testListingThroughTaskPool() throws Throwable {
    describe("verify the list operations are updated through taskpool");

    S3AFileSystem fs = getFileSystem();
    Path path = methodPath();
    fs.mkdirs(methodPath());

    // after all setup, get the reset context
    IOStatisticsContext context =
        getAndResetThreadStatisticsContext();
    IOStatistics ioStatistics = context.getIOStatistics();

    CloseableTaskPoolSubmitter submitter =
        new CloseableTaskPoolSubmitter(executor);
    TaskPool.foreach(fs.listStatusIterator(path))
        .executeWith(submitter)
        .run(i -> {});

    verifyStatisticCounterValue(ioStatistics,
        StoreStatisticNames.OBJECT_LIST_REQUEST,
        1);

  }

  /**
   * Simulating doing some work in a separate thread.
   * If constructed with an IOStatisticsContext then
   * that context is switched to before performing the IO.
   */
  private class TestWorkerThread extends Thread implements Runnable {
    private final Path workerThreadPath;

    private final IOStatisticsContext ioStatisticsContext;

    /**
     * create.
     * @param workerThreadPath thread path.
     * @param ioStatisticsContext optional statistics context     *
     */
    TestWorkerThread(
        final Path workerThreadPath,
        final IOStatisticsContext ioStatisticsContext) {
      this.workerThreadPath = workerThreadPath;
      this.ioStatisticsContext = ioStatisticsContext;
    }

    @Override
    public void run() {
      // Setting the worker thread's name.
      Thread.currentThread().setName("worker thread");
      S3AFileSystem fs = getFileSystem();
      byte[] data = new byte[BYTES_SMALL];

      // maybe switch context
      if (ioStatisticsContext != null) {
        IOStatisticsContext.setThreadIOStatisticsContext(ioStatisticsContext);
      }
      // Storing context in a field to not lose the reference in a GC.
      IOStatisticsContext ioStatisticsContextWorkerThread =
          getCurrentIOStatisticsContext();

      // Write in the worker thread.
      try (FSDataOutputStream out = fs.create(workerThreadPath)) {
        out.write(data);
      } catch (IOException e) {
        throw new UncheckedIOException("Failure while writing", e);
      }

      //Read in the worker thread.
      try (FSDataInputStream in = fs.open(workerThreadPath)) {
        in.read(data);
      } catch (IOException e) {
        throw new UncheckedIOException("Failure while reading", e);
      }
    }
  }
}
