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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.CommonConfigurationKeys.THREAD_LEVEL_IOSTATISTICS_ENABLED;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsContext.currentIOStatisticsContext;

/**
 * Tests to verify the Thread-level IOStatistics.
 */
public class ITestS3AIOStatisticsContext extends AbstractS3ATestBase {

  private static final int SMALL_THREADS = 2;
  private static final int BYTES_BIG = 100;
  private static final int BYTES_SMALL = 50;
  private static final long TEST_THREAD_ID = Thread.currentThread().getId();

  /**
   * Run this before the tests once, to note down some work in the
   * constructor thread to be verified later on in a test.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    // Do some work in constructor thread.
    S3AFileSystem fs = new S3AFileSystem();
    Configuration conf = new Configuration();
    fs.initialize(new URI(conf.get(TEST_FS_S3A_NAME)), conf);
    Path path = new Path("testConstructor");
    try (FSDataOutputStream out = fs.create(path)) {
      out.write('a');
    }
    try (FSDataInputStream in = fs.open(path)) {
      in.read();
    }
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    removeBaseAndBucketOverrides(configuration,
        THREAD_LEVEL_IOSTATISTICS_ENABLED);
    return configuration;
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

    final ExecutorService executor =
        HadoopExecutors.newFixedThreadPool(SMALL_THREADS);
    CountDownLatch latch = new CountDownLatch(SMALL_THREADS);

    try {
      for (int i = 0; i < SMALL_THREADS; i++) {
        executor.submit(() -> {
          try {
            IOStatistics ioStatisticsFirst;
            try (FSDataInputStream in = fs.open(path)) {
              in.seek(50);
              in.read(readDataFirst);
              in.close();
              ioStatisticsFirst = assertThreadStatisticsBytesRead(in,
                  BYTES_BIG);
            }
            // Stream is closed for a thread. Re-open and do more operations.
            IOStatistics ioStatisticsSecond;
            try (FSDataInputStream in = fs.open(path)) {
              in.seek(100);
              in.read(readDataSecond);
              in.close();
              ioStatisticsSecond = assertThreadStatisticsBytesRead(in,
                  BYTES_BIG + BYTES_SMALL);
            }
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

    // Check if an Excp or ASE was caught while the test threads were running.
    maybeReThrowFutureException();
    maybeReThrowFutureASE();

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

    final ExecutorService executor =
        HadoopExecutors.newFixedThreadPool(SMALL_THREADS);
    CountDownLatch latch = new CountDownLatch(SMALL_THREADS);

    try {
      for (int i = 0; i < SMALL_THREADS; i++) {
        executor.submit(() -> {
          try {
            IOStatistics ioStatisticsFirst;
            try (FSDataOutputStream out = fs.create(path)) {
              out.write(writeDataFirst);
              out.close();
              ioStatisticsFirst = assertThreadStatisticsBytesWrite(out,
                  BYTES_BIG);
            }
            // Stream is closed for a thread. Re-open and do more operations.
            IOStatistics ioStatisticsSecond;
            try (FSDataOutputStream out = fs.create(path)) {
              out.write(writeDataSecond);
              out.close();
              ioStatisticsSecond = assertThreadStatisticsBytesWrite(out,
                  BYTES_BIG + BYTES_SMALL);
            }
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

    // Write in the Junit thread.
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(data);
    }

    // Read in the Junit thread.
    try (FSDataInputStream in = fs.open(path)) {
      in.read(data);
    }

    // Worker thread work and wait for it to finish.
    testWorkerThread workerThread = new testWorkerThread();
    long workerThreadID = workerThread.getId();
    workerThread.start();
    workerThread.join();

    // Work done in constructor: Wrote and Read 1 byte.
    // Work done in Junit thread: Wrote and Read BYTES_BIG bytes.
    // Work done in Junit's worker thread: Wrote and Read BYTES_SMALL bytes.
    assertThreadStatisticsForThread(TEST_THREAD_ID, 1);
    assertThreadStatisticsForThread(threadIdForTest, BYTES_BIG);
    assertThreadStatisticsForThread(workerThreadID, BYTES_SMALL);

  }

  /**
   * Assert bytes wrote by the current thread.
   *
   * @param out        OutputStream.
   * @param writeBytes expected bytes.
   * @return IOStatistics for this stream.
   */
  private IOStatistics assertThreadStatisticsBytesWrite(FSDataOutputStream out,
      int writeBytes) {
    S3ABlockOutputStream s3aOut = (S3ABlockOutputStream) out.getWrappedStream();
    IOStatistics ioStatistics =
        (IOStatisticsSnapshot) s3aOut.getThreadIOStatistics();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_WRITE_BYTES)
        .describedAs("Bytes wrote are not as expected")
        .isEqualTo(writeBytes);

    return ioStatistics;
  }

  /**
   * Assert bytes read by the current thread.
   *
   * @param in        InputStream.
   * @param readBytes expected bytes.
   * @return IOStatistics for this stream.
   */
  private IOStatistics assertThreadStatisticsBytesRead(FSDataInputStream in,
      int readBytes) {
    S3AInputStream s3AInputStream =
        (S3AInputStream) in.getWrappedStream();
    IOStatistics ioStatistics = s3AInputStream.getThreadIOStatistics();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_READ_BYTES)
        .describedAs("Bytes read are not as expected")
        .isEqualTo(readBytes);

    return ioStatistics;
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
    IOStatistics ioStatistics =
        currentIOStatisticsContext().getThreadIOStatistics(testThreadId);

    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_WRITE_BYTES)
        .describedAs("Bytes wrote are not as expected for thread :{}",
            testThreadId)
        .isEqualTo(expectedBytesWrittenAndRead);

    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_READ_BYTES)
        .describedAs("Bytes read are not as expected for thread :{}",
            testThreadId)
        .isEqualTo(expectedBytesWrittenAndRead);
  }

  /**
   * Simulating doing some work in a separate thread.
   */
  private class testWorkerThread extends Thread implements Runnable {
    @Override
    public void run() {
      S3AFileSystem fs = getFileSystem();
      Path path = new Path("workerThread");
      byte[] data = new byte[BYTES_SMALL];

      // Write in the worker thread.
      try (FSDataOutputStream out = fs.create(path)) {
        out.write(data);
      } catch (IOException e) {
        throw new UncheckedIOException("Failure while writing", e);
      }

      //Read in the worker thread.
      try (FSDataInputStream in = fs.open(path)) {
        in.read(data);
      } catch (IOException e) {
        throw new UncheckedIOException("Failure while reading", e);
      }
    }
  }
}
