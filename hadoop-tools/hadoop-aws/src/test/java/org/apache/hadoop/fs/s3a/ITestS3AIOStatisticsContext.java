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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.StreamStatisticNames;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.THREAD_LEVEL_IOSTATS_ENABLED;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.maybeReThrowFutureASE;
import static org.apache.hadoop.test.LambdaTestUtils.maybeReThrowFutureException;
import static org.apache.hadoop.test.LambdaTestUtils.setFutureASE;
import static org.apache.hadoop.test.LambdaTestUtils.setFutureException;

public class ITestS3AIOStatisticsContext extends AbstractS3ATestBase {

  private static final int SMALL_THREADS = 2;
  private static final int READ_BYTES_FIRST = 100;
  private static final int READ_BYTES_SECOND = 50;

  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    removeBaseAndBucketOverrides(configuration, THREAD_LEVEL_IOSTATS_ENABLED);
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
    byte[] readDataFirst = new byte[READ_BYTES_FIRST];
    byte[] readDataSecond = new byte[READ_BYTES_SECOND];
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
                  READ_BYTES_FIRST);
            }
            // Stream is closed for a thread. Re-open and do more operations.
            IOStatistics ioStatisticsSecond;
            try (FSDataInputStream in = fs.open(path)) {
              in.seek(100);
              in.read(readDataSecond);
              in.close();
              ioStatisticsSecond = assertThreadStatisticsBytesRead(in,
                  READ_BYTES_FIRST + READ_BYTES_SECOND);
            }
            latch.countDown();
          } catch (Exception e) {
            latch.countDown();
            setFutureException(e);
            LOG.error("An error occurred while doing a task in the thread", e);
          } catch (AssertionError ase) {
            latch.countDown();
            setFutureASE(ase);
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

  @Test
  public void testS3ABlockOutputStreamIOStatisticsContext()
      throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path path = path(getMethodName());
    byte[] data = dataset(256, 'a', 'z');
    byte[] writeDataFirst = new byte[READ_BYTES_FIRST];
    byte[] writeDataSecond = new byte[READ_BYTES_SECOND];
    writeDataset(fs, path, data, data.length, 1024, true);

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
                  READ_BYTES_FIRST);
            }
            // Stream is closed for a thread. Re-open and do more operations.
            IOStatistics ioStatisticsSecond;
            try (FSDataOutputStream out = fs.create(path)) {
              out.write(writeDataSecond);
              out.close();
              ioStatisticsSecond = assertThreadStatisticsBytesWrite(out,
                  READ_BYTES_FIRST + READ_BYTES_SECOND);
            }
            latch.countDown();
          } catch (Exception e) {
            latch.countDown();
            setFutureException(e);
            LOG.error("An error occurred while doing a task in the thread", e);
          } catch (AssertionError ase) {
            latch.countDown();
            setFutureASE(ase);
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

  private IOStatistics assertThreadStatisticsBytesWrite(FSDataOutputStream out, int writeBytes) {
    S3ABlockOutputStream s3aOut = (S3ABlockOutputStream) out.getWrappedStream();
    IOStatisticsContext ioStatisticsContext = s3aOut.getIoStatisticsContext();
    IOStatistics ioStatistics = ioStatisticsContext.getThreadIOStatistics();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_WRITE_BYTES)
        .describedAs("Bytes wrote are not as expected")
        .isEqualTo(writeBytes);

    return ioStatistics;
  }

  private IOStatistics assertThreadStatisticsBytesRead(FSDataInputStream in,
      int readBytes) {
    S3AInputStream s3AInputStream =
        (S3AInputStream) in.getWrappedStream();
    IOStatisticsContext ioStatisticsContext =
        s3AInputStream.getIoStatisticsContext();
    IOStatistics ioStatistics =
        ioStatisticsContext.getThreadIOStatistics();

    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
        StreamStatisticNames.STREAM_READ_BYTES)
        .describedAs("Bytes read are not as expected")
        .isEqualTo(readBytes);

    return ioStatistics;
  }
}
