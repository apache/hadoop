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

package org.apache.hadoop.fs.s3a;

import java.lang.ref.WeakReference;
import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_LEAKS;
import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;

/**
 * Test Stream leakage.
 */
public class ITestS3AInputStreamLeakage extends AbstractS3ATestBase {

  /**
   * How big a file to create?
   */
  public static final int FILE_SIZE = 1024;

  public static final byte[] DATASET = dataset(FILE_SIZE, '0', 10);

  /**
   * Time to wait after a GC/finalize is triggered before looking at the log.
   */
  public static final long GC_DELAY = Duration.ofSeconds(1).toMillis();

  @Override
  public void setup() throws Exception {
    super.setup();
    assume("Stream leak detection not avaialable",
        getFileSystem().hasCapability(STREAM_LEAKS));
  }

  /**
   * This test forces a GC of an open file then verifies that the
   * log contains the error message.
   * <p>
   * Care is needed here to ensure that no strong references are held to the
   * stream, otherwise: no GC.
   * <p>
   * It also assumes that {@code System.gc()} will do enough of a treewalk to
   * prepare the stream for garbage collection (a weak ref is used to verify
   * that it was removed as a reference), and that
   * {@code System.runFinalization()} will then
   * invoke the finalization.
   * <p>
   * The finalize code runs its own thread "Finalizer"; this is async enough
   * that assertions on log entries only work if there is a pause after
   * finalization is triggered and the log is reviewed.
   * <p>
   * The stream leak counter of the FileSystem is also updated; this
   * is verified.
   * <p>
   * Note: if the stream under test is not an S3AInputStream (i.e. is a prefetching one,
   * this test is skipped. If/when the prefetching stream adds the same code,
   * this check can be removed.
   */
  @Test
  public void testFinalizer() throws Throwable {
    Path path = methodPath();
    final S3AFileSystem fs = getFileSystem();

    ContractTestUtils.createFile(fs, path, true, DATASET);

    // DO NOT use try-with-resources; this
    // test MUST be able to remove all references
    // to the stream
    FSDataInputStream in = fs.open(path);

    try {
      Assertions.assertThat(in.hasCapability(STREAM_LEAKS))
          .describedAs("Stream leak detection not supported in: " + in.getClass())
          .isTrue();

      Assertions.assertThat(in.read())
          .describedAs("first byte read from %s", in)
          .isEqualTo(DATASET[0]);

      // get a weak ref so that after a GC we can look for it and verify it is gone
      Assertions.assertThat(((S3AInputStream) in.getWrappedStream()).isObjectStreamOpen())
          .describedAs("stream http connection status")
          .isTrue();
      // weak reference to track GC progress
      WeakReference<S3AInputStream> wrs =
          new WeakReference<>((S3AInputStream) in.getWrappedStream());

      // Capture the logs
      GenericTestUtils.LogCapturer logs =
          captureLogs(LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));

      LOG.info("captured log");

      // remove strong reference to the stream
      in = null;
      // force the gc.
      System.gc();
      // make sure the GC removed the S3AInputStream.
      Assertions.assertThat(wrs.get())
          .describedAs("weak stream reference wasn't GC'd")
          .isNull();

      // finalize
      System.runFinalization();

      // finalize is async, so add a brief wait for it to be called.
      // without this the log may or may not be empty
      Thread.sleep(GC_DELAY);
      LOG.info("end of log");

      // check the log
      logs.stopCapturing();
      final String output = logs.getOutput();
      LOG.info("output of leak log is {}", output);
      Assertions.assertThat(output)
          .describedAs("output from the logs during GC")
          .contains("drain or abort reason finalize()")  // stream release
          .contains(path.toUri().toString())             // path
          .contains(Thread.currentThread().getName())    // thread
          .contains("testFinalizer");                    // stack

      // verify that leakages are added to the FS statistics
      assertThatStatisticCounter(fs.getIOStatistics(), STREAM_LEAKS)
          .isEqualTo(1);
    } finally {
      if (in != null) {
        IOUtils.cleanupWithLogger(LOG, in);
      }
    }
  }
}
