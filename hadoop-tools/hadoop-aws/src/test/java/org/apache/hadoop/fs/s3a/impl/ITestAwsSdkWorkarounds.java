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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.test.GenericTestUtils.LogCapturer.captureLogs;

/**
 * Verify that noisy transfer manager logs are turned off.
 * <p>
 * This is done by creating new FS instances and then
 * requesting an on-demand transfer manager from the store.
 * As this is only done once per FS instance, a new FS is
 * required per test case.
 */
public class ITestAwsSdkWorkarounds extends AbstractS3ATestBase {

  /**
   * Test logger.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAwsSdkWorkarounds.class);

  /**
   * Transfer Manager log.
   */
  private static final Logger XFER_LOG =
      LoggerFactory.getLogger(AwsSdkWorkarounds.TRANSFER_MANAGER);

  /**
   * This is the string which keeps being printed.
   * {@value}.
   */
  private static final String FORBIDDEN =
      "The provided S3AsyncClient is an instance of MultipartS3AsyncClient";

  /**
   * Marginal test run speedup by skipping needless test dir cleanup.
   * @throws IOException failure
   */
  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    /* no-op */
  }

  /**
   * Test instantiation with logging disabled.
   */
  @Test
  public void testQuietLogging() throws Throwable {
    // simulate the base state of logging
    noisyLogging();
    // creating a new FS switches to quiet logging
    try (S3AFileSystem newFs = newFileSystem()) {
      String output = createAndLogTransferManager(newFs);
      Assertions.assertThat(output)
          .describedAs("LOG output")
          .doesNotContain(FORBIDDEN);
    }
  }

  /**
   * Test instantiation with logging disabled.
   */
  @Test
  public void testNoisyLogging() throws Throwable {
    skipIfClientSideEncryption();
    try (S3AFileSystem newFs = newFileSystem()) {
      noisyLogging();
      String output = createAndLogTransferManager(newFs);
      Assertions.assertThat(output)
          .describedAs("LOG output does not contain the forbidden text."
              + " Has the SDK been fixed?")
          .contains(FORBIDDEN);
    }
  }

  /**
   * Create a new filesystem using the configuration
   * of the base test fs.
   * @return the new FS.
   * @throws IOException failure.
   */
  private S3AFileSystem newFileSystem() throws IOException {
    S3AFileSystem newFs = new S3AFileSystem();
    try {
      newFs.initialize(getFileSystem().getUri(), getFileSystem().getConf());
      return newFs;
    } catch (IOException e) {
      newFs.close();
      throw e;
    }
  }

  /**
   * Instantiate the transfer manager, if one is not already
   * created for this FS instance.
   * <p>
   * Does not create one if it has already been called on this fs.
   * @param fs filesystem.
   * @return the log for the creation.
   * @throws IOException failure to instantiate.
   */
  private String createAndLogTransferManager(final S3AFileSystem fs)
      throws IOException {
    LOG.info("====== Creating transfer manager =====");
    GenericTestUtils.LogCapturer capturer = captureXferManagerLogs();
    try {
      fs.getS3AInternals().getStore()
          .getOrCreateTransferManager();
      LOG.info("====== Created transfer manager ======");
      return capturer.getOutput();
    } finally {
      capturer.stopCapturing();
    }
  }

  /**
   * turn on noisy logging.
   */
  private static void noisyLogging() {
    Assertions.assertThat(AwsSdkWorkarounds.restoreNoisyLogging())
        .describedAs("Enabled Log4J logging")
        .isTrue();
  }

  /**
   * Start capturing the logs.
   * Stop this afterwards.
   * @return a capturer.
   */
  private GenericTestUtils.LogCapturer captureXferManagerLogs() {
    return captureLogs(XFER_LOG);
  }
}
