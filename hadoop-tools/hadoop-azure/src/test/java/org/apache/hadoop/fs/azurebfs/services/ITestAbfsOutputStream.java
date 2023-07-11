/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.test.LambdaTestUtils;

/**
 * Test create operation.
 */
public class ITestAbfsOutputStream extends AbstractAbfsIntegrationTest {

  private static final int TEST_EXECUTION_TIMEOUT = 2 * 60 * 1000;
  private static final String TEST_FILE_PATH = "testfile";

  public ITestAbfsOutputStream() throws Exception {
    super();
  }

  @Test
  public void testMaxRequestsAndQueueCapacityDefaults() throws Exception {
    Configuration conf = getRawConfiguration();
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
    AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      int maxConcurrentRequests
          = getConfiguration().getWriteMaxConcurrentRequestCount();
      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

    Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
        "maxConcurrentRequests should be " + maxConcurrentRequests)
        .isEqualTo(maxConcurrentRequests);
    Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs(
        "maxRequestsToQueue should be " + getConfiguration()
            .getMaxWriteRequestsToQueue())
        .isEqualTo(getConfiguration().getMaxWriteRequestsToQueue());
    }
  }

  @Test
  public void testMaxRequestsAndQueueCapacity() throws Exception {
    Configuration conf = getRawConfiguration();
    int maxConcurrentRequests = 6;
    int maxRequestsToQueue = 10;
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_CONCURRENT_REQUESTS,
        "" + maxConcurrentRequests);
    conf.set(ConfigurationKeys.AZURE_WRITE_MAX_REQUESTS_TO_QUEUE,
        "" + maxRequestsToQueue);
    final AzureBlobFileSystem fs = getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(path(TEST_FILE_PATH))) {
      AbfsOutputStream stream = (AbfsOutputStream) out.getWrappedStream();

      if (stream.isAppendBlobStream()) {
        maxConcurrentRequests = 1;
      }

      Assertions.assertThat(stream.getMaxConcurrentRequestCount()).describedAs(
          "maxConcurrentRequests should be " + maxConcurrentRequests).isEqualTo(maxConcurrentRequests);
      Assertions.assertThat(stream.getMaxRequestsThatCanBeQueued()).describedAs("maxRequestsToQueue should be " + maxRequestsToQueue)
          .isEqualTo(maxRequestsToQueue);
    }
  }

  /**
   * Verify the passing of AzureBlobFileSystem reference to AbfsOutputStream
   * to make sure that the FS instance is not eligible for GC while writing.
   */
  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testAzureBlobFileSystemBackReferenceInOutputStream()
      throws Exception {
    byte[] testBytes = new byte[5 * 1024];
    // Creating an output stream using a FS in a separate method to make the
    // FS instance used eligible for GC. Since when a method is popped from
    // the stack frame, it's variables become anonymous, this creates higher
    // chance of getting Garbage collected.
    try (AbfsOutputStream out = getStream()) {
      // Every 5KB block written is flushed and a GC is hinted, if the
      // executor service is shut down in between, the test should fail
      // indicating premature shutdown while writing.
      for (int i = 0; i < 5; i++) {
        out.write(testBytes);
        out.flush();
        System.gc();
        Assertions.assertThat(
            out.getExecutorService().isShutdown() || out.getExecutorService()
                .isTerminated())
            .describedAs("Executor Service should not be closed before "
                + "OutputStream while writing")
            .isFalse();
        Assertions.assertThat(out.getFsBackRef().isNull())
            .describedAs("BackReference in output stream should not be null")
            .isFalse();
      }
    }
  }

  /**
   * Verify AbfsOutputStream close() behaviour of throwing a PathIOE when the
   * FS instance is closed before the stream.
   */
  @Test
  public void testAbfsOutputStreamClosingFsBeforeStream()
      throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    fs.initialize(new URI(getTestUrl()), new Configuration());
    Path pathFs = path(getMethodName());
    byte[] inputBytes = new byte[5 * 1024];
    try (AbfsOutputStream out = createAbfsOutputStreamWithFlushEnabled(fs,
        pathFs)) {
      out.write(inputBytes);
      fs.close();
      // verify that output stream close after fs.close() would raise a
      // pathIOE containing the path being written to.
      LambdaTestUtils
          .intercept(PathIOException.class, getMethodName(), out::close);
    }
  }

  /**
   * Separate method to create an outputStream using a local FS instance so
   * that once this method has returned, the FS instance can be eligible for GC.
   *
   * @return AbfsOutputStream used for writing.
   */
  private AbfsOutputStream getStream() throws URISyntaxException, IOException {
    AzureBlobFileSystem fs1 = new AzureBlobFileSystem();
    fs1.initialize(new URI(getTestUrl()), new Configuration());
    Path pathFs1 = path(getMethodName() + "1");

    return createAbfsOutputStreamWithFlushEnabled(fs1, pathFs1);
  }

}
