/**
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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;


/**
 * A simple benchmark to find out the difference in speed between block
 * and page blobs.
 */
public class ITestBlobTypeSpeedDifference extends AbstractWasbTestBase {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  /**
   * Writes data to the given stream of the given size, flushing every
   * x bytes.
   */
  private static void writeTestFile(OutputStream writeStream,
      long size, long flushInterval) throws IOException {
    int bufferSize = (int) Math.min(1000, flushInterval);
    byte[] buffer = new byte[bufferSize];
    Arrays.fill(buffer, (byte) 7);
    int bytesWritten = 0;
    int bytesUnflushed = 0;
    while (bytesWritten < size) {
      int numberToWrite = (int) Math.min(bufferSize, size - bytesWritten);
      writeStream.write(buffer, 0, numberToWrite);
      bytesWritten += numberToWrite;
      bytesUnflushed += numberToWrite;
      if (bytesUnflushed >= flushInterval) {
        writeStream.flush();
        bytesUnflushed = 0;
      }
    }
  }

  private static class TestResult {
    final long timeTakenInMs;
    final long totalNumberOfRequests;

    TestResult(long timeTakenInMs, long totalNumberOfRequests) {
      this.timeTakenInMs = timeTakenInMs;
      this.totalNumberOfRequests = totalNumberOfRequests;
    }
  }

  /**
   * Writes data to the given file of the given size, flushing every
   * x bytes. Measure performance of that and return it.
   */
  private static TestResult writeTestFile(NativeAzureFileSystem fs, Path path,
      long size, long flushInterval) throws IOException {
    AzureFileSystemInstrumentation instrumentation =
        fs.getInstrumentation();
    long initialRequests = instrumentation.getCurrentWebResponses();
    Date start = new Date();
    OutputStream output = fs.create(path);
    writeTestFile(output, size, flushInterval);
    output.close();
    long finalRequests = instrumentation.getCurrentWebResponses();
    return new TestResult(new Date().getTime() - start.getTime(),
        finalRequests - initialRequests);
  }

  /**
   * Writes data to a block blob of the given size, flushing every
   * x bytes. Measure performance of that and return it.
   */
  private static TestResult writeBlockBlobTestFile(NativeAzureFileSystem fs,
      long size, long flushInterval) throws IOException {
    return writeTestFile(fs, new Path("/blockBlob"), size, flushInterval);
  }

  /**
   * Writes data to a page blob of the given size, flushing every
   * x bytes. Measure performance of that and return it.
   */
  private static TestResult writePageBlobTestFile(NativeAzureFileSystem fs,
      long size, long flushInterval) throws IOException {
    Path testFile = AzureTestUtils.blobPathForTests(fs,
        "writePageBlobTestFile");
    return writeTestFile(fs,
        testFile,
        size, flushInterval);
  }

  /**
   * Runs the benchmark over a small 10 KB file, flushing every 500 bytes.
   */
  @Test
  public void testTenKbFileFrequentFlush() throws Exception {
    testForSizeAndFlushInterval(getFileSystem(), 10 * 1000, 500);
  }

  /**
   * Runs the benchmark for the given file size and flush frequency.
   */
  private static void testForSizeAndFlushInterval(NativeAzureFileSystem fs,
      final long size, final long flushInterval) throws IOException {
    for (int i = 0; i < 5; i++) {
      TestResult pageBlobResults = writePageBlobTestFile(fs, size, flushInterval);
      System.out.printf(
          "Page blob upload took %d ms. Total number of requests: %d.\n",
          pageBlobResults.timeTakenInMs, pageBlobResults.totalNumberOfRequests);
      TestResult blockBlobResults = writeBlockBlobTestFile(fs, size, flushInterval);
      System.out.printf(
          "Block blob upload took %d ms. Total number of requests: %d.\n",
          blockBlobResults.timeTakenInMs, blockBlobResults.totalNumberOfRequests);
    }
  }

  /**
   * Runs the benchmark for the given file size and flush frequency from the
   * command line.
   */
  public static void main(String[] argv) throws Exception {
    Configuration conf = new Configuration();
    long size = 10 * 1000 * 1000;
    long flushInterval = 2000;
    if (argv.length > 0) {
      size = Long.parseLong(argv[0]);
    }
    if (argv.length > 1) {
      flushInterval = Long.parseLong(argv[1]);
    }
    testForSizeAndFlushInterval(
        (NativeAzureFileSystem) FileSystem.get(conf),
        size,
        flushInterval);
  }
}
