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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;

/**
 * Test AbfsOutputStream statistics.
 */
public class ITestAbfsOutputStreamStatistics
    extends AbstractAbfsIntegrationTest {
  private static final int LARGE_OPERATIONS = 10;

  public ITestAbfsOutputStreamStatistics() throws Exception {
  }

  /**
   * Tests to check bytes Uploaded successfully in {@link AbfsOutputStream}.
   *
   * @throws IOException
   */
  @Test
  public void testAbfsOutputStreamUploadingBytes() throws IOException {
    describe("Testing Bytes uploaded successfully in AbfsOutputSteam");
    final AzureBlobFileSystem fs = getFileSystem();
    Path uploadBytesFilePath = path(getMethodName());
    String testBytesToUpload = "bytes";

    try (
        AbfsOutputStream outForSomeBytes = createAbfsOutputStreamWithFlushEnabled(
            fs,
            uploadBytesFilePath)
    ) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatisticsForUploadBytes =
          outForSomeBytes.getOutputStreamStatistics();

      //Test for zero bytes To upload.
      assertValues("bytes to upload", 0,
          abfsOutputStreamStatisticsForUploadBytes.getBytesToUpload());

      outForSomeBytes.write(testBytesToUpload.getBytes());
      outForSomeBytes.flush();
      abfsOutputStreamStatisticsForUploadBytes =
          outForSomeBytes.getOutputStreamStatistics();

      //Test for bytes to upload.
      assertValues("bytes to upload", testBytesToUpload.getBytes().length,
          abfsOutputStreamStatisticsForUploadBytes.getBytesToUpload());

      //Test for successful bytes uploaded.
      assertValues("successful bytes uploaded",
          testBytesToUpload.getBytes().length,
          abfsOutputStreamStatisticsForUploadBytes.getBytesUploadSuccessful());

    }

    try (
        AbfsOutputStream outForLargeBytes = createAbfsOutputStreamWithFlushEnabled(
            fs,
            uploadBytesFilePath)) {

      for (int i = 0; i < LARGE_OPERATIONS; i++) {
        outForLargeBytes.write(testBytesToUpload.getBytes());
      }
      outForLargeBytes.flush();
      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForLargeBytes.getOutputStreamStatistics();

      //Test for bytes to upload.
      assertValues("bytes to upload",
          LARGE_OPERATIONS * (testBytesToUpload.getBytes().length),
          abfsOutputStreamStatistics.getBytesToUpload());

      //Test for successful bytes uploaded.
      assertValues("successful bytes uploaded",
          LARGE_OPERATIONS * (testBytesToUpload.getBytes().length),
          abfsOutputStreamStatistics.getBytesUploadSuccessful());

    }
  }

  /**
   * Tests to check number of {@code
   * AbfsOutputStream#shrinkWriteOperationQueue()} calls.
   * After writing data, AbfsOutputStream doesn't upload the data until
   * Flushed. Hence, flush() method is called after write() to test Queue
   * shrink calls.
   *
   * @throws IOException
   */
  @Test
  public void testAbfsOutputStreamQueueShrink() throws IOException {
    describe("Testing Queue Shrink calls in AbfsOutputStream");
    final AzureBlobFileSystem fs = getFileSystem();
    Path queueShrinkFilePath = path(getMethodName());
    String testQueueShrink = "testQueue";

    try (AbfsOutputStream outForOneOp = createAbfsOutputStreamWithFlushEnabled(
        fs,
        queueShrinkFilePath)) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForOneOp.getOutputStreamStatistics();

      //Test for shrinking Queue zero time.
      assertValues("Queue shrunk operations", 0,
          abfsOutputStreamStatistics.getQueueShrunkOps());

      outForOneOp.write(testQueueShrink.getBytes());
      // Queue is shrunk 2 times when outputStream is flushed.
      outForOneOp.flush();

      abfsOutputStreamStatistics = outForOneOp.getOutputStreamStatistics();

      //Test for shrinking Queue 2 times.
      assertValues("Queue shrunk operations", 2,
          abfsOutputStreamStatistics.getQueueShrunkOps());

    }

    /*
     * After writing in the loop we flush inside the loop to ensure the write
     * operation done in that loop is considered to be done which would help
     * us triggering the shrinkWriteOperationQueue() method each time after
     * the write operation.
     * If we call flush outside the loop, then it will take all the write
     * operations inside the loop as one write operation.
     *
     */
    try (
        AbfsOutputStream outForLargeOps = createAbfsOutputStreamWithFlushEnabled(
            fs,
            queueShrinkFilePath)) {
      for (int i = 0; i < LARGE_OPERATIONS; i++) {
        outForLargeOps.write(testQueueShrink.getBytes());
        outForLargeOps.flush();
      }

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForLargeOps.getOutputStreamStatistics();

      /*
       * After each write operation we trigger the shrinkWriteOperationQueue
       * () method 2 times. Hence, after running the write operations 10
       * times inside the loop. We expect 20(2 * number_of_operations)
       * shrinkWriteOperationQueue() calls.
       */
      assertValues("Queue shrunk operations",
          2 * LARGE_OPERATIONS,
          abfsOutputStreamStatistics.getQueueShrunkOps());
    }

  }

  /**
   * Test to check number of {@code
   * AbfsOutputStream#writeCurrentBufferToService()} calls.
   * After writing data, AbfsOutputStream doesn't upload data till flush() is
   * called. Hence, flush() calls were made after write().
   *
   * @throws IOException
   */
  @Test
  public void testAbfsOutputStreamWriteBuffer() throws IOException {
    describe("Testing writeCurrentBufferToService() calls");
    final AzureBlobFileSystem fs = getFileSystem();
    Path writeBufferFilePath = path(getMethodName());
    String testWriteBuffer = "Buffer";

    try (AbfsOutputStream outForOneOp = createAbfsOutputStreamWithFlushEnabled(
        fs,
        writeBufferFilePath)) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForOneOp.getOutputStreamStatistics();

      //Test for zero time writing Buffer to service.
      assertValues("number writeCurrentBufferToService() calls", 0,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());

      outForOneOp.write(testWriteBuffer.getBytes());
      outForOneOp.flush();

      abfsOutputStreamStatistics = outForOneOp.getOutputStreamStatistics();

      //Test for one time writeCurrentBuffer() call.
      assertValues("number writeCurrentBufferToService() calls", 1,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());
    }

    try (
        AbfsOutputStream outForLargeOps = createAbfsOutputStreamWithFlushEnabled(
            fs,
            writeBufferFilePath)) {

      for (int i = 0; i < LARGE_OPERATIONS; i++) {
        outForLargeOps.write(testWriteBuffer.getBytes());
        outForLargeOps.flush();
      }
      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForLargeOps.getOutputStreamStatistics();
      //Test for 10 writeBufferOperations.
      assertValues("number of writeCurrentBufferToService() calls",
          LARGE_OPERATIONS,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());
    }

  }

}
