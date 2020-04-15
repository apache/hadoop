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
  private static final int OPERATIONS = 10;

  public ITestAbfsOutputStreamStatistics() throws Exception {
  }

  /**
   * Tests to check bytes uploaded successfully in {@link AbfsOutputStream}.
   */
  @Test
  public void testAbfsOutputStreamUploadingBytes() throws IOException {
    describe("Testing bytes uploaded successfully by AbfsOutputSteam");
    final AzureBlobFileSystem fs = getFileSystem();
    Path uploadBytesFilePath = path(getMethodName());
    String testBytesToUpload = "bytes";

    try (
        AbfsOutputStream outForSomeBytes = createAbfsOutputStreamWithFlushEnabled(
            fs, uploadBytesFilePath)
    ) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatisticsForUploadBytes =
          outForSomeBytes.getOutputStreamStatistics();

      //Test for zero bytes To upload.
      assertEquals("Mismatch in bytes to upload", 0,
          abfsOutputStreamStatisticsForUploadBytes.getBytesToUpload());

      outForSomeBytes.write(testBytesToUpload.getBytes());
      outForSomeBytes.flush();
      abfsOutputStreamStatisticsForUploadBytes =
          outForSomeBytes.getOutputStreamStatistics();

      //Test for bytes to upload.
      assertEquals("Mismatch in bytes to upload",
          testBytesToUpload.getBytes().length,
          abfsOutputStreamStatisticsForUploadBytes.getBytesToUpload());

      //Test for successful bytes uploaded.
      assertEquals("Mismatch in successful bytes uploaded",
          testBytesToUpload.getBytes().length,
          abfsOutputStreamStatisticsForUploadBytes.getBytesUploadSuccessful());

    }

    try (
        AbfsOutputStream outForLargeBytes = createAbfsOutputStreamWithFlushEnabled(
            fs, uploadBytesFilePath)) {

      for (int i = 0; i < OPERATIONS; i++) {
        outForLargeBytes.write(testBytesToUpload.getBytes());
      }
      outForLargeBytes.flush();
      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForLargeBytes.getOutputStreamStatistics();

      //Test for bytes to upload.
      assertEquals("Mismatch in bytes to upload",
          OPERATIONS * (testBytesToUpload.getBytes().length),
          abfsOutputStreamStatistics.getBytesToUpload());

      //Test for successful bytes uploaded.
      assertEquals("Mismatch in successful bytes uploaded",
          OPERATIONS * (testBytesToUpload.getBytes().length),
          abfsOutputStreamStatistics.getBytesUploadSuccessful());

    }
  }

  /**
   * Tests to check correct values of queue shrunk operations in
   * AbfsOutputStream.
   *
   * After writing data, AbfsOutputStream doesn't upload the data until
   * flushed. Hence, flush() method is called after write() to test queue
   * shrink operations.
   */
  @Test
  public void testAbfsOutputStreamQueueShrink() throws IOException {
    describe("Testing queue shrink operations by AbfsOutputStream");
    final AzureBlobFileSystem fs = getFileSystem();
    Path queueShrinkFilePath = path(getMethodName());
    String testQueueShrink = "testQueue";

    try (AbfsOutputStream outForOneOp = createAbfsOutputStreamWithFlushEnabled(
        fs, queueShrinkFilePath)) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForOneOp.getOutputStreamStatistics();

      //Test for shrinking Queue zero time.
      assertEquals("Mismatch in queue shrunk operations", 0,
          abfsOutputStreamStatistics.getQueueShrunkOps());

      outForOneOp.write(testQueueShrink.getBytes());
      //Queue is shrunk 2 times when outputStream is flushed.
      outForOneOp.flush();

      abfsOutputStreamStatistics = outForOneOp.getOutputStreamStatistics();

      //Test for shrinking Queue 2 times.
      assertEquals("Mismatch in queue shrunk operations", 2,
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
            fs, queueShrinkFilePath)) {
      for (int i = 0; i < OPERATIONS; i++) {
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
      assertEquals("Mismatch in queue shrunk operations",
          2 * OPERATIONS, abfsOutputStreamStatistics.getQueueShrunkOps());
    }

  }

  /**
   * Tests to check correct values of write current buffer operations done by
   * AbfsOutputStream.
   *
   * After writing data, AbfsOutputStream doesn't upload data till flush() is
   * called. Hence, flush() calls were made after write().
   */
  @Test
  public void testAbfsOutputStreamWriteBuffer() throws IOException {
    describe("Testing write current buffer operations by AbfsOutputStream");
    final AzureBlobFileSystem fs = getFileSystem();
    Path writeBufferFilePath = path(getMethodName());
    String testWriteBuffer = "Buffer";

    try (AbfsOutputStream outForOneOp = createAbfsOutputStreamWithFlushEnabled(
        fs, writeBufferFilePath)) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForOneOp.getOutputStreamStatistics();

      //Test for zero time writing buffer to service.
      assertEquals("Mismatch in write current buffer operations", 0,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());

      outForOneOp.write(testWriteBuffer.getBytes());
      outForOneOp.flush();

      abfsOutputStreamStatistics = outForOneOp.getOutputStreamStatistics();

      //Test for one time writing buffer to service.
      assertEquals("Mismatch in write current buffer operations", 1,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());
    }

    try (
        AbfsOutputStream outForLargeOps = createAbfsOutputStreamWithFlushEnabled(
            fs, writeBufferFilePath)) {

      /*
       * Need to flush each time after we write to actually write the data
       * into the data store and thus, get the writeCurrentBufferToService()
       * method triggered and increment the statistic.
       */
      for (int i = 0; i < OPERATIONS; i++) {
        outForLargeOps.write(testWriteBuffer.getBytes());
        outForLargeOps.flush();
      }
      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          outForLargeOps.getOutputStreamStatistics();
      //Test for 10 times writing buffer to service.
      assertEquals("Mismatch in write current buffer operations",
          OPERATIONS,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());
    }
  }
}
