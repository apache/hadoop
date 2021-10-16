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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

/**
 * Test AbfsOutputStream statistics.
 */
public class ITestAbfsOutputStreamStatistics
    extends AbstractAbfsIntegrationTest {

  private static final int OPERATIONS = 10;
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsOutputStreamStatistics.class);

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
          getAbfsOutputStreamStatistics(outForSomeBytes);

      //Test for zero bytes To upload.
      assertEquals("Mismatch in bytes to upload", 0,
          abfsOutputStreamStatisticsForUploadBytes.getBytesToUpload());

      outForSomeBytes.write(testBytesToUpload.getBytes());
      outForSomeBytes.flush();
      abfsOutputStreamStatisticsForUploadBytes =
          getAbfsOutputStreamStatistics(outForSomeBytes);

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
          getAbfsOutputStreamStatistics(outForLargeBytes);

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
    if (fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(queueShrinkFilePath).toString())) {
      // writeOperationsQueue is not used for appendBlob, hence queueShrink is 0
      return;
    }

    try (AbfsOutputStream outForOneOp = createAbfsOutputStreamWithFlushEnabled(
        fs, queueShrinkFilePath)) {

      AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
          getAbfsOutputStreamStatistics(outForOneOp);

      //Test for shrinking queue zero time.
      assertEquals("Mismatch in queue shrunk operations", 0,
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
          getAbfsOutputStreamStatistics(outForLargeOps);
      /*
       * After a write operation is done, it is in a task queue where it is
       * removed. Hence, to get the correct expected value we get the size of
       * the task queue from AbfsOutputStream and subtract it with total
       * write operations done to get the number of queue shrinks done.
       *
       */
      assertEquals("Mismatch in queue shrunk operations",
          OPERATIONS - outForLargeOps.getWriteOperationsSize(),
          abfsOutputStreamStatistics.getQueueShrunkOps());
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
          getAbfsOutputStreamStatistics(outForOneOp);

      //Test for zero time writing buffer to service.
      assertEquals("Mismatch in write current buffer operations", 0,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());

      outForOneOp.write(testWriteBuffer.getBytes());
      outForOneOp.flush();

      abfsOutputStreamStatistics = getAbfsOutputStreamStatistics(outForOneOp);

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
          getAbfsOutputStreamStatistics(outForLargeOps);
      //Test for 10 times writing buffer to service.
      assertEquals("Mismatch in write current buffer operations",
          OPERATIONS,
          abfsOutputStreamStatistics.getWriteCurrentBufferOperations());
    }
  }

  /**
   * Test to check correct value of time spent on a PUT request in
   * AbfsOutputStream.
   */
  @Test
  public void testAbfsOutputStreamDurationTrackerPutRequest() throws IOException {
    describe("Testing to check if DurationTracker for PUT request is working "
        + "correctly.");
    AzureBlobFileSystem fs = getFileSystem();
    Path pathForPutRequest = path(getMethodName());

    try(AbfsOutputStream outputStream =
        createAbfsOutputStreamWithFlushEnabled(fs, pathForPutRequest)) {
      outputStream.write('a');
      outputStream.hflush();

      IOStatistics ioStatistics = extractStatistics(fs);
      LOG.info("AbfsOutputStreamStats info: {}",
          ioStatisticsToPrettyString(ioStatistics));
      Assertions.assertThat(
          lookupMeanStatistic(ioStatistics,
              AbfsStatistic.HTTP_PUT_REQUEST.getStatName()
                  + StoreStatisticNames.SUFFIX_MEAN).mean())
          .describedAs("Mismatch in timeSpentOnPutRequest DurationTracker")
          .isGreaterThan(0.0);
    }
  }

  /**
   * Method to get the AbfsOutputStream statistics.
   *
   * @param out AbfsOutputStream whose statistics is needed.
   * @return AbfsOutputStream statistics implementation class to get the
   * values of the counters.
   */
  private static AbfsOutputStreamStatisticsImpl getAbfsOutputStreamStatistics(
      AbfsOutputStream out) {
    return (AbfsOutputStreamStatisticsImpl) out.getOutputStreamStatistics();
  }
}
