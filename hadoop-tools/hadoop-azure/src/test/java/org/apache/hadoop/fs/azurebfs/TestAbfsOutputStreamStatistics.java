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

import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;

/**
 * Unit tests for AbfsOutputStream statistics.
 */
public class TestAbfsOutputStreamStatistics
    extends AbstractAbfsIntegrationTest {

  private static final int LOW_RANGE_FOR_RANDOM_VALUE = 49;
  private static final int HIGH_RANGE_FOR_RANDOM_VALUE = 9999;
  private static final int OPERATIONS = 10;

  public TestAbfsOutputStreamStatistics() throws Exception {
  }

  /**
   * Tests to check number of bytes failed to upload in
   * {@link AbfsOutputStream}.
   */
  @Test
  public void testAbfsOutputStreamBytesFailed() {
    describe("Testing number of bytes failed during upload in AbfsOutputSteam");

    AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
        new AbfsOutputStreamStatisticsImpl();

    //Test for zero bytes uploaded.
    assertEquals(0, abfsOutputStreamStatistics.getBytesUploadFailed(),
        "Mismatch in number of bytes failed to upload");

    //Populating small random value for bytesFailed.
    int randomBytesFailed = new Random().nextInt(LOW_RANGE_FOR_RANDOM_VALUE);
    abfsOutputStreamStatistics.uploadFailed(randomBytesFailed);
    //Test for bytes failed to upload.
    assertEquals(randomBytesFailed, abfsOutputStreamStatistics.getBytesUploadFailed(),
        "Mismatch in number of bytes failed to upload");

    //Reset statistics for the next test.
    abfsOutputStreamStatistics = new AbfsOutputStreamStatisticsImpl();

    /*
     * Entering multiple random values for bytesFailed to check correct
     * summation of values.
     */
    int expectedBytesFailed = 0;
    for (int i = 0; i < OPERATIONS; i++) {
      randomBytesFailed = new Random().nextInt(HIGH_RANGE_FOR_RANDOM_VALUE);
      abfsOutputStreamStatistics.uploadFailed(randomBytesFailed);
      expectedBytesFailed += randomBytesFailed;
    }
    //Test for bytes failed to upload.
    assertEquals(expectedBytesFailed, abfsOutputStreamStatistics.getBytesUploadFailed(),
        "Mismatch in number of bytes failed to upload");
  }

  /**
   * Tests to check time spent on waiting for tasks to be complete on a
   * blocking queue in {@link AbfsOutputStream}.
   */
  @Test
  public void testAbfsOutputStreamTimeSpentOnWaitTask() {
    describe("Testing time Spent on waiting for task to be completed in "
        + "AbfsOutputStream");

    AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
        new AbfsOutputStreamStatisticsImpl();

    //Test for initial value of timeSpentWaitTask.
    assertEquals(0, abfsOutputStreamStatistics.getTimeSpentOnTaskWait(),
        "Mismatch in time spent on waiting for tasks to complete");

    abfsOutputStreamStatistics
        .timeSpentTaskWait();
    //Test for one op call value of timeSpentWaitTask.
    assertEquals(1, abfsOutputStreamStatistics.getTimeSpentOnTaskWait(),
        "Mismatch in time spent on waiting for tasks to complete");

    //Reset statistics for the next test.
    abfsOutputStreamStatistics = new AbfsOutputStreamStatisticsImpl();

    /*
     * Entering multiple values for timeSpentTaskWait() to check the
     * summation is happening correctly. Also calculating the expected result.
     */
    for (int i = 0; i < OPERATIONS; i++) {
       abfsOutputStreamStatistics.timeSpentTaskWait();
    }

    /*
     * Test to check correct value of timeSpentTaskWait after OPERATIONS
     * number of op calls.
     */
    assertEquals(OPERATIONS, abfsOutputStreamStatistics.getTimeSpentOnTaskWait(),
        "Mismatch in time spent on waiting for tasks to complete");
  }

  /**
   * Unit Tests to check correct values of queue shrunk operations in
   * AbfsOutputStream.
   *
   */
  @Test
  public void testAbfsOutputStreamQueueShrink() {
    describe("Testing queue shrink operations by AbfsOutputStream");

    AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
        new AbfsOutputStreamStatisticsImpl();

    //Test for shrinking queue zero time.
    assertEquals(0, abfsOutputStreamStatistics.getQueueShrunkOps(),
        "Mismatch in queue shrunk operations");

    abfsOutputStreamStatistics.queueShrunk();

    //Test for shrinking queue 1 time.
    assertEquals(1, abfsOutputStreamStatistics.getQueueShrunkOps(),
        "Mismatch in queue shrunk operations");

    //Reset statistics for the next test.
    abfsOutputStreamStatistics = new AbfsOutputStreamStatisticsImpl();

    /*
     * Entering random values for queueShrunkOps and checking the correctness
     * of summation for the statistic.
     */
    int randomQueueValues = new Random().nextInt(HIGH_RANGE_FOR_RANDOM_VALUE);
    for (int i = 0; i < randomQueueValues * OPERATIONS; i++) {
      abfsOutputStreamStatistics.queueShrunk();
    }
    /*
     * Test for random times incrementing queue shrunk operations.
     */
    assertEquals(randomQueueValues * OPERATIONS, abfsOutputStreamStatistics.getQueueShrunkOps(),
        "Mismatch in queue shrunk operations");
  }
}
