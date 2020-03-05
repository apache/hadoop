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
import java.util.Random;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;

/**
 * Unit Tests for AbfsOutputStream Statistics.
 */
public class TestAbfsOutputStreamStatistics
    extends AbstractAbfsIntegrationTest {

  private static final int LOW_RANGE_FOR_RANDOM_VALUE = 49;
  private static final int HIGH_RANGE_FOR_RANDOM_VALUE = 9999;

  public TestAbfsOutputStreamStatistics() throws Exception {
  }

  /**
   * Tests to check bytes failed to Upload in {@link AbfsOutputStream}.
   *
   * @throws IOException
   */
  @Test
  public void testAbfsOutputStreamBytesFailed() {
    describe("Testing Bytes Failed during uploading in AbfsOutputSteam");

    AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
        new AbfsOutputStreamStatisticsImpl();

    //Test for zero bytes uploaded.
    assertValues("number fo bytes failed to upload", 0,
        abfsOutputStreamStatistics.getBytesUploadFailed());

    //Populating small random value for bytesFailed.
    int randomBytesFailed = new Random().nextInt(LOW_RANGE_FOR_RANDOM_VALUE);
    abfsOutputStreamStatistics.uploadFailed(randomBytesFailed);
    //Test for bytes failed to upload.
    assertValues("number fo bytes failed to upload", randomBytesFailed,
        abfsOutputStreamStatistics.getBytesUploadFailed());

    //Initializing again to reset the statistics.
    abfsOutputStreamStatistics = new AbfsOutputStreamStatisticsImpl();

    //Populating large random values for bytesFailed.
    randomBytesFailed = new Random().nextInt(HIGH_RANGE_FOR_RANDOM_VALUE);
    abfsOutputStreamStatistics.uploadFailed(randomBytesFailed);
    //Test for bytes failed to upload.
    assertValues("number fo bytes failed to upload", randomBytesFailed,
        abfsOutputStreamStatistics.getBytesUploadFailed());
  }

  /**
   * Tests to check time spent on waiting for tasks to be complete on a
   * blocking queue in {@link AbfsOutputStream}.
   *
   * @throws IOException
   */
  @Test
  public void testAbfsOutputStreamTimeSpentOnWaitTask() {
    describe("Testing Time Spend on Waiting for Task to be complete");

    AbfsOutputStreamStatisticsImpl abfsOutputStreamStatistics =
        new AbfsOutputStreamStatisticsImpl();

    //Test for initial value of timeSpentWaitTask.
    assertValues("Time spend on waiting for tasks to complete", 0,
        abfsOutputStreamStatistics.getTimeSpendOnTaskWait());

    int smallRandomStartTime =
        new Random().nextInt(LOW_RANGE_FOR_RANDOM_VALUE);
    int smallRandomEndTime =
        new Random().nextInt(LOW_RANGE_FOR_RANDOM_VALUE)
            + smallRandomStartTime;
    int smallDiff = smallRandomEndTime - smallRandomStartTime;
    abfsOutputStreamStatistics
        .timeSpentTaskWait(smallRandomStartTime, smallRandomEndTime);
    //Test for small random value of timeSpentWaitTask.
    assertValues("Time spend on waiting for tasks to complete", smallDiff,
        abfsOutputStreamStatistics.getTimeSpendOnTaskWait());

    int largeRandomStartTime =
        new Random().nextInt(HIGH_RANGE_FOR_RANDOM_VALUE);
    int largeRandomEndTime = new Random().nextInt(HIGH_RANGE_FOR_RANDOM_VALUE)
        + largeRandomStartTime;
    int randomDiff = largeRandomEndTime - largeRandomStartTime;
    abfsOutputStreamStatistics
        .timeSpentTaskWait(largeRandomStartTime, largeRandomEndTime);
      /*
      Test for large random value of timeSpentWaitTask plus the time spent
      waiting in previous test.
       */
    assertValues("Time spend on waiting for tasks to complete",
        smallDiff + randomDiff,
        abfsOutputStreamStatistics.getTimeSpendOnTaskWait());
  }

}