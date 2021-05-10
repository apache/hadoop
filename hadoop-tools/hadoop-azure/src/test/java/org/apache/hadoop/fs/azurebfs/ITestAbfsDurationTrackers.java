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
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_DELETE_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_GET_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_HEAD_REQUEST;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.HTTP_PUT_REQUEST;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.lookupMeanStatistic;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

public class ITestAbfsDurationTrackers extends AbstractAbfsIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsDurationTrackers.class);
  private static final AbfsStatistic[] HTTP_DURATION_TRACKER_LIST = {
      HTTP_HEAD_REQUEST,
      HTTP_GET_REQUEST,
      HTTP_DELETE_REQUEST,
      HTTP_PUT_REQUEST,
  };

  public ITestAbfsDurationTrackers() throws Exception {
  }

  /**
   * Test to check if DurationTrackers for Abfs HTTP calls work correctly and
   * track the duration of the http calls.
   */
  @Test
  public void testAbfsHttpCallsDurations() throws IOException {
    describe("test to verify if the DurationTrackers for abfs http calls "
        + "work as expected.");

    AzureBlobFileSystem fs = getFileSystem();
    Path testFilePath = path(getMethodName());

    // Declaring output and input stream.
    AbfsOutputStream out = null;
    AbfsInputStream in = null;
    try {
      // PUT the file.
      out = createAbfsOutputStreamWithFlushEnabled(fs, testFilePath);
      out.write('a');
      out.hflush();

      // GET the file.
      in = fs.getAbfsStore().openFileForRead(testFilePath, fs.getFsStatistics());
      int res = in.read();
      LOG.info("Result of Read: {}", res);

      // DELETE the file.
      fs.delete(testFilePath, false);

      // extract the IOStatistics from the filesystem.
      IOStatistics ioStatistics = extractStatistics(fs);
      LOG.info(ioStatisticsToPrettyString(ioStatistics));
      assertDurationTracker(ioStatistics);
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, in);
    }
  }

  /**
   * A method to assert that all the DurationTrackers for the http calls are
   * working correctly.
   *
   * @param ioStatistics the IOStatisticsSource in use.
   */
  private void assertDurationTracker(IOStatistics ioStatistics) {
    for (AbfsStatistic abfsStatistic : HTTP_DURATION_TRACKER_LIST) {
      Assertions.assertThat(lookupMeanStatistic(ioStatistics,
          abfsStatistic.getStatName() + StoreStatisticNames.SUFFIX_MEAN).mean())
          .describedAs("The DurationTracker Named " + abfsStatistic.getStatName()
                  + " Doesn't match the expected value.")
          .isGreaterThan(0.0);
    }
  }
}
