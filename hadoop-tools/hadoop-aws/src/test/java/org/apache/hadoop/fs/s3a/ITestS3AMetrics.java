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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test s3a performance metrics register and output.
 */
public class ITestS3AMetrics extends AbstractS3ATestBase {

  @Test
  public void testMetricsRegister()
      throws IOException, InterruptedException {
    S3AFileSystem fs = getFileSystem();
    Path dest = path("testMetricsRegister");
    ContractTestUtils.touch(fs, dest);

    String targetMetricSource = "S3AMetrics1" + "-" + fs.getBucket();
    assertNotNull("No metrics under test fs for " + targetMetricSource,
        fs.getInstrumentation().getMetricsSystem()
            .getSource(targetMetricSource));

    MutableCounterLong fileCreated =
        (MutableCounterLong) fs.getInstrumentation().getRegistry()
            .get(Statistic.FILES_CREATED.getSymbol());
    assertEquals("Metrics system should report single file created event",
        1, fileCreated.value());
  }

  @Test
  public void testStreamStatistics() throws IOException {
    S3AFileSystem fs = getFileSystem();
    Path file = path("testStreamStatistics");
    byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
    ContractTestUtils.createFile(fs, file, false, data);

    try (InputStream inputStream = fs.open(file)) {
      while (inputStream.read(data) != -1) {
        LOG.debug("Read batch of data from input stream...");
      }
    }

    MutableCounterLong read = (MutableCounterLong)
        fs.getInstrumentation().getRegistry()
        .get(Statistic.STREAM_SEEK_BYTES_READ.getSymbol());
    assertEquals("Stream statistics were not merged", 26, read.value());
  }


}
