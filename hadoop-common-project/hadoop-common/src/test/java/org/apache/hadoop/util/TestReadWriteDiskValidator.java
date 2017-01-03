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

package org.apache.hadoop.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecords;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * The class to test {@link ReadWriteDiskValidator} and
 * {@link ReadWriteDiskValidatorMetrics}.
 */
public class TestReadWriteDiskValidator {

  private MetricsSystem ms;

  @Before
  public void setUp() {
    ms = DefaultMetricsSystem.instance();
  }

  @Test
  public void testReadWriteDiskValidator()
      throws DiskErrorException, InterruptedException {
    int count = 100;
    File testDir = new File(System.getProperty("test.build.data"));
    ReadWriteDiskValidator readWriteDiskValidator =
        (ReadWriteDiskValidator) DiskValidatorFactory.getInstance(
            ReadWriteDiskValidator.NAME);

    for (int i = 0; i < count; i++) {
      readWriteDiskValidator.checkStatus(testDir);
    }

    ReadWriteDiskValidatorMetrics metric =
        ReadWriteDiskValidatorMetrics.getMetric(testDir.toString());
    Assert.assertEquals("The count number of estimator in MutableQuantiles"
        + "metrics of file read is not right",
        metric.getFileReadQuantiles()[0].getEstimator().getCount(), count);

    Assert.assertEquals("The count number of estimator in MutableQuantiles"
        + "metrics of file write is not right",
        metric.getFileWriteQuantiles()[0].getEstimator().getCount(),
        count);

    MetricsSource source = ms.getSource(
        ReadWriteDiskValidatorMetrics.sourceName(testDir.toString()));
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);

    MetricsRecords.assertMetric(collector.getRecords().get(0),
        "FailureCount", 0);
    MetricsRecords.assertMetric(collector.getRecords().get(0),
        "LastFailureTime", (long)0);

    // All MutableQuantiles haven't rolled over yet because the minimum
    // interval is 1 hours, so we just test if these metrics exist.
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "WriteLatency3600sNumOps");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "WriteLatency3600s50thPercentileLatencyMicros");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "WriteLatency86400sNumOps");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "WriteLatency864000sNumOps");

    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "ReadLatency3600sNumOps");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "ReadLatency3600s50thPercentileLatencyMicros");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "ReadLatency86400sNumOps");
    MetricsRecords.assertMetricNotNull(collector.getRecords().get(0),
        "ReadLatency864000sNumOps");
  }

  @Test
  public void testCheckFailures() throws Throwable {
    ReadWriteDiskValidator readWriteDiskValidator =
        (ReadWriteDiskValidator) DiskValidatorFactory.getInstance(
            ReadWriteDiskValidator.NAME);

    // create a temporary test directory under the system test directory
    File testDir = Files.createTempDirectory(
        Paths.get(System.getProperty("test.build.data")), "test").toFile();

    try {
      Shell.execCommand(Shell.getSetPermissionCommand("000", false,
          testDir.getAbsolutePath()));
    } catch (Exception e){
      testDir.delete();
      throw e;
    }

    try {
      readWriteDiskValidator.checkStatus(testDir);
      fail("Disk check should fail.");
    } catch (DiskErrorException e) {
      assertTrue(e.getMessage().equals("Disk Check failed!"));
    }

    MetricsSource source = ms.getSource(
        ReadWriteDiskValidatorMetrics.sourceName(testDir.toString()));
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    source.getMetrics(collector, true);

    try {
      readWriteDiskValidator.checkStatus(testDir);
      fail("Disk check should fail.");
    } catch (DiskErrorException e) {
      assertTrue(e.getMessage().equals("Disk Check failed!"));
    }

    source.getMetrics(collector, true);

    // verify the first metrics record
    MetricsRecords.assertMetric(collector.getRecords().get(0),
        "FailureCount", 1);
    Long lastFailureTime1 = (Long) MetricsRecords.getMetricValueByName(
        collector.getRecords().get(0), "LastFailureTime");

    // verify the second metrics record
    MetricsRecords.assertMetric(collector.getRecords().get(1),
        "FailureCount", 2);
    Long lastFailureTime2 = (Long) MetricsRecords.getMetricValueByName(
        collector.getRecords().get(1), "LastFailureTime");
    assertTrue("The first failure time should be less than the second one",
        lastFailureTime1 < lastFailureTime2);

    testDir.delete();
  }
}
