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
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCleanerMetrics {

  Configuration conf = new Configuration();
  CleanerMetrics cleanerMetrics;

  @BeforeEach
  public void init() {
    cleanerMetrics = CleanerMetrics.getInstance();
  }

  @Test
  void testMetricsOverMultiplePeriods() {
    simulateACleanerRun();
    assertMetrics(4, 4, 1, 1);
    simulateACleanerRun();
    assertMetrics(4, 8, 1, 2);
  }

  public void simulateACleanerRun() {
    cleanerMetrics.reportCleaningStart();
    cleanerMetrics.reportAFileProcess();
    cleanerMetrics.reportAFileDelete();
    cleanerMetrics.reportAFileProcess();
    cleanerMetrics.reportAFileProcess();
  }

  void assertMetrics(int proc, int totalProc, int del, int totalDel) {
    assertEquals(
        proc,
        cleanerMetrics.getProcessedFiles(),
        "Processed files in the last period are not measured correctly");
    assertEquals(totalProc, cleanerMetrics.getTotalProcessedFiles(),
        "Total processed files are not measured correctly");
    assertEquals(del, cleanerMetrics.getDeletedFiles(),
        "Deleted files in the last period are not measured correctly");
    assertEquals(totalDel, cleanerMetrics.getTotalDeletedFiles(),
        "Total deleted files are not measured correctly");
  }
}
