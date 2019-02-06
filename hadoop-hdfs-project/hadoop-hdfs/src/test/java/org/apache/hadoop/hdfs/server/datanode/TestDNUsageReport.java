/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReport;
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReportUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link DataNodeUsageReport}.
 */
public class TestDNUsageReport {

  private DataNodeUsageReportUtil dnUsageUtil;
  private long bytesWritten;
  private long bytesRead;
  private long writeTime;
  private long readTime;
  private long writeBlock;
  private long readBlock;
  private long timeSinceLastReport;

  @Before
  public void setup() throws IOException {
    dnUsageUtil = new DataNodeUsageReportUtil();
  }

  @After
  public void clear() throws IOException {
    dnUsageUtil = null;
  }

  /**
   * Ensure that storage type and storage state are propagated
   * in Storage Reports.
   */
  @Test(timeout = 60000)
  public void testUsageReport() throws IOException {

    // Test1
    DataNodeUsageReport report = dnUsageUtil.getUsageReport(0,
        0, 0, 0, 0, 0, 0);
    Assert.assertEquals(report, DataNodeUsageReport.EMPTY_REPORT);

    // Test2
    bytesWritten = 200;
    bytesRead = 200;
    writeTime = 50;
    readTime = 50;
    writeBlock = 20;
    readBlock = 10;
    timeSinceLastReport = 5;
    report = dnUsageUtil.getUsageReport(bytesWritten,
        bytesRead, writeTime, readTime, writeBlock, readBlock,
        timeSinceLastReport);

    Assert.assertEquals(bytesWritten / timeSinceLastReport,
        report.getBytesWrittenPerSec());
    Assert.assertEquals(bytesRead / timeSinceLastReport,
        report.getBytesReadPerSec());
    Assert.assertEquals(writeTime, report.getWriteTime());
    Assert.assertEquals(readTime, report.getReadTime());
    Assert.assertEquals(writeBlock / timeSinceLastReport,
        report.getBlocksWrittenPerSec());
    Assert.assertEquals(readBlock / timeSinceLastReport,
        report.getBlocksReadPerSec());

    // Test3
    DataNodeUsageReport report2 = dnUsageUtil.getUsageReport(bytesWritten,
        bytesRead, writeTime, readTime, writeBlock, readBlock,
        0);
    Assert.assertEquals(report, report2);

    // Test4
    long bytesWritten2 = 50000;
    long bytesRead2 = 40000;
    long writeTime2 = 5000;
    long readTime2 = 1500;
    long writeBlock2 = 1000;
    long readBlock2 = 200;
    timeSinceLastReport = 60;
    report2 = dnUsageUtil.getUsageReport(bytesWritten2,
        bytesRead2, writeTime2, readTime2, writeBlock2, readBlock2,
        timeSinceLastReport);

    Assert.assertEquals((bytesWritten2 - bytesWritten) / timeSinceLastReport,
        report2.getBytesWrittenPerSec());
    Assert.assertEquals((bytesRead2 - bytesRead) / timeSinceLastReport,
        report2.getBytesReadPerSec());
    Assert.assertEquals(writeTime2 - writeTime, report2.getWriteTime());
    Assert.assertEquals(readTime2 - readTime, report2.getReadTime());
    Assert.assertEquals((writeBlock2 - writeBlock) / timeSinceLastReport,
        report2.getBlocksWrittenPerSec());
    Assert.assertEquals((readBlock2 - readBlock) / timeSinceLastReport,
        report2.getBlocksReadPerSec());
  }
}
