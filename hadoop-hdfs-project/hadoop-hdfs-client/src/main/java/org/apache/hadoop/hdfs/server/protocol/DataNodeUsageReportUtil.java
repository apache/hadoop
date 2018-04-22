/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Time;

/**
 * This class is helper class to generate a live usage report by calculating
 * the delta between  current DataNode usage metrics and the usage metrics
 * captured at the time of the last report.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DataNodeUsageReportUtil {

  private long bytesWritten;
  private long bytesRead;
  private long writeTime;
  private long readTime;
  private long blocksWritten;
  private long blocksRead;
  private DataNodeUsageReport lastReport;

  public DataNodeUsageReport getUsageReport(long bWritten, long
      bRead, long wTime, long rTime, long wBlockOp, long
      rBlockOp, long timeSinceLastReport) {
    if (timeSinceLastReport == 0) {
      if (lastReport == null) {
        lastReport = DataNodeUsageReport.EMPTY_REPORT;
      }
      return lastReport;
    }
    DataNodeUsageReport.Builder builder = new DataNodeUsageReport.Builder();
    DataNodeUsageReport report = builder.setBytesWrittenPerSec(
        getBytesWrittenPerSec(bWritten, timeSinceLastReport))
        .setBytesReadPerSec(getBytesReadPerSec(bRead, timeSinceLastReport))
        .setWriteTime(getWriteTime(wTime))
        .setReadTime(getReadTime(rTime)).setBlocksWrittenPerSec(
            getWriteBlockOpPerSec(wBlockOp, timeSinceLastReport))
        .setBlocksReadPerSec(
            getReadBlockOpPerSec(rBlockOp, timeSinceLastReport))
        .setTimestamp(Time.monotonicNow()).build();

    // Save raw metrics
    this.bytesRead = bRead;
    this.bytesWritten = bWritten;
    this.blocksWritten = wBlockOp;
    this.blocksRead = rBlockOp;
    this.readTime = rTime;
    this.writeTime = wTime;
    lastReport = report;
    return report;
  }

  private long getBytesReadPerSec(long bRead, long
      timeInSec) {
    return (bRead - this.bytesRead) / timeInSec;
  }

  private long getBytesWrittenPerSec(long
      bWritten, long timeInSec) {
    return (bWritten - this.bytesWritten) / timeInSec;
  }

  private long getWriteBlockOpPerSec(
      long totalWriteBlocks, long timeInSec) {
    return (totalWriteBlocks - this.blocksWritten) / timeInSec;
  }

  private long getReadBlockOpPerSec(long totalReadBlockOp,
      long timeInSec) {
    return (totalReadBlockOp - this.blocksRead) / timeInSec;
  }

  private long getReadTime(long totalReadTime) {
    return totalReadTime - this.readTime;

  }

  private long getWriteTime(long totalWriteTime) {
    return totalWriteTime - this.writeTime;
  }

}
