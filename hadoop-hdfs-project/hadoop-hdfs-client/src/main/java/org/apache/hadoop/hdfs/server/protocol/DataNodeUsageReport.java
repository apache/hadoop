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

/**
 * A class that allows DataNode to communicate information about
 * usage statistics/metrics to NameNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DataNodeUsageReport {

  private long bytesWrittenPerSec;
  private long bytesReadPerSec;
  private long writeTime;
  private long readTime;
  private long blocksWrittenPerSec;
  private long blocksReadPerSec;
  private long timestamp;

  DataNodeUsageReport() {
  }

  private DataNodeUsageReport(Builder builder) {
    this.bytesWrittenPerSec = builder.bytesWrittenPerSec;
    this.bytesReadPerSec = builder.bytesReadPerSec;
    this.writeTime = builder.writeTime;
    this.readTime = builder.readTime;
    this.blocksWrittenPerSec = builder.blocksWrittenPerSec;
    this.blocksReadPerSec = builder.blocksReadPerSec;
    this.timestamp = builder.timestamp;
  }

  /**
   * An object representing a DataNodeUsageReport with default values. Should
   * be used instead of null or creating new objects when there are
   * no statistics to report.
   */
  public static final DataNodeUsageReport EMPTY_REPORT =
      new DataNodeUsageReport();

  @Override
  public String toString() {
    return "bytesWrittenPerSec:" + bytesWrittenPerSec + " "
        + " bytesReadPerSec:"
        + bytesReadPerSec + " writeTime:" + writeTime + " readTime:" + readTime
        + " blocksWrittenPerSec:" + blocksWrittenPerSec + " blocksReadPerSec:" +
        blocksReadPerSec + " timestamp:" + timestamp;
  }

  @Override
  public int hashCode() {
    return (int) (timestamp + bytesWrittenPerSec + bytesReadPerSec + writeTime
        + readTime + blocksWrittenPerSec + blocksReadPerSec);
  }

  @Override
  public boolean equals(Object o) {
    // If the object is compared with itself then return true
    if (o == this) {
      return true;
    }

    if (!(o instanceof DataNodeUsageReport)) {
      return false;
    }

    DataNodeUsageReport c = (DataNodeUsageReport) o;
    return this.timestamp == c.timestamp
        && this.readTime == c.readTime
        && this.writeTime == c.writeTime
        && this.bytesWrittenPerSec == c.bytesWrittenPerSec
        && this.bytesReadPerSec == c.bytesReadPerSec
        && this.blocksWrittenPerSec == c.blocksWrittenPerSec
        && this.blocksReadPerSec == c.blocksReadPerSec;
  }

  public long getBytesWrittenPerSec() {
    return bytesWrittenPerSec;
  }

  public long getBytesReadPerSec() {
    return bytesReadPerSec;
  }

  public long getWriteTime() {
    return writeTime;
  }

  public long getReadTime() {
    return readTime;
  }

  public long getBlocksWrittenPerSec() {
    return blocksWrittenPerSec;
  }

  public long getBlocksReadPerSec() {
    return blocksReadPerSec;
  }

  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Builder class for {@link DataNodeUsageReport}.
   */
  public static class Builder {

    private long bytesWrittenPerSec;
    private long bytesReadPerSec;
    private long writeTime;
    private long readTime;
    private long blocksWrittenPerSec;
    private long blocksReadPerSec;
    private long timestamp;

    public DataNodeUsageReport build() {
      return new DataNodeUsageReport(this);
    }

    public Builder setBytesWrittenPerSec(long bWrittenPerSec) {
      this.bytesWrittenPerSec = bWrittenPerSec;
      return this;
    }

    public Builder setBytesReadPerSec(long bReadPerSec) {
      this.bytesReadPerSec = bReadPerSec;
      return this;
    }

    public Builder setWriteTime(long wTime) {
      this.writeTime = wTime;
      return this;
    }

    public Builder setReadTime(long rTime) {
      this.readTime = rTime;
      return this;
    }

    public Builder setBlocksWrittenPerSec(long wBlock) {
      this.blocksWrittenPerSec = wBlock;
      return this;
    }

    public Builder setBlocksReadPerSec(long rBlock) {
      this.blocksReadPerSec = rBlock;
      return this;
    }

    public Builder setTimestamp(long ts) {
      this.timestamp = ts;
      return this;
    }

    public Builder() {
    }

  }

}
