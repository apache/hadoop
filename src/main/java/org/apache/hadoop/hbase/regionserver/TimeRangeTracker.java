/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Stores the minimum and maximum timestamp values.
 * Can be used to find if any given time range overlaps with its time range
 * MemStores use this class to track its minimum and maximum timestamps.
 * When writing StoreFiles, this information is stored in meta blocks and used
 * at read time to match against the required TimeRange
 */
public class TimeRangeTracker implements Writable {

  long minimumTimestamp = -1;
  long maximumTimestamp = -1;

  /**
   * Default constructor.
   * Initializes TimeRange to be null
   */
  public TimeRangeTracker() {

  }

  /**
   * Copy Constructor
   * @param trt source TimeRangeTracker
   */
  public TimeRangeTracker(final TimeRangeTracker trt) {
    this.minimumTimestamp = trt.getMinimumTimestamp();
    this.maximumTimestamp = trt.getMaximumTimestamp();
  }

  public TimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
    this.minimumTimestamp = minimumTimestamp;
    this.maximumTimestamp = maximumTimestamp;
  }

  /**
   * Update the current TimestampRange to include the timestamp from KeyValue
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param kv the KeyValue to include
   */
  public void includeTimestamp(final KeyValue kv) {
    includeTimestamp(kv.getTimestamp());
    if (kv.isDeleteColumnOrFamily()) {
      includeTimestamp(0);
    }
  }

  /**
   * Update the current TimestampRange to include the timestamp from Key.
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param key
   */
  public void includeTimestamp(final byte[] key) {
    includeTimestamp(Bytes.toLong(key,key.length-KeyValue.TIMESTAMP_TYPE_SIZE));
    int type = key[key.length - 1];
    if (type == Type.DeleteColumn.getCode() ||
        type == Type.DeleteFamily.getCode()) {
      includeTimestamp(0);
    }
  }

  /**
   * If required, update the current TimestampRange to include timestamp
   * @param timestamp the timestamp value to include
   */
  private void includeTimestamp(final long timestamp) {
    if (maximumTimestamp == -1) {
      minimumTimestamp = timestamp;
      maximumTimestamp = timestamp;
    }
    else if (minimumTimestamp > timestamp) {
      minimumTimestamp = timestamp;
    }
    else if (maximumTimestamp < timestamp) {
      maximumTimestamp = timestamp;
    }
    return;
  }

  /**
   * Check if the range has any overlap with TimeRange
   * @param tr TimeRange
   * @return True if there is overlap, false otherwise
   */
  public boolean includesTimeRange(final TimeRange tr) {
    return (this.minimumTimestamp < tr.getMax() &&
        this.maximumTimestamp >= tr.getMin());
  }

  /**
   * @return the minimumTimestamp
   */
  public long getMinimumTimestamp() {
    return minimumTimestamp;
  }

  /**
   * @return the maximumTimestamp
   */
  public long getMaximumTimestamp() {
    return maximumTimestamp;
  }

  public void write(final DataOutput out) throws IOException {
    out.writeLong(minimumTimestamp);
    out.writeLong(maximumTimestamp);
  }

  public void readFields(final DataInput in) throws IOException {
    this.minimumTimestamp = in.readLong();
    this.maximumTimestamp = in.readLong();
  }

}

