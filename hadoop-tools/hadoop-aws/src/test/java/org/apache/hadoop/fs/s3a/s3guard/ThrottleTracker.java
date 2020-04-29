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

package org.apache.hadoop.fs.s3a.s3guard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Something to track throttles in DynamoDB metastores.
 * The constructor sets the counters to the current count in the
 * DDB table; a call to {@link #reset()} will set it to the latest values.
 * The {@link #probe()} will pick up the latest values to compare them with
 * the original counts.
 * <p>
 * The toString value logs the state.
 * <p>
 * This class was originally part of ITestDynamoDBMetadataStoreScale;
 * it was converted to a toplevel class for broader use.
 */
class ThrottleTracker {

  private static final Logger LOG = LoggerFactory.getLogger(
      ThrottleTracker.class);
  private final DynamoDBMetadataStore ddbms;

  private long writeThrottleEventOrig;

  private long readThrottleEventOrig;

  private long batchWriteThrottleCountOrig;

  private long scanThrottleCountOrig;

  private long readThrottles;

  private long writeThrottles;

  private long batchThrottles;

  private long scanThrottles;

  ThrottleTracker(final DynamoDBMetadataStore ddbms) {
    this.ddbms = ddbms;
    reset();
  }

  /**
   * Reset the counters.
   */
  public synchronized void reset() {
    writeThrottleEventOrig
        = ddbms.getWriteThrottleEventCount();

    readThrottleEventOrig
        = ddbms.getReadThrottleEventCount();

    batchWriteThrottleCountOrig
        = ddbms.getBatchWriteCapacityExceededCount();

    scanThrottleCountOrig
        = ddbms.getScanThrottleEventCount();
  }

  /**
   * Update the latest throttle count; synchronized.
   * @return true if throttling has been detected.
   */
  public synchronized boolean probe() {
    setReadThrottles(
        ddbms.getReadThrottleEventCount() - readThrottleEventOrig);
    setWriteThrottles(ddbms.getWriteThrottleEventCount()
        - writeThrottleEventOrig);
    setBatchThrottles(ddbms.getBatchWriteCapacityExceededCount()
        - batchWriteThrottleCountOrig);
    setScanThrottles(ddbms.getScanThrottleEventCount()
        - scanThrottleCountOrig);
    return isThrottlingDetected();
  }

  @Override
  public String toString() {
    return String.format(
        "Tracker with read throttle events = %d;"
            + " write throttles = %d;"
            + " batch throttles = %d;"
            + " scan throttles = %d",
        getReadThrottles(), getWriteThrottles(), getBatchThrottles(),
        getScanThrottles());
  }

  /**
   * Check that throttling was detected; Warn if not.
   * @return true if throttling took place.
   */
  public boolean probeThrottlingDetected() {
    if (!isThrottlingDetected()) {
      LOG.warn("No throttling detected in {} against {}",
          this, ddbms);
      return false;
    }
    return true;
  }

  /**
   * Has there been any throttling on an operation?
   * @return true if any operations were throttled.
   */
  public boolean isThrottlingDetected() {
    return getReadThrottles() > 0
        || getWriteThrottles() > 0
        || getBatchThrottles() > 0
        || getScanThrottles() > 0;
  }

  public long getReadThrottles() {
    return readThrottles;
  }

  public void setReadThrottles(long readThrottles) {
    this.readThrottles = readThrottles;
  }

  public long getWriteThrottles() {
    return writeThrottles;
  }

  public void setWriteThrottles(long writeThrottles) {
    this.writeThrottles = writeThrottles;
  }

  public long getBatchThrottles() {
    return batchThrottles;
  }

  public void setBatchThrottles(long batchThrottles) {
    this.batchThrottles = batchThrottles;
  }

  public long getScanThrottles() {
    return scanThrottles;
  }

  public void setScanThrottles(final long scanThrottles) {
    this.scanThrottles = scanThrottles;
  }
}
