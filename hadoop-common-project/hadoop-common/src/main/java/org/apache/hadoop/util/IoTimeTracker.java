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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.math.BigInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IoTimeTracker {
  public static final int UNAVAILABLE = -1;
  final long MINIMUM_UPDATE_INTERVAL;

  // IO used time since system is on (ms)
  BigInteger cumulativeIoTime = BigInteger.ZERO;

  // IO used time read last time (ms)
  BigInteger lastCumulativeIoTime = BigInteger.ZERO;

  // Unix timestamp while reading the IO time (ms)
  long sampleTime;
  long lastSampleTime;
  float ioUsage;
  BigInteger jiffyLengthInMillis;

  private ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Lock readLock = rwLock.readLock();
  public Lock writeLock = rwLock.writeLock();

  public IoTimeTracker(long jiffyLengthInMillis) {
    this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
    this.ioUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    MINIMUM_UPDATE_INTERVAL =  10 * jiffyLengthInMillis;
  }

  /**
   * Return percentage of io time spent over the time since last update.
   * IO time spent is based on elapsed jiffies multiplied by amount of
   * time for 1 disk. Thus, if you use 2 disks completely you would have spent
   * twice the actual time between updates and this will return 200%.
   *
   * @return Return percentage of io usage since last update, {@link
   * IoTimeTracker#UNAVAILABLE} if there haven't been 2 updates more than
   * {@link IoTimeTracker#MINIMUM_UPDATE_INTERVAL} apart
   */
  public float getIoTrackerUsagePercent() {
    readLock.lock();
    try {
      if (lastSampleTime == UNAVAILABLE || lastSampleTime > sampleTime) {
        // lastSampleTime > sampleTime may happen when the system time is changed
        lastSampleTime = sampleTime;
        lastCumulativeIoTime = cumulativeIoTime;
        return ioUsage;
      }
      // When lastSampleTime is sufficiently old, update ioUsage.
      // Also take a sample of the current time and cumulative IO time for the
      // use of the next calculation.
      if (sampleTime > lastSampleTime + MINIMUM_UPDATE_INTERVAL) {
        ioUsage = ((cumulativeIoTime.subtract(lastCumulativeIoTime)).floatValue())
            / ((float) (sampleTime - lastSampleTime));
        lastSampleTime = sampleTime;
        lastCumulativeIoTime = cumulativeIoTime;
      }
    } finally {
      readLock.unlock();
    }
    return ioUsage;
  }

  public void updateElapsedJiffies(BigInteger totalIoPerDisk, long sampleTime) {
    this.cumulativeIoTime = this.cumulativeIoTime.add(totalIoPerDisk);
    this.sampleTime = sampleTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime " + this.sampleTime);
    sb.append(" CummulativeIoTime " + this.cumulativeIoTime);
    sb.append(" LastSampleTime " + this.lastSampleTime);
    sb.append(" LastCummulativeIoTime " + this.lastCumulativeIoTime);
    sb.append(" IoUsage " + this.ioUsage);
    sb.append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
    return sb.toString();
  }
}