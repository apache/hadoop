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

/**
 * Utility for sampling and computing CPU usage.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CpuTimeTracker {
  public static final int UNAVAILABLE = -1;
  private final long minimumTimeInterval;

  // CPU used time since system is on (ms)
  private BigInteger cumulativeCpuTime = BigInteger.ZERO;

  // CPU used time read last time (ms)
  private BigInteger lastCumulativeCpuTime = BigInteger.ZERO;

  // Unix timestamp while reading the CPU time (ms)
  private long sampleTime;
  private long lastSampleTime;
  private float cpuUsage;
  private BigInteger jiffyLengthInMillis;

  public CpuTimeTracker(long jiffyLengthInMillis) {
    this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
    this.cpuUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    minimumTimeInterval =  10 * jiffyLengthInMillis;
  }

  /**
   * Return percentage of cpu time spent over the time since last update.
   * CPU time spent is based on elapsed jiffies multiplied by amount of
   * time for 1 core. Thus, if you use 2 cores completely you would have spent
   * twice the actual time between updates and this will return 200%.
   *
   * @return Return percentage of cpu usage since last update, {@link
   * CpuTimeTracker#UNAVAILABLE} if there haven't been 2 updates more than
   * {@link CpuTimeTracker#minimumTimeInterval} apart
   */
  public float getCpuTrackerUsagePercent() {
    if (lastSampleTime == UNAVAILABLE ||
        lastSampleTime > sampleTime) {
      // lastSampleTime > sampleTime may happen when the system time is changed
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
      return cpuUsage;
    }
    // When lastSampleTime is sufficiently old, update cpuUsage.
    // Also take a sample of the current time and cumulative CPU time for the
    // use of the next calculation.
    if (sampleTime > lastSampleTime + minimumTimeInterval) {
      cpuUsage =
          ((cumulativeCpuTime.subtract(lastCumulativeCpuTime)).floatValue())
          * 100F / ((float) (sampleTime - lastSampleTime));
      lastSampleTime = sampleTime;
      lastCumulativeCpuTime = cumulativeCpuTime;
    }
    return cpuUsage;
  }

  /**
   * Obtain the cumulative CPU time since the system is on.
   * @return cumulative CPU time in milliseconds
   */
  public long getCumulativeCpuTime() {
    return cumulativeCpuTime.longValue();
  }

  /**
   * Apply delta to accumulators.
   * @param elapsedJiffies updated jiffies
   * @param newTime new sample time
   */
  public void updateElapsedJiffies(BigInteger elapsedJiffies, long newTime) {
    cumulativeCpuTime = elapsedJiffies.multiply(jiffyLengthInMillis);
    sampleTime = newTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime " + this.sampleTime);
    sb.append(" CummulativeCpuTime " + this.cumulativeCpuTime);
    sb.append(" LastSampleTime " + this.lastSampleTime);
    sb.append(" LastCummulativeCpuTime " + this.lastCumulativeCpuTime);
    sb.append(" CpuUsage " + this.cpuUsage);
    sb.append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
    return sb.toString();
  }
}
