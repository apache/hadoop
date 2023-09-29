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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for exposing a rolling window view on the event that occur over time.
 * Events are reported based on occurrence time. The total number of events in
 * the last period covered by the rolling window can be retrieved by the
 * {@link #getSum(long)} method.
 * <p>
 *
 * Assumptions:
 * <p>
 *
 * (1) Concurrent invocation of {@link #incAt} method are possible
 * <p>
 *
 * (2) The time parameter of two consecutive invocation of {@link #incAt} could
 * be in any given order
 * <p>
 *
 * (3) The buffering delays are not more than the window length, i.e., after two
 * consecutive invocation {@link #incAt(long time1, long)} and
 * {@link #incAt(long time2, long)}, time1 &lt; time2 || time1 - time2 &lt;
 * windowLenMs.
 * This assumption helps avoiding unnecessary synchronizations.
 * <p>
 *
 * Thread-safety is built in the {@link RollingWindow.Bucket}
 */
@InterfaceAudience.Private
public class RollingWindow {
  private static final Logger LOG = LoggerFactory.getLogger(RollingWindow.class);

  /**
   * Each window is composed of buckets, which offer a trade-off between
   * accuracy and space complexity: the lower the number of buckets, the less
   * memory is required by the rolling window but more inaccuracy is possible in
   * reading window total values.
   */
  Bucket[] buckets;
  final int windowLenMs;
  final int bucketSize;

  /**
   * @param windowLenMs The period that is covered by the window. This period must
   *          be more than the buffering delays.
   * @param numBuckets number of buckets in the window
   */
  RollingWindow(int windowLenMs, int numBuckets) {
    buckets = new Bucket[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = new Bucket();
    }
    this.windowLenMs = windowLenMs;
    this.bucketSize = windowLenMs / numBuckets;
    if (this.bucketSize % bucketSize != 0) {
      throw new IllegalArgumentException(
          "The bucket size in the rolling window is not integer: windowLenMs= "
              + windowLenMs + " numBuckets= " + numBuckets);
    }
  }

  /**
   * When an event occurs at the specified time, this method reflects that in
   * the rolling window.
   * <p>
   *
   * @param time the time at which the event occurred
   * @param delta the delta that will be added to the window
   */
  public void incAt(long time, long delta) {
    int bi = computeBucketIndex(time);
    Bucket bucket = buckets[bi];
    // If the last time the bucket was updated is out of the scope of the
    // rolling window, reset the bucket.
    if (bucket.isStaleNow(time)) {
      bucket.safeReset(time);
    }
    bucket.inc(delta);
  }

  private int computeBucketIndex(long time) {
    int positionOnWindow = (int) (time % windowLenMs);
    int bucketIndex = positionOnWindow * buckets.length / windowLenMs;
    return bucketIndex;
  }

  /**
   * Thread-safety is provided by synchronization when resetting the update time
   * as well as atomic fields.
   */
  private class Bucket {
    private AtomicLong value = new AtomicLong(0);
    private AtomicLong updateTime = new AtomicLong(-1); // -1 = never updated.

    /**
     * Check whether the last time that the bucket was updated is no longer
     * covered by rolling window.
     *
     * @param time the current time
     * @return true if the bucket state is stale
     */
    boolean isStaleNow(long time) {
      long utime = updateTime.get();
      return (utime == -1) || (time - utime >= windowLenMs);
    }

    /**
     * Safely reset the bucket state considering concurrent updates (inc) and
     * resets.
     *
     * @param time the current time
     */
    void safeReset(long time) {
      // At any point in time, only one thread is allowed to reset the
      // bucket
      synchronized (this) {
        if (isStaleNow(time)) {
          // reset the value before setting the time, it allows other
          // threads to safely assume that the value is updated if the
          // time is not stale
          value.set(0);
          updateTime.set(time);
        }
        // else a concurrent thread has already reset it: do nothing
      }
    }

    /**
     * Increment the bucket. It assumes that staleness check is already
     * performed. We do not need to update the {@link #updateTime} because as
     * long as the {@link #updateTime} belongs to the current view of the
     * rolling window, the algorithm works fine.
     * @param delta
     */
    void inc(long delta) {
      value.addAndGet(delta);
    }
  }

  /**
   * Get value represented by this window at the specified time
   * <p>
   *
   * If time lags behind the latest update time, the new updates are still
   * included in the sum
   *
   * @param time
   * @return number of events occurred in the past period
   */
  public long getSum(long time) {
    long sum = 0;
    for (Bucket bucket : buckets) {
      boolean stale = bucket.isStaleNow(time);
      if (!stale) {
        sum += bucket.value.get();
      }
      if (LOG.isDebugEnabled()) {
        long bucketTime = bucket.updateTime.get();
        String timeStr = new Date(bucketTime).toString();
        LOG.debug("Sum: + " + sum + " Bucket: updateTime: " + timeStr + " ("
            + bucketTime + ") isStale " + stale + " at " + time);
      }
    }
    return sum;
  }

}
