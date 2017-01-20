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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Utility class that allows HBase coprocessors to interact with unique
 * timestamps.
 */
public class TimestampGenerator {

  /*
   * if this is changed, then reading cell timestamps written with older
   * multiplier value will not work
   */
  public static final long TS_MULTIPLIER = 1000000L;

  private final AtomicLong lastTimestamp = new AtomicLong();

  /**
   * Returns the current wall clock time in milliseconds, multiplied by the
   * required precision.
   *
   * @return current timestamp.
   */
  public long currentTime() {
    // We want to align cell timestamps with current time.
    // cell timestamps are not be less than
    // System.currentTimeMillis() * TS_MULTIPLIER.
    return System.currentTimeMillis() * TS_MULTIPLIER;
  }

  /**
   * Returns a timestamp value unique within the scope of this
   * {@code TimestampGenerator} instance. For usage by HBase
   * {@code RegionObserver} coprocessors, this normally means unique within a
   * given region.
   *
   * Unlikely scenario of generating a non-unique timestamp: if there is a
   * sustained rate of more than 1M hbase writes per second AND if region fails
   * over within that time range of timestamps being generated then there may be
   * collisions writing to a cell version of the same column.
   *
   * @return unique timestamp.
   */
  public long getUniqueTimestamp() {
    long lastTs;
    long nextTs;
    do {
      lastTs = lastTimestamp.get();
      nextTs = Math.max(lastTs + 1, currentTime());
    } while (!lastTimestamp.compareAndSet(lastTs, nextTs));
    return nextTs;
  }

  /**
   * Returns a timestamp multiplied with TS_MULTIPLIER and last few digits of
   * application id.
   *
   * Unlikely scenario of generating a timestamp that is a duplicate: If more
   * than a 1M concurrent apps are running in one flow run AND write to same
   * column at the same time, then say appId of 1M and 1 will overlap
   * with appId of 001 and there may be collisions for that flow run's
   * specific column.
   *
   * @param incomingTS Timestamp to be converted.
   * @param appId Application Id.
   * @return a timestamp multiplied with TS_MULTIPLIER and last few digits of
   *         application id
   */
  public static long getSupplementedTimestamp(long incomingTS, String appId) {
    long suffix = getAppIdSuffix(appId);
    long outgoingTS = incomingTS * TS_MULTIPLIER + suffix;
    return outgoingTS;

  }

  private static long getAppIdSuffix(String appIdStr) {
    if (appIdStr == null) {
      return 0L;
    }
    ApplicationId appId = ApplicationId.fromString(appIdStr);
    long id = appId.getId() % TS_MULTIPLIER;
    return id;
  }

  /**
   * truncates the last few digits of the timestamp which were supplemented by
   * the TimestampGenerator#getSupplementedTimestamp function.
   *
   * @param incomingTS Timestamp to be truncated.
   * @return a truncated timestamp value
   */
  public static long getTruncatedTimestamp(long incomingTS) {
    return incomingTS / TS_MULTIPLIER;
  }
}