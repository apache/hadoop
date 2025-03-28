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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.time.Instant;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

/**
 * A monotonic clock from some arbitrary time base in the past, counting in
 * milliseconds, and not affected by settimeofday or similar system clock
 * changes.
 * This is appropriate to use when computing how much longer to wait for an
 * interval to expire.
 * This function can return a negative value and it must be handled correctly
 * by callers. See the documentation of System#nanoTime for caveats.
 */
@Public
@Evolving
public class MonotonicClock extends Clock {

  @Override
  public ZoneId getZone() {
    return UTC;
  }

  @Override
  public java.time.Clock withZone(ZoneId zone) {
    if (!UTC.equals(zone)) {
      throw new IllegalArgumentException("Only UTC is supported; to use other zones use Clock.system(ZoneId)");
    }
    return this;
  }

  /**
   * Get current time from some arbitrary time base in the past, counting in
   * milliseconds, and not affected by settimeofday or similar system clock
   * changes.
   * @return a monotonic clock that counts in milliseconds.
   */
  @Override
  public long millis() {
    return Time.monotonicNow();
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

}
