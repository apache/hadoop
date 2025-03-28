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
import org.apache.hadoop.classification.InterfaceStability.Stable;

import java.time.Instant;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

/**
 * Implementation of {@link Clock} that gives the current time from the system
 * clock in milliseconds.
 *
 * NOTE: Do not use this to calculate a duration of expire or interval to sleep,
 * because it will be broken by settimeofday. Please use {@link MonotonicClock}
 * instead.
 *
 * @deprecated use {@link java.time.Clock#systemUTC()} instead
 */
@Public
@Stable
@Deprecated
public final class SystemClock extends Clock {

  private static final SystemClock INSTANCE = new SystemClock();

  public static SystemClock getInstance() {
    return INSTANCE;
  }

  private SystemClock() {
    // do nothing
  }

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

  @Override
  public long millis() {
    return System.currentTimeMillis();
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

}
