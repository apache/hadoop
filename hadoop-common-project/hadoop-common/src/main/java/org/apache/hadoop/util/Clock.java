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
 * A former clock interface retro-fitted to be eventually replaced by java.time.Clock.
 *
 * @deprecated use {@link java.time.Clock} instead
 */
@Public
@Stable
@Deprecated
public abstract class Clock extends java.time.Clock {

  /**
   * Gets the current millisecond instant of the clock by delegating to {@link java.time.Clock#millis()}.
   *
   * @deprecated use {@link #millis()} instead
   */
  @Deprecated
  public final long getTime() {
    return millis();
  }

  // default java.time.Clock implementation

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
   * Overridden as abstract because legacy implementations work with millis natively.
   */
  @Override
  public abstract long millis();

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

}