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
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.util.Clock;

import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.ZoneOffset.UTC;

public class ControlledClock extends Clock {
  private final AtomicLong time = new AtomicLong(-1);
  private final java.time.Clock actualClock;
  // Convenience for getting a controlled clock with overridden time
  public ControlledClock() {
    this(java.time.Clock.systemUTC());
    setTime(0);
  }
  public ControlledClock(java.time.Clock actualClock) {
    this.actualClock = actualClock;
  }
  public void setTime(long time) {
    this.time.set(time);
  }
  public void reset() {
    setTime(-1);
  }
  public void tickSec(int seconds) {
    tickMsec(seconds * 1000L);
  }
  public void tickMsec(long millisec) {
    if (time.get() == -1) {
      throw new IllegalStateException("ControlledClock setTime should be " +
          "called before incrementing time");
    }
    time.addAndGet(millisec);
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
    long currVal = time.get();
    if (currVal != -1) {
      return currVal;
    }
    return actualClock.millis();
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

}
