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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a counter whose range can be given a min and a max
 */
public class RangeLimitedCounter implements Metric, Counting {

  private final AtomicLong value;
  private final long min, max;

  /**
   * Instantiate
   * @param val current value
   * @param min minimum value
   * @param max max value (or 0 for no max)
   */
  public RangeLimitedCounter(long val, long min, long max) {
    this.value = new AtomicLong(val);
    this.min = min;
    this.max = max;
  }

  /**
   * Set to a new value. If below the min, set to the minimum. If the max is non
   * zero and the value is above that maximum, set it to the maximum instead.
   * @param val value
   */
  public synchronized void set(long val) {
    if (val < min) {
      val = min;
    } else if (max > 0  && val > max) {
      val = max;
    }
    value.set(val);
  }

  public void inc() {
    inc(1);
  }

  public void dec() {
    dec(1);
  }

  public synchronized void inc(int delta) {
    set(value.get() + delta);
  }

  public synchronized void dec(int delta) {
    set(value.get() - delta);
  }

  public long get() {
    return value.get();
  }

  @Override
  public long getCount() {
    return value.get();
  }
}
