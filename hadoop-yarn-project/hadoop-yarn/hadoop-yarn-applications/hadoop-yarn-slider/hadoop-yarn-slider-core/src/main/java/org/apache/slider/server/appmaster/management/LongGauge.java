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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a {@link AtomicLong} which acts as a metrics gauge: its state can be exposed as
 * a metrics.
 * It also exposes some of the same method names as the Codahale Counter class, so that
 * it's easy to swap in.
 *
 */
public class LongGauge extends AtomicLong implements Metric, Gauge<Long> {

  /**
   * Instantiate
   * @param val current value
   */
  public LongGauge(long val) {
    super(val);
  }

  /**
   * Instantiate with value 0
   */
  public LongGauge() {
    this(0);
  }

  /**
   * Get the value as a metric
   * @return current value
   */
  @Override
  public Long getValue() {
    return get();
  }

  /**
   * Method from {@Code counter}; used here for drop-in replacement
   * without any recompile
   * @return current value
   */
  public Long getCount() {
    return get();
  }

  /**
   * {@code ++}
   */
  public void inc() {
    incrementAndGet();
  }

  /**
   * {@code --}
   */
  public void dec() {
    decrementAndGet();
  }

  /**
   * Decrement to the floor of 0. Operations in parallel may cause confusion here,
   * but it will still never go below zero
   * @param delta delta
   * @return the current value
   */
  public long decToFloor(long delta) {
    long l = get();
    long r = l - delta;
    if (r < 0) {
      r = 0;
    }
    // if this fails, the decrement has been lost
    compareAndSet(l, r);
    return get();
  }
}
