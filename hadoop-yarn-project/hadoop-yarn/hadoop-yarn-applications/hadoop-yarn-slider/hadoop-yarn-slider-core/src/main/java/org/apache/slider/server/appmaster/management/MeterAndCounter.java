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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * A combined meter and counter that can be used to measure load.
 * Hash and equality are derived from the name
 */
public class MeterAndCounter {

  /**
   * suffix for counters: {@value}
   */
  public static final String COUNTER = ".counter";

  /**
   * suffix for meters: {@value}
   */
  public static final String METER = ".meter";

  final Meter meter;
  final Counter counter;
  final String name;

  /**
   * Construct an instance
   * @param metrics metrics to bond to
   * @param name name before suffixes are appended
   */
  public MeterAndCounter(MetricRegistry metrics, String name) {
    this.name = name;
    counter = metrics.counter(name + COUNTER);
    meter = metrics.meter(name + METER);
  }

  /**
   * Construct an instance
   * @param metrics metrics to bond to
   * @param clazz class to use to derive name
   * @param name name before suffixes are appended
   */

  public MeterAndCounter(MetricRegistry metrics, Class clazz, String name) {
    this.name = name;
    counter = metrics.counter(MetricRegistry.name(clazz, name + COUNTER));
    meter = metrics.meter(MetricRegistry.name(clazz, name + METER));
  }

  /**
   * Increment the counter, mark the meter
   */
  public void mark() {
    counter.inc();
    meter.mark();
  }

  public void inc() {
    mark();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MeterAndCounter that = (MeterAndCounter) o;

    return name.equals(that.name);

  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  /**
   * Get the count.
   * @return the current count
   */
  public long getCount() {
    return counter.getCount();
  }
}
