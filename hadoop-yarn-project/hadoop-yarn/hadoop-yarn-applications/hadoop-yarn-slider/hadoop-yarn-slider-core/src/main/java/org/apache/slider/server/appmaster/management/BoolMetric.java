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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A bool metric, mapped to an integer. true maps to 1,  false to zero,
 */
public class BoolMetric implements Metric, Gauge<Integer> {

  private final AtomicBoolean value;

  public BoolMetric(boolean b) {
    value = new AtomicBoolean(b);
  }

  public void set(boolean b) {
    value.set(b);
  }

  public boolean get() {
    return value.get();
  }

  @Override
  public Integer getValue() {
    return value.get() ? 1 : 0;
  }

  /**
   * Evaluate from a string. Returns true if the string is considered to match 'true',
   * false otherwise.
   * @param s source
   * @return true if the input parses to an integer other than 0. False if it doesn't parse
   * or parses to 0.
   */
  public static boolean fromString(String s) {
    try {
      return Integer.valueOf(s) != 0;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BoolMetric that = (BoolMetric) o;
    return get() == that.get();
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
