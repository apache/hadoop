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
package org.apache.hadoop.metrics2.util;

import java.util.PriorityQueue;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Utility class to simplify creation of hadoop metrics2 source/sink.
 */
@InterfaceAudience.Private
public class Metrics2Util {
  /**
   * A pair of a name and its corresponding value. Defines a custom
   * comparator so the TopN PriorityQueue sorts based on the count.
   */
  @InterfaceAudience.Private
  public static class NameValuePair implements Comparable<NameValuePair> {
    private String name;
    private long value;

    public NameValuePair(String metricName, long value) {
      this.name = metricName;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public long getValue() {
      return value;
    }

    @Override
    public int compareTo(NameValuePair other) {
      return (int) (value - other.value);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof NameValuePair) {
        return compareTo((NameValuePair)other) == 0;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Long.valueOf(value).hashCode();
    }
  }

  /**
   * A fixed-size priority queue, used to retrieve top-n of offered entries.
   */
  @InterfaceAudience.Private
  public static class TopN extends PriorityQueue<NameValuePair> {
    private static final long serialVersionUID = 5134028249611535803L;
    private int n; // > 0
    private long total = 0;

    public TopN(int n) {
      super(n);
      this.n = n;
    }

    @Override
    public boolean offer(NameValuePair entry) {
      updateTotal(entry.value);
      if (size() == n) {
        NameValuePair smallest = peek();
        if (smallest.value >= entry.value) {
          return false;
        }
        poll(); // remove smallest
      }
      return super.offer(entry);
    }

    private void updateTotal(long value) {
      total += value;
    }

    public long getTotal() {
      return total;
    }
  }
}
