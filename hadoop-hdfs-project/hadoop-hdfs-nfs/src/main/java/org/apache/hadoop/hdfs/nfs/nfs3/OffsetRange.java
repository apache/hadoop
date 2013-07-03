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
package org.apache.hadoop.hdfs.nfs.nfs3;

/**
 * OffsetRange is the range of read/write request. A single point (e.g.,[5,5])
 * is not a valid range.
 */
public class OffsetRange implements Comparable<OffsetRange> {
  private final long min;
  private final long max;

  OffsetRange(long min, long max) {
    if ((min >= max) || (min < 0) || (max < 0)) {
      throw new IllegalArgumentException("Wrong offset range: (" + min + ","
          + max + ")");
    }
    this.min = min;
    this.max = max;
  }

  long getMin() {
    return min;
  }

  long getMax() {
    return max;
  }

  @Override
  public int hashCode() {
    return (int) (min ^ max);
  }

  @Override
  public boolean equals(Object o) {
    assert (o instanceof OffsetRange);
    OffsetRange range = (OffsetRange) o;
    return (min == range.getMin()) && (max == range.getMax());
  }

  private static int compareTo(long left, long right) {
    if (left < right) {
      return -1;
    } else if (left > right) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public int compareTo(OffsetRange other) {
    final int d = compareTo(min, other.getMin());
    return d != 0 ? d : compareTo(max, other.getMax());
  }
}
