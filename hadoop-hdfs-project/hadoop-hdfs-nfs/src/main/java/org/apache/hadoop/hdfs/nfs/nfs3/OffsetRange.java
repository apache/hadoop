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

import java.util.Comparator;

import com.google.common.base.Preconditions;

/**
 * OffsetRange is the range of read/write request. A single point (e.g.,[5,5])
 * is not a valid range.
 */
public class OffsetRange {
  
  public static final Comparator<OffsetRange> ReverseComparatorOnMin = 
      new Comparator<OffsetRange>() {
    @Override
    public int compare(OffsetRange o1, OffsetRange o2) {
      if (o1.getMin() == o2.getMin()) {
        return o1.getMax() < o2.getMax() ? 
            1 : (o1.getMax() > o2.getMax() ? -1 : 0);
      } else {
        return o1.getMin() < o2.getMin() ? 1 : -1;
      }
    }
  };
  
  private final long min;
  private final long max;

  OffsetRange(long min, long max) {
    Preconditions.checkArgument(min >= 0 && max >= 0 && min < max);
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
    if (o instanceof OffsetRange) {
      OffsetRange range = (OffsetRange) o;
      return (min == range.getMin()) && (max == range.getMax());
    }
    return false;
  }
}
