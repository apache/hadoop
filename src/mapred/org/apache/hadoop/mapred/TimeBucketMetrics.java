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
package org.apache.hadoop.mapred;

import java.util.HashMap;

/**
 * Create a set of buckets that hold key-time pairs. When the values of the 
 * buckets is queried, the number of objects with time differences in the
 * different buckets is returned.
 */
class TimeBucketMetrics<OBJ> {

  private final HashMap<OBJ, Long> map = new HashMap<OBJ, Long>();
  private final int[] counts;
  private final long[] cuts;

  /**
   * Create a set of buckets based on a set of time points. The number of 
   * buckets is one more than the number of points.
   */
  TimeBucketMetrics(long[] cuts) {
    this.cuts = cuts;
    counts = new int[cuts.length + 1];
  }

  /**
   * Add an object to be counted
   */
  synchronized void add(OBJ key, long time) {
    map.put(key, time);
  }

  /**
   * Remove an object to be counted
   */
  synchronized void remove(OBJ key) {
    map.remove(key);
  }

  /**
   * Find the bucket based on the cut points.
   */
  private int findBucket(long val) {
    for(int i=0; i < cuts.length; ++i) {
      if (val < cuts[i]) {
	return i;
      }
    }
    return cuts.length;
  }

  /**
   * Get the counts of how many keys are in each bucket. The same array is
   * returned by each call to this method.
   */
  synchronized int[] getBucketCounts(long now) {
    for(int i=0; i < counts.length; ++i) {
      counts[i] = 0;
    }
    for(Long time: map.values()) {
      counts[findBucket(now - time)] += 1;
    }
    return counts;
  }
}