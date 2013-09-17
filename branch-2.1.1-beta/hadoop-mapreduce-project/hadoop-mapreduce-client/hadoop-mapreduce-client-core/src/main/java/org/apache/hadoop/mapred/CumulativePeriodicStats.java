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

/**
 *
 * This class is a concrete PeriodicStatsAccumulator that deals with
 *  measurements where the raw data are a measurement of an
 *  accumulation.  The result in each bucket is the estimate 
 *  of the progress-weighted change in that quantity over the
 *  progress range covered by the bucket.
 *
 * <p>An easy-to-understand example of this kind of quantity would be
 *  a distance traveled.  It makes sense to consider that portion of
 *  the total travel that can be apportioned to each bucket.
 *
 */
class CumulativePeriodicStats extends PeriodicStatsAccumulator {
  // int's are acceptable here, even though times are normally
  // long's, because these are a difference and an int won't
  // overflow for 24 days.  Tasks can't run for more than about a
  // week for other reasons, and most jobs would be written 
  int previousValue = 0;

  CumulativePeriodicStats(int count) {
    super(count);
  }

  /**
   *
   * accumulates a new reading by keeping a running account of the
   *  value distance from the beginning of the bucket to the end of
   *  this reading
   */
  @Override
    protected void extendInternal(double newProgress, int newValue) {
    if (state == null) {
      return;
    }

    state.currentAccumulation += (double)(newValue - previousValue);
    previousValue = newValue;
  }
}
