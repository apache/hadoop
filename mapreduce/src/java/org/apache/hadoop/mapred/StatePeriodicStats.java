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
 *  measurements where the raw data are a measurement of a
 *  time-varying quantity.  The result in each bucket is the estimate
 *  of the progress-weighted mean value of that quantity over the
 *  progress range covered by the bucket.
 *
 * <p>An easy-to-understand example of this kind of quantity would be
 *  a temperature.  It makes sense to consider the mean temperature
 *  over a progress range.
 *
 */
class StatePeriodicStats extends PeriodicStatsAccumulator {
  StatePeriodicStats(int count) {
    super(count);
  }

  /**
   *
   * accumulates a new reading by keeping a running account of the
   *  area under the piecewise linear curve marked by pairs of
   *  {@code newProgress, newValue} .
   */
  @Override
    protected void extendInternal(double newProgress, int newValue) {
    if (state == null) {
      return;
    }

    // the effective height of this trapezoid if rectangularized
    double mean = ((double)newValue + (double)state.oldValue)/2.0D;

    // conceptually mean *  (newProgress - state.oldProgress) / (1 / count)
    state.currentAccumulation += mean * (newProgress - state.oldProgress) * count;
  }
}
