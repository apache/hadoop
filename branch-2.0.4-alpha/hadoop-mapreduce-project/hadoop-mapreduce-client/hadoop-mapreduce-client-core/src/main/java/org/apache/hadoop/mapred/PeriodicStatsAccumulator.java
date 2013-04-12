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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 *
 * This abstract class that represents a bucketed series of
 *  measurements of a quantity being measured in a running task
 *  attempt. 
 *
 * <p>The sole constructor is called with a count, which is the
 *  number of buckets into which we evenly divide the spectrum of
 *  progress from 0.0D to 1.0D .  In the future we may provide for
 *  custom split points that don't have to be uniform.
 *
 * <p>A subclass determines how we fold readings for portions of a
 *  bucket and how we interpret the readings by overriding
 *  {@code extendInternal(...)} and {@code initializeInterval()}
 */
@Private
@Unstable
public abstract class PeriodicStatsAccumulator {
  // The range of progress from 0.0D through 1.0D is divided into
  //  count "progress segments".  This object accumulates an
  //  estimate of the effective value of a time-varying value during
  //  the zero-based i'th progress segment, ranging from i/count
  //  through (i+1)/count . 
  // This is an abstract class.  We have two implementations: one
  //  for monotonically increasing time-dependent variables
  //  [currently, CPU time in milliseconds and wallclock time in
  //  milliseconds] and one for quantities that can vary arbitrarily
  //  over time, currently virtual and physical memory used, in
  //  kilobytes. 
  // We carry int's here.  This saves a lot of JVM heap space in the
  //  job tracker per running task attempt [200 bytes per] but it
  //  has a small downside.
  // No task attempt can run for more than 57 days nor occupy more
  //  than two terabytes of virtual memory. 
  protected final int count;
  protected final int[] values;
    
  static class StatsetState {
    int oldValue = 0;
    double oldProgress = 0.0D;

    double currentAccumulation = 0.0D;
  }

  // We provide this level of indirection to reduce the memory
  //  footprint of done task attempts.  When a task's progress
  //  reaches 1.0D, we delete this objecte StatsetState.
  StatsetState state = new StatsetState();

  PeriodicStatsAccumulator(int count) {
    this.count = count;
    this.values = new int[count];
    for (int i = 0; i < count; ++i) {
      values[i] = -1;
    }
  }

  protected int[] getValues() {
    return values;
  }

  // The concrete implementation of this abstract function
  //  accumulates more data into the current progress segment.
  //  newProgress [from the call] and oldProgress [from the object]
  //  must be in [or at the border of] a single progress segment.
  /**
   *
   * adds a new reading to the current bucket.
   *
   * @param newProgress the endpoint of the interval this new
   *                      reading covers
   * @param newValue the value of the reading at {@code newProgress} 
   *
   * The class has three instance variables, {@code oldProgress} and
   *  {@code oldValue} and {@code currentAccumulation}. 
   *
   * {@code extendInternal} can count on three things: 
   *
   *   1: The first time it's called in a particular instance, both
   *      oldXXX's will be zero.
   *
   *   2: oldXXX for a later call is the value of newXXX of the
   *      previous call.  This ensures continuity in accumulation from
   *      one call to the next.
   *
   *   3: {@code currentAccumulation} is owned by 
   *      {@code initializeInterval} and {@code extendInternal}.
   */
  protected abstract void extendInternal(double newProgress, int newValue);

  // What has to be done when you open a new interval
  /**
   * initializes the state variables to be ready for a new interval
   */
  protected void initializeInterval() {
    state.currentAccumulation = 0.0D;
  }

  // called for each new reading
  /**
   * This method calls {@code extendInternal} at least once.  It
   *  divides the current progress interval [from the last call's
   *  {@code newProgress}  to this call's {@code newProgress} ]
   *  into one or more subintervals by splitting at any point which
   *  is an interval boundary if there are any such points.  It
   *  then calls {@code extendInternal} for each subinterval, or the
   *  whole interval if there are no splitting points.
   * 
   *  <p>For example, if the value was {@code 300} last time with
   *  {@code 0.3}  progress, and count is {@code 5}, and you get a
   *  new reading with the variable at {@code 700} and progress at
   *  {@code 0.7}, you get three calls to {@code extendInternal}:
   *  one extending from progress {@code 0.3} to {@code 0.4} [the
   *  next boundary] with a value of {@code 400}, the next one
   *  through {@code 0.6} with a  value of {@code 600}, and finally
   *  one at {@code 700} with a progress of {@code 0.7} . 
   *
   * @param newProgress the endpoint of the progress range this new
   *                      reading covers
   * @param newValue the value of the reading at {@code newProgress} 
   */    
  protected void extend(double newProgress, int newValue) {
    if (state == null || newProgress < state.oldProgress) {
      return;
    }

    // This correctness of this code depends on 100% * count = count.
    int oldIndex = (int)(state.oldProgress * count);
    int newIndex = (int)(newProgress * count);
    int originalOldValue = state.oldValue;

    double fullValueDistance = (double)newValue - state.oldValue;
    double fullProgressDistance = newProgress - state.oldProgress;
    double originalOldProgress = state.oldProgress;

    // In this loop we detect each subinterval boundary within the
    //  range from the old progress to the new one.  Then we
    //  interpolate the value from the old value to the new one to
    //  infer what its value might have been at each such boundary.
    //  Lastly we make the necessary calls to extendInternal to fold
    //  in the data for each trapazoid where no such trapazoid
    //  crosses a boundary.
    for (int closee = oldIndex; closee < newIndex; ++closee) {
      double interpolationProgress = (double)(closee + 1) / count;
      // In floats, x * y / y might not equal y.
      interpolationProgress = Math.min(interpolationProgress, newProgress);

      double progressLength = (interpolationProgress - originalOldProgress);
      double interpolationProportion = progressLength / fullProgressDistance;

      double interpolationValueDistance
        = fullValueDistance * interpolationProportion;

      // estimates the value at the next [interpolated] subsegment boundary
      int interpolationValue
        = (int)interpolationValueDistance + originalOldValue;

      extendInternal(interpolationProgress, interpolationValue);

      advanceState(interpolationProgress, interpolationValue);

      values[closee] = (int)state.currentAccumulation;
      initializeInterval();

    }

    extendInternal(newProgress, newValue);
    advanceState(newProgress, newValue);

    if (newIndex == count) {
      state = null;
    }
  }

  protected void advanceState(double newProgress, int newValue) {
    state.oldValue = newValue;
    state.oldProgress = newProgress;
  }    

  int getCount() {
    return count;
  }

  int get(int index) {
    return values[index];
  }
}
