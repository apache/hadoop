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

package org.apache.hadoop.fs.slive;

import java.util.Random;

/**
 * Class that represents a numeric minimum and a maximum
 * 
 * @param <T>
 *          the type of number being used
 */
class Range<T extends Number> {

  private static final String SEP = ",";

  private T min;
  private T max;

  Range(T min, T max) {
    this.min = min;
    this.max = max;
  }

  /**
   * @return the minimum value
   */
  T getLower() {
    return min;
  }

  /**
   * @return the maximum value
   */
  T getUpper() {
    return max;
  }

  public String toString() {
    return min + SEP + max;
  }

  /**
   * Gets a long number between two values
   * 
   * @param rnd
   * @param range
   * 
   * @return long
   */
  static long betweenPositive(Random rnd, Range<Long> range) {
    if (range.getLower().equals(range.getUpper())) {
      return range.getLower();
    }
    long nextRnd = rnd.nextLong();
    long normRange = (range.getUpper() - range.getLower() + 1);
    return Math.abs(nextRnd % normRange) + range.getLower();
  }

}
