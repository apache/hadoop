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

public class InverseQuantiles extends SampleQuantiles{

  public InverseQuantiles(Quantile[] quantiles) {
    super(quantiles);
  }

  /**
   * Get the desired location from the sample for inverse of the specified quantile. 
   * Eg: return (1 - 0.99)*count position for quantile 0.99.
   * When count is 100, the desired location for quantile 0.99 is the 1st position
   * @param quantile queried quantile, e.g. 0.50 or 0.99.
   * @param count sample size count
   * @return Desired location inverse position of that quantile.
   */
  int getDesiredLocation(final double quantile, final long count) {
    return (int) ((1 - quantile) * count);
  }

  /**
   * Return the best (minimum) value from given sample
   * @return minimum value from given sample
   */
  long getMaxValue() {
    return samples.get(0).value;
  }
}
