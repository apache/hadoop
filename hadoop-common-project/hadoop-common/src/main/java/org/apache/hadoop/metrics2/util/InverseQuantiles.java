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

import java.util.ListIterator;

public class InverseQuantiles extends SampleQuantiles{

  public InverseQuantiles(Quantile[] quantiles) {
    super(quantiles);
  }

  /**
   * Get the estimated value at the inverse of the specified quantile.
   * Eg: return the value at (1 - 0.99)*count position for quantile 0.99.
   * When count is 100, quantile 0.99 is desired to return the value at the 1st position
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  long query(double quantile) {
    int rankMin = 0;
    int desired = (int) (quantile * getCount());

    ListIterator<SampleItem> it = getSamples().listIterator(getSampleCount());
    SampleItem prev;
    SampleItem cur = it.previous();
    for (int i = 1; i < getSamples().size(); i++) {
      prev = cur;
      cur = it.previous();
      rankMin += prev.g;
      if (rankMin + cur.g + cur.delta > desired + (allowableError(i) / 2)) {
        return prev.value;
      }
    }

    // edge case of wanting min value
    return getSamples().get(0).value;
  }
}
