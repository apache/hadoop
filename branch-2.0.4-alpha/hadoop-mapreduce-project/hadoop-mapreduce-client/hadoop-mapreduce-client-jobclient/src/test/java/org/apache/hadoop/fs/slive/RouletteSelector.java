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

import java.util.List;
import java.util.Random;

/**
 * A selection object which simulates a roulette wheel whereby all operation
 * have a weight and the total value of the wheel is the combined weight and
 * during selection a random number (0, total weight) is selected and then the
 * operation that is at that value will be selected. So for a set of operations
 * with uniform weight they will all have the same probability of being
 * selected. Operations which choose to have higher weights will have higher
 * likelihood of being selected (and the same goes for lower weights).
 */
class RouletteSelector {

  private Random picker;

  RouletteSelector(Random rnd) {
    picker = rnd;
  }

  Operation select(List<OperationWeight> ops) {
    if (ops.isEmpty()) {
      return null;
    }
    double totalWeight = 0;
    for (OperationWeight w : ops) {
      if (w.getWeight() < 0) {
        throw new IllegalArgumentException("Negative weights not allowed");
      }
      totalWeight += w.getWeight();
    }
    // roulette wheel selection
    double sAm = picker.nextDouble() * totalWeight;
    int index = 0;
    for (int i = 0; i < ops.size(); ++i) {
      sAm -= ops.get(i).getWeight();
      if (sAm <= 0) {
        index = i;
        break;
      }
    }
    return ops.get(index).getOperation();
  }

}
