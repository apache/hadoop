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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Utility class that converts Fair Scheduler weights to capacities in
 * percentages.
 *
 * It also makes sure that the sum of the capacities adds up to exactly 100.0.
 *
 * There is a special case when one or more queues have a capacity of 0. This
 * can happen if the weight was originally 0 in the FS configuration. In
 * this case, we need an extra queue with a capacity of 100.0 to have a valid
 * CS configuration.
 */
final class WeightToCapacityConversionUtil {
  private static final BigDecimal HUNDRED = new BigDecimal(100).setScale(3);
  private static final BigDecimal ZERO = new BigDecimal(0).setScale(3);

  private WeightToCapacityConversionUtil() {
    // no instances
  }

  @VisibleForTesting
  static Pair<Map<String, BigDecimal>, Boolean> getCapacities(int totalWeight,
      List<FSQueue> children, FSConfigToCSConfigRuleHandler ruleHandler) {

    if (children.size() == 0) {
      return Pair.of(new HashMap<>(), false);
    } else if (children.size() == 1) {
      Map<String, BigDecimal> capacity = new HashMap<>();
      String queueName = children.get(0).getName();
      capacity.put(queueName, HUNDRED);

      return Pair.of(capacity, false);
    } else {
      Map<String, BigDecimal> capacities = new HashMap<>();

      children
          .stream()
          .forEach(queue -> {
            BigDecimal pct;

            if (totalWeight == 0) {
              pct = ZERO;
            } else {
              BigDecimal total = new BigDecimal(totalWeight);
              BigDecimal weight = new BigDecimal(queue.getWeight());
              pct = weight
                  .setScale(5)
                  .divide(total, RoundingMode.HALF_UP)
                  .multiply(HUNDRED)
                  .setScale(3);
            }

            if (Resources.none().compareTo(queue.getMinShare()) != 0) {
              ruleHandler.handleMinResources();
            }

            capacities.put(queue.getName(), pct);
          });

      BigDecimal totalPct = ZERO;
      for (Map.Entry<String, BigDecimal> entry : capacities.entrySet()) {
        totalPct = totalPct.add(entry.getValue());
      }

      // fix capacities if total != 100.000
      boolean shouldAllowZeroSumCapacity = false;
      if (!totalPct.equals(HUNDRED)) {
        shouldAllowZeroSumCapacity = fixCapacities(capacities, totalPct);
      }

      return Pair.of(capacities, shouldAllowZeroSumCapacity);
    }
  }

  @VisibleForTesting
  static boolean fixCapacities(Map<String, BigDecimal> capacities,
      BigDecimal totalPct) {
    final BigDecimal hundred = new BigDecimal(100).setScale(3);
    boolean shouldAllowZeroSumCapacity = false;

    // Sort the list so we'll adjust the highest capacity value,
    // because that will affected less by a small change.
    // Also, it's legal to have weight = 0 and we have to avoid picking
    // that value as well.
    List<Map.Entry<String, BigDecimal>> sortedEntries = capacities
        .entrySet()
        .stream()
        .sorted(new Comparator<Map.Entry<String, BigDecimal>>() {
          @Override
          public int compare(Map.Entry<String, BigDecimal> e1,
              Map.Entry<String, BigDecimal> e2) {
            return e2.getValue().compareTo(e1.getValue());
          }
        })
        .collect(Collectors.toList());

    String highestCapacityQueue = sortedEntries.get(0).getKey();
    BigDecimal highestCapacity = sortedEntries.get(0).getValue();

    if (highestCapacity.equals(ZERO)) {
      // need to set allow-zero-capacity-sum on this queue
      // because we have zero weights on this level
      shouldAllowZeroSumCapacity = true;
    } else {
      BigDecimal diff = hundred.subtract(totalPct);
      BigDecimal correctedHighest = highestCapacity.add(diff);
      capacities.put(highestCapacityQueue, correctedHighest);
    }

    return shouldAllowZeroSumCapacity;
  }
}