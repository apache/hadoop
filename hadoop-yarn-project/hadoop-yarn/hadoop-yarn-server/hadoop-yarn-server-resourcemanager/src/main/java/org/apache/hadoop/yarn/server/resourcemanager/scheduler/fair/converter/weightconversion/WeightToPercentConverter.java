/*
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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;

public class WeightToPercentConverter
    implements CapacityConverter {

  private static final BigDecimal HUNDRED = new BigDecimal(100).setScale(3);
  private static final BigDecimal ZERO = new BigDecimal(0).setScale(3);

  @Override
  public void convertWeightsForChildQueues(FSQueue queue,
      CapacitySchedulerConfiguration csConfig) {
    List<FSQueue> children = queue.getChildQueues();

    int totalWeight = getTotalWeight(children);
    Pair<Map<String, BigDecimal>, Boolean> result =
        getCapacities(totalWeight, children);

    Map<String, BigDecimal> capacities = result.getLeft();
    boolean shouldAllowZeroSumCapacity = result.getRight();

    capacities
        .forEach((key, value) -> csConfig.setCapacity(key, value.toString()));

    if (shouldAllowZeroSumCapacity) {
      String queueName = queue.getName();
      csConfig.setAllowZeroCapacitySum(queueName, true);
    }
  }

  private Pair<Map<String, BigDecimal>, Boolean> getCapacities(int totalWeight,
      List<FSQueue> children) {

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
  boolean fixCapacities(Map<String, BigDecimal> capacities,
      BigDecimal totalPct) {
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
      BigDecimal diff = HUNDRED.subtract(totalPct);
      BigDecimal correctedHighest = highestCapacity.add(diff);
      capacities.put(highestCapacityQueue, correctedHighest);
    }

    return shouldAllowZeroSumCapacity;
  }

  private int getTotalWeight(List<FSQueue> children) {
    double sum = children
                  .stream()
                  .mapToDouble(c -> c.getWeight())
                  .sum();
    return (int) sum;
  }
}
