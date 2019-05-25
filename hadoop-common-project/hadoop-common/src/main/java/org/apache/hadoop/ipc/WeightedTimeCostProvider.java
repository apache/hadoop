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

package org.apache.hadoop.ipc;

import java.util.Locale;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.ipc.ProcessingDetails.Timing;

/**
 * A {@link CostProvider} that calculates the cost for an operation
 * as a weighted sum of its processing time values (see
 * {@link ProcessingDetails}). This can be used by specifying the
 * {@link org.apache.hadoop.fs.CommonConfigurationKeys#IPC_COST_PROVIDER_KEY}
 * configuration key.
 *
 * <p/>This allows for configuration of how heavily each of the operations
 * within {@link ProcessingDetails} is weighted. By default,
 * {@link ProcessingDetails.Timing#LOCKFREE},
 * {@link ProcessingDetails.Timing#RESPONSE}, and
 * {@link ProcessingDetails.Timing#HANDLER} times have a weight of
 * {@value #DEFAULT_LOCKFREE_WEIGHT},
 * {@link ProcessingDetails.Timing#LOCKSHARED} has a weight of
 * {@value #DEFAULT_LOCKSHARED_WEIGHT},
 * {@link ProcessingDetails.Timing#LOCKEXCLUSIVE} has a weight of
 * {@value #DEFAULT_LOCKEXCLUSIVE_WEIGHT}, and others are ignored.
 * These values can all be configured using the {@link #WEIGHT_CONFIG_PREFIX}
 * key, prefixed with the IPC namespace, and suffixed with the name of the
 * timing measurement from {@link ProcessingDetails} (all lowercase).
 * For example, to set the lock exclusive weight to be 1000, set:
 * <pre>
 *   ipc.8020.cost-provider.impl=org.apache.hadoop.ipc.WeightedTimeCostProvider
 *   ipc.8020.weighted-cost.lockexclusive=1000
 * </pre>
 */
public class WeightedTimeCostProvider implements CostProvider {

  /**
   * The prefix used in configuration values specifying the weight to use when
   * determining the cost of an operation. See the class Javadoc for more info.
   */
  public static final String WEIGHT_CONFIG_PREFIX = ".weighted-cost.";
  static final int DEFAULT_LOCKFREE_WEIGHT = 1;
  static final int DEFAULT_LOCKSHARED_WEIGHT = 10;
  static final int DEFAULT_LOCKEXCLUSIVE_WEIGHT = 100;

  private long[] weights;

  @Override
  public void init(String namespace, Configuration conf) {
    weights = new long[Timing.values().length];
    for (Timing timing : ProcessingDetails.Timing.values()) {
      final int defaultValue;
      switch (timing) {
      case LOCKFREE:
      case RESPONSE:
      case HANDLER:
        defaultValue = DEFAULT_LOCKFREE_WEIGHT;
        break;
      case LOCKSHARED:
        defaultValue = DEFAULT_LOCKSHARED_WEIGHT;
        break;
      case LOCKEXCLUSIVE:
        defaultValue = DEFAULT_LOCKEXCLUSIVE_WEIGHT;
        break;
      default:
        // by default don't bill for queueing or lock wait time
        defaultValue = 0;
      }
      String key = namespace + WEIGHT_CONFIG_PREFIX
          + timing.name().toLowerCase(Locale.ENGLISH);
      weights[timing.ordinal()] = conf.getInt(key, defaultValue);
    }
  }

  /**
   * Calculates a weighted sum of the times stored on the provided processing
   * details to be used as the cost in {@link DecayRpcScheduler}.
   *
   * @param details Processing details
   * @return The weighted sum of the times. The returned unit is the same
   *         as the default unit used by the provided processing details.
   */
  @Override
  public long getCost(ProcessingDetails details) {
    assert weights != null : "Cost provider must be initialized before use";
    long cost = 0;
    // weights was initialized to the same length as Timing.values()
    for (int i = 0; i < Timing.values().length; i++) {
      cost += details.get(Timing.values()[i]) * weights[i];
    }
    return cost;
  }
}
