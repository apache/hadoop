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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProcessingDetails.Timing;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.ipc.WeightedTimeCostProvider.DEFAULT_LOCKEXCLUSIVE_WEIGHT;
import static org.apache.hadoop.ipc.WeightedTimeCostProvider.DEFAULT_LOCKFREE_WEIGHT;
import static org.apache.hadoop.ipc.WeightedTimeCostProvider.DEFAULT_LOCKSHARED_WEIGHT;
import static org.junit.Assert.assertEquals;

/** Tests for {@link WeightedTimeCostProvider}. */
public class TestWeightedTimeCostProvider {

  private static final int QUEUE_TIME = 3;
  private static final int LOCKFREE_TIME = 5;
  private static final int LOCKSHARED_TIME = 7;
  private static final int LOCKEXCLUSIVE_TIME = 11;

  private WeightedTimeCostProvider costProvider;
  private ProcessingDetails processingDetails;

  @Before
  public void setup() {
    costProvider = new WeightedTimeCostProvider();
    processingDetails = new ProcessingDetails(TimeUnit.MILLISECONDS);
    processingDetails.set(Timing.QUEUE, QUEUE_TIME);
    processingDetails.set(Timing.LOCKFREE, LOCKFREE_TIME);
    processingDetails.set(Timing.LOCKSHARED, LOCKSHARED_TIME);
    processingDetails.set(Timing.LOCKEXCLUSIVE, LOCKEXCLUSIVE_TIME);
  }

  @Test(expected = AssertionError.class)
  public void testGetCostBeforeInit() {
    costProvider.getCost(null);
  }

  @Test
  public void testGetCostDefaultWeights() {
    costProvider.init("foo", new Configuration());
    long actualCost = costProvider.getCost(processingDetails);
    long expectedCost = DEFAULT_LOCKFREE_WEIGHT * LOCKFREE_TIME
        + DEFAULT_LOCKSHARED_WEIGHT * LOCKSHARED_TIME
        + DEFAULT_LOCKEXCLUSIVE_WEIGHT * LOCKEXCLUSIVE_TIME;
    assertEquals(expectedCost, actualCost);
  }

  @Test
  public void testGetCostConfiguredWeights() {
    Configuration conf = new Configuration();
    int queueWeight = 1000;
    int lockfreeWeight = 10000;
    int locksharedWeight = 100000;
    conf.setInt("foo.weighted-cost.queue", queueWeight);
    conf.setInt("foo.weighted-cost.lockfree", lockfreeWeight);
    conf.setInt("foo.weighted-cost.lockshared", locksharedWeight);
    conf.setInt("bar.weighted-cost.lockexclusive", 0); // should not apply
    costProvider.init("foo", conf);
    long actualCost = costProvider.getCost(processingDetails);
    long expectedCost = queueWeight * QUEUE_TIME
        + lockfreeWeight * LOCKFREE_TIME
        + locksharedWeight * LOCKSHARED_TIME
        + DEFAULT_LOCKEXCLUSIVE_WEIGHT * LOCKEXCLUSIVE_TIME;
    assertEquals(expectedCost, actualCost);
  }
}
