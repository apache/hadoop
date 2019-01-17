/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.placement;

import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that test Metrics that support placement.
 */
public class TestDatanodeMetrics {
  @Test
  public void testSCMNodeMetric() {
    SCMNodeStat stat = new SCMNodeStat(100L, 10L, 90L);
    assertEquals((long) stat.getCapacity().get(), 100L);
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());
    SCMNodeMetric metric = new SCMNodeMetric(stat);

    SCMNodeStat newStat = new SCMNodeStat(100L, 10L, 90L);
    assertEquals(100L, (long) stat.getCapacity().get());
    assertEquals(10L, (long) stat.getScmUsed().get());
    assertEquals(90L, (long) stat.getRemaining().get());

    SCMNodeMetric newMetric = new SCMNodeMetric(newStat);
    assertTrue(metric.isEqual(newMetric.get()));

    newMetric.add(stat);
    assertTrue(newMetric.isGreater(metric.get()));

    SCMNodeMetric zeroMetric = new SCMNodeMetric(new SCMNodeStat());
    // Assert we can handle zero capacity.
    assertTrue(metric.isGreater(zeroMetric.get()));

  }
}
