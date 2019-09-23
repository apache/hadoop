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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFSParentQueue {

  private QueueManager queueManager;

  @Before
  public void setUp() {
    FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
    RMContext rmContext = mock(RMContext.class);
    SystemClock clock = SystemClock.getInstance();
    PlacementManager placementManager = new PlacementManager();
    FairScheduler scheduler = mock(FairScheduler.class);
    when(scheduler.getRMContext()).thenReturn(rmContext);
    when(scheduler.getConfig()).thenReturn(conf);
    when(scheduler.getConf()).thenReturn(conf);
    when(scheduler.getResourceCalculator()).thenReturn(
        new DefaultResourceCalculator());
    when(scheduler.getClock()).thenReturn(clock);
    when(rmContext.getQueuePlacementManager()).thenReturn(placementManager);
    AllocationConfiguration allocConf = new AllocationConfiguration(scheduler);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    queueManager = new QueueManager(scheduler);
    FSQueueMetrics.forQueue("root", null, true, conf);
    queueManager.initialize();
  }

  @Test
  public void testConcurrentChangeToGetChildQueue() {

    queueManager.getLeafQueue("parent.child", true);
    queueManager.getLeafQueue("parent.child2", true);
    FSParentQueue test = queueManager.getParentQueue("parent", false);
    assertEquals(2, test.getChildQueues().size());

    boolean first = true;
    int childQueuesFound = 0;
    for (FSQueue childQueue:test.getChildQueues()) {
      if (first) {
        first = false;
        queueManager.getLeafQueue("parent.child3", true);
      }
      childQueuesFound++;
    }

    assertEquals(2, childQueuesFound);
    assertEquals(3, test.getChildQueues().size());
  }
}
