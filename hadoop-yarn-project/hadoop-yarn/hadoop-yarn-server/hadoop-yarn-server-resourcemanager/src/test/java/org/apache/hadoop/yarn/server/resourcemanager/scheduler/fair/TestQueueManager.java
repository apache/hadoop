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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestQueueManager {
  private FairSchedulerConfiguration conf;
  private QueueManager queueManager;
  private Set<FSQueue> notEmptyQueues;
  
  @Before
  public void setUp() throws Exception {
    conf = new FairSchedulerConfiguration();
    FairScheduler scheduler = mock(FairScheduler.class);
    AllocationConfiguration allocConf = new AllocationConfiguration(conf);
    when(scheduler.getAllocationConfiguration()).thenReturn(allocConf);
    when(scheduler.getConf()).thenReturn(conf);
    SystemClock clock = new SystemClock();
    when(scheduler.getClock()).thenReturn(clock);
    notEmptyQueues = new HashSet<FSQueue>();
    queueManager = new QueueManager(scheduler) {
      @Override
      public boolean isEmpty(FSQueue queue) {
        return !notEmptyQueues.contains(queue);
      }
    };
    FSQueueMetrics.forQueue("root", null, true, conf);
    queueManager.initialize(conf);
  }
  
  @Test
  public void testReloadTurnsLeafQueueIntoParent() throws Exception {
    updateConfiguredQueues(queueManager, "queue1");
    
    // When no apps are running in the leaf queue, should be fine turning it
    // into a parent.
    updateConfiguredQueues(queueManager, "queue1.queue2");
    assertNull(queueManager.getLeafQueue("queue1", false));
    assertNotNull(queueManager.getLeafQueue("queue1.queue2", false));
    
    // When leaf queues are empty, should be ok deleting them and
    // turning parent into a leaf.
    updateConfiguredQueues(queueManager, "queue1");
    assertNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNotNull(queueManager.getLeafQueue("queue1", false));
    
    // When apps exist in leaf queue, we shouldn't be able to create
    // children under it, but things should work otherwise.
    notEmptyQueues.add(queueManager.getLeafQueue("queue1", false));
    updateConfiguredQueues(queueManager, "queue1.queue2");
    assertNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNotNull(queueManager.getLeafQueue("queue1", false));
    
    // When apps exist in leaf queues under a parent queue, shouldn't be
    // able to turn it into a leaf queue, but things should work otherwise.
    notEmptyQueues.clear();
    updateConfiguredQueues(queueManager, "queue1.queue2");
    notEmptyQueues.add(queueManager.getQueue("root.queue1"));
    updateConfiguredQueues(queueManager, "queue1");
    assertNotNull(queueManager.getLeafQueue("queue1.queue2", false));
    assertNull(queueManager.getLeafQueue("queue1", false));
    
    // Should never to be able to create a queue under the default queue
    updateConfiguredQueues(queueManager, "default.queue3");
    assertNull(queueManager.getLeafQueue("default.queue3", false));
    assertNotNull(queueManager.getLeafQueue("default", false));
  }
  
  private void updateConfiguredQueues(QueueManager queueMgr, String... confQueues) {
    AllocationConfiguration allocConf = new AllocationConfiguration(conf);
    allocConf.queueNames = Sets.newHashSet(confQueues);
    queueMgr.updateAllocationConfiguration(allocConf);
  }
}
