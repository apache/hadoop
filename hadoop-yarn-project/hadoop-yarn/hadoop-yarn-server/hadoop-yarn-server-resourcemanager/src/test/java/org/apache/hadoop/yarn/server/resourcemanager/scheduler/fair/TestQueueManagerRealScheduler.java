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

import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * QueueManager tests that require a real scheduler
 */
public class TestQueueManagerRealScheduler extends FairSchedulerTestBase {
  private final static File ALLOC_FILE = new File(TEST_DIR, "test-queue-mgr");

  @Before
  public void setup() throws IOException {
    createConfiguration();
    writeAllocFile(30);
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
        ALLOC_FILE.getAbsolutePath());

    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();
  }

  @After
  public void teardown() {
    ALLOC_FILE.deleteOnExit();
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
  }

  private void writeAllocFile(int defaultFairShareTimeout) {
    AllocationFileWriter.create()
        .addQueue(new AllocationFileQueue.Builder("default")
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueA").build())
        .addQueue(new AllocationFileQueue.Builder("queueB")
            .subQueue(new AllocationFileQueue.Builder("queueB1")
              .minSharePreemptionTimeout(5).build())
            .subQueue(new AllocationFileQueue.Builder("queueB2").build())
            .build())
        .addQueue(new AllocationFileQueue.Builder("queueC").build())
        .defaultMinSharePreemptionTimeout(15)
        .defaultFairSharePreemptionTimeout(defaultFairShareTimeout)
        .writeToFile(ALLOC_FILE.getAbsolutePath());
  }

  @Test
  public void testBackwardsCompatiblePreemptionConfiguration()
      throws IOException {
    // Check the min/fair share preemption timeout for each queue
    QueueManager queueMgr = scheduler.getQueueManager();
    assertEquals(30000, queueMgr.getQueue("root")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("default")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueA")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB1")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueB.queueB2")
        .getFairSharePreemptionTimeout());
    assertEquals(30000, queueMgr.getQueue("queueC")
        .getFairSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("root")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("default")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueA")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB")
        .getMinSharePreemptionTimeout());
    assertEquals(5000, queueMgr.getQueue("queueB.queueB1")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueB.queueB2")
        .getMinSharePreemptionTimeout());
    assertEquals(15000, queueMgr.getQueue("queueC")
        .getMinSharePreemptionTimeout());

    // Lower the fairshare preemption timeouts and verify it is picked
    // correctly.
    writeAllocFile(25);
    scheduler.reinitialize(conf, resourceManager.getRMContext());
    assertEquals(25000, queueMgr.getQueue("root")
        .getFairSharePreemptionTimeout());
  }
}
