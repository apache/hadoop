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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;

import java.util.Set;

public class CapacitySchedulerTestBase {
  protected final int GB = 1024;

  protected static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  protected static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  protected static final String A1 = A + ".a1";
  protected static final String A2 = A + ".a2";
  protected static final String B1 = B + ".b1";
  protected static final String B2 = B + ".b2";
  protected static final String B3 = B + ".b3";
  protected static float A_CAPACITY = 10.5f;
  protected static float B_CAPACITY = 89.5f;
  protected static final String P1 = CapacitySchedulerConfiguration.ROOT + ".p1";
  protected static final String P2 = CapacitySchedulerConfiguration.ROOT + ".p2";
  protected static final String X1 = P1 + ".x1";
  protected static final String X2 = P1 + ".x2";
  protected static final String Y1 = P2 + ".y1";
  protected static final String Y2 = P2 + ".y2";
  protected static float A1_CAPACITY = 30;
  protected static float A2_CAPACITY = 70;
  protected static float B1_CAPACITY = 79.2f;
  protected static float B2_CAPACITY = 0.8f;
  protected static float B3_CAPACITY = 20;


  @SuppressWarnings("unchecked")
  protected <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }

  protected void checkPendingResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(
        memory,
        queue.getQueueResourceUsage()
            .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }


  protected void checkPendingResourceGreaterThanZero(MockRM rm, String queueName,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertTrue(queue.getQueueResourceUsage()
        .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
        .getMemorySize() > 0);
  }

  protected void waitforNMRegistered(ResourceScheduler scheduler, int nodecount,
      int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (scheduler.getNumClusterNodes() < nodecount) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }
}
