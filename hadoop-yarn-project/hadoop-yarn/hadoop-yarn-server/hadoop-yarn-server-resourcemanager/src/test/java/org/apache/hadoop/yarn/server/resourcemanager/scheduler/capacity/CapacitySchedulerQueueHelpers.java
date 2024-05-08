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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import static org.junit.Assert.assertEquals;

public final class CapacitySchedulerQueueHelpers {

  public static final String DEFAULT_PATH = CapacitySchedulerConfiguration.ROOT + ".default";
  public static final String A_PATH = CapacitySchedulerConfiguration.ROOT + ".a";
  public static final String B_PATH = CapacitySchedulerConfiguration.ROOT + ".b";
  public static final String A_CHILD_PATH = A_PATH + ".a";
  public static final String A1_PATH = A_PATH + ".a1";
  public static final String A2_PATH = A_PATH + ".a2";
  public static final String A3_PATH = A_PATH + ".a3";
  public static final String B1_PATH = B_PATH + ".b1";
  public static final String B2_PATH = B_PATH + ".b2";
  public static final String B3_PATH = B_PATH + ".b3";
  public static final String A1_B1_PATH = A1_PATH + ".b1";
  public static final QueuePath ROOT = new QueuePath(CapacitySchedulerConfiguration.ROOT);
  public static final QueuePath DEFAULT = new QueuePath(DEFAULT_PATH);
  public static final QueuePath A = new QueuePath(A_PATH);
  public static final QueuePath A_CHILD = new QueuePath(A_CHILD_PATH);
  public static final QueuePath A1 = new QueuePath(A1_PATH);
  public static final QueuePath A2 = new QueuePath(A2_PATH);
  public static final QueuePath A3 = new QueuePath(A3_PATH);
  public static final QueuePath B = new QueuePath(B_PATH);
  public static final QueuePath B1 = new QueuePath(B1_PATH);
  public static final QueuePath B2 = new QueuePath(B2_PATH);
  public static final QueuePath B3 = new QueuePath(B3_PATH);
  public static final QueuePath A1_B1 = new QueuePath(A1_B1_PATH);
  public static final float A_CAPACITY = 10.5f;
  public static final float B_CAPACITY = 89.5f;
  public static final String P1_PATH = CapacitySchedulerConfiguration.ROOT + ".p1";
  public static final String P2_PATH = CapacitySchedulerConfiguration.ROOT + ".p2";
  public static final String X1_PATH = P1_PATH + ".x1";
  public static final String X2_PATH = P1_PATH + ".x2";
  public static final String Y1_PATH = P2_PATH + ".y1";
  public static final String Y2_PATH = P2_PATH + ".y2";
  public static final QueuePath P1 = new QueuePath(P1_PATH);
  public static final QueuePath P2 = new QueuePath(P2_PATH);
  public static final QueuePath X1 = new QueuePath(X1_PATH);
  public static final QueuePath X2 = new QueuePath(X2_PATH);
  public static final QueuePath Y1 = new QueuePath(Y1_PATH);
  public static final QueuePath Y2 = new QueuePath(Y2_PATH);
  public static final float A1_CAPACITY = 30;
  public static final float A2_CAPACITY = 70;
  public static final float B1_CAPACITY = 79.2f;
  public static final float B2_CAPACITY = 0.8f;
  public static final float B3_CAPACITY = 20;

  private CapacitySchedulerQueueHelpers() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * @param conf, to be modified
   * @return
   *           root
   *          /      \
   *        a         b
   *       / \     /  |  \
   *      a1  a2  b1  b2 b3
   *
   */
  public static CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT, new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    return conf;
  }

  public static CapacitySchedulerConfiguration setupAdditionalQueues(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT, new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2", "a3"});
    conf.setCapacity(A1, 30.0f);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, 30.0f);
    conf.setUserLimitFactor(A2, 100.0f);
    conf.setCapacity(A3, 40.0f);
    conf.setUserLimitFactor(A3, 100.0f);

    conf.setQueues(B, new String[]{"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted all children of queue(b)
   *           root
   *          /     \
   *        a        b
   *       / \
   *      a1  a2
   */
  public static CapacitySchedulerConfiguration setupQueueConfAmbiguousQueue(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a", "a1"});
    conf.setCapacity(A_CHILD, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A1, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted all childred of queue(b)
   *           root
   *          /     \
   *        a        b
   *       / \
   *      a1  a2
   */
  public static CapacitySchedulerConfiguration setupQueueConfWithoutChildrenOfB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted a queue(b1)
   *           root
   *          /     \
   *        a        b
   *       / \       | \
   *      a1  a2    b2  b3
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithoutB1(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b2", "b3"});
    conf.setCapacity(B2, B2_CAPACITY + B1_CAPACITY); //as B1 is deleted
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has converted b1 to parent queue
   *           root
   *          /     \
   *        a        b
   *       / \    /  |  \
   *      a1  a2 b1  b2  b3
   *              |
   *             b11
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithB1AsParentQueue(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    // Set childQueue for B1
    conf.setQueues(B1, new String[]{"b11"});
    final String b11Path = B1 + ".b11";
    final QueuePath b11 = new QueuePath(b11Path);
    conf.setCapacity(b11, 100.0f);
    conf.setUserLimitFactor(b11, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted a Parent queue(b)
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithoutB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT, new String[]{"a"});

    conf.setCapacity(A, A_CAPACITY + B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    return conf;
  }

  public static CapacitySchedulerConfiguration setupBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    conf.setUserLimitFactor(A, 100);
    conf.setUserLimitFactor(B, 100);
    conf.setMaximumCapacity(A, 100);
    conf.setMaximumCapacity(B, 100);
    return conf;
  }

  public static CapacitySchedulerConfiguration setupOtherBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(ROOT,
        new String[]{"p1", "p2"});

    conf.setCapacity(P1, 50f);
    conf.setMaximumCapacity(P1, 50f);
    conf.setCapacity(P2, 50f);
    conf.setMaximumCapacity(P2, 100f);
    // Define 2nd-level queues
    conf.setQueues(P1, new String[]{"x1", "x2"});
    conf.setCapacity(X1, 80f);
    conf.setMaximumCapacity(X1, 100f);
    conf.setUserLimitFactor(X1, 2f);
    conf.setCapacity(X2, 20f);
    conf.setMaximumCapacity(X2, 100f);
    conf.setUserLimitFactor(X2, 2f);

    conf.setQueues(P2, new String[]{"y1", "y2"});
    conf.setCapacity(Y1, 80f);
    conf.setUserLimitFactor(Y1, 2f);
    conf.setCapacity(Y2, 20f);
    conf.setUserLimitFactor(Y2, 2f);
    return conf;
  }

  public static class ExpectedCapacities {
    private final float capacity;
    private final float absCapacity;

    public ExpectedCapacities(float capacity, float parentCapacity) {
      this.capacity = capacity;
      absCapacity = this.capacity * parentCapacity;
    }

    public float getCapacity() {
      return capacity;
    }

    public float getAbsCapacity() {
      return absCapacity;
    }
  }

  public static Map<String, ExpectedCapacities> getDefaultCapacities(float capA, float capB) {
    Map<String, ExpectedCapacities> capacities = new HashMap<>();
    capacities.put(A.getFullPath(), new ExpectedCapacities(capA, 1.0f));
    capacities.put(B.getFullPath(), new ExpectedCapacities(capB, 1.0f));
    capacities.put(A1.getFullPath(), new ExpectedCapacities((A1_CAPACITY / 100.0f), capA));
    capacities.put(A2.getFullPath(), new ExpectedCapacities((A2_CAPACITY / 100.0f), capA));
    capacities.put(B1.getFullPath(), new ExpectedCapacities((B1_CAPACITY / 100.0f), capB));
    capacities.put(B2.getFullPath(), new ExpectedCapacities((B2_CAPACITY / 100.0f), capB));
    capacities.put(B3.getFullPath(), new ExpectedCapacities((B3_CAPACITY / 100.0f), capB));
    return capacities;
  }

  public static void checkQueueStructureCapacities(CapacityScheduler cs) {
    float capA = A_CAPACITY / 100.0f;
    float capB = B_CAPACITY / 100.0f;
    checkQueueStructureCapacities(cs, getDefaultCapacities(capA, capB));
  }

  public static void checkQueueStructureCapacities(CapacityScheduler cs,
                                                   Map<String, ExpectedCapacities> capacities) {
    CSQueue rootQueue = cs.getRootQueue();
    for (Map.Entry<String, ExpectedCapacities> entry : capacities.entrySet()) {
      CSQueue queue = findQueue(rootQueue, entry.getKey());
      Assert.assertNotNull(queue);
      assertQueueCapacities(queue, entry.getValue());
    }
  }

  public static void assertQueueCapacities(CSQueue q, ExpectedCapacities capacities) {
    final float epsilon = 1e-4f;
    assertEquals("capacity", capacities.getCapacity(), q.getCapacity(), epsilon);
    assertEquals("absolute capacity", capacities.getAbsCapacity(),
        q.getAbsoluteCapacity(), epsilon);
    assertEquals("maximum capacity", 1.0f, q.getMaximumCapacity(), epsilon);
    assertEquals("absolute maximum capacity", 1.0f, q.getAbsoluteMaximumCapacity(), epsilon);
  }

  public static CSQueue findQueue(CSQueue root, String queuePath) {
    if (root.getQueuePath().equals(queuePath)) {
      return root;
    }

    List<CSQueue> childQueues = root.getChildQueues();
    if (childQueues != null) {
      for (CSQueue q : childQueues) {
        if (queuePath.startsWith(q.getQueuePath())) {
          CSQueue result = findQueue(q, queuePath);
          if (result != null) {
            return result;
          }
        }
      }
    }

    return null;
  }

}
