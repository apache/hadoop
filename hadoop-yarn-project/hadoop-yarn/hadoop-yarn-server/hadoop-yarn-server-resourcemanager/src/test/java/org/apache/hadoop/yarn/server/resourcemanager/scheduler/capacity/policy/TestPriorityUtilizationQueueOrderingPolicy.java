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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Collections;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPriorityUtilizationQueueOrderingPolicy {
  private List<CSQueue> mockCSQueues(String[] queueNames, int[] priorities,
      float[] utilizations, float[] absCapacities, String partition) {
    // sanity check
    assert queueNames != null && priorities != null && utilizations != null
        && queueNames.length > 0 && queueNames.length == priorities.length
        && priorities.length == utilizations.length;

    List<CSQueue> list = new ArrayList<>();
    for (int i = 0; i < queueNames.length; i++) {
      CSQueue q = mock(CSQueue.class);
      when(q.getQueuePath()).thenReturn(queueNames[i]);

      QueueCapacities qc = new QueueCapacities(false);
      qc.setAbsoluteCapacity(partition, absCapacities[i]);
      qc.setUsedCapacity(partition, utilizations[i]);

      when(q.getQueueCapacities()).thenReturn(qc);
      when(q.getPriority()).thenReturn(Priority.newInstance(priorities[i]));

      QueueResourceQuotas qr = new QueueResourceQuotas();
      when(q.getQueueResourceQuotas()).thenReturn(qr);
      list.add(q);
    }

    return list;
  }

  private void verifyOrder(QueueOrderingPolicy orderingPolicy, String partition,
      String[] expectedOrder) {
    Iterator<CSQueue> iter = orderingPolicy.getAssignmentIterator(partition);
    int i = 0;
    while (iter.hasNext()) {
      CSQueue q = iter.next();
      Assert.assertEquals(expectedOrder[i], q.getQueuePath());
      i++;
    }

    assert i == expectedOrder.length;
  }

  @Test
  public void testUtilizationOrdering() {
    PriorityUtilizationQueueOrderingPolicy policy =
        new PriorityUtilizationQueueOrderingPolicy(false);

    // Case 1, one queue
    policy.setQueues(mockCSQueues(new String[] { "a" }, new int[] { 0 },
        new float[] { 0.1f }, new float[] {0.2f},  ""));
    verifyOrder(policy, "", new String[] { "a" });

    // Case 2, 2 queues
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 0, 0 },
        new float[] { 0.1f, 0.0f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 3, 3 queues
    policy.setQueues(
        mockCSQueues(new String[] { "a", "b", "c" }, new int[] { 0, 0, 0 },
            new float[] { 0.1f, 0.0f, 0.2f }, new float[] {0.2f, 0.3f, 0.4f},
            ""));
    verifyOrder(policy, "", new String[] { "b", "a", "c" });

    // Case 4, 3 queues, ignore priority
    policy.setQueues(
        mockCSQueues(new String[] { "a", "b", "c" }, new int[] { 2, 1, 0 },
            new float[] { 0.1f, 0.0f, 0.2f }, new float[] {0.2f, 0.3f, 0.4f},
            ""));
    verifyOrder(policy, "", new String[] { "b", "a", "c" });

    // Case 5, 3 queues, look at partition (default)
    policy.setQueues(
        mockCSQueues(new String[] { "a", "b", "c" }, new int[] { 2, 1, 0 },
            new float[] { 0.1f, 0.0f, 0.2f }, new float[] {0.2f, 0.3f, 0.4f},
            "x"));
    verifyOrder(policy, "", new String[] { "a", "b", "c" });

    // Case 5, 3 queues, look at partition (x)
    policy.setQueues(
        mockCSQueues(new String[] { "a", "b", "c" }, new int[] { 2, 1, 0 },
            new float[] { 0.1f, 0.0f, 0.2f }, new float[] {0.2f, 0.3f, 0.4f},
            "x"));
    verifyOrder(policy, "x", new String[] { "b", "a", "c" });

    // Case 6, 3 queues, with different accessibility to partition
    List<CSQueue> queues = mockCSQueues(new String[] { "a", "b", "c" }, new int[] { 2, 1, 0 },
        new float[] { 0.1f, 0.0f, 0.2f }, new float[] {0.2f, 0.3f, 0.4f},  "x");
    // a can access "x"
    when(queues.get(0).getAccessibleNodeLabels()).thenReturn(ImmutableSet.of("x", "y"));
    // c can access "x"
    when(queues.get(2).getAccessibleNodeLabels()).thenReturn(ImmutableSet.of("x", "y"));
    policy.setQueues(queues);
    verifyOrder(policy, "x", new String[] { "a", "c", "b" });
  }

  @Test
  public void testPriorityUtilizationOrdering() {
    PriorityUtilizationQueueOrderingPolicy policy =
        new PriorityUtilizationQueueOrderingPolicy(true);

    // Case 1, one queue
    policy.setQueues(mockCSQueues(new String[] { "a" }, new int[] { 1 },
        new float[] { 0.1f }, new float[] {0.2f}, ""));
    verifyOrder(policy, "", new String[] { "a" });

    // Case 2, 2 queues, both under utilized, same priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 0.2f, 0.1f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 3, 2 queues, both over utilized, same priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 1.1f, 1.2f },new float[] {0.2f, 0.3f},  ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 4, 2 queues, one under and one over, same priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 0.1f, 1.2f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 5, 2 queues, both over utilized, different priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 2 },
        new float[] { 1.1f, 1.2f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 6, 2 queues, both under utilized, different priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 2 },
        new float[] { 0.1f, 0.2f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 7, 2 queues, one under utilized and one over utilized,
    // different priority (1)
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 2 },
        new float[] { 0.1f, 1.2f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 8, 2 queues, one under utilized and one over utilized,
    // different priority (1)
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 2, 1 },
        new float[] { 0.1f, 1.2f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 9, 2 queues, one under utilized and one meet, different priority (1)
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 2 },
        new float[] { 0.1f, 1.0f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 10, 2 queues, one under utilized and one meet, different priority (2)
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 2, 1 },
        new float[] { 0.1f, 1.0f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 11, 2 queues, one under utilized and one meet, same priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 0.1f, 1.0f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    // Case 12, 2 queues, both meet, different priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 2 },
        new float[] { 1.0f, 1.0f }, new float[] {0.2f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 13, 5 queues, different priority
    policy.setQueues(mockCSQueues(new String[] { "a", "b", "c", "d", "e" },
        new int[] { 1, 2, 0, 0, 3 },
        new float[] { 1.2f, 1.0f, 0.2f, 1.1f, 0.2f },
        new float[] { 0.2f, 0.1f, 0.1f, 0.3f, 0.3f }, ""));
    verifyOrder(policy, "", new String[] { "e", "c", "b", "a", "d" });

    // Case 14, 5 queues, different priority,
    // partition default - abs capacity is 0;
    policy.setQueues(mockCSQueues(new String[] { "a", "b", "c", "d", "e" },
        new int[] { 1, 2, 0, 0, 3 },
        new float[] { 1.2f, 1.0f, 0.2f, 1.1f, 0.2f },
        new float[] { 0.2f, 0.1f, 0.1f, 0.3f, 0.3f }, "x"));
    verifyOrder(policy, "", new String[] { "e", "b", "a", "c", "d" });

    // Case 15, 5 queues, different priority, partition x;
    policy.setQueues(mockCSQueues(new String[] { "a", "b", "c", "d", "e" },
        new int[] { 1, 2, 0, 0, 3 },
        new float[] { 1.2f, 1.0f, 0.2f, 1.1f, 0.2f },
        new float[] { 0.2f, 0.1f, 0.1f, 0.3f, 0.3f }, "x"));
    verifyOrder(policy, "x", new String[] { "e", "c", "b", "a", "d" });

    // Case 16, 5 queues, different priority, partition x; and different
    // accessibility
    List<CSQueue> queues = mockCSQueues(new String[] { "a", "b", "c", "d", "e" },
        new int[] { 1, 2, 0, 0, 3 },
        new float[] { 1.2f, 1.0f, 0.2f, 1.1f, 0.2f },
        new float[] { 0.2f, 0.1f, 0.1f, 0.3f, 0.3f }, "x");
    // Only a/d has access to x
    when(queues.get(0).getAccessibleNodeLabels()).thenReturn(
        ImmutableSet.of("x"));
    when(queues.get(3).getAccessibleNodeLabels()).thenReturn(
        ImmutableSet.of("x"));
    policy.setQueues(queues);
    verifyOrder(policy, "x", new String[] { "a", "d", "e", "c", "b" });

    // Case 17, 2 queues, one's abs capacity is 0
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 0.1f, 1.2f }, new float[] {0.0f, 0.3f}, ""));
    verifyOrder(policy, "", new String[] { "b", "a" });

    // Case 18, 2 queues, one's abs capacity is 0
    policy.setQueues(mockCSQueues(new String[] { "a", "b" }, new int[] { 1, 1 },
        new float[] { 0.1f, 1.2f }, new float[] {0.3f, 0.0f}, ""));
    verifyOrder(policy, "", new String[] { "a", "b" });

    //Case 19, 5 queues with 2 having abs capacity 0 are prioritized last
    policy.setQueues(mockCSQueues(new String[] { "a", "b", "c", "d", "e" },
        new int[] { 1, 2, 0, 0, 3 },
        new float[] { 1.2f, 1.0f, 0.2f, 1.1f, 0.2f },
        new float[] { 0.0f, 0.0f, 0.1f, 0.3f, 0.3f }, "x"));
    verifyOrder(policy, "x", new String[] { "e", "c", "d", "b", "a" });

  }

  @Test
  public void testComparatorDoesNotValidateGeneralContract() {
    final String[] nodeLabels = {"x", "y", "z"};
    PriorityUtilizationQueueOrderingPolicy policy =
        new PriorityUtilizationQueueOrderingPolicy(true);

    final String partition = nodeLabels[randInt(0, nodeLabels.length - 1)];
    List<CSQueue> list = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      CSQueue q = mock(CSQueue.class);
      when(q.getQueuePath()).thenReturn(String.format("%d", i));

      // simulating change in queueCapacities
      when(q.getQueueCapacities())
          .thenReturn(randomQueueCapacities(partition))
          .thenReturn(randomQueueCapacities(partition))
          .thenReturn(randomQueueCapacities(partition))
          .thenReturn(randomQueueCapacities(partition))
          .thenReturn(randomQueueCapacities(partition));

      // simulating change in the priority
      when(q.getPriority())
          .thenReturn(Priority.newInstance(randInt(0, 10)))
          .thenReturn(Priority.newInstance(randInt(0, 10)))
          .thenReturn(Priority.newInstance(randInt(0, 10)))
          .thenReturn(Priority.newInstance(randInt(0, 10)))
          .thenReturn(Priority.newInstance(randInt(0, 10)));

      if (randInt(0, nodeLabels.length) == 1) {
        // simulating change in nodeLabels
        when(q.getAccessibleNodeLabels())
            .thenReturn(randomNodeLabels(nodeLabels))
            .thenReturn(randomNodeLabels(nodeLabels))
            .thenReturn(randomNodeLabels(nodeLabels))
            .thenReturn(randomNodeLabels(nodeLabels))
            .thenReturn(randomNodeLabels(nodeLabels));
      }

      // simulating change in configuredMinResource
      when(q.getQueueResourceQuotas())
          .thenReturn(randomResourceQuotas(partition))
          .thenReturn(randomResourceQuotas(partition))
          .thenReturn(randomResourceQuotas(partition))
          .thenReturn(randomResourceQuotas(partition))
          .thenReturn(randomResourceQuotas(partition));
      list.add(q);
    }

    policy.setQueues(list);
    // java.lang.IllegalArgumentException: Comparison method violates its general contract!
    assertDoesNotThrow(() -> policy.getAssignmentIterator(partition));
  }

  @Test
  public void testComparatorClassDoesNotViolateTimSortContract() {
    String partition = "testPartition";

    List<PriorityUtilizationQueueOrderingPolicy.
            PriorityQueueResourcesForSorting> queues = new ArrayList<>();
    for (int i = 0; i < 1000; i++) { // 1000 queues to have enough queues so the exception occur
      queues.add(createMockPriorityQueueResourcesForSorting(partition));
    }

    Collections.shuffle(queues);
    // java.lang.IllegalArgumentException: Comparison method violates its general contract!
    assertDoesNotThrow(() -> queues.sort(new PriorityUtilizationQueueOrderingPolicy(true)
            .new PriorityQueueComparator(partition)));

  }

  private PriorityUtilizationQueueOrderingPolicy.
          PriorityQueueResourcesForSorting createMockPriorityQueueResourcesForSorting(
          String partition) {
    QueueResourceQuotas resourceQuotas = randomResourceQuotas(partition);

    boolean isZeroResource = ThreadLocalRandom.current().nextBoolean();
    if (isZeroResource) {
      resourceQuotas.setConfiguredMinResource(partition,  Resource.newInstance(0, 0));
    }

    QueueCapacities mockQueueCapacities = mock(QueueCapacities.class);
    when(mockQueueCapacities.getAbsoluteUsedCapacity(partition))
            .thenReturn(4.2f); // could be any specific number, so that there are equal values
    when(mockQueueCapacities.getUsedCapacity(partition))
            .thenReturn(1.0f); // could be any specific number, so that there are equal values
    when(mockQueueCapacities.getAbsoluteCapacity(partition))
            .thenReturn(6.2f); // could be any specific number, so that there are equal values

    CSQueue mockQueue = mock(CSQueue.class);
    when(mockQueue.getQueueCapacities())
            .thenReturn(mockQueueCapacities);
    when(mockQueue.getPriority())
            .thenReturn(Priority.newInstance(7)); // could be any specific number,
    // so that there are equal values
    when(mockQueue.getAccessibleNodeLabels())
            .thenReturn(Collections.singleton(partition));
    when(mockQueue.getQueueResourceQuotas())
            .thenReturn(resourceQuotas);

    return new PriorityUtilizationQueueOrderingPolicy.PriorityQueueResourcesForSorting(
            mockQueue, partition
    );

  }

  private QueueCapacities randomQueueCapacities(String partition) {
    QueueCapacities qc = new QueueCapacities(false);
    qc.setAbsoluteCapacity(partition, (float) randFloat(0.0d, 100.0d));
    qc.setUsedCapacity(partition, (float) randFloat(0.0d, 100.0d));
    qc.setAbsoluteUsedCapacity(partition, (float) randFloat(0.0d, 100.0d));
    return qc;
  }

  private Set<String> randomNodeLabels(String[] availableNodeLabels) {
    Set<String> nodeLabels = new HashSet<>();
    for (String label : availableNodeLabels) {
      if (randInt(0, 1) == 1) {
        nodeLabels.add(label);
      }
    }
    return nodeLabels;
  }

  private QueueResourceQuotas randomResourceQuotas(String partition) {
    QueueResourceQuotas qr = new QueueResourceQuotas();
    qr.setConfiguredMinResource(partition,
        Resource.newInstance(randInt(1, 10) * 1024, randInt(1, 10)));
    return qr;
  }

  private static double randFloat(double min, double max) {
    return min + ThreadLocalRandom.current().nextFloat() * (max - min);
  }

  private static int randInt(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max + 1);
  }
}
