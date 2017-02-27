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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * For two queues with the same priority:
 * - The queue with less relative used-capacity goes first - today’s behavior.
 * - The default priority for all queues is 0 and equal. So, we get today’s
 *   behaviour at every level - the queue with the lowest used-capacity
 *   percentage gets the resources
 *
 * For two queues with different priorities:
 * - Both the queues are under their guaranteed capacities: The queue with
 *   the higher priority gets resources
 * - Both the queues are over or meeting their guaranteed capacities:
 *   The queue with the higher priority gets resources
 * - One of the queues is over or meeting their guaranteed capacities and the
 *   other is under: The queue that is under its capacity guarantee gets the
 *   resources.
 */
public class PriorityUtilizationQueueOrderingPolicy implements QueueOrderingPolicy {
  private List<CSQueue> queues;
  private boolean respectPriority;

  // This makes multiple threads can sort queues at the same time
  // For different partitions.
  private static ThreadLocal<String> partitionToLookAt =
      new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
          return RMNodeLabelsManager.NO_LABEL;
        }
      };

  /**
   * Compare two queues with possibly different priority and assigned capacity,
   * Will be used by preemption policy as well.
   *
   * @param relativeAssigned1 relativeAssigned1
   * @param relativeAssigned2 relativeAssigned2
   * @param priority1 p1
   * @param priority2 p2
   * @return compared result
   */
  public static int compare(double relativeAssigned1, double relativeAssigned2,
      int priority1, int priority2) {
    if (priority1 == priority2) {
      // The queue with less relative used-capacity goes first
      return Double.compare(relativeAssigned1, relativeAssigned2);
    } else {
      // When priority is different:
      if ((relativeAssigned1 < 1.0f && relativeAssigned2 < 1.0f) || (
          relativeAssigned1 >= 1.0f && relativeAssigned2 >= 1.0f)) {
        // When both the queues are under their guaranteed capacities,
        // Or both the queues are over or meeting their guaranteed capacities
        // queue with higher used-capacity goes first
        return Integer.compare(priority2, priority1);
      } else {
        // Otherwise, when one of the queues is over or meeting their
        // guaranteed capacities and the other is under: The queue that is
        // under its capacity guarantee gets the resources.
        return Double.compare(relativeAssigned1, relativeAssigned2);
      }
    }
  }

  /**
   * Comparator that both looks at priority and utilization
   */
  private class PriorityQueueComparator implements  Comparator<CSQueue> {

    @Override
    public int compare(CSQueue q1, CSQueue q2) {
      String p = partitionToLookAt.get();

      int rc = compareQueueAccessToPartition(q1, q2, p);
      if (0 != rc) {
        return rc;
      }

      float used1 = q1.getQueueCapacities().getUsedCapacity(p);
      float used2 = q2.getQueueCapacities().getUsedCapacity(p);
      int p1 = 0;
      int p2 = 0;
      if (respectPriority) {
        p1 = q1.getPriority().getPriority();
        p2 = q2.getPriority().getPriority();
      }

      rc = PriorityUtilizationQueueOrderingPolicy.compare(used1, used2, p1, p2);

      // For queue with same used ratio / priority, queue with higher configured
      // capacity goes first
      if (0 == rc) {
        float abs1 = q1.getQueueCapacities().getAbsoluteCapacity(p);
        float abs2 = q2.getQueueCapacities().getAbsoluteCapacity(p);
        return Float.compare(abs2, abs1);
      }

      return rc;
    }

    private int compareQueueAccessToPartition(CSQueue q1, CSQueue q2, String partition) {
      // Everybody has access to default partition
      if (StringUtils.equals(partition, RMNodeLabelsManager.NO_LABEL)) {
        return 0;
      }

      /*
       * Check accessible to given partition, if one queue accessible and
       * the other not, accessible queue goes first.
       */
      boolean q1Accessible =
          q1.getAccessibleNodeLabels() != null && q1.getAccessibleNodeLabels()
              .contains(partition) || q1.getAccessibleNodeLabels().contains(
              RMNodeLabelsManager.ANY);
      boolean q2Accessible =
          q2.getAccessibleNodeLabels() != null && q2.getAccessibleNodeLabels()
              .contains(partition) || q2.getAccessibleNodeLabels().contains(
              RMNodeLabelsManager.ANY);
      if (q1Accessible && !q2Accessible) {
        return -1;
      } else if (!q1Accessible && q2Accessible) {
        return 1;
      }

      return 0;
    }
  }

  public PriorityUtilizationQueueOrderingPolicy(boolean respectPriority) {
    this.respectPriority = respectPriority;
  }

  @Override
  public void setQueues(List<CSQueue> queues) {
    this.queues = queues;
  }

  @Override
  public Iterator<CSQueue> getAssignmentIterator(String partition) {
    // Since partitionToLookAt is a thread local variable, and every time we
    // copy and sort queues, so it's safe for multi-threading environment.
    PriorityUtilizationQueueOrderingPolicy.partitionToLookAt.set(partition);
    List<CSQueue> sortedQueue = new ArrayList<>(queues);
    Collections.sort(sortedQueue, new PriorityQueueComparator());
    return sortedQueue.iterator();
  }

  @Override
  public String getConfigName() {
    if (respectPriority) {
      return CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY;
    } else{
      return CapacitySchedulerConfiguration.QUEUE_UTILIZATION_ORDERING_POLICY;
    }
  }

  @VisibleForTesting
  public List<CSQueue> getQueues() {
    return queues;
  }
}
