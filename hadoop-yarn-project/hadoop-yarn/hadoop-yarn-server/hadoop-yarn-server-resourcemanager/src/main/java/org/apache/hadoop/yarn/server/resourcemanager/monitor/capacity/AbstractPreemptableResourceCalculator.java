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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}.
 */
public class AbstractPreemptableResourceCalculator {

  protected final CapacitySchedulerPreemptionContext context;
  protected final ResourceCalculator rc;
  private boolean isReservedPreemptionCandidatesSelector;

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
      if (getIdealPctOfGuaranteed(tq1) > getIdealPctOfGuaranteed(tq2)) {
        return 1;
      }
      return 0;
    }

    // Calculates idealAssigned / guaranteed
    // TempQueues with 0 guarantees are always considered the most over
    // capacity and therefore considered last for resources.
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(rc, clusterRes, q.getGuaranteed(),
          Resources.none())) {
        pctOver = Resources.divide(rc, clusterRes, q.idealAssigned,
            q.getGuaranteed());
      }
      return (pctOver);
    }
  }

  /**
   * PreemptableResourceCalculator constructor.
   *
   * @param preemptionContext context
   * @param isReservedPreemptionCandidatesSelector
   *          this will be set by different implementation of candidate
   *          selectors, please refer to TempQueuePerPartition#offer for
   *          details.
   */
  public AbstractPreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
  }

  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   *
   * @param totGuarant
   *          total guaranteed resource
   * @param qAlloc
   *          List of child queues
   * @param unassigned
   *          Unassigned resource per queue
   * @param ignoreGuarantee
   *          ignore guarantee per queue.
   */
  protected void computeFixpointAllocation(Resource totGuarant,
      Collection<TempQueuePerPartition> qAlloc, Resource unassigned,
      boolean ignoreGuarantee) {
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.
    TQComparator tqComparator = new TQComparator(rc, totGuarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed = new PriorityQueue<>(10,
        tqComparator);
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext();) {
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      if (Resources.greaterThan(rc, totGuarant, used, q.getGuaranteed())) {
        q.idealAssigned = Resources.add(q.getGuaranteed(), q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(used);
      }
      Resources.subtractFrom(unassigned, q.idealAssigned);
      // If idealAssigned < (allocated + used + pending), q needs more
      // resources, so
      // add it to the list of underserved queues, ordered by need.
      Resource curPlusPend = Resources.add(q.getUsed(), q.pending);
      if (Resources.lessThan(rc, totGuarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    // assign all cluster resources until no more demand, or no resources are
    // left
    while (!orderedByNeed.isEmpty() && Resources.greaterThan(rc, totGuarant,
        unassigned, Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      resetCapacity(unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      Collection<TempQueuePerPartition> underserved = getMostUnderservedQueues(
          orderedByNeed, tqComparator);
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        TempQueuePerPartition sub = i.next();
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc, unassigned,
            sub.normalizedGuarantee, Resource.newInstance(1, 1));
        Resource wQidle = sub.offer(wQavail, rc, totGuarant,
            isReservedPreemptionCandidatesSelector);
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        if (Resources.greaterThan(rc, totGuarant, wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
      Resources.subtractFrom(unassigned, wQassigned);
    }

    // Sometimes its possible that, all queues are properly served. So intra
    // queue preemption will not try for any preemption. How ever there are
    // chances that within a queue, there are some imbalances. Hence make sure
    // all queues are added to list.
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
    }
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues.
   *
   * @param clusterResource
   *          the total amount of resources in the cluster
   * @param queues
   *          the list of queues to consider
   * @param ignoreGuar
   *          ignore guarantee.
   */
  private void resetCapacity(Resource clusterResource,
      Collection<TempQueuePerPartition> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);

    if (ignoreGuar) {
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = 1.0f / queues.size();
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
      }
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = Resources.divide(rc, clusterResource,
            q.getGuaranteed(), activeCap);
      }
    }
  }

  // Take the most underserved TempQueue (the one on the head). Collect and
  // return the list of all queues that have the same idealAssigned
  // percentage of guaranteed.
  private Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed,
      TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);

      // Add underserved queues in order for later uses
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
      TempQueuePerPartition q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1, q2) < 0) {
        if (null != q2) {
          context.addPartitionToUnderServedQueues(q2.queueName, q2.partition);
        }
        return underserved;
      }
    }
    return underserved;
  }
}