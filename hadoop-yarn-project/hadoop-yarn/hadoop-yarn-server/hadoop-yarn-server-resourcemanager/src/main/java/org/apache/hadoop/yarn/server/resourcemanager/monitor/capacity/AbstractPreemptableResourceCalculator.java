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
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.PriorityUtilizationQueueOrderingPolicy;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
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
  protected boolean isReservedPreemptionCandidatesSelector;
  private Resource stepFactor;
  private boolean allowQueuesBalanceAfterAllQueuesSatisfied;

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      double assigned1 = getIdealPctOfGuaranteed(tq1);
      double assigned2 = getIdealPctOfGuaranteed(tq2);

      return PriorityUtilizationQueueOrderingPolicy.compare(assigned1,
          assigned2, tq1.relativePriority, tq2.relativePriority);
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
   * @param allowQueuesBalanceAfterAllQueuesSatisfied
   *          Should resources be preempted from an over-served queue when the
   *          requesting queues are all at or over their guarantees?
   *          An example is, there're 10 queues under root, guaranteed resource
   *          of them are all 10%.
   *          Assume there're two queues are using resources, queueA uses 10%
   *          queueB uses 90%. For all queues are guaranteed, but it's not fair
   *          for queueA.
   *          We wanna make this behavior can be configured. By default it is
   *          not allowed.
   *
   */
  public AbstractPreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
    this.allowQueuesBalanceAfterAllQueuesSatisfied =
        allowQueuesBalanceAfterAllQueuesSatisfied;
    stepFactor = Resource.newInstance(0, 0);
    for (ResourceInformation ri : stepFactor.getResources()) {
      ri.setValue(1);
    }
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
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext(); ) {
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      Resource initIdealAssigned;
      if (Resources.greaterThan(rc, totGuarant, used, q.getGuaranteed())) {
        initIdealAssigned = Resources.add(
            Resources.componentwiseMin(q.getGuaranteed(), q.getUsed()),
            q.untouchableExtra);
      } else{
        initIdealAssigned = Resources.clone(used);
      }

      // perform initial assignment
      initIdealAssignment(totGuarant, q, initIdealAssigned);

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

      // This value will be used in every round to calculate ideal allocation.
      // So make a copy to avoid it changed during calculation.
      Resource dupUnassignedForTheRound = Resources.clone(unassigned);

      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        if (!rc.isAnyMajorResourceAboveZero(unassigned)) {
          break;
        }

        TempQueuePerPartition sub = i.next();

        // How much resource we offer to the queue (to increase its ideal_alloc
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            dupUnassignedForTheRound,
            sub.normalizedGuarantee, this.stepFactor);

        // Make sure it is not beyond unassigned
        wQavail = Resources.componentwiseMin(wQavail, unassigned);

        Resource wQidle = sub.offer(wQavail, rc, totGuarant,
            isReservedPreemptionCandidatesSelector,
            allowQueuesBalanceAfterAllQueuesSatisfied);
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        if (Resources.greaterThan(rc, totGuarant, wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }

        Resources.subtractFrom(unassigned, wQdone);

        // Make sure unassigned is always larger than 0
        unassigned = Resources.componentwiseMax(unassigned, Resources.none());
      }
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
   * This method is visible to allow sub-classes to override the initialization
   * behavior.
   *
   * @param totGuarant total resources (useful for {@code ResourceCalculator}
   *          operations)
   * @param q the {@code TempQueuePerPartition} being initialized
   * @param initIdealAssigned the proposed initialization value.
   */
  protected void initIdealAssignment(Resource totGuarant,
      TempQueuePerPartition q, Resource initIdealAssigned) {
    q.idealAssigned = initIdealAssigned;
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
    int maxLength = ResourceUtils.getNumberOfKnownResourceTypes();

    if (ignoreGuar) {
      for (TempQueuePerPartition q : queues) {
        for (int i = 0; i < maxLength; i++) {
          q.normalizedGuarantee[i] = 1.0f / queues.size();
        }
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
      }
      for (TempQueuePerPartition q : queues) {
        for (int i = 0; i < maxLength; i++) {
          ResourceInformation nResourceInformation = q.getGuaranteed()
              .getResourceInformation(i);
          ResourceInformation dResourceInformation = activeCap
              .getResourceInformation(i);

          long nValue = nResourceInformation.getValue();
          long dValue = UnitsConversionUtil.convert(
              dResourceInformation.getUnits(), nResourceInformation.getUnits(),
              dResourceInformation.getValue());
          if (dValue != 0) {
            q.normalizedGuarantee[i] = (float) nValue / dValue;
          }
        }
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