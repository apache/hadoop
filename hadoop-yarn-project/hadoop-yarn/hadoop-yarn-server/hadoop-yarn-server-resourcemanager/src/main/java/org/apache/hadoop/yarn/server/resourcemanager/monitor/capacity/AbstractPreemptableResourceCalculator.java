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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}.
 */
public class AbstractPreemptableResourceCalculator {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractPreemptableResourceCalculator.class);

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

  private static class NormalizationTuple {
    private Resource numerator;
    private Resource denominator;

    NormalizationTuple(Resource numer, Resource denom) {
      this.numerator = numer;
      this.denominator = denom;
    }

    long getNumeratorValue(int i) {
      return numerator.getResourceInformation(i).getValue();
    }

    long getDenominatorValue(int i) {
      String nUnits = numerator.getResourceInformation(i).getUnits();
      ResourceInformation dResourceInformation = denominator
          .getResourceInformation(i);
      return UnitsConversionUtil.convert(
          dResourceInformation.getUnits(), nUnits, dResourceInformation.getValue());
    }

    float getNormalizedValue(int i) {
      long nValue = getNumeratorValue(i);
      long dValue = getDenominatorValue(i);
      return dValue == 0 ? 0.0f : (float) nValue / dValue;
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
      resetCapacity(orderedByNeed, ignoreGuarantee);

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
        // Exit the loop once any of the unassigned resources reach zero, except the optional
        // ones which the queue has no guarantee on. This should ensure that extreme cases
        // where one of the resources are significantly larger than the other (i.e: way below 1
        // vcore per GB of memory)
        if (rc.isAnyRequestedResourceZeroOrNegative(totGuarant, unassigned)) {
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
   * @param queues
   *          the list of queues to consider
   * @param ignoreGuar
   *          ignore guarantee.
   */
  private void resetCapacity(Collection<TempQueuePerPartition> queues,
                             boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);
    float activeTotalAbsCap = 0.0f;
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    if (ignoreGuar) {
      for (int i = 0; i < maxLength; i++) {
        for (TempQueuePerPartition q : queues) {
          computeNormGuarEvenly(q, queues.size(), i);
        }
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
        activeTotalAbsCap += q.getAbsCapacity();
      }

      // loop through all resource types and normalize guaranteed capacity for all queues
      for (int i = 0; i < maxLength; i++) {
        boolean useAbsCapBasedNorm = false;
        // if the sum of absolute capacity of all queues involved is 0,
        // we should normalize evenly
        boolean useEvenlyDistNorm = activeTotalAbsCap == 0;

        // loop through all the queues once to determine the
        // right normalization strategy for current processing resource type
        for (TempQueuePerPartition q : queues) {
          NormalizationTuple normTuple = new NormalizationTuple(
              q.getGuaranteed(), activeCap);
          long queueGuaranValue = normTuple.getNumeratorValue(i);
          long totalActiveGuaranValue = normTuple.getDenominatorValue(i);

          if (queueGuaranValue == 0 && q.getAbsCapacity() != 0 && totalActiveGuaranValue != 0) {
            // when the rounded value of a resource type is 0 but its absolute capacity is not 0,
            // we should consider taking the normalized guarantee based on absolute capacity
            useAbsCapBasedNorm = true;
            break;
          }

          if (totalActiveGuaranValue == 0) {
            // If totalActiveGuaranValue from activeCap is zero, that means the guaranteed capacity
            // of this resource dimension for all active queues is tiny (close to 0).
            // For example, if a queue has 1% of minCapacity on a cluster with a totalVcores of 48,
            // then the idealAssigned Vcores for this queue is (48 * 0.01)=0.48 which then
            // get rounded/casted into 0 (double -> long)
            // In this scenario where the denominator is 0, we can just spread resources across
            // all tiny queues evenly since their absoluteCapacity are roughly the same
            useEvenlyDistNorm = true;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Queue normalization strategy: " +
              "absoluteCapacityBasedNormalization(" + useAbsCapBasedNorm +
              "), evenlyDistributedNormalization(" + useEvenlyDistNorm +
              "), defaultNormalization(" + !(useAbsCapBasedNorm || useEvenlyDistNorm) + ")");
        }

        // loop through all the queues again to apply normalization strategy
        for (TempQueuePerPartition q : queues) {
          if (useAbsCapBasedNorm) {
            computeNormGuarFromAbsCapacity(q, activeTotalAbsCap, i);
          } else if (useEvenlyDistNorm) {
            computeNormGuarEvenly(q, queues.size(), i);
          } else {
            computeDefaultNormGuar(q, activeCap, i);
          }
        }
      }
    }
  }

  /**
   * Computes the normalized guaranteed capacity based on the weight of a queue's abs capacity.
   *
   * Example:
   *  There are two active queues: queueA & queueB, and
   *  their configured absolute minimum capacity is 1% and 3% respectively.
   *
   *  Then their normalized guaranteed capacity are:
   *    normalized_guar_queueA = 0.01 / (0.01 + 0.03) = 0.25
   *    normalized_guar_queueB = 0.03 / (0.01 + 0.03) = 0.75
   *
   * @param q
   *          the queue to consider
   * @param activeTotalAbsCap
   *          the sum of absolute capacity of all active queues
   * @param resourceTypeIdx
   *          index of the processing resource type
   */
  private static void computeNormGuarFromAbsCapacity(TempQueuePerPartition q,
                                                     float activeTotalAbsCap,
                                                     int resourceTypeIdx) {
    if (activeTotalAbsCap != 0) {
      q.normalizedGuarantee[resourceTypeIdx] = q.getAbsCapacity() / activeTotalAbsCap;
    }
  }

  /**
   * Computes the normalized guaranteed capacity evenly based on num of active queues.
   *
   * @param q
   *          the queue to consider
   * @param numOfActiveQueues
   *          number of active queues
   * @param resourceTypeIdx
   *          index of the processing resource type
   */
  private static void computeNormGuarEvenly(TempQueuePerPartition q,
                                            int numOfActiveQueues,
                                            int resourceTypeIdx) {
    q.normalizedGuarantee[resourceTypeIdx] = 1.0f / numOfActiveQueues;
  }

  /**
   * The default way to compute a queue's normalized guaranteed capacity.
   *
   * For each resource type, divide a queue's configured guaranteed amount (MBs/Vcores) by
   * the total amount of guaranteed resource of all active queues
   *
   * @param q
   *          the queue to consider
   * @param activeCap
   *          total guaranteed resources of all active queues
   * @param resourceTypeIdx
   *          index of the processing resource type
   */
  private static void computeDefaultNormGuar(TempQueuePerPartition q,
                                             Resource activeCap,
                                             int resourceTypeIdx) {
    NormalizationTuple normTuple = new NormalizationTuple(q.getGuaranteed(), activeCap);
    q.normalizedGuarantee[resourceTypeIdx] = normTuple.getNormalizedValue(resourceTypeIdx);
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