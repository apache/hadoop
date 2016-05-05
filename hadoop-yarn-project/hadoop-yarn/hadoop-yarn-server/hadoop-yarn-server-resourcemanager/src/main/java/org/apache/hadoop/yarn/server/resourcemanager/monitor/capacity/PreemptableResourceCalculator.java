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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}
 */
public class PreemptableResourceCalculator {
  private static final Log LOG =
      LogFactory.getLog(PreemptableResourceCalculator.class);

  private final CapacitySchedulerPreemptionContext context;
  private final ResourceCalculator rc;
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
      if (q != null && Resources.greaterThan(rc, clusterRes,
          q.getGuaranteed(),
          Resources.none())) {
        pctOver = Resources.divide(rc, clusterRes, q.idealAssigned,
            q.getGuaranteed());
      }
      return (pctOver);
    }
  }

  /**
   * PreemptableResourceCalculator constructor
   *
   * @param preemptionContext
   * @param isReservedPreemptionCandidatesSelector this will be set by
   * different implementation of candidate selectors, please refer to
   * TempQueuePerPartition#offer for details.
   */
  public PreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues
   * @param rc resource calculator
   * @param clusterResource the total amount of resources in the cluster
   * @param queues the list of queues to consider
   */
  private void resetCapacity(ResourceCalculator rc, Resource clusterResource,
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
  protected Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed,
      TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);
      TempQueuePerPartition q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1,q2) < 0) {
        return underserved;
      }
    }
    return underserved;
  }


  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   */
  private void computeFixpointAllocation(ResourceCalculator rc,
      Resource tot_guarant, Collection<TempQueuePerPartition> qAlloc,
      Resource unassigned, boolean ignoreGuarantee) {
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.
    TQComparator tqComparator = new TQComparator(rc, tot_guarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed = new PriorityQueue<>(10,
        tqComparator);
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext();) {
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      if (Resources.greaterThan(rc, tot_guarant, used,
          q.getGuaranteed())) {
        q.idealAssigned = Resources.add(
            q.getGuaranteed(), q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(used);
      }
      Resources.subtractFrom(unassigned, q.idealAssigned);
      // If idealAssigned < (allocated + used + pending), q needs more resources, so
      // add it to the list of underserved queues, ordered by need.
      Resource curPlusPend = Resources.add(q.getUsed(), q.pending);
      if (Resources.lessThan(rc, tot_guarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    //assign all cluster resources until no more demand, or no resources are left
    while (!orderedByNeed.isEmpty()
        && Resources.greaterThan(rc,tot_guarant, unassigned,Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      resetCapacity(rc, unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      Collection<TempQueuePerPartition> underserved =
          getMostUnderservedQueues(orderedByNeed, tqComparator);
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        TempQueuePerPartition sub = i.next();
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc,
            unassigned, sub.normalizedGuarantee, Resource.newInstance(1, 1));
        Resource wQidle = sub.offer(wQavail, rc, tot_guarant,
            isReservedPreemptionCandidatesSelector);
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        if (Resources.greaterThan(rc, tot_guarant,
            wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
      Resources.subtractFrom(unassigned, wQassigned);
    }
  }

  /**
   * This method computes (for a single level in the tree, passed as a {@code
   * List<TempQueue>}) the ideal assignment of resources. This is done
   * recursively to allocate capacity fairly across all queues with pending
   * demands. It terminates when no resources are left to assign, or when all
   * demand is satisfied.
   *
   * @param rc resource calculator
   * @param queues a list of cloned queues to be assigned capacity to (this is
   * an out param)
   * @param totalPreemptionAllowed total amount of preemption we allow
   * @param tot_guarant the amount of capacity assigned to this pool of queues
   */
  private void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueuePerPartition> qAlloc = new ArrayList<>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    Resource unassigned = Resources.clone(tot_guarant);

    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<>();

    for (TempQueuePerPartition q : qAlloc) {
      if (Resources.greaterThan(rc, tot_guarant,
          q.getGuaranteed(), Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // first compute the allocation as a fixpoint based on guaranteed capacity
    computeFixpointAllocation(rc, tot_guarant, nonZeroGuarQueues, unassigned,
        false);

    // if any capacity is left unassigned, distributed among zero-guarantee
    // queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      computeFixpointAllocation(rc, tot_guarant, zeroGuarQueues, unassigned,
          true);
    }

    // based on ideal assignment computed above and current assignment we derive
    // how much preemption is required overall
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      if (Resources.greaterThan(rc, tot_guarant,
          t.getUsed(), t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded, Resources
            .subtract(t.getUsed(), t.idealAssigned));
      }
    }

    /**
     * if we need to preempt more than is allowed, compute a factor (0<f<1)
     * that is used to scale down how much we ask back from each queue
     */
    float scalingFactor = 1.0F;
    if (Resources.greaterThan(rc,
        tot_guarant, totPreemptionNeeded, totalPreemptionAllowed)) {
      scalingFactor = Resources.divide(rc, tot_guarant, totalPreemptionAllowed,
          totPreemptionNeeded);
    }

    // assign to each queue the amount of actual preemption based on local
    // information of ideal preemption and scaling factor
    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
  }

  /**
   * This method recursively computes the ideal assignment of resources to each
   * level of the hierarchy. This ensures that leafs that are over-capacity but
   * with parents within capacity will not be preemptionCandidates. Preemptions are allowed
   * within each subtree according to local over/under capacity.
   *
   * @param root the root of the cloned queue hierachy
   * @param totalPreemptionAllowed maximum amount of preemption allowed
   * @return a list of leaf queues updated with preemption targets
   */
  private void recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // compute recursively for lower levels and build list of leafs
      for(TempQueuePerPartition t : root.getChildren()) {
        recursivelyComputeIdealAssignment(t, totalPreemptionAllowed);
      }
    }
  }


  private void calculateResToObtainByPartitionForLeafQueues(
      Set<String> leafQueueNames, Resource clusterResource) {
    // Loop all leaf queues
    for (String queueName : leafQueueNames) {
      // check if preemption disabled for the queue
      if (context.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
        // we act only if we are violating balance by more than
        // maxIgnoredOverCapacity
        if (Resources.greaterThan(rc, clusterResource,
            qT.getUsed(), Resources
                .multiply(qT.getGuaranteed(),
                    1.0 + context.getMaxIgnoreOverCapacity()))) {
          /*
           * We introduce a dampening factor naturalTerminationFactor that
           * accounts for natural termination of containers.
           *
           * This is added to control pace of preemption, let's say:
           * If preemption policy calculated a queue *should be* preempted 20 GB
           * And the nature_termination_factor set to 0.1. As a result, preemption
           * policy will select 20 GB * 0.1 = 2GB containers to be preempted.
           *
           * However, it doesn't work for YARN-4390:
           * For example, if a queue needs to be preempted 20GB for *one single*
           * large container, preempt 10% of such resource isn't useful.
           * So to make it simple, only apply nature_termination_factor when
           * selector is not reservedPreemptionCandidatesSelector.
           */
          Resource resToObtain = qT.toBePreempted;
          if (!isReservedPreemptionCandidatesSelector) {
            resToObtain = Resources.multiply(qT.toBePreempted,
                context.getNaturalTerminationFactor());
          }

          // Only add resToObtain when it >= 0
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queue=" + queueName + " partition=" + qT.partition
                  + " resource-to-obtain=" + resToObtain);
            }
          }
          qT.setActuallyToBePreempted(Resources.clone(resToObtain));
        } else {
          qT.setActuallyToBePreempted(Resources.none());
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(qT);
        }
      }
    }
  }

  private void updatePreemptableExtras(TempQueuePerPartition cur) {
    if (cur.children == null || cur.children.isEmpty()) {
      cur.updatePreemptableExtras(rc);
    } else {
      for (TempQueuePerPartition child : cur.children) {
        updatePreemptableExtras(child);
      }
      cur.updatePreemptableExtras(rc);
    }
  }

  public void computeIdealAllocation(Resource clusterResource,
      Resource totalPreemptionAllowed) {
    for (String partition : context.getAllPartitions()) {
      TempQueuePerPartition tRoot = context.getQueueByPartition(
          CapacitySchedulerConfiguration.ROOT, partition);
      updatePreemptableExtras(tRoot);

      // compute the ideal distribution of resources among queues
      // updates cloned queues state accordingly
      tRoot.idealAssigned = tRoot.getGuaranteed();
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    // based on ideal allocation select containers to be preempted from each
    // calculate resource-to-obtain by partition for each leaf queues
    calculateResToObtainByPartitionForLeafQueues(context.getLeafQueueNames(),
        clusterResource);
  }
}