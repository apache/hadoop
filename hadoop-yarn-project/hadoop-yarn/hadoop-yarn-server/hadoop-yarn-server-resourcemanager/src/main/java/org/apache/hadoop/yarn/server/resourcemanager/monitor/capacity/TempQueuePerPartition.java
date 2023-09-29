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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization. This is per-queue-per-partition data structure
 */
public class TempQueuePerPartition extends AbstractPreemptionEntity {
  // Following fields are copied from scheduler
  final String partition;

  private final Resource killable;
  private final float absCapacity;
  private final float absMaxCapacity;
  final Resource totalPartitionResource;

  // Following fields are settled and used by candidate selection policies
  Resource untouchableExtra;
  Resource preemptableExtra;

  double[] normalizedGuarantee;

  private Resource effMinRes;
  private Resource effMaxRes;

  final ArrayList<TempQueuePerPartition> children;
  private Collection<TempAppPerPartition> apps;
  AbstractLeafQueue leafQueue;
  AbstractParentQueue parentQueue;
  boolean preemptionDisabled;

  protected Resource pendingDeductReserved;

  // Relative priority of this queue to its parent
  // If parent queue's ordering policy doesn't respect priority,
  // this will be always 0
  int relativePriority = 0;
  TempQueuePerPartition parent = null;

  // This will hold a temp user data structure and will hold userlimit,
  // idealAssigned, used etc.
  Map<String, TempUserPerPartition> usersPerPartition = new LinkedHashMap<>();

  @SuppressWarnings("checkstyle:parameternumber")
  public TempQueuePerPartition(String queueName, Resource current,
      boolean preemptionDisabled, String partition, Resource killable,
      float absCapacity, float absMaxCapacity, Resource totalPartitionResource,
      Resource reserved, CSQueue queue, Resource effMinRes,
      Resource effMaxRes) {
    super(queueName, current, Resource.newInstance(0, 0), reserved,
        Resource.newInstance(0, 0));

    if (queue instanceof AbstractLeafQueue) {
      AbstractLeafQueue l = (AbstractLeafQueue) queue;
      pending = l.getTotalPendingResourcesConsideringUserLimit(
          totalPartitionResource, partition, false);
      pendingDeductReserved = l.getTotalPendingResourcesConsideringUserLimit(
          totalPartitionResource, partition, true);
      leafQueue = l;
    } else {
      pending = Resources.createResource(0);
      pendingDeductReserved = Resources.createResource(0);
    }

    if (queue != null && ParentQueue.class.isAssignableFrom(queue.getClass())) {
      parentQueue = (ParentQueue) queue;
    }

    this.normalizedGuarantee = new double[ResourceUtils
        .getNumberOfCountableResourceTypes()];
    this.children = new ArrayList<>();
    this.apps = new ArrayList<>();
    this.untouchableExtra = Resource.newInstance(0, 0);
    this.preemptableExtra = Resource.newInstance(0, 0);
    this.preemptionDisabled = preemptionDisabled;
    this.partition = partition;
    this.killable = killable;
    this.absCapacity = absCapacity;
    this.absMaxCapacity = absMaxCapacity;
    this.totalPartitionResource = totalPartitionResource;
    this.effMinRes = effMinRes;
    this.effMaxRes = effMaxRes;
  }

  public void setLeafQueue(AbstractLeafQueue l) {
    assert children.size() == 0;
    this.leafQueue = l;
  }

  /**
   * When adding a child we also aggregate its pending resource needs.
   *
   * @param q
   *          the child queue to add to this queue
   */
  public void addChild(TempQueuePerPartition q) {
    assert leafQueue == null;
    children.add(q);
    Resources.addTo(pending, q.pending);
    Resources.addTo(pendingDeductReserved, q.pendingDeductReserved);
  }

  public ArrayList<TempQueuePerPartition> getChildren() {
    return children;
  }

  // This function "accepts" all the resources it can (pending) and return
  // the unused ones
  Resource offer(Resource avail, ResourceCalculator rc,
      Resource clusterResource, boolean considersReservedResource,
      boolean allowQueueBalanceAfterAllSafisfied) {
    Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
        Resources.subtract(getMax(), idealAssigned),
        Resource.newInstance(0, 0));
    // accepted = min{avail,
    //               max - assigned,
    //               current + pending - assigned,
    //               # Make sure a queue will not get more than max of its
    //               # used/guaranteed, this is to make sure preemption won't
    //               # happen if all active queues are beyond their guaranteed
    //               # This is for leaf queue only.
    //               max(guaranteed, used) - assigned}
    // remain = avail - accepted
    Resource accepted = Resources.componentwiseMin(
        absMaxCapIdealAssignedDelta,
        Resources.min(rc, clusterResource, avail, Resources
            /*
             * When we're using FifoPreemptionSelector (considerReservedResource
             * = false).
             *
             * We should deduct reserved resource from pending to avoid excessive
             * preemption:
             *
             * For example, if an under-utilized queue has used = reserved = 20.
             * Preemption policy will try to preempt 20 containers (which is not
             * satisfied) from different hosts.
             *
             * In FifoPreemptionSelector, there's no guarantee that preempted
             * resource can be used by pending request, so policy will preempt
             * resources repeatly.
             */
            .subtract(Resources.add(getUsed(),
                (considersReservedResource ? pending : pendingDeductReserved)),
                idealAssigned)));

    // For leaf queue: accept = min(accept, max(guaranteed, used) - assigned)
    // Why only for leaf queue?
    // Because for a satisfied parent queue, it could have some under-utilized
    // leaf queues. Such under-utilized leaf queue could preemption resources
    // from over-utilized leaf queue located at other hierarchies.

    // Allow queues can continue grow and balance even if all queues are satisfied.
    if (!allowQueueBalanceAfterAllSafisfied) {
      accepted = filterByMaxDeductAssigned(rc, clusterResource, accepted);
    }

    // accepted so far contains the "quota acceptable" amount, we now filter by
    // locality acceptable

    accepted = acceptedByLocality(rc, accepted);

    // accept should never be < 0
    accepted = Resources.componentwiseMax(accepted, Resources.none());

    // or more than offered
    accepted = Resources.componentwiseMin(accepted, avail);

    Resource remain = Resources.subtract(avail, accepted);
    Resources.addTo(idealAssigned, accepted);
    return remain;
  }

  public float getAbsCapacity() {
    return absCapacity;
  }

  public Resource getGuaranteed() {
    if(!effMinRes.equals(Resources.none())) {
      return Resources.clone(effMinRes);
    }

    return Resources.multiply(totalPartitionResource, absCapacity);
  }

  public Resource getMax() {
    if(!effMaxRes.equals(Resources.none())) {
      return Resources.clone(effMaxRes);
    }

    return Resources.multiply(totalPartitionResource, absMaxCapacity);
  }

  public void updatePreemptableExtras(ResourceCalculator rc) {
    // Reset untouchableExtra and preemptableExtra
    untouchableExtra = Resources.none();
    preemptableExtra = Resources.none();

    Resource extra = Resources.subtract(getUsed(), getGuaranteed());
    if (Resources.lessThan(rc, totalPartitionResource, extra,
        Resources.none())) {
      extra = Resources.none();
    }

    if (null == children || children.isEmpty()) {
      // If it is a leaf queue
      if (preemptionDisabled) {
        untouchableExtra = extra;
      } else {
        preemptableExtra = extra;
      }
    } else {
      // If it is a parent queue
      Resource childrensPreemptable = Resource.newInstance(0, 0);
      for (TempQueuePerPartition child : children) {
        Resources.addTo(childrensPreemptable, child.preemptableExtra);
      }
      // untouchableExtra = max(extra - childrenPreemptable, 0)
      if (Resources.greaterThanOrEqual(rc, totalPartitionResource,
          childrensPreemptable, extra)) {
        untouchableExtra = Resource.newInstance(0, 0);
      } else {
        untouchableExtra = Resources.subtract(extra, childrensPreemptable);
      }
      preemptableExtra = Resources.min(rc, totalPartitionResource,
          childrensPreemptable, extra);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + queueName).append(" CUR: ").append(current)
        .append(" PEN: ").append(pending).append(" RESERVED: ").append(reserved)
        .append(" GAR: ").append(getGuaranteed()).append(" NORM: ")
        .append(Arrays.toString(normalizedGuarantee))
        .append(" IDEAL_ASSIGNED: ").append(idealAssigned)
        .append(" IDEAL_PREEMPT: ").append(toBePreempted)
        .append(" ACTUAL_PREEMPT: ").append(getActuallyToBePreempted())
        .append(" UNTOUCHABLE: ").append(untouchableExtra)
        .append(" PREEMPTABLE: ").append(preemptableExtra).append("\n");

    return sb.toString();
  }

  public void assignPreemption(float scalingFactor, ResourceCalculator rc,
      Resource clusterResource) {
    Resource usedDeductKillable = Resources.subtract(getUsed(), killable);
    Resource totalResource = Resources.add(getUsed(), pending);

    // The minimum resource that we need to keep for a queue is:
    // max(idealAssigned, min(used + pending, guaranteed)).
    //
    // Doing this because when we calculate ideal allocation doesn't consider
    // reserved resource, ideal-allocation calculated could be less than
    // guaranteed and total. We should avoid preempt from a queue if it is
    // already
    // <= its guaranteed resource.
    Resource minimumQueueResource = Resources.max(rc, clusterResource,
        Resources.min(rc, clusterResource, totalResource, getGuaranteed()),
        idealAssigned);

    if (Resources.greaterThan(rc, clusterResource, usedDeductKillable,
        minimumQueueResource)) {
      toBePreempted = Resources.multiply(
          Resources.subtract(usedDeductKillable, minimumQueueResource),
          scalingFactor);
    } else {
      toBePreempted = Resources.none();
    }
  }

  public void deductActuallyToBePreempted(ResourceCalculator rc,
      Resource cluster, Resource toBeDeduct) {
    if (Resources.greaterThan(rc, cluster, getActuallyToBePreempted(),
        toBeDeduct)) {
      Resources.subtractFrom(getActuallyToBePreempted(), toBeDeduct);
    }
    setActuallyToBePreempted(Resources.max(rc, cluster,
        getActuallyToBePreempted(), Resources.none()));
  }

  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ").append(current.getMemorySize())
        .append(", ").append(current.getVirtualCores()).append(", ")
        .append(pending.getMemorySize()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(getGuaranteed().getMemorySize()).append(", ")
        .append(getGuaranteed().getVirtualCores()).append(", ")
        .append(idealAssigned.getMemorySize()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemorySize()).append(", ")
        .append(toBePreempted.getVirtualCores()).append(", ")
        .append(getActuallyToBePreempted().getMemorySize()).append(", ")
        .append(getActuallyToBePreempted().getVirtualCores());
  }

  public void addAllApps(Collection<TempAppPerPartition> orderedApps) {
    this.apps = orderedApps;
  }

  public Collection<TempAppPerPartition> getApps() {
    return apps;
  }

  public void addUserPerPartition(String userName,
      TempUserPerPartition tmpUser) {
    this.usersPerPartition.put(userName, tmpUser);
  }

  public Map<String, TempUserPerPartition> getUsersPerPartition() {
    return usersPerPartition;
  }

  public void setPending(Resource pending) {
    this.pending = pending;
  }

  public Resource getIdealAssigned() {
    return idealAssigned;
  }

  public String toGlobalString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n").append(toString());
    for (TempQueuePerPartition c : children) {
      sb.append(c.toGlobalString());
    }
    return sb.toString();
  }

  /**
   * This method is visible to allow sub-classes to override the behavior,
   * specifically to take into account locality-based limitations of how much
   * the queue can consumed.
   *
   * @param rc the ResourceCalculator to be used.
   * @param offered the input amount of Resource offered to this queue.
   *
   * @return  the subset of Resource(s) that the queue can consumed after
   *          accounting for locality effects.
   */
  protected Resource acceptedByLocality(ResourceCalculator rc,
      Resource offered) {
    return offered;
  }

  /**
   * This method is visible to allow sub-classes to override the behavior,
   * specifically for federation purposes we do not want to cap resources as it
   * is done here.
   *
   * @param rc the {@code ResourceCalculator} to be used
   * @param clusterResource the total cluster resources
   * @param offered the resources offered to this queue
   * @return the amount of resources accepted after considering max and
   *         deducting assigned.
   */
  protected Resource filterByMaxDeductAssigned(ResourceCalculator rc,
      Resource clusterResource, Resource offered) {
    if (null == children || children.isEmpty()) {
      Resource maxOfGuranteedAndUsedDeductAssigned = Resources.subtract(
          Resources.max(rc, clusterResource, getUsed(), getGuaranteed()),
          idealAssigned);
      maxOfGuranteedAndUsedDeductAssigned = Resources.max(rc, clusterResource,
          maxOfGuranteedAndUsedDeductAssigned, Resources.none());
      offered = Resources.min(rc, clusterResource, offered,
          maxOfGuranteedAndUsedDeductAssigned);
    }
    return offered;
  }

  /**
   * This method is visible to allow sub-classes to ovverride the behavior,
   * specifically for federation purposes we need to initialize per-sub-cluster
   * roots as well as the global one.
   */
  protected void initializeRootIdealWithGuarangeed() {
    idealAssigned = Resources.clone(getGuaranteed());
  }

}
