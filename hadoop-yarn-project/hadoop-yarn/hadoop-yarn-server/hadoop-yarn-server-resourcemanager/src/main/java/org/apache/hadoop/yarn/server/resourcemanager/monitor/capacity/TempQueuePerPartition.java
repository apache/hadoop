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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;

/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization. This is per-queue-per-partition data structure
 */
public class TempQueuePerPartition {
  // Following fields are copied from scheduler
  final String queueName;
  final String partition;
  final Resource pending;

  private final Resource current;
  private final Resource killable;
  private final Resource reserved;
  private final float absCapacity;
  private final float absMaxCapacity;
  final Resource totalPartitionResource;

  // Following fields are setted and used by candidate selection policies
  Resource idealAssigned;
  Resource toBePreempted;
  Resource untouchableExtra;
  Resource preemptableExtra;
  private Resource actuallyToBePreempted;

  double normalizedGuarantee;

  final ArrayList<TempQueuePerPartition> children;
  LeafQueue leafQueue;
  boolean preemptionDisabled;

  TempQueuePerPartition(String queueName, Resource current,
      boolean preemptionDisabled, String partition, Resource killable,
      float absCapacity, float absMaxCapacity, Resource totalPartitionResource,
      Resource reserved, CSQueue queue) {
    this.queueName = queueName;
    this.current = current;

    if (queue instanceof LeafQueue) {
      LeafQueue l = (LeafQueue) queue;
      pending = l.getTotalPendingResourcesConsideringUserLimit(
          totalPartitionResource, partition);
      leafQueue = l;
    } else {
      pending = Resources.createResource(0);
    }

    this.idealAssigned = Resource.newInstance(0, 0);
    this.actuallyToBePreempted = Resource.newInstance(0, 0);
    this.toBePreempted = Resource.newInstance(0, 0);
    this.normalizedGuarantee = Float.NaN;
    this.children = new ArrayList<>();
    this.untouchableExtra = Resource.newInstance(0, 0);
    this.preemptableExtra = Resource.newInstance(0, 0);
    this.preemptionDisabled = preemptionDisabled;
    this.partition = partition;
    this.killable = killable;
    this.absCapacity = absCapacity;
    this.absMaxCapacity = absMaxCapacity;
    this.totalPartitionResource = totalPartitionResource;
    this.reserved = reserved;
  }

  public void setLeafQueue(LeafQueue l) {
    assert children.size() == 0;
    this.leafQueue = l;
  }

  /**
   * When adding a child we also aggregate its pending resource needs.
   * @param q the child queue to add to this queue
   */
  public void addChild(TempQueuePerPartition q) {
    assert leafQueue == null;
    children.add(q);
    Resources.addTo(pending, q.pending);
  }

  public ArrayList<TempQueuePerPartition> getChildren(){
    return children;
  }

  public Resource getUsed() {
    return current;
  }

  public Resource getUsedDeductReservd() {
    return Resources.subtract(current, reserved);
  }

  // This function "accepts" all the resources it can (pending) and return
  // the unused ones
  Resource offer(Resource avail, ResourceCalculator rc,
      Resource clusterResource, boolean considersReservedResource) {
    Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
        Resources.subtract(getMax(), idealAssigned),
        Resource.newInstance(0, 0));
    // remain = avail - min(avail, (max - assigned), (current + pending - assigned))
    Resource accepted = Resources.min(rc, clusterResource,
        absMaxCapIdealAssignedDelta, Resources.min(rc, clusterResource, avail,
            Resources
                /*
                 * When we're using FifoPreemptionSelector
                 * (considerReservedResource = false).
                 *
                 * We should deduct reserved resource to avoid excessive preemption:
                 *
                 * For example, if an under-utilized queue has used = reserved = 20.
                 * Preemption policy will try to preempt 20 containers
                 * (which is not satisfied) from different hosts.
                 *
                 * In FifoPreemptionSelector, there's no guarantee that preempted
                 * resource can be used by pending request, so policy will preempt
                 * resources repeatly.
                 */
                .subtract(Resources.add(
                    (considersReservedResource ? getUsed() :
                      getUsedDeductReservd()),
                    pending), idealAssigned)));
    Resource remain = Resources.subtract(avail, accepted);
    Resources.addTo(idealAssigned, accepted);
    return remain;
  }

  public Resource getGuaranteed() {
    return Resources.multiply(totalPartitionResource, absCapacity);
  }

  public Resource getMax() {
    return Resources.multiply(totalPartitionResource, absMaxCapacity);
  }

  public void updatePreemptableExtras(ResourceCalculator rc) {
    // Reset untouchableExtra and preemptableExtra
    untouchableExtra = Resources.none();
    preemptableExtra = Resources.none();

    Resource extra = Resources.subtract(getUsed(),
        getGuaranteed());
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
    sb.append(" NAME: " + queueName)
        .append(" CUR: ").append(current)
        .append(" PEN: ").append(pending)
        .append(" RESERVED: ").append(reserved)
        .append(" GAR: ").append(getGuaranteed())
        .append(" NORM: ").append(normalizedGuarantee)
        .append(" IDEAL_ASSIGNED: ").append(idealAssigned)
        .append(" IDEAL_PREEMPT: ").append(toBePreempted)
        .append(" ACTUAL_PREEMPT: ").append(actuallyToBePreempted)
        .append(" UNTOUCHABLE: ").append(untouchableExtra)
        .append(" PREEMPTABLE: ").append(preemptableExtra)
        .append("\n");

    return sb.toString();
  }

  public void assignPreemption(float scalingFactor, ResourceCalculator rc,
      Resource clusterResource) {
    Resource usedDeductKillable = Resources.subtract(
        getUsed(), killable);
    Resource totalResource = Resources.add(getUsed(), pending);

    // The minimum resource that we need to keep for a queue is:
    // max(idealAssigned, min(used + pending, guaranteed)).
    //
    // Doing this because when we calculate ideal allocation doesn't consider
    // reserved resource, ideal-allocation calculated could be less than
    // guaranteed and total. We should avoid preempt from a queue if it is already
    // <= its guaranteed resource.
    Resource minimumQueueResource = Resources.max(rc, clusterResource,
        Resources.min(rc, clusterResource, totalResource, getGuaranteed()),
        idealAssigned);

    if (Resources.greaterThan(rc, clusterResource, usedDeductKillable,
        minimumQueueResource)) {
      toBePreempted = Resources.multiply(
          Resources.subtract(usedDeductKillable, minimumQueueResource), scalingFactor);
    } else {
      toBePreempted = Resources.none();
    }
  }

  public Resource getActuallyToBePreempted() {
    return actuallyToBePreempted;
  }

  public void setActuallyToBePreempted(Resource res) {
    this.actuallyToBePreempted = res;
  }

  public void deductActuallyToBePreempted(ResourceCalculator rc,
      Resource cluster, Resource toBeDeduct) {
    if (Resources.greaterThan(rc, cluster, actuallyToBePreempted, toBeDeduct)) {
      Resources.subtractFrom(actuallyToBePreempted, toBeDeduct);
    }
    actuallyToBePreempted = Resources.max(rc, cluster, actuallyToBePreempted,
        Resources.none());
  }

  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ")
        .append(current.getMemorySize()).append(", ")
        .append(current.getVirtualCores()).append(", ")
        .append(pending.getMemorySize()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(getGuaranteed().getMemorySize()).append(", ")
        .append(getGuaranteed().getVirtualCores()).append(", ")
        .append(idealAssigned.getMemorySize()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemorySize()).append(", ")
        .append(toBePreempted.getVirtualCores() ).append(", ")
        .append(actuallyToBePreempted.getMemorySize()).append(", ")
        .append(actuallyToBePreempted.getVirtualCores());
  }

}
