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
  final Resource current;
  final Resource pending;
  final Resource guaranteed;
  final Resource maxCapacity;
  final Resource killable;
  final String partition;

  // Following fields are setted and used by candidate selection policies
  Resource idealAssigned;
  Resource toBePreempted;
  Resource untouchableExtra;
  Resource preemptableExtra;
  // For logging purpose
  Resource actuallyToBePreempted;

  double normalizedGuarantee;

  final ArrayList<TempQueuePerPartition> children;
  LeafQueue leafQueue;
  boolean preemptionDisabled;

  TempQueuePerPartition(String queueName, Resource current, Resource pending,
      Resource guaranteed, Resource maxCapacity, boolean preemptionDisabled,
      String partition, Resource killable) {
    this.queueName = queueName;
    this.current = current;
    this.pending = pending;
    this.guaranteed = guaranteed;
    this.maxCapacity = maxCapacity;
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

  // This function "accepts" all the resources it can (pending) and return
  // the unused ones
  Resource offer(Resource avail, ResourceCalculator rc,
      Resource clusterResource) {
    Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
        Resources.subtract(maxCapacity, idealAssigned),
        Resource.newInstance(0, 0));
    // remain = avail - min(avail, (max - assigned), (current + pending - assigned))
    Resource accepted =
        Resources.min(rc, clusterResource,
            absMaxCapIdealAssignedDelta,
            Resources.min(rc, clusterResource, avail, Resources.subtract(
                Resources.add(current, pending), idealAssigned)));
    Resource remain = Resources.subtract(avail, accepted);
    Resources.addTo(idealAssigned, accepted);
    return remain;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + queueName)
        .append(" CUR: ").append(current)
        .append(" PEN: ").append(pending)
        .append(" GAR: ").append(guaranteed)
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
    if (Resources.greaterThan(rc, clusterResource,
        Resources.subtract(current, killable), idealAssigned)) {
      toBePreempted = Resources.multiply(Resources
              .subtract(Resources.subtract(current, killable), idealAssigned),
          scalingFactor);
    } else {
      toBePreempted = Resource.newInstance(0, 0);
    }
  }

  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ")
        .append(current.getMemory()).append(", ")
        .append(current.getVirtualCores()).append(", ")
        .append(pending.getMemory()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(guaranteed.getMemory()).append(", ")
        .append(guaranteed.getVirtualCores()).append(", ")
        .append(idealAssigned.getMemory()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemory()).append(", ")
        .append(toBePreempted.getVirtualCores() ).append(", ")
        .append(actuallyToBePreempted.getMemory()).append(", ")
        .append(actuallyToBePreempted.getVirtualCores());
  }

}
