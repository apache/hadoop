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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization for an application.
 */
public class TempAppPerPartition extends AbstractPreemptionEntity {

  // Following fields are settled and used by candidate selection policies
  private final int priority;
  private final ApplicationId applicationId;
  private TempUserPerPartition tempUser;

  FiCaSchedulerApp app;

  TempAppPerPartition(FiCaSchedulerApp app, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    super(app.getQueueName(), usedPerPartition, amUsedPerPartition, reserved,
        pendingPerPartition);

    this.priority = app.getPriority().getPriority();
    this.applicationId = app.getApplicationId();
    this.app = app;
  }

  public FiCaSchedulerApp getFiCaSchedulerApp() {
    return app;
  }

  public void assignPreemption(Resource killable) {
    Resources.addTo(toBePreempted, killable);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + getApplicationId()).append(" PRIO: ").append(priority)
        .append(" CUR: ").append(getUsed()).append(" PEN: ").append(pending)
        .append(" RESERVED: ").append(reserved).append(" IDEAL_ASSIGNED: ")
        .append(idealAssigned).append(" PREEMPT_OTHER: ")
        .append(getToBePreemptFromOther()).append(" IDEAL_PREEMPT: ")
        .append(toBePreempted).append(" ACTUAL_PREEMPT: ")
        .append(getActuallyToBePreempted()).append("\n");

    return sb.toString();
  }

  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ").append(getUsed().getMemorySize())
        .append(", ").append(getUsed().getVirtualCores()).append(", ")
        .append(pending.getMemorySize()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(idealAssigned.getMemorySize()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemorySize()).append(", ")
        .append(toBePreempted.getVirtualCores()).append(", ")
        .append(getActuallyToBePreempted().getMemorySize()).append(", ")
        .append(getActuallyToBePreempted().getVirtualCores());
  }

  public int getPriority() {
    return priority;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getUser() {
    return this.app.getUser();
  }

  public void deductActuallyToBePreempted(ResourceCalculator resourceCalculator,
      Resource cluster, Resource toBeDeduct) {
    if (Resources.greaterThan(resourceCalculator, cluster,
        getActuallyToBePreempted(), toBeDeduct)) {
      Resources.subtractFrom(getActuallyToBePreempted(), toBeDeduct);
    }
  }

  public void setTempUserPerPartition(TempUserPerPartition tu) {
    tempUser = tu;
  }

  public TempUserPerPartition getTempUserPerPartition() {
    return tempUser;
  }
}
