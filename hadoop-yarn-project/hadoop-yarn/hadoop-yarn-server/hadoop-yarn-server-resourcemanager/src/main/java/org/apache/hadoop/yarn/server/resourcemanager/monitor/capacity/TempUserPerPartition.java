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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;


/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization for an application.
 */
public class TempUserPerPartition extends AbstractPreemptionEntity {

  private final User user;
  private Resource userLimit;
  private boolean donePreemptionQuotaForULDelta = false;

  TempUserPerPartition(User user, String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    super(queueName, usedPerPartition, amUsedPerPartition, reserved,
        pendingPerPartition);
    this.user = user;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + getUserName()).append(" CUR: ").append(getUsed())
        .append(" PEN: ").append(pending).append(" RESERVED: ").append(reserved)
        .append(" AM_USED: ").append(amUsed).append(" USER_LIMIT: ")
        .append(getUserLimit()).append(" IDEAL_ASSIGNED: ")
        .append(idealAssigned).append(" USED_WO_AMUSED: ")
        .append(getUsedDeductAM()).append(" IDEAL_PREEMPT: ")
        .append(toBePreempted).append(" ACTUAL_PREEMPT: ")
        .append(getActuallyToBePreempted()).append("\n");

    return sb.toString();
  }

  public String getUserName() {
    return user.getUserName();
  }

  public Resource getUserLimit() {
    return userLimit;
  }

  public void setUserLimit(Resource userLimitResource) {
    this.userLimit = userLimitResource;
  }

  public boolean isUserLimitReached(ResourceCalculator rc,
      Resource clusterResource) {
    if (Resources.greaterThan(rc, clusterResource, getUsedDeductAM(),
        userLimit)) {
      return true;
    }
    return false;
  }

  public boolean isPreemptionQuotaForULDeltaDone() {
    return this.donePreemptionQuotaForULDelta;
  }

  public void updatePreemptionQuotaForULDeltaAsDone(boolean done) {
    this.donePreemptionQuotaForULDelta = done;
  }
}
