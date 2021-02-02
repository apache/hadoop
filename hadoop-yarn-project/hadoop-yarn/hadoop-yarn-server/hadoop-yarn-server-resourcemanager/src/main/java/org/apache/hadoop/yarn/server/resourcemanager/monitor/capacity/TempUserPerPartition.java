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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization for an application.
 */
public class TempUserPerPartition extends AbstractPreemptionEntity {

  private final User user;
  private Resource userLimit;
  private boolean donePreemptionQuotaForULDelta = false;

  private Map<ApplicationId, TempAppPerPartition> apps;

  TempUserPerPartition(User user, String queueName, Resource usedPerPartition,
      Resource amUsedPerPartition, Resource reserved,
      Resource pendingPerPartition) {
    super(queueName, usedPerPartition, amUsedPerPartition, reserved,
        pendingPerPartition);
    this.user = user;
    this.apps = new HashMap<>();
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
        .append(getActuallyToBePreempted());
    if(getFairShare() != null) {
      sb.append(" FAIR-SHARE: ").append(getFairShare());
    }

    return sb.append("\n").toString();
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

  /**
   * Method to add a new app under this user.
   * @param applicationId application_id of the app
   * @param tempAppPerPartition TempAppPerPartition object of the app
   */
  public void addApp(ApplicationId applicationId,
      TempAppPerPartition tempAppPerPartition) {
    apps.put(applicationId, tempAppPerPartition);
  }

  /**
   * Getter method to return TempAppPerPartition for given application_id.
   * @param applicationId application_id of the app
   * @return TempAppPerPartition corresponding to given app_id.
   *         Null if app_id is absent.
   */
  public TempAppPerPartition getApp(ApplicationId applicationId) {
    if(!apps.containsKey(applicationId)) {
      return null;
    }
    return apps.get(applicationId);
  }

  /**
   * Getter method to return all the TempAppPerPartition under given user.
   * @return collection of TempAppPerPartition user this user.
   */
  public Collection<TempAppPerPartition> getApps() {
    return apps.values();
  }
}
