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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class PreemptionCandidatesSelector {
  protected CapacitySchedulerPreemptionContext preemptionContext;
  protected ResourceCalculator rc;
  private long maximumKillWaitTime = -1;

  PreemptionCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    this.preemptionContext = preemptionContext;
    this.rc = preemptionContext.getResourceCalculator();
  }

  /**
   * Get preemption candidates from computed resource sharing and already
   * selected candidates.
   *
   * @param selectedCandidates already selected candidates from previous policies
   * @param clusterResource total resource
   * @param totalPreemptedResourceAllowed how many resources allowed to be
   *                                      preempted in this round. Should be
   *                                      updated(in-place set) after the call
   * @return merged selected candidates.
   */
  public abstract Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed);

  /**
   * Compare by reversed priority order first, and then reversed containerId
   * order.
   *
   * @param containers list of containers to sort for.
   */
  @VisibleForTesting
  static void sortContainers(List<RMContainer> containers) {
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
        int schedKeyComp = b.getAllocatedSchedulerKey()
            .compareTo(a.getAllocatedSchedulerKey());
        if (schedKeyComp != 0) {
          return schedKeyComp;
        }
        return b.getContainerId().compareTo(a.getContainerId());
      }
    });
  }

  public long getMaximumKillWaitTimeMs() {
    if (maximumKillWaitTime > 0) {
      return maximumKillWaitTime;
    }
    return preemptionContext.getDefaultMaximumKillWaitTimeout();
  }

  public void setMaximumKillWaitTime(long maximumKillWaitTime) {
    this.maximumKillWaitTime = maximumKillWaitTime;
  }
}
