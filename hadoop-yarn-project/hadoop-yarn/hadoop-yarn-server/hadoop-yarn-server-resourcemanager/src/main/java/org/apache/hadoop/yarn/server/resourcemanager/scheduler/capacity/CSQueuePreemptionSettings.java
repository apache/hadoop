/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class CSQueuePreemptionSettings {
  private final boolean preemptionDisabled;
  // Indicates if the in-queue preemption setting is ever disabled within the
  // hierarchy of this queue.
  private final boolean intraQueuePreemptionDisabledInHierarchy;

  public CSQueuePreemptionSettings(
      CSQueue queue,
      CapacitySchedulerConfiguration configuration) {
    this.preemptionDisabled = isQueueHierarchyPreemptionDisabled(queue, configuration);
    this.intraQueuePreemptionDisabledInHierarchy =
        isIntraQueueHierarchyPreemptionDisabled(queue, configuration);
  }

  /**
   * The specified queue is cross-queue preemptable if system-wide cross-queue
   * preemption is turned on unless any queue in the <em>qPath</em> hierarchy
   * has explicitly turned cross-queue preemption off.
   * NOTE: Cross-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has cross-queue preemption disabled, false otherwise
   */
  private boolean isQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWidePreemption =
        configuration
            .getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    CSQueue parentQ = q.getParent();

    // If the system-wide preemption switch is turned off, all of the queues in
    // the qPath hierarchy have preemption disabled, so return true.
    if (!systemWidePreemption) return true;

    // If q is the root queue and the system-wide preemption switch is turned
    // on, then q does not have preemption disabled (default=false, below)
    // unless the preemption_disabled property is explicitly set.
    if (parentQ == null) {
      return configuration.getPreemptionDisabled(q.getQueuePathObject(), false);
    }

    // If this is not the root queue, inherit the default value for the
    // preemption_disabled property from the parent. Preemptability will be
    // inherited from the parent's hierarchy unless explicitly overridden at
    // this level.
    return configuration.getPreemptionDisabled(q.getQueuePathObject(),
        parentQ.getPreemptionDisabled());
  }

  /**
   * The specified queue is intra-queue preemptable if
   * 1) system-wide intra-queue preemption is turned on
   * 2) no queue in the <em>qPath</em> hierarchy has explicitly turned off intra
   *    queue preemption.
   * NOTE: Intra-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check intra-queue preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has intra-queue preemption disabled, false otherwise
   */
  private boolean isIntraQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWideIntraQueuePreemption =
        configuration.getBoolean(
            CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
            CapacitySchedulerConfiguration
                .DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    // Intra-queue preemption is disabled for this queue if the system-wide
    // intra-queue preemption flag is false
    if (!systemWideIntraQueuePreemption) return true;

    // Check if this is the root queue and the root queue's intra-queue
    // preemption disable switch is set
    CSQueue parentQ = q.getParent();
    if (parentQ == null) {
      return configuration
          .getIntraQueuePreemptionDisabled(q.getQueuePathObject(), false);
    }

    // At this point, the master preemption switch is enabled down to this
    // queue's level. Determine whether intra-queue preemption is enabled
    // down to this queue's level and return that value.
    return configuration.getIntraQueuePreemptionDisabled(q.getQueuePathObject(),
        parentQ.getIntraQueuePreemptionDisabledInHierarchy());
  }

  public boolean isIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabledInHierarchy || preemptionDisabled;
  }

  public boolean isIntraQueuePreemptionDisabledInHierarchy() {
    return intraQueuePreemptionDisabledInHierarchy;
  }

  public boolean isPreemptionDisabled() {
    return preemptionDisabled;
  }
}
