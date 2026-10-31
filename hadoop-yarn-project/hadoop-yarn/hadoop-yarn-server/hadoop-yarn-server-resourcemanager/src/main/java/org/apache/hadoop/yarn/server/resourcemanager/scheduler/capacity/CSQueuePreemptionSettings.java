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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;

/**
 * The preemption settings of a queue, taken from its resolved configuration.
 * Cross-queue and intra-queue preemption are disabled for every queue when
 * they are turned off system-wide, and are otherwise inherited from the parent
 * unless the queue sets them.
 */
public class CSQueuePreemptionSettings {
  private final boolean preemptionDisabled;
  // Indicates if the in-queue preemption setting is ever disabled within the
  // hierarchy of this queue.
  private final boolean intraQueuePreemptionDisabledInHierarchy;

  public CSQueuePreemptionSettings(ResolvedQueueConfig resolved) {
    this.preemptionDisabled =
        resolved.get(QueueProperties.PREEMPTION_DISABLED).getValue();
    this.intraQueuePreemptionDisabledInHierarchy =
        resolved.get(QueueProperties.INTRA_QUEUE_PREEMPTION_DISABLED).getValue();
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
