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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The kind of a queue, as far as the shared configuration checks are concerned.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum QueueKind {
  /** Any leaf queue. */
  LEAF,
  /** A {@link ParentQueue}, which can be an auto queue creation v2 parent. */
  PARENT,
  /** A {@link ManagedParentQueue} (auto queue creation v1 parent). */
  MANAGED_PARENT,
  /** Any other parent queue, for example a reservation {@link PlanQueue}. */
  OTHER_PARENT,
  /** A queue that is neither a leaf nor a parent queue. */
  OTHER;

  /**
   * @return true for every kind of parent queue
   */
  public boolean isParent() {
    return this == PARENT || this == MANAGED_PARENT || this == OTHER_PARENT;
  }

  /**
   * @param queue a live queue
   * @return the kind of the queue
   */
  public static QueueKind of(CSQueue queue) {
    if (queue instanceof AbstractLeafQueue) {
      return LEAF;
    } else if (queue instanceof ManagedParentQueue) {
      return MANAGED_PARENT;
    } else if (queue instanceof ParentQueue) {
      return PARENT;
    } else if (queue instanceof AbstractParentQueue) {
      return OTHER_PARENT;
    }
    return OTHER;
  }
}
