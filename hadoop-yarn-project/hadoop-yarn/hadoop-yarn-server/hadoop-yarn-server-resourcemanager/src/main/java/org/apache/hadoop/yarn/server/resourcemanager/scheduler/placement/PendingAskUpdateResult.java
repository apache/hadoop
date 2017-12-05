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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;

/**
 * Result of a resource-request update. This will be used by
 * {@link org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo}
 * to update queue metrics and application/queue's overall pending resources.
 * And this is per-scheduler-key.
 *
 * Following fields will be set if pending ask changed for a given scheduler key
 * - lastPendingAsk: how many resource asked before.
 * - newPendingAsk: how many resource asked now.
 * - lastNodePartition: what's the node partition before.
 * - newNodePartition: what's the node partition now.
 */
public class PendingAskUpdateResult {
  private final PendingAsk lastPendingAsk;
  private final String lastNodePartition;
  private final PendingAsk newPendingAsk;
  private final String newNodePartition;

  public PendingAskUpdateResult(PendingAsk lastPendingAsk,
      PendingAsk newPendingAsk, String lastNodePartition,
      String newNodePartition) {
    this.lastPendingAsk = lastPendingAsk;
    this.newPendingAsk = newPendingAsk;
    this.lastNodePartition = lastNodePartition;
    this.newNodePartition = newNodePartition;
  }

  public PendingAsk getLastPendingAsk() {
    return lastPendingAsk;
  }

  public PendingAsk getNewPendingAsk() {
    return newPendingAsk;
  }

  public String getLastNodePartition() {
    return lastNodePartition;
  }

  public String getNewNodePartition() {
    return newNodePartition;
  }
}
