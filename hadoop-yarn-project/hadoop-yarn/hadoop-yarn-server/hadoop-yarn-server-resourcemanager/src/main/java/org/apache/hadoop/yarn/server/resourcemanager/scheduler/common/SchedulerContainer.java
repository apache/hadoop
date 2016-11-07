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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;

/**
 * Contexts for a container inside scheduler
 */
public class SchedulerContainer<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  private RMContainer rmContainer;
  private String nodePartition;
  private A schedulerApplicationAttempt;
  private N schedulerNode;
  private boolean allocated; // Allocated (True) or reserved (False)

  public SchedulerContainer(A app, N node, RMContainer rmContainer,
      String nodePartition, boolean allocated) {
    this.schedulerApplicationAttempt = app;
    this.schedulerNode = node;
    this.rmContainer = rmContainer;
    this.nodePartition = nodePartition;
    this.allocated = allocated;
  }

  public String getNodePartition() {
    return nodePartition;
  }

  public RMContainer getRmContainer() {
    return rmContainer;
  }

  public A getSchedulerApplicationAttempt() {
    return schedulerApplicationAttempt;
  }

  public N getSchedulerNode() {
    return schedulerNode;
  }

  public boolean isAllocated() {
    return allocated;
  }

  public SchedulerRequestKey getSchedulerRequestKey() {
    if (rmContainer.getState() == RMContainerState.RESERVED) {
      return rmContainer.getReservedSchedulerKey();
    }
    return rmContainer.getAllocatedSchedulerKey();
  }

  @Override
  public String toString() {
    return "(Application=" + schedulerApplicationAttempt
        .getApplicationAttemptId() + "; Node=" + schedulerNode.getNodeID()
        + "; Resource=" + rmContainer.getAllocatedOrReservedResource() + ")";
  }
}