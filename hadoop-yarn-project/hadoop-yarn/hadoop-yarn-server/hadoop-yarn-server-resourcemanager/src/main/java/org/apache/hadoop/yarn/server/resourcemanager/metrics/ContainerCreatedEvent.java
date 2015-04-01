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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class ContainerCreatedEvent extends SystemMetricsEvent {

  private ContainerId containerId;
  private Resource allocatedResource;
  private NodeId allocatedNode;
  private Priority allocatedPriority;
  private String nodeHttpAddress;

  public ContainerCreatedEvent(
      ContainerId containerId,
      Resource allocatedResource,
      NodeId allocatedNode,
      Priority allocatedPriority,
      long createdTime,
      String nodeHttpAddress) {
    super(SystemMetricsEventType.CONTAINER_CREATED, createdTime);
    this.containerId = containerId;
    this.allocatedResource = allocatedResource;
    this.allocatedNode = allocatedNode;
    this.allocatedPriority = allocatedPriority;
    this.nodeHttpAddress = nodeHttpAddress;
  }

  @Override
  public int hashCode() {
    return containerId.getApplicationAttemptId().getApplicationId().hashCode();
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Resource getAllocatedResource() {
    return allocatedResource;
  }

  public NodeId getAllocatedNode() {
    return allocatedNode;
  }

  public Priority getAllocatedPriority() {
    return allocatedPriority;
  }

  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }
}
