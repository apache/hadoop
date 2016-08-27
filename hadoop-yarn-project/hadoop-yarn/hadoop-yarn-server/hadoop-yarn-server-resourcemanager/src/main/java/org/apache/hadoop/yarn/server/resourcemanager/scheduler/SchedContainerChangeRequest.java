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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * This is UpdateContainerRequest in scheduler side, it contains some
 * pointers to runtime objects like RMContainer, SchedulerNode, etc. This will
 * be easier for scheduler making decision.
 */
public class SchedContainerChangeRequest implements
    Comparable<SchedContainerChangeRequest> {
  private RMContext rmContext;
  private RMContainer rmContainer;
  private Resource targetCapacity;
  private SchedulerNode schedulerNode;
  private Resource deltaCapacity;

  public SchedContainerChangeRequest(
      RMContext rmContext, SchedulerNode schedulerNode,
      RMContainer rmContainer, Resource targetCapacity) {
    this.rmContext = rmContext;
    this.rmContainer = rmContainer;
    this.targetCapacity = targetCapacity;
    this.schedulerNode = schedulerNode;
  }
  
  public NodeId getNodeId() {
    return this.rmContainer.getAllocatedNode();
  }

  public RMContainer getRMContainer() {
    return this.rmContainer;
  }

  public Resource getTargetCapacity() {
    return this.targetCapacity;
  }

  public RMContext getRmContext() {
    return this.rmContext;
  }
  /**
   * Delta capacity = target - before, so if it is a decrease request, delta
   * capacity will be negative
   */
  public synchronized Resource getDeltaCapacity() {
    // Only calculate deltaCapacity once
    if (deltaCapacity == null) {
      deltaCapacity = Resources.subtract(
          targetCapacity, rmContainer.getAllocatedResource());
    }
    return deltaCapacity;
  }
  
  public Priority getPriority() {
    return rmContainer.getContainer().getPriority();
  }
  
  public ContainerId getContainerId() {
    return rmContainer.getContainerId();
  }
  
  public String getNodePartition() {
    return schedulerNode.getPartition();
  }
  
  public SchedulerNode getSchedulerNode() {
    return schedulerNode;
  }

  @Override
  public int hashCode() {
    return (getContainerId().hashCode() << 16) + targetCapacity.hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SchedContainerChangeRequest)) {
      return false;
    }
    return compareTo((SchedContainerChangeRequest)other) == 0;
  }

  @Override
  public int compareTo(SchedContainerChangeRequest other) {
    if (other == null) {
      return -1;
    }
    
    int rc = getPriority().compareTo(other.getPriority());
    if (0 != rc) {
      return rc;
    }
    
    return getContainerId().compareTo(other.getContainerId());
  }
  
  @Override
  public String toString() {
    return "<container=" + getContainerId() + ", targetCapacity="
        + targetCapacity + ", node=" + getNodeId().toString() + ">";
  }
}
