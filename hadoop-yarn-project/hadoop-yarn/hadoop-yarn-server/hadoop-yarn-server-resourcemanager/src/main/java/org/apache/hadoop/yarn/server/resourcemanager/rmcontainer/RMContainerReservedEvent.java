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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The event signifying that a container has been reserved.
 * 
 * The event encapsulates information on the amount of reservation
 * and the node on which the reservation is in effect.
 */
public class RMContainerReservedEvent extends RMContainerEvent {

  private final Resource reservedResource;
  private final NodeId reservedNode;
  private final Priority reservedPriority;
  
  public RMContainerReservedEvent(ContainerId containerId,
      Resource reservedResource, NodeId reservedNode, 
      Priority reservedPriority) {
    super(containerId, RMContainerEventType.RESERVED);
    this.reservedResource = reservedResource;
    this.reservedNode = reservedNode;
    this.reservedPriority = reservedPriority;
  }

  public Resource getReservedResource() {
    return reservedResource;
  }

  public NodeId getReservedNode() {
    return reservedNode;
  }

  public Priority getReservedPriority() {
    return reservedPriority;
  }

}
