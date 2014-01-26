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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class contains the fields that can be determined when
 * <code>RMContainer</code> starts, and that need to be stored persistently.
 */
@Public
@Unstable
public abstract class ContainerStartData {

  @Public
  @Unstable
  public static ContainerStartData newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long startTime) {
    ContainerStartData containerSD =
        Records.newRecord(ContainerStartData.class);
    containerSD.setContainerId(containerId);
    containerSD.setAllocatedResource(allocatedResource);
    containerSD.setAssignedNode(assignedNode);
    containerSD.setPriority(priority);
    containerSD.setStartTime(startTime);
    return containerSD;
  }

  @Public
  @Unstable
  public abstract ContainerId getContainerId();

  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  @Public
  @Unstable
  public abstract Resource getAllocatedResource();

  @Public
  @Unstable
  public abstract void setAllocatedResource(Resource resource);

  @Public
  @Unstable
  public abstract NodeId getAssignedNode();

  @Public
  @Unstable
  public abstract void setAssignedNode(NodeId nodeId);

  @Public
  @Unstable
  public abstract Priority getPriority();

  @Public
  @Unstable
  public abstract void setPriority(Priority priority);

  @Public
  @Unstable
  public abstract long getStartTime();

  @Public
  @Unstable
  public abstract void setStartTime(long startTime);

}
