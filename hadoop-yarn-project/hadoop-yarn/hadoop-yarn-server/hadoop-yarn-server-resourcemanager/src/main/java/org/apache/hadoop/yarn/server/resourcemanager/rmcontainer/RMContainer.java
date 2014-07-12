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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;

/**
 * Represents the ResourceManager's view of an application container. See 
 * {@link RMContainerImpl} for an implementation. Containers may be in one
 * of several states, given in {@link RMContainerState}. An RMContainer
 * instance may exist even if there is no actual running container, such as 
 * when resources are being reserved to fill space for a future container 
 * allocation.
 */
public interface RMContainer extends EventHandler<RMContainerEvent> {

  ContainerId getContainerId();

  ApplicationAttemptId getApplicationAttemptId();

  RMContainerState getState();

  Container getContainer();

  Resource getReservedResource();

  NodeId getReservedNode();
  
  Priority getReservedPriority();

  Resource getAllocatedResource();

  NodeId getAllocatedNode();

  Priority getAllocatedPriority();

  long getCreationTime();

  long getFinishTime();

  String getDiagnosticsInfo();

  String getLogURL();

  int getContainerExitStatus();

  ContainerState getContainerState();
  
  ContainerReport createContainerReport();
  
  boolean isAMContainer();

}
