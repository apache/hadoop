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
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The class contains all the fields that need to be stored persistently for
 * <code>RMContainer</code>.
 */
@Public
@Unstable
public interface ContainerHistoryData {

  @Public
  @Unstable
  ContainerId getContainerId();

  @Public
  @Unstable
  void setContainerId(ContainerId containerId);

  @Public
  @Unstable
  Resource getAllocatedResource();

  @Public
  @Unstable
  void setAllocatedResource(Resource resource);

  @Public
  @Unstable
  NodeId getAssignedNode();

  @Public
  @Unstable
  void setAssignedNode(NodeId nodeId);

  @Public
  @Unstable
  Priority getPriority();

  @Public
  @Unstable
  void setPriority(Priority priority);

  @Public
  @Unstable
  long getStartTime();

  @Public
  @Unstable
  void setStartTime(long startTime);

  @Public
  @Unstable
  long getFinishTime();

  @Public
  @Unstable
  void setFinishTime(long finishTime);

  @Public
  @Unstable
  String getDiagnosticsInfo();

  @Public
  @Unstable
  void setDiagnosticsInfo(String diagnosticInfo);

  @Public
  @Unstable
  String getLogURL();

  @Public
  @Unstable
  void setLogURL(String logURL);

  @Public
  @Unstable
  ContainerState getFinalContainerStatus();

  @Public
  @Unstable
  void setFinalContainerStatus(ContainerState finalContainerState);

}
