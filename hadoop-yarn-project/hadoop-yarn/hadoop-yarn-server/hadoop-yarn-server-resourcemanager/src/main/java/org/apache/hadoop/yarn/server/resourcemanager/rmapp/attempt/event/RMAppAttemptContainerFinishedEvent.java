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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptContainerFinishedEvent extends RMAppAttemptEvent {

  private final ContainerStatus containerStatus;
  private final NodeId nodeId;

  public RMAppAttemptContainerFinishedEvent(ApplicationAttemptId appAttemptId, 
      ContainerStatus containerStatus, NodeId nodeId) {
    super(appAttemptId, RMAppAttemptEventType.CONTAINER_FINISHED);
    this.containerStatus = containerStatus;
    this.nodeId = nodeId;
  }

  public ContainerStatus getContainerStatus() {
    return this.containerStatus;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }
}
