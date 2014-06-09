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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import java.util.List;

import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class NodeAddedSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;
  private final List<NMContainerStatus> containerReports;

  public NodeAddedSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_ADDED);
    this.rmNode = rmNode;
    this.containerReports = null;
  }

  public NodeAddedSchedulerEvent(RMNode rmNode,
      List<NMContainerStatus> containerReports) {
    super(SchedulerEventType.NODE_ADDED);
    this.rmNode = rmNode;
    this.containerReports = containerReports;
  }

  public RMNode getAddedRMNode() {
    return rmNode;
  }

  public List<NMContainerStatus> getContainerReports() {
    return containerReports;
  }
}
