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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ContainersMonitor;

/**
 * The ContainerManager is an entity that manages the life cycle of Containers.
 */
public interface ContainerManager extends ServiceStateChangeListener,
    ContainerManagementProtocol,
    EventHandler<ContainerManagerEvent> {

  ContainersMonitor getContainersMonitor();

  OpportunisticContainersStatus getOpportunisticContainersStatus();

  void updateQueuingLimit(ContainerQueuingLimit queuingLimit);

  void setBlockNewContainerRequests(boolean blockNewContainerRequests);

}
