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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;

/**
 * Context of the ResourceManager.
 */
public interface RMContext {

  Dispatcher getDispatcher();
  
  RMStateStore getStateStore();

  ConcurrentMap<ApplicationId, RMApp> getRMApps();
  
  ConcurrentMap<String, RMNode> getInactiveRMNodes();

  ConcurrentMap<NodeId, RMNode> getRMNodes();

  AMLivelinessMonitor getAMLivelinessMonitor();

  AMLivelinessMonitor getAMFinishingMonitor();

  ContainerAllocationExpirer getContainerAllocationExpirer();
  
  DelegationTokenRenewer getDelegationTokenRenewer();

  ApplicationTokenSecretManager getApplicationTokenSecretManager();

  RMContainerTokenSecretManager getContainerTokenSecretManager();

  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();
}