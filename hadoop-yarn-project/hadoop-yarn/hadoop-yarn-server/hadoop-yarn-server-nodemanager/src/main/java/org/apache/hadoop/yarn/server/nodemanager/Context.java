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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * Context interface for sharing information across components in the
 * NodeManager.
 */
public interface Context {

  /**
   * Return the nodeId. Usable only when the ContainerManager is started.
   * 
   * @return the NodeId
   */
  NodeId getNodeId();

  /**
   * Return the node http-address. Usable only after the Webserver is started.
   * 
   * @return the http-port
   */
  int getHttpPort();

  ConcurrentMap<ApplicationId, Application> getApplications();

  Map<ApplicationId, Credentials> getSystemCredentialsForApps();

  ConcurrentMap<ContainerId, Container> getContainers();

  NMContainerTokenSecretManager getContainerTokenSecretManager();
  
  NMTokenSecretManagerInNM getNMTokenSecretManager();

  NodeHealthStatus getNodeHealthStatus();

  ContainerManagementProtocol getContainerManager();

  LocalDirsHandlerService getLocalDirsHandler();

  ApplicationACLsManager getApplicationACLsManager();

  NMStateStoreService getNMStateStore();

  boolean getDecommissioned();

  void setDecommissioned(boolean isDecommissioned);

  NodeStatusUpdater getNodeStatusUpdater();
}
