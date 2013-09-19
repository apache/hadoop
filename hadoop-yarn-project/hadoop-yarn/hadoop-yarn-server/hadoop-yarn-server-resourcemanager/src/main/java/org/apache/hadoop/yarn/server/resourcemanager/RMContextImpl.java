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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;

import com.google.common.annotations.VisibleForTesting;

public class RMContextImpl implements RMContext {

  private final Dispatcher rmDispatcher;

  private final ConcurrentMap<ApplicationId, RMApp> applications
    = new ConcurrentHashMap<ApplicationId, RMApp>();

  private final ConcurrentMap<NodeId, RMNode> nodes
    = new ConcurrentHashMap<NodeId, RMNode>();
  
  private final ConcurrentMap<String, RMNode> inactiveNodes
    = new ConcurrentHashMap<String, RMNode>();

  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMStateStore stateStore = null;
  private ContainerAllocationExpirer containerAllocationExpirer;
  private final DelegationTokenRenewer delegationTokenRenewer;
  private final AMRMTokenSecretManager amRMTokenSecretManager;
  private final RMContainerTokenSecretManager containerTokenSecretManager;
  private final NMTokenSecretManagerInRM nmTokenSecretManager;
  private final ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;
  private ClientRMService clientRMService;
  private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;

  public RMContextImpl(Dispatcher rmDispatcher,
      RMStateStore store,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager amRMTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this.rmDispatcher = rmDispatcher;
    this.stateStore = store;
    this.containerAllocationExpirer = containerAllocationExpirer;
    this.amLivelinessMonitor = amLivelinessMonitor;
    this.amFinishingMonitor = amFinishingMonitor;
    this.delegationTokenRenewer = delegationTokenRenewer;
    this.amRMTokenSecretManager = amRMTokenSecretManager;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmTokenSecretManager = nmTokenSecretManager;
    this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
  }

  @VisibleForTesting
  // helper constructor for tests
  public RMContextImpl(Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager appTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this(rmDispatcher, null, containerAllocationExpirer, amLivelinessMonitor, 
          amFinishingMonitor, delegationTokenRenewer, appTokenSecretManager, 
          containerTokenSecretManager, nmTokenSecretManager,
          clientToAMTokenSecretManager);
    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }
  }
  
  @Override
  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }
  
  @Override 
  public RMStateStore getStateStore() {
    return stateStore;
  }

  @Override
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return this.applications;
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return this.nodes;
  }
  
  @Override
  public ConcurrentMap<String, RMNode> getInactiveRMNodes() {
    return this.inactiveNodes;
  }

  @Override
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return this.containerAllocationExpirer;
  }

  @Override
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }

  @Override
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return this.amFinishingMonitor;
  }

  @Override
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return delegationTokenRenewer;
  }

  @Override
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return this.amRMTokenSecretManager;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }
  
  @Override
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return this.nmTokenSecretManager;
  }
  
  @Override
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMTokenSecretManager;
  }
  
  @VisibleForTesting
  public void setStateStore(RMStateStore store) {
    stateStore = store;
  }
  
  @Override
  public ClientRMService getClientRMService() {
    return this.clientRMService;
  }
  
  @Override
  public void setClientRMService(ClientRMService clientRMService) {
    this.clientRMService = clientRMService;
  }
  
  @Override
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return this.rmDelegationTokenSecretManager;
  }
  
  @Override
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    this.rmDelegationTokenSecretManager = delegationTokenSecretManager;
  }
}