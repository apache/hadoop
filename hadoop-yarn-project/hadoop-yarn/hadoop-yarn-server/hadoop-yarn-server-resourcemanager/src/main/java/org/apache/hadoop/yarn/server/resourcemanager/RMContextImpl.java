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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import com.google.common.annotations.VisibleForTesting;

public class RMContextImpl implements RMContext {

  private Dispatcher rmDispatcher;

  private final ConcurrentMap<ApplicationId, RMApp> applications
    = new ConcurrentHashMap<ApplicationId, RMApp>();

  private final ConcurrentMap<NodeId, RMNode> nodes
    = new ConcurrentHashMap<NodeId, RMNode>();
  
  private final ConcurrentMap<String, RMNode> inactiveNodes
    = new ConcurrentHashMap<String, RMNode>();

  private final ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
      new ConcurrentHashMap<ApplicationId, ByteBuffer>();

  private boolean isHAEnabled;
  private boolean isWorkPreservingRecoveryEnabled;
  private HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING;
  
  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMStateStore stateStore = null;
  private ContainerAllocationExpirer containerAllocationExpirer;
  private DelegationTokenRenewer delegationTokenRenewer;
  private AMRMTokenSecretManager amRMTokenSecretManager;
  private RMContainerTokenSecretManager containerTokenSecretManager;
  private NMTokenSecretManagerInRM nmTokenSecretManager;
  private ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;
  private AdminService adminService;
  private ClientRMService clientRMService;
  private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;
  private ResourceScheduler scheduler;
  private ReservationSystem reservationSystem;
  private NodesListManager nodesListManager;
  private ResourceTrackerService resourceTrackerService;
  private ApplicationMasterService applicationMasterService;
  private RMApplicationHistoryWriter rmApplicationHistoryWriter;
  private SystemMetricsPublisher systemMetricsPublisher;
  private ConfigurationProvider configurationProvider;
  private RMNodeLabelsManager nodeLabelManager;
  private long epoch;
  private Clock systemClock = new SystemClock();
  private long schedulerRecoveryStartTime = 0;
  private long schedulerRecoveryWaitTime = 0;
  private boolean printLog = true;
  private boolean isSchedulerReady = false;

  private static final Log LOG = LogFactory.getLog(RMContextImpl.class);

  /**
   * Default constructor. To be used in conjunction with setter methods for
   * individual fields.
   */
  public RMContextImpl() {

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
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
      RMApplicationHistoryWriter rmApplicationHistoryWriter) {
    this();
    this.setDispatcher(rmDispatcher);
    this.setContainerAllocationExpirer(containerAllocationExpirer);
    this.setAMLivelinessMonitor(amLivelinessMonitor);
    this.setAMFinishingMonitor(amFinishingMonitor);
    this.setDelegationTokenRenewer(delegationTokenRenewer);
    this.setAMRMTokenSecretManager(appTokenSecretManager);
    this.setContainerTokenSecretManager(containerTokenSecretManager);
    this.setNMTokenSecretManager(nmTokenSecretManager);
    this.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    this.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }

    ConfigurationProvider provider = new LocalConfigurationProvider();
    setConfigurationProvider(provider);
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
  public ResourceScheduler getScheduler() {
    return this.scheduler;
  }

  @Override
  public ReservationSystem getReservationSystem() {
    return this.reservationSystem;
  }
  
  @Override
  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  @Override
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMTokenSecretManager;
  }

  @Override
  public AdminService getRMAdminService() {
    return this.adminService;
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
  public ApplicationMasterService getApplicationMasterService() {
    return applicationMasterService;
  }

  @Override
  public ResourceTrackerService getResourceTrackerService() {
    return resourceTrackerService;
  }

  void setHAEnabled(boolean isHAEnabled) {
    this.isHAEnabled = isHAEnabled;
  }

  void setHAServiceState(HAServiceState haServiceState) {
    synchronized (haServiceState) {
      this.haServiceState = haServiceState;
    }
  }

  void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  void setRMAdminService(AdminService adminService) {
    this.adminService = adminService;
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

  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.containerAllocationExpirer = containerAllocationExpirer;
  }

  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    this.amLivelinessMonitor = amLivelinessMonitor;
  }

  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    this.amFinishingMonitor = amFinishingMonitor;
  }

  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  void setNMTokenSecretManager(
      NMTokenSecretManagerInRM nmTokenSecretManager) {
    this.nmTokenSecretManager = nmTokenSecretManager;
  }

  void setScheduler(ResourceScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  void setReservationSystem(ReservationSystem reservationSystem) {
    this.reservationSystem = reservationSystem;
  }

  void setDelegationTokenRenewer(
      DelegationTokenRenewer delegationTokenRenewer) {
    this.delegationTokenRenewer = delegationTokenRenewer;
  }

  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
  }

  void setAMRMTokenSecretManager(
      AMRMTokenSecretManager amRMTokenSecretManager) {
    this.amRMTokenSecretManager = amRMTokenSecretManager;
  }

  void setNodesListManager(NodesListManager nodesListManager) {
    this.nodesListManager = nodesListManager;
  }

  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    this.applicationMasterService = applicationMasterService;
  }

  void setResourceTrackerService(
      ResourceTrackerService resourceTrackerService) {
    this.resourceTrackerService = resourceTrackerService;
  }

  @Override
  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  @Override
  public HAServiceState getHAServiceState() {
    synchronized (haServiceState) {
      return haServiceState;
    }
  }

  public void setWorkPreservingRecoveryEnabled(boolean enabled) {
    this.isWorkPreservingRecoveryEnabled = enabled;
  }

  @Override
  public boolean isWorkPreservingRecoveryEnabled() {
    return this.isWorkPreservingRecoveryEnabled;
  }

  @Override
  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return rmApplicationHistoryWriter;
  }

  @Override
  public void setSystemMetricsPublisher(
      SystemMetricsPublisher systemMetricsPublisher) {
    this.systemMetricsPublisher = systemMetricsPublisher;
  }

  @Override
  public SystemMetricsPublisher getSystemMetricsPublisher() {
    return systemMetricsPublisher;
  }

  @Override
  public void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter) {
    this.rmApplicationHistoryWriter = rmApplicationHistoryWriter;
  }

  @Override
  public ConfigurationProvider getConfigurationProvider() {
    return this.configurationProvider;
  }

  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public long getEpoch() {
    return this.epoch;
  }

  void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  @Override
  public RMNodeLabelsManager getNodeLabelManager() {
    return nodeLabelManager;
  }
  
  @Override
  public void setNodeLabelManager(RMNodeLabelsManager mgr) {
    nodeLabelManager = mgr;
  }

  public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
    this.schedulerRecoveryStartTime = systemClock.getTime();
    this.schedulerRecoveryWaitTime = waitTime;
  }

  public boolean isSchedulerReadyForAllocatingContainers() {
    if (isSchedulerReady) {
      return isSchedulerReady;
    }
    isSchedulerReady = (systemClock.getTime() - schedulerRecoveryStartTime)
        > schedulerRecoveryWaitTime;
    if (!isSchedulerReady && printLog) {
      LOG.info("Skip allocating containers. Scheduler is waiting for recovery.");
      printLog = false;
    }
    if (isSchedulerReady) {
      LOG.info("Scheduler recovery is done. Start allocating new containers.");
    }
    return isSchedulerReady;
  }

  @Private
  @VisibleForTesting
  public void setSystemClock(Clock clock) {
    this.systemClock = clock;
  }

  public ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
    return systemCredentials;
  }
}
