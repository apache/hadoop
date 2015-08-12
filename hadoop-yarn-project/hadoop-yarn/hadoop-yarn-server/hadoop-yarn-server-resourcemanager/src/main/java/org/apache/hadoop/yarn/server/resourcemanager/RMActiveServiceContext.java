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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
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

/**
 * The RMActiveServiceContext is the class that maintains all the
 * RMActiveService contexts.This is expected to be used only by ResourceManager
 * and RMContext.
 */
@Private
@Unstable
public class RMActiveServiceContext {

  private static final Log LOG = LogFactory
      .getLog(RMActiveServiceContext.class);

  private final ConcurrentMap<ApplicationId, RMApp> applications =
      new ConcurrentHashMap<ApplicationId, RMApp>();

  private final ConcurrentMap<NodeId, RMNode> nodes =
      new ConcurrentHashMap<NodeId, RMNode>();

  private final ConcurrentMap<String, RMNode> inactiveNodes =
      new ConcurrentHashMap<String, RMNode>();

  private final ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
      new ConcurrentHashMap<ApplicationId, ByteBuffer>();

  private boolean isWorkPreservingRecoveryEnabled;

  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMStateStore stateStore = null;
  private ContainerAllocationExpirer containerAllocationExpirer;
  private DelegationTokenRenewer delegationTokenRenewer;
  private AMRMTokenSecretManager amRMTokenSecretManager;
  private RMContainerTokenSecretManager containerTokenSecretManager;
  private NMTokenSecretManagerInRM nmTokenSecretManager;
  private ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;
  private ClientRMService clientRMService;
  private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;
  private ResourceScheduler scheduler;
  private ReservationSystem reservationSystem;
  private NodesListManager nodesListManager;
  private ResourceTrackerService resourceTrackerService;
  private ApplicationMasterService applicationMasterService;
  private RMNodeLabelsManager nodeLabelManager;
  private long epoch;
  private Clock systemClock = new SystemClock();
  private long schedulerRecoveryStartTime = 0;
  private long schedulerRecoveryWaitTime = 0;
  private boolean printLog = true;
  private boolean isSchedulerReady = false;

  public RMActiveServiceContext() {

  }

  @Private
  @Unstable
  public RMActiveServiceContext(Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager appTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
      ResourceScheduler scheduler) {
    this();
    this.setContainerAllocationExpirer(containerAllocationExpirer);
    this.setAMLivelinessMonitor(amLivelinessMonitor);
    this.setAMFinishingMonitor(amFinishingMonitor);
    this.setDelegationTokenRenewer(delegationTokenRenewer);
    this.setAMRMTokenSecretManager(appTokenSecretManager);
    this.setContainerTokenSecretManager(containerTokenSecretManager);
    this.setNMTokenSecretManager(nmTokenSecretManager);
    this.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    this.setScheduler(scheduler);

    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }
  }

  @Private
  @Unstable
  public void setStateStore(RMStateStore store) {
    stateStore = store;
  }

  @Private
  @Unstable
  public ClientRMService getClientRMService() {
    return clientRMService;
  }

  @Private
  @Unstable
  public ApplicationMasterService getApplicationMasterService() {
    return applicationMasterService;
  }

  @Private
  @Unstable
  public ResourceTrackerService getResourceTrackerService() {
    return resourceTrackerService;
  }

  @Private
  @Unstable
  public RMStateStore getStateStore() {
    return stateStore;
  }

  @Private
  @Unstable
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return this.applications;
  }

  @Private
  @Unstable
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return this.nodes;
  }

  @Private
  @Unstable
  public ConcurrentMap<String, RMNode> getInactiveRMNodes() {
    return this.inactiveNodes;
  }

  @Private
  @Unstable
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return this.containerAllocationExpirer;
  }

  @Private
  @Unstable
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }

  @Private
  @Unstable
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return this.amFinishingMonitor;
  }

  @Private
  @Unstable
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return delegationTokenRenewer;
  }

  @Private
  @Unstable
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return this.amRMTokenSecretManager;
  }

  @Private
  @Unstable
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }

  @Private
  @Unstable
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return this.nmTokenSecretManager;
  }

  @Private
  @Unstable
  public ResourceScheduler getScheduler() {
    return this.scheduler;
  }

  @Private
  @Unstable
  public ReservationSystem getReservationSystem() {
    return this.reservationSystem;
  }

  @Private
  @Unstable
  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  @Private
  @Unstable
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMTokenSecretManager;
  }

  @Private
  @Unstable
  public void setClientRMService(ClientRMService clientRMService) {
    this.clientRMService = clientRMService;
  }

  @Private
  @Unstable
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return this.rmDelegationTokenSecretManager;
  }

  @Private
  @Unstable
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    this.rmDelegationTokenSecretManager = delegationTokenSecretManager;
  }

  @Private
  @Unstable
  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.containerAllocationExpirer = containerAllocationExpirer;
  }

  @Private
  @Unstable
  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    this.amLivelinessMonitor = amLivelinessMonitor;
  }

  @Private
  @Unstable
  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    this.amFinishingMonitor = amFinishingMonitor;
  }

  @Private
  @Unstable
  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  @Private
  @Unstable
  void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
    this.nmTokenSecretManager = nmTokenSecretManager;
  }

  @Private
  @Unstable
  void setScheduler(ResourceScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Private
  @Unstable
  void setReservationSystem(ReservationSystem reservationSystem) {
    this.reservationSystem = reservationSystem;
  }

  @Private
  @Unstable
  void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
    this.delegationTokenRenewer = delegationTokenRenewer;
  }

  @Private
  @Unstable
  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
  }

  @Private
  @Unstable
  void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
    this.amRMTokenSecretManager = amRMTokenSecretManager;
  }

  @Private
  @Unstable
  void setNodesListManager(NodesListManager nodesListManager) {
    this.nodesListManager = nodesListManager;
  }

  @Private
  @Unstable
  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    this.applicationMasterService = applicationMasterService;
  }

  @Private
  @Unstable
  void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
    this.resourceTrackerService = resourceTrackerService;
  }

  @Private
  @Unstable
  public void setWorkPreservingRecoveryEnabled(boolean enabled) {
    this.isWorkPreservingRecoveryEnabled = enabled;
  }

  @Private
  @Unstable
  public boolean isWorkPreservingRecoveryEnabled() {
    return this.isWorkPreservingRecoveryEnabled;
  }

  @Private
  @Unstable
  public long getEpoch() {
    return this.epoch;
  }

  @Private
  @Unstable
  void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  @Private
  @Unstable
  public RMNodeLabelsManager getNodeLabelManager() {
    return nodeLabelManager;
  }

  @Private
  @Unstable
  public void setNodeLabelManager(RMNodeLabelsManager mgr) {
    nodeLabelManager = mgr;
  }

  @Private
  @Unstable
  public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
    this.schedulerRecoveryStartTime = systemClock.getTime();
    this.schedulerRecoveryWaitTime = waitTime;
  }

  @Private
  @Unstable
  public boolean isSchedulerReadyForAllocatingContainers() {
    if (isSchedulerReady) {
      return isSchedulerReady;
    }
    isSchedulerReady =
        (systemClock.getTime() - schedulerRecoveryStartTime) > schedulerRecoveryWaitTime;
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
  @Unstable
  public void setSystemClock(Clock clock) {
    this.systemClock = clock;
  }

  @Private
  @Unstable
  public ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
    return systemCredentials;
  }
}
