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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * RMContextImpl class holds two services context.
 * <ul>
 * <li>serviceContext : These services called as <b>Always On</b> services.
 * Services that need to run always irrespective of the HA state of the RM.</li>
 * <li>activeServiceCotext : Active services context. Services that need to run
 * only on the Active RM.</li>
 * </ul>
 * <p>
 * <b>Note:</b> If any new service to be added to context, add it to a right
 * context as per above description.
 */
public class RMContextImpl implements RMContext {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMContextImpl.class);
  private static final String UNAVAILABLE = "N/A";
  /**
   * RM service contexts which runs through out RM life span. These are created
   * once during start of RM.
   */
  private RMServiceContext serviceContext;

  /**
   * RM Active service context. This will be recreated for every transition from
   * ACTIVE->STANDBY.
   */
  private RMActiveServiceContext activeServiceContext;

  private String proxyHostAndPort = null;

  /**
   * Default constructor. To be used in conjunction with setter methods for
   * individual fields.
   */
  public RMContextImpl() {
    this.serviceContext = new RMServiceContext();
    this.activeServiceContext = new RMActiveServiceContext();
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
      ResourceScheduler scheduler) {
    this();
    this.setDispatcher(rmDispatcher);
    setActiveServiceContext(new RMActiveServiceContext(rmDispatcher,
        containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
        delegationTokenRenewer, appTokenSecretManager,
        containerTokenSecretManager, nmTokenSecretManager,
        clientToAMTokenSecretManager,
        scheduler));

    ConfigurationProvider provider = new LocalConfigurationProvider();
    setConfigurationProvider(provider);
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
    this(
      rmDispatcher,
      containerAllocationExpirer,
      amLivelinessMonitor,
      amFinishingMonitor,
      delegationTokenRenewer,
      appTokenSecretManager,
      containerTokenSecretManager,
      nmTokenSecretManager,
      clientToAMTokenSecretManager, null);
  }

  /**
   * RM service contexts which runs through out JVM life span. These are created
   * once during start of RM.
   * @return serviceContext of RM
   */
  @Private
  @Unstable
  public RMServiceContext getServiceContext() {
    return serviceContext;
  }

  /**
   * <b>Note:</b> setting service context clears all services embedded with it.
   * @param context rm service context
   */
  @Private
  @Unstable
  public void setServiceContext(RMServiceContext context) {
    this.serviceContext = context;
  }

  @Override
  public ResourceManager getResourceManager() {
    return serviceContext.getResourceManager();
  }

  public void setResourceManager(ResourceManager rm) {
    serviceContext.setResourceManager(rm);
  }

  @Override
  public EmbeddedElector getLeaderElectorService() {
    return serviceContext.getLeaderElectorService();
  }

  @Override
  public void setLeaderElectorService(EmbeddedElector elector) {
    serviceContext.setLeaderElectorService(elector);
  }

  @Override
  public Dispatcher getDispatcher() {
    return serviceContext.getDispatcher();
  }

  void setDispatcher(Dispatcher dispatcher) {
    serviceContext.setDispatcher(dispatcher);
  }

  @Override
  public AdminService getRMAdminService() {
    return serviceContext.getRMAdminService();
  }

  void setRMAdminService(AdminService adminService) {
    serviceContext.setRMAdminService(adminService);
  }

  @Override
  public boolean isHAEnabled() {
    return serviceContext.isHAEnabled();
  }

  void setHAEnabled(boolean isHAEnabled) {
    serviceContext.setHAEnabled(isHAEnabled);
  }

  @Override
  public HAServiceState getHAServiceState() {
    return serviceContext.getHAServiceState();
  }

  void setHAServiceState(HAServiceState serviceState) {
    serviceContext.setHAServiceState(serviceState);
  }

  @Override
  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return serviceContext.getRMApplicationHistoryWriter();
  }

  @Override
  public void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter) {
    serviceContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);
  }

  @Override
  public SystemMetricsPublisher getSystemMetricsPublisher() {
    return serviceContext.getSystemMetricsPublisher();
  }

  @Override
  public void setSystemMetricsPublisher(
      SystemMetricsPublisher metricsPublisher) {
    serviceContext.setSystemMetricsPublisher(metricsPublisher);
  }

  @Override
  public RMTimelineCollectorManager getRMTimelineCollectorManager() {
    return serviceContext.getRMTimelineCollectorManager();
  }

  @Override
  public void setRMTimelineCollectorManager(
      RMTimelineCollectorManager timelineCollectorManager) {
    serviceContext.setRMTimelineCollectorManager(timelineCollectorManager);
  }

  @Override
  public ConfigurationProvider getConfigurationProvider() {
    return serviceContext.getConfigurationProvider();
  }

  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    serviceContext.setConfigurationProvider(configurationProvider);
  }

  @Override
  public Configuration getYarnConfiguration() {
    return serviceContext.getYarnConfiguration();
  }

  public void setYarnConfiguration(Configuration yarnConfiguration) {
    serviceContext.setYarnConfiguration(yarnConfiguration);
  }

  public String getHAZookeeperConnectionState() {
    return serviceContext.getHAZookeeperConnectionState();
  }

  // ==========================================================================
  /**
   * RM Active service context. This will be recreated for every transition from
   * ACTIVE to STANDBY.
   * @return activeServiceContext of active services
   */
  @Private
  @Unstable
  public RMActiveServiceContext getActiveServiceContext() {
    return activeServiceContext;
  }

  @Private
  @Unstable
  void setActiveServiceContext(RMActiveServiceContext activeServiceContext) {
    this.activeServiceContext = activeServiceContext;
  }

  @Override
  public RMStateStore getStateStore() {
    return activeServiceContext.getStateStore();
  }

  @Override
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return activeServiceContext.getRMApps();
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return activeServiceContext.getRMNodes();
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
    return activeServiceContext.getInactiveRMNodes();
  }

  @Override
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return activeServiceContext.getContainerAllocationExpirer();
  }

  @Override
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return activeServiceContext.getAMLivelinessMonitor();
  }

  @Override
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return activeServiceContext.getAMFinishingMonitor();
  }

  @Override
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return activeServiceContext.getDelegationTokenRenewer();
  }

  @Override
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return activeServiceContext.getAMRMTokenSecretManager();
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return activeServiceContext.getContainerTokenSecretManager();
  }

  @Override
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return activeServiceContext.getNMTokenSecretManager();
  }

  @Override
  public ResourceScheduler getScheduler() {
    return activeServiceContext.getScheduler();
  }

  @Override
  public ReservationSystem getReservationSystem() {
    return activeServiceContext.getReservationSystem();
  }

  @Override
  public NodesListManager getNodesListManager() {
    return activeServiceContext.getNodesListManager();
  }

  @Override
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return activeServiceContext.getClientToAMTokenSecretManager();
  }

  @VisibleForTesting
  public void setStateStore(RMStateStore store) {
    activeServiceContext.setStateStore(store);
  }

  @Override
  public ClientRMService getClientRMService() {
    return activeServiceContext.getClientRMService();
  }

  @Override
  public ApplicationMasterService getApplicationMasterService() {
    return activeServiceContext.getApplicationMasterService();
  }

  @Override
  public ResourceTrackerService getResourceTrackerService() {
    return activeServiceContext.getResourceTrackerService();
  }

  @Override
  public void setClientRMService(ClientRMService clientRMService) {
    activeServiceContext.setClientRMService(clientRMService);
  }

  @Override
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return activeServiceContext.getRMDelegationTokenSecretManager();
  }

  @Override
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    activeServiceContext
        .setRMDelegationTokenSecretManager(delegationTokenSecretManager);
  }

  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    activeServiceContext
        .setContainerAllocationExpirer(containerAllocationExpirer);
  }

  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    activeServiceContext.setAMLivelinessMonitor(amLivelinessMonitor);
  }

  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    activeServiceContext.setAMFinishingMonitor(amFinishingMonitor);
  }

  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    activeServiceContext
        .setContainerTokenSecretManager(containerTokenSecretManager);
  }

  void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
    activeServiceContext.setNMTokenSecretManager(nmTokenSecretManager);
  }

  @VisibleForTesting
  public void setScheduler(ResourceScheduler scheduler) {
    activeServiceContext.setScheduler(scheduler);
  }

  void setReservationSystem(ReservationSystem reservationSystem) {
    activeServiceContext.setReservationSystem(reservationSystem);
  }

  void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
    activeServiceContext.setDelegationTokenRenewer(delegationTokenRenewer);
  }

  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    activeServiceContext
        .setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
  }

  void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
    activeServiceContext.setAMRMTokenSecretManager(amRMTokenSecretManager);
  }

  void setNodesListManager(NodesListManager nodesListManager) {
    activeServiceContext.setNodesListManager(nodesListManager);
  }

  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    activeServiceContext.setApplicationMasterService(applicationMasterService);
  }

  void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
    activeServiceContext.setResourceTrackerService(resourceTrackerService);
  }

  public void setWorkPreservingRecoveryEnabled(boolean enabled) {
    activeServiceContext.setWorkPreservingRecoveryEnabled(enabled);
  }

  @Override
  public boolean isWorkPreservingRecoveryEnabled() {
    return activeServiceContext.isWorkPreservingRecoveryEnabled();
  }


  @Override
  public long getEpoch() {
    return activeServiceContext.getEpoch();
  }

  void setEpoch(long epoch) {
    activeServiceContext.setEpoch(epoch);
  }

  @Override
  public RMNodeLabelsManager getNodeLabelManager() {
    return activeServiceContext.getNodeLabelManager();
  }

  @Override
  public void setNodeLabelManager(RMNodeLabelsManager mgr) {
    activeServiceContext.setNodeLabelManager(mgr);
  }

  @Override
  public void setNodeAttributesManager(NodeAttributesManager mgr) {
    activeServiceContext.setNodeAttributesManager(mgr);
  }

  @Override
  public AllocationTagsManager getAllocationTagsManager() {
    return activeServiceContext.getAllocationTagsManager();
  }

  @Override
  public void setAllocationTagsManager(
      AllocationTagsManager allocationTagsManager) {
    activeServiceContext.setAllocationTagsManager(allocationTagsManager);
  }

  @Override
  public PlacementConstraintManager getPlacementConstraintManager() {
    return activeServiceContext.getPlacementConstraintManager();
  }

  @Override
  public void setPlacementConstraintManager(
      PlacementConstraintManager placementConstraintManager) {
    activeServiceContext
        .setPlacementConstraintManager(placementConstraintManager);
  }

  @Override
  public RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater() {
    return activeServiceContext.getRMDelegatedNodeLabelsUpdater();
  }

  @Override
  public void setRMDelegatedNodeLabelsUpdater(
      RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater) {
    activeServiceContext.setRMDelegatedNodeLabelsUpdater(
        delegatedNodeLabelsUpdater);
  }

  @Override
  public MultiNodeSortingManager<SchedulerNode> getMultiNodeSortingManager() {
    return activeServiceContext.getMultiNodeSortingManager();
  }

  @Override
  public void setMultiNodeSortingManager(
      MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager) {
    activeServiceContext.setMultiNodeSortingManager(multiNodeSortingManager);
  }

  public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
    activeServiceContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
  }

  public boolean isSchedulerReadyForAllocatingContainers() {
    return activeServiceContext.isSchedulerReadyForAllocatingContainers();
  }

  @Private
  @VisibleForTesting
  public void setSystemClock(Clock clock) {
    activeServiceContext.setSystemClock(clock);
  }

  public ConcurrentMap<ApplicationId, SystemCredentialsForAppsProto>
      getSystemCredentialsForApps() {
    return activeServiceContext.getSystemCredentialsForApps();
  }

  @Override
  public PlacementManager getQueuePlacementManager() {
    return this.activeServiceContext.getQueuePlacementManager();
  }

  @Override
  public void setQueuePlacementManager(PlacementManager placementMgr) {
    this.activeServiceContext.setQueuePlacementManager(placementMgr);
  }

  @Override
  public QueueLimitCalculator getNodeManagerQueueLimitCalculator() {
    return activeServiceContext.getNodeManagerQueueLimitCalculator();
  }

  public void setContainerQueueLimitCalculator(
      QueueLimitCalculator limitCalculator) {
    activeServiceContext.setContainerQueueLimitCalculator(limitCalculator);
  }

  @Override
  public void setRMAppLifetimeMonitor(
      RMAppLifetimeMonitor rmAppLifetimeMonitor) {
    this.activeServiceContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);
  }

  @Override
  public RMAppLifetimeMonitor getRMAppLifetimeMonitor() {
    return this.activeServiceContext.getRMAppLifetimeMonitor();
  }

  @Override
  public ResourceProfilesManager getResourceProfilesManager() {
    return this.activeServiceContext.getResourceProfilesManager();
  }

  String getProxyHostAndPort(Configuration conf) {
    if (proxyHostAndPort == null) {
      proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
    }
    return proxyHostAndPort;
  }

  @Override
  public String getAppProxyUrl(Configuration conf, ApplicationId applicationId)
  {
    try {
      final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme,
          getProxyHostAndPort(conf));
      URI result = ProxyUriUtils.getProxyUri(null, proxyUri, applicationId);
      return result.toASCIIString();
    } catch(URISyntaxException e) {
      LOG.warn("Could not generate default proxy tracking URL for " +
          applicationId);
      return UNAVAILABLE;
    }
  }

  @Override
  public void setResourceProfilesManager(ResourceProfilesManager mgr) {
    this.activeServiceContext.setResourceProfilesManager(mgr);
  }

  @Override
  public ProxyCAManager getProxyCAManager() {
    return this.activeServiceContext.getProxyCAManager();
  }

  @Override
  public void setProxyCAManager(ProxyCAManager proxyCAManager) {
    this.activeServiceContext.setProxyCAManager(proxyCAManager);
  }

  @Override
  public VolumeManager getVolumeManager() {
    return activeServiceContext.getVolumeManager();
  }

  @Override
  public void setVolumeManager(VolumeManager volumeManager) {
    this.activeServiceContext.setVolumeManager(volumeManager);
  }

  // Note: Read java doc before adding any services over here.

  @Override
  public NodeAttributesManager getNodeAttributesManager() {
    return activeServiceContext.getNodeAttributesManager();
  }

  @Override
  public long getTokenSequenceNo() {
    return this.activeServiceContext.getTokenSequenceNo();
  }

  @Override
  public void incrTokenSequenceNo() {
    this.activeServiceContext.incrTokenSequenceNo();
  }
}
