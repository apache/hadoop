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

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  public static final String YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS =
      YarnConfiguration.NM_PREFIX + "duration-to-track-stopped-containers";

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();
  private final Object shutdownMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;
  private long nextHeartBeatInterval;
  private ResourceTracker resourceTracker;
  private Resource totalResource;
  private Resource physicalResource;
  private int httpPort;
  private String nodeManagerVersionId;
  private String minimumResourceManagerVersion;
  private volatile boolean isStopped;
  private boolean tokenKeepAliveEnabled;
  private long tokenRemovalDelayMs;
  /** Keeps track of when the next keep alive request should be sent for an app*/
  private Map<ApplicationId, Long> appTokenKeepAliveMap =
      new HashMap<ApplicationId, Long>();
  private Random keepAliveDelayRandom = new Random();
  // It will be used to track recently stopped containers on node manager, this
  // is to avoid the misleading no-such-container exception messages on NM, when
  // the AM finishes it informs the RM to stop the may-be-already-completed
  // containers.
  private final Map<ContainerId, Long> recentlyStoppedContainers;
  // Save the reported completed containers in case of lost heartbeat responses.
  // These completed containers will be sent again till a successful response.
  private final Map<ContainerId, ContainerStatus> pendingCompletedContainers;
  // Duration for which to track recently stopped container.
  private long durationToTrackStoppedContainers;

  private boolean logAggregationEnabled;

  private final List<LogAggregationReport> logAggregationReportForAppsTempList;

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private boolean failedToConnect = false;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;
  private boolean registeredWithRM = false;
  Set<ContainerId> pendingContainersToRemove = new HashSet<ContainerId>();

  private NMNodeLabelsHandler nodeLabelsHandler;
  private NMNodeAttributesHandler nodeAttributesHandler;
  private NodeLabelsProvider nodeLabelsProvider;
  private NodeAttributesProvider nodeAttributesProvider;
  private long tokenSequenceNo;
  private boolean timelineServiceV2Enabled;

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.recentlyStoppedContainers = new LinkedHashMap<ContainerId, Long>();
    this.pendingCompletedContainers =
        new HashMap<ContainerId, ContainerStatus>();
    this.logAggregationReportForAppsTempList =
        new ArrayList<LogAggregationReport>();
  }

  @Override
  public void setNodeAttributesProvider(NodeAttributesProvider provider) {
    this.nodeAttributesProvider = provider;
  }

  @Override
  public void setNodeLabelsProvider(NodeLabelsProvider provider) {
    this.nodeLabelsProvider = provider;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.totalResource = NodeManagerHardwareUtils.getNodeResources(conf);
    long memoryMb = totalResource.getMemorySize();
    float vMemToPMem =
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO,
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    long virtualMemoryMb = (long)Math.ceil(memoryMb * vMemToPMem);
    int virtualCores = totalResource.getVirtualCores();

    // Update configured resources via plugins.
    updateConfiguredResourcesViaPlugins(totalResource);

    LOG.info("Nodemanager resources is set to: " + totalResource);

    metrics.addResource(totalResource);

    // Get actual node physical resources
    long physicalMemoryMb = memoryMb;
    int physicalCores = virtualCores;
    ResourceCalculatorPlugin rcp =
        ResourceCalculatorPlugin.getNodeResourceMonitorPlugin(conf);
    if (rcp != null) {
      physicalMemoryMb = rcp.getPhysicalMemorySize() / (1024 * 1024);
      physicalCores = rcp.getNumProcessors();
    }
    this.physicalResource =
        Resource.newInstance(physicalMemoryMb, physicalCores);

    this.tokenKeepAliveEnabled = isTokenKeepAliveEnabled(conf);
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);

    this.minimumResourceManagerVersion = conf.get(
        YarnConfiguration.NM_RESOURCEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_NM_RESOURCEMANAGER_MINIMUM_VERSION);

    nodeLabelsHandler =
        createNMNodeLabelsHandler(nodeLabelsProvider);
    nodeAttributesHandler =
        createNMNodeAttributesHandler(nodeAttributesProvider);

    // Default duration to track stopped containers on nodemanager is 10Min.
    // This should not be assigned very large value as it will remember all the
    // containers stopped during that time.
    durationToTrackStoppedContainers =
        conf.getLong(YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
          600000);
    if (durationToTrackStoppedContainers < 0) {
      String message = "Invalid configuration for "
        + YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS + " default "
          + "value is 10Min(600000).";
      LOG.error(message);
      throw new YarnException(message);
    }
    LOG.debug("{} :{}", YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
        durationToTrackStoppedContainers);
    super.serviceInit(conf);
    LOG.info("Initialized nodemanager with :" +
        " physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
        " virtual-cores=" + virtualCores);

    this.logAggregationEnabled =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
          YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);

  }

  @Override
  protected void serviceStart() throws Exception {

    // NodeManager is the last service to start, so NodeId is available.
    this.nodeId = this.context.getNodeId();
    LOG.info("Node ID assigned is : " + this.nodeId);
    this.httpPort = this.context.getHttpPort();
    this.nodeManagerVersionId = YarnVersionInfo.getVersion();
    try {
      // Registration has to be in start so that ContainerManager can get the
      // perNM tokens needed to authenticate ContainerTokens.
      this.resourceTracker = getRMClient();
      registerWithRM();
      super.serviceStart();
      startStatusUpdater();
    } catch (Exception e) {
      String errorMessage = "Unexpected error starting NodeStatusUpdater";
      LOG.error(errorMessage, e);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    // the isStopped check is for avoiding multiple unregistrations.
    synchronized(shutdownMonitor) {
      if (this.registeredWithRM && !this.isStopped
          && !isNMUnderSupervisionWithRecoveryEnabled()
          && !context.getDecommissioned() && !failedToConnect) {
        unRegisterNM();
      }
      // Interrupt the updater.
      this.isStopped = true;
      stopRMProxy();
      super.serviceStop();
    }
  }

  private boolean isNMUnderSupervisionWithRecoveryEnabled() {
    Configuration config = getConfig();
    return config.getBoolean(YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED)
        && config.getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
            YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
  }

  private void unRegisterNM() {
    RecordFactory recordFactory = RecordFactoryPBImpl.get();
    UnRegisterNodeManagerRequest request = recordFactory
        .newRecordInstance(UnRegisterNodeManagerRequest.class);
    request.setNodeId(this.nodeId);
    try {
      resourceTracker.unRegisterNodeManager(request);
      LOG.info("Successfully Unregistered the Node " + this.nodeId
          + " with ResourceManager.");
    } catch (Exception e) {
      LOG.warn("Unregistration of the Node " + this.nodeId + " failed.", e);
    }
  }

  protected void rebootNodeStatusUpdaterAndRegisterWithRM() {
    // Interrupt the updater.
    synchronized(shutdownMonitor) {
      if(this.isStopped) {
        LOG.info("Currently being shutdown. Aborting reboot");
        return;
      }
      this.isStopped = true;
      sendOutofBandHeartBeat();
      try {
        statusUpdater.join();
        registerWithRM();
        statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
        this.isStopped = false;
        statusUpdater.start();
        LOG.info("NodeStatusUpdater thread is reRegistered and restarted");
      } catch (Exception e) {
        String errorMessage = "Unexpected error rebooting NodeStatusUpdater";
        LOG.error(errorMessage, e);
        throw new YarnRuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  protected void stopRMProxy() {
    if(this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  @Private
  protected boolean isTokenKeepAliveEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && UserGroupInformation.isSecurityEnabled();
  }

  @VisibleForTesting
  protected ResourceTracker getRMClient() throws IOException {
    Configuration conf = getConfig();
    return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
  }

  private void updateConfiguredResourcesViaPlugins(
      Resource configuredResource) throws YarnException {
    ResourcePluginManager pluginManager = context.getResourcePluginManager();
    if (pluginManager != null && pluginManager.getNameToPlugins() != null) {
      // Update configured resource
      for (ResourcePlugin resourcePlugin : pluginManager.getNameToPlugins()
          .values()) {
        if (resourcePlugin.getNodeResourceHandlerInstance() != null) {
          resourcePlugin.getNodeResourceHandlerInstance()
              .updateConfiguredResource(configuredResource);
        }
      }
    }
  }

  @VisibleForTesting
  protected void registerWithRM()
      throws YarnException, IOException {
    RegisterNodeManagerResponse regNMResponse;
    Set<NodeLabel> nodeLabels = nodeLabelsHandler.getNodeLabelsForRegistration();
    Set<NodeAttribute> nodeAttributes =
        nodeAttributesHandler.getNodeAttributesForRegistration();

    // Synchronize NM-RM registration with
    // ContainerManagerImpl#increaseContainersResource and
    // ContainerManagerImpl#startContainers to avoid race condition
    // during RM recovery
    synchronized (this.context) {
      List<NMContainerStatus> containerReports = getNMContainerStatuses();
      NodeStatus nodeStatus = getNodeStatus(0);
      RegisterNodeManagerRequest request =
          RegisterNodeManagerRequest.newInstance(nodeId, httpPort, totalResource,
              nodeManagerVersionId, containerReports, getRunningApplications(),
              nodeLabels, physicalResource, nodeAttributes, nodeStatus);

      if (containerReports != null && !containerReports.isEmpty()) {
        LOG.info("Registering with RM using containers :" + containerReports);
      }
      if (logAggregationEnabled) {
        // pull log aggregation status for application running in this NM
        List<LogAggregationReport> logAggregationReports =
            context.getNMLogAggregationStatusTracker()
                .pullCachedLogAggregationReports();
        if (logAggregationReports != null
            && !logAggregationReports.isEmpty()) {
          LOG.debug("The cache log aggregation status size:{}",
              logAggregationReports.size());
          request.setLogAggregationReportsForApps(logAggregationReports);
        }
      }
      regNMResponse =
          resourceTracker.registerNodeManager(request);
      // Make sure rmIdentifier is set before we release the lock
      this.rmIdentifier = regNMResponse.getRMIdentifier();
    }

    // if the Resource Manager instructs NM to shutdown.
    if (NodeAction.SHUTDOWN.equals(regNMResponse.getNodeAction())) {
      String message =
          "Message from ResourceManager: "
              + regNMResponse.getDiagnosticsMessage();
      throw new YarnRuntimeException(
        "Received SHUTDOWN signal from Resourcemanager, Registration of NodeManager failed, "
            + message);
    }

    // if ResourceManager version is too old then shutdown
    if (!minimumResourceManagerVersion.equals("NONE")){
      if (minimumResourceManagerVersion.equals("EqualToNM")){
        minimumResourceManagerVersion = nodeManagerVersionId;
      }
      String rmVersion = regNMResponse.getRMVersion();
      if (rmVersion == null) {
        String message = "The Resource Manager's did not return a version. "
            + "Valid version cannot be checked.";
        throw new YarnRuntimeException("Shutting down the Node Manager. "
            + message);
      }
      if (VersionUtil.compareVersions(rmVersion,minimumResourceManagerVersion) < 0) {
        String message = "The Resource Manager's version ("
            + rmVersion +") is less than the minimum "
            + "allowed version " + minimumResourceManagerVersion;
        throw new YarnRuntimeException("Shutting down the Node Manager on RM "
            + "version error, " + message);
      }
    }
    this.registeredWithRM = true;
    MasterKey masterKey = regNMResponse.getContainerTokenMasterKey();
    // do this now so that its set before we start heartbeating to RM
    // It is expected that status updater is started by this point and
    // RM gives the shared secret in registration during
    // StatusUpdater#start().
    if (masterKey != null) {
      this.context.getContainerTokenSecretManager().setMasterKey(masterKey);
    }

    masterKey = regNMResponse.getNMTokenMasterKey();
    if (masterKey != null) {
      this.context.getNMTokenSecretManager().setMasterKey(masterKey);
    }

    StringBuilder successfullRegistrationMsg = new StringBuilder();
    successfullRegistrationMsg.append("Registered with ResourceManager as ")
        .append(this.nodeId);

    Resource newResource = regNMResponse.getResource();
    if (newResource != null) {
      updateNMResource(newResource);
      successfullRegistrationMsg.append(" with updated total resource of ")
          .append(this.totalResource);
    } else {
      successfullRegistrationMsg.append(" with total resource of ")
          .append(this.totalResource);
    }

    successfullRegistrationMsg.append(nodeLabelsHandler
        .verifyRMRegistrationResponseForNodeLabels(regNMResponse));
    successfullRegistrationMsg.append(nodeAttributesHandler
        .verifyRMRegistrationResponseForNodeAttributes(regNMResponse));

    LOG.info(successfullRegistrationMsg.toString());
  }

  private List<ApplicationId> createKeepAliveApplicationList() {
    if (!tokenKeepAliveEnabled) {
      return Collections.emptyList();
    }

    List<ApplicationId> appList = new ArrayList<ApplicationId>();
    for (Iterator<Entry<ApplicationId, Long>> i =
        this.appTokenKeepAliveMap.entrySet().iterator(); i.hasNext();) {
      Entry<ApplicationId, Long> e = i.next();
      ApplicationId appId = e.getKey();
      Long nextKeepAlive = e.getValue();
      if (!this.context.getApplications().containsKey(appId)) {
        // Remove if the application has finished.
        i.remove();
      } else if (System.currentTimeMillis() > nextKeepAlive) {
        // KeepAlive list for the next hearbeat.
        appList.add(appId);
        trackAppForKeepAlive(appId);
      }
    }
    return appList;
  }

  @VisibleForTesting
  protected NodeStatus getNodeStatus(int responseId) throws IOException {

    NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
    nodeHealthStatus.setHealthReport(healthChecker.getHealthReport());
    nodeHealthStatus.setIsNodeHealthy(healthChecker.isHealthy());
    nodeHealthStatus.setLastHealthReportTime(healthChecker
      .getLastHealthReportTime());
    LOG.debug("Node's health-status : {}, {}",
        nodeHealthStatus.getIsNodeHealthy(),
        nodeHealthStatus.getHealthReport());
    List<ContainerStatus> containersStatuses = getContainerStatuses();
    ResourceUtilization containersUtilization = getContainersUtilization();
    ResourceUtilization nodeUtilization = getNodeUtilization();
    List<org.apache.hadoop.yarn.api.records.Container> increasedContainers
        = getIncreasedContainers();
    NodeStatus nodeStatus =
        NodeStatus.newInstance(nodeId, responseId, containersStatuses,
          createKeepAliveApplicationList(), nodeHealthStatus,
          containersUtilization, nodeUtilization, increasedContainers);

    nodeStatus.setOpportunisticContainersStatus(
        getOpportunisticContainersStatus());
    return nodeStatus;
  }

  /**
   * Get the status of the OPPORTUNISTIC containers.
   * @return the status of the OPPORTUNISTIC containers.
   */
  private OpportunisticContainersStatus getOpportunisticContainersStatus() {
    OpportunisticContainersStatus status =
        this.context.getContainerManager().getOpportunisticContainersStatus();
    return status;
  }

  /**
   * Get the aggregated utilization of the containers in this node.
   * @return Resource utilization of all the containers.
   */
  private ResourceUtilization getContainersUtilization() {
    ContainersMonitor containersMonitor =
        this.context.getContainerManager().getContainersMonitor();
    return containersMonitor.getContainersUtilization();
  }

  /**
   * Get the utilization of the node. This includes the containers.
   * @return Resource utilization of the node.
   */
  private ResourceUtilization getNodeUtilization() {
    NodeResourceMonitorImpl nodeResourceMonitor =
        (NodeResourceMonitorImpl) this.context.getNodeResourceMonitor();
    return nodeResourceMonitor.getUtilization();
  }

  /* Get the containers whose resource has been increased since last
   * NM-RM heartbeat.
   */
  private List<org.apache.hadoop.yarn.api.records.Container>
      getIncreasedContainers() {
    List<org.apache.hadoop.yarn.api.records.Container>
        increasedContainers = new ArrayList<>(
            this.context.getIncreasedContainers().values());
    for (org.apache.hadoop.yarn.api.records.Container
        container : increasedContainers) {
      this.context.getIncreasedContainers().remove(container.getId());
    }
    return increasedContainers;
  }

  // Update NM's Resource.
  private void updateNMResource(Resource resource) {
    metrics.addResource(Resources.subtract(resource, totalResource));
    this.totalResource = resource;

    // Update the containers monitor
    ContainersMonitor containersMonitor =
        this.context.getContainerManager().getContainersMonitor();
    containersMonitor.setAllocatedResourcesForContainers(totalResource);
  }

  // Iterate through the NMContext and clone and get all the containers'
  // statuses. If it's a completed container, add into the
  // recentlyStoppedContainers collections.
  @VisibleForTesting
  protected List<ContainerStatus> getContainerStatuses() throws IOException {
    List<ContainerStatus> containerStatuses = new ArrayList<ContainerStatus>();
    for (Container container : this.context.getContainers().values()) {
      ContainerId containerId = container.getContainerId();
      ApplicationId applicationId = containerId.getApplicationAttemptId()
          .getApplicationId();
      org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus =
          container.cloneAndGetContainerStatus();
      if (containerStatus.getState() == ContainerState.COMPLETE) {
        if (isApplicationStopped(applicationId)) {
          LOG.debug("{} is completing, remove {} from NM context.",
              applicationId, containerId);
          context.getContainers().remove(containerId);
          pendingCompletedContainers.put(containerId, containerStatus);
        } else {
          if (!isContainerRecentlyStopped(containerId)) {
            pendingCompletedContainers.put(containerId, containerStatus);
          }
        }
        // Adding to finished containers cache. Cache will keep it around at
        // least for #durationToTrackStoppedContainers duration. In the
        // subsequent call to stop container it will get removed from cache.
        addCompletedContainer(containerId);
      } else {
        containerStatuses.add(containerStatus);
      }
    }

    containerStatuses.addAll(pendingCompletedContainers.values());
    if (!containerStatuses.isEmpty()) {
      LOG.debug("Sending out {} container statuses: {}",
          containerStatuses.size(), containerStatuses);
    }

    return containerStatuses;
  }

  private List<ApplicationId> getRunningApplications() {
    List<ApplicationId> runningApplications = new ArrayList<ApplicationId>();
    for (Entry<ApplicationId, Application> appEntry : this.context
        .getApplications().entrySet()) {
      if (ApplicationState.FINISHED != appEntry.getValue()
          .getApplicationState()) {
        runningApplications.add(appEntry.getKey());
      }
    }
    return runningApplications;
  }

  // These NMContainerStatus are sent on NM registration and used by YARN only.
  private List<NMContainerStatus> getNMContainerStatuses() throws IOException {
    List<NMContainerStatus> containerStatuses =
        new ArrayList<NMContainerStatus>();
    for (Container container : this.context.getContainers().values()) {
      ContainerId containerId = container.getContainerId();
      ApplicationId applicationId = containerId.getApplicationAttemptId()
          .getApplicationId();
      if (!this.context.getApplications().containsKey(applicationId)) {
        context.getContainers().remove(containerId);
        continue;
      }
      NMContainerStatus status =
          container.getNMContainerStatus();
      containerStatuses.add(status);
      if (status.getContainerState() == ContainerState.COMPLETE) {
        // Adding to finished containers cache. Cache will keep it around at
        // least for #durationToTrackStoppedContainers duration. In the
        // subsequent call to stop container it will get removed from cache.
        addCompletedContainer(containerId);
      }
    }
    if (!containerStatuses.isEmpty()) {
      LOG.info("Sending out " + containerStatuses.size()
          + " NM container statuses: " + containerStatuses);
    }
    return containerStatuses;
  }

  private boolean isApplicationStopped(ApplicationId applicationId) {
    if (!this.context.getApplications().containsKey(applicationId)) {
      return true;
    }

    ApplicationState applicationState = this.context.getApplications().get(
        applicationId).getApplicationState();
    if (applicationState == ApplicationState.FINISHING_CONTAINERS_WAIT
        || applicationState == ApplicationState.APPLICATION_RESOURCES_CLEANINGUP
        || applicationState == ApplicationState.FINISHED) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void addCompletedContainer(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      removeVeryOldStoppedContainersFromCache();
      if (!recentlyStoppedContainers.containsKey(containerId)) {
        recentlyStoppedContainers.put(containerId,
            System.currentTimeMillis() + durationToTrackStoppedContainers);
      }
    }
  }

  @VisibleForTesting
  @Private
  public void removeOrTrackCompletedContainersFromContext(
      List<ContainerId> containerIds) {
    Set<ContainerId> removedContainers = new HashSet<ContainerId>();

    pendingContainersToRemove.addAll(containerIds);
    Iterator<ContainerId> iter = pendingContainersToRemove.iterator();
    while (iter.hasNext()) {
      ContainerId containerId = iter.next();
      // remove the container only if the container is at DONE state
      Container nmContainer = context.getContainers().get(containerId);
      if (nmContainer == null) {
        iter.remove();
      } else if (nmContainer.getContainerState().equals(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE)) {
        context.getContainers().remove(containerId);
        removedContainers.add(containerId);
        iter.remove();
      }
      pendingCompletedContainers.remove(containerId);
    }

    if (!removedContainers.isEmpty()) {
      LOG.info("Removed completed containers from NM context: "
          + removedContainers);
    }
  }

  private void trackAppsForKeepAlive(List<ApplicationId> appIds) {
    if (tokenKeepAliveEnabled && appIds != null && appIds.size() > 0) {
      for (ApplicationId appId : appIds) {
        trackAppForKeepAlive(appId);
      }
    }
  }

  private void trackAppForKeepAlive(ApplicationId appId) {
    // Next keepAlive request for app between 0.7 & 0.9 of when the token will
    // likely expire.
    long nextTime = System.currentTimeMillis()
    + (long) (0.7 * tokenRemovalDelayMs + (0.2 * tokenRemovalDelayMs
        * keepAliveDelayRandom.nextInt(100))/100);
    appTokenKeepAliveMap.put(appId, nextTime);
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  @VisibleForTesting
  Thread.State getStatusUpdaterThreadState() {
    return statusUpdater.getState();
  }

  public boolean isContainerRecentlyStopped(ContainerId containerId) {
    synchronized (recentlyStoppedContainers) {
      return recentlyStoppedContainers.containsKey(containerId);
    }
  }

  @Override
  public void clearFinishedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      recentlyStoppedContainers.clear();
    }
  }

  @Private
  @VisibleForTesting
  public void removeVeryOldStoppedContainersFromCache() {
    synchronized (recentlyStoppedContainers) {
      long currentTime = System.currentTimeMillis();
      Iterator<Entry<ContainerId, Long>> i =
          recentlyStoppedContainers.entrySet().iterator();
      while (i.hasNext()) {
        Entry<ContainerId, Long> mapEntry = i.next();
        ContainerId cid = mapEntry.getKey();
        if (mapEntry.getValue() >= currentTime) {
          break;
        }
        if (!context.getContainers().containsKey(cid)) {
          ApplicationId appId =
              cid.getApplicationAttemptId().getApplicationId();
          if (isApplicationStopped(appId)) {
            i.remove();
            try {
              context.getNMStateStore().removeContainer(cid);
            } catch (IOException e) {
              LOG.error("Unable to remove container " + cid + " in store", e);
            }
          }
        }
      }
    }
  }

  @Override
  public long getRMIdentifier() {
    return this.rmIdentifier;
  }

  private static Map<ApplicationId, Credentials> parseCredentials(
      Map<ApplicationId, ByteBuffer> systemCredentials) throws IOException {
    Map<ApplicationId, Credentials> map =
        new HashMap<ApplicationId, Credentials>();
    for (Map.Entry<ApplicationId, ByteBuffer> entry : systemCredentials.entrySet()) {
      Credentials credentials = new Credentials();
      DataInputByteBuffer buf = new DataInputByteBuffer();
      ByteBuffer buffer = entry.getValue();
      buffer.rewind();
      buf.reset(buffer);
      credentials.readTokenStorageStream(buf);
      map.put(entry.getKey(), credentials);
    }
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<ApplicationId, Credentials> entry : map.entrySet()) {
        LOG.debug("Retrieved credentials form RM for {}: {}",
            entry.getKey(), entry.getValue().getAllTokens());
      }
    }
    return map;
  }

  protected void startStatusUpdater() {
    statusUpdaterRunnable = new StatusUpdaterRunnable();
    statusUpdater =
        new Thread(statusUpdaterRunnable, "Node Status Updater");
    statusUpdater.start();
  }

  private boolean handleShutdownOrResyncCommand(
      NodeHeartbeatResponse response) {
    if (response.getNodeAction() == NodeAction.SHUTDOWN) {
      LOG.warn("Received SHUTDOWN signal from Resourcemanager as part of"
          + " heartbeat, hence shutting down.");
      LOG.warn("Message from ResourceManager: "
          + response.getDiagnosticsMessage());
      context.setDecommissioned(true);
      dispatcher.getEventHandler().handle(
          new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
      return true;
    }
    if (response.getNodeAction() == NodeAction.RESYNC) {
      LOG.warn("Node is out of sync with ResourceManager,"
          + " hence resyncing.");
      LOG.warn("Message from ResourceManager: "
          + response.getDiagnosticsMessage());
      // Invalidate the RMIdentifier while resync
      NodeStatusUpdaterImpl.this.rmIdentifier =
          ResourceManagerConstants.RM_INVALID_IDENTIFIER;
      dispatcher.getEventHandler().handle(
          new NodeManagerEvent(NodeManagerEventType.RESYNC));
      pendingCompletedContainers.clear();
      return true;
    }
    return false;
  }

  @Override
  public void reportException(Exception ex) {
    healthChecker.reportException(ex);
    sendOutofBandHeartBeat();
  }

  private List<LogAggregationReport> getLogAggregationReportsForApps(
      ConcurrentLinkedQueue<LogAggregationReport> lastestLogAggregationStatus) {
    LogAggregationReport status;
    while ((status = lastestLogAggregationStatus.poll()) != null) {
      this.logAggregationReportForAppsTempList.add(status);
    }
    List<LogAggregationReport> reports = new ArrayList<LogAggregationReport>();
    reports.addAll(logAggregationReportForAppsTempList);
    return reports;
  }

  private NMNodeLabelsHandler createNMNodeLabelsHandler(
      NodeLabelsProvider nodeLabelsProvider) {
    if (nodeLabelsProvider == null) {
      return new NMCentralizedNodeLabelsHandler();
    } else {
      return new NMDistributedNodeLabelsHandler(nodeLabelsProvider,
          this.getConfig());
    }
  }

  /**
   * Returns a handler based on the configured node attributes provider.
   * returns null if no provider is configured.
   * @param provider
   * @return attributes handler
   */
  private NMNodeAttributesHandler createNMNodeAttributesHandler(
      NodeAttributesProvider provider) {
    if (provider == null) {
      return new NMCentralizedNodeAttributesHandler();
    } else {
      return new NMDistributedNodeAttributesHandler(provider, this.getConfig());
    }
  }

  private static abstract class CachedNodeDescriptorHandler<T> {
    private final long resyncInterval;
    private final T defaultValue;
    private T previousValue;
    private long lastSendMills = 0L;
    private boolean isValueSented;

    CachedNodeDescriptorHandler(T defaultValue,
        long resyncInterval) {
      this.defaultValue = defaultValue;
      this.resyncInterval = resyncInterval;
    }

    public abstract T getValueFromProvider();

    public T getValueForRegistration() {
      T value = getValueFromProvider();
      if (defaultValue != null) {
        value = (null == value) ? defaultValue : value;
      }
      previousValue = value;
      try {
        validate(value);
      } catch (IOException e) {
        value = null;
      }
      return value;
    }

    public T getValueForHeartbeat() {
      T value = getValueFromProvider();
      // if the provider returns null then consider default value are set
      if (defaultValue != null) {
        value = (null == value) ? defaultValue : value;
      }
      // take some action only on modification of value
      boolean isValueUpdated = isValueUpdated(value);

      isValueSented = false;
      // When value updated or resync time is elapsed will send again in
      // heartbeat.
      if (isValueUpdated || isResyncIntervalElapsed()) {
        previousValue = value;
        try {
          validate(value);
          isValueSented = true;
        } catch (IOException e) {
          // take previous value to replace invalid value, so that invalid
          // value are not verified for every HB, and send empty set
          // to RM to have same value which was earlier set.
          value = null;
        } finally {
          // Set last send time in heartbeat
          lastSendMills = System.currentTimeMillis();
        }
      } else {
        // if value have not changed then no need to send
        value = null;
      }
      return value;
    }

    /**
     * This method checks resync interval is elapsed or not.
     */
    public boolean isResyncIntervalElapsed() {
      long elapsedTimeSinceLastSync =
          System.currentTimeMillis() - lastSendMills;
      if (elapsedTimeSinceLastSync > resyncInterval) {
        return true;
      }
      return false;
    }

    protected abstract void validate(T value) throws IOException;

    protected abstract boolean isValueUpdated(T value);

    public long getResyncInterval() {
      return resyncInterval;
    }

    public T getDefaultValue() {
      return defaultValue;
    }

    public T getPreviousValue() {
      return previousValue;
    }

    public long getLastSendMills() {
      return lastSendMills;
    }

    public boolean isValueSented() {
      return isValueSented;
    }
  }

  private interface NMNodeAttributesHandler {

    /**
     * validates nodeAttributes From Provider and returns it to the caller. Also
     * ensures that if provider returns null then empty set is considered
     */
    Set<NodeAttribute> getNodeAttributesForRegistration();

    /**
     * @return the node attributes of this node manager.
     */
    Set<NodeAttribute> getNodeAttributesForHeartbeat();

    /**
     * @return RMRegistration Success message and on failure will log
     *         independently and returns empty string
     */
    String verifyRMRegistrationResponseForNodeAttributes(
        RegisterNodeManagerResponse regNMResponse);

    /**
     * check whether if updated attributes sent to RM was accepted or not.
     * @param response
     */
    void verifyRMHeartbeatResponseForNodeAttributes(
        NodeHeartbeatResponse response);
  }


  /**
   * In centralized configuration, NM need not send Node attributes or process
   * the response.
   */
  private static class NMCentralizedNodeAttributesHandler
      implements NMNodeAttributesHandler {
    @Override
    public Set<NodeAttribute> getNodeAttributesForHeartbeat() {
      return null;
    }

    @Override
    public Set<NodeAttribute> getNodeAttributesForRegistration() {
      return null;
    }

    @Override
    public void verifyRMHeartbeatResponseForNodeAttributes(
        NodeHeartbeatResponse response) {
    }

    @Override
    public String verifyRMRegistrationResponseForNodeAttributes(
        RegisterNodeManagerResponse regNMResponse) {
      return "";
    }
  }

  private static class NMDistributedNodeAttributesHandler
      extends CachedNodeDescriptorHandler<Set<NodeAttribute>>
      implements NMNodeAttributesHandler {

    private final NodeAttributesProvider attributesProvider;

    protected NMDistributedNodeAttributesHandler(
        NodeAttributesProvider provider, Configuration conf) {
      super(Collections.unmodifiableSet(new HashSet<>(0)),
          conf.getLong(YarnConfiguration.NM_NODE_ATTRIBUTES_RESYNC_INTERVAL,
              YarnConfiguration.DEFAULT_NM_NODE_ATTRIBUTES_RESYNC_INTERVAL));
      this.attributesProvider = provider;
    }

    @Override
    public Set<NodeAttribute> getNodeAttributesForRegistration() {
      return getValueForRegistration();
    }

    @Override
    public Set<NodeAttribute> getNodeAttributesForHeartbeat() {
      return getValueForHeartbeat();
    }

    @Override
    public Set<NodeAttribute> getValueFromProvider() {
      return attributesProvider.getDescriptors();
    }

    @Override
    protected void validate(Set<NodeAttribute> nodeAttributes)
        throws IOException {
      try {
        NodeLabelUtil.validateNodeAttributes(nodeAttributes);
      } catch (IOException e) {
        LOG.error(
            "Invalid node attribute(s) from Provider : " + e.getMessage());
        throw e;
      }
    }

    @Override
    protected boolean isValueUpdated(Set<NodeAttribute> value) {
      return !NodeLabelUtil.isNodeAttributesEquals(getPreviousValue(), value);
    }

    @Override
    public String verifyRMRegistrationResponseForNodeAttributes(
        RegisterNodeManagerResponse regNMResponse) {
      StringBuilder successfulNodeAttributesRegistrationMsg =
          new StringBuilder();
      if (regNMResponse.getAreNodeAttributesAcceptedByRM()) {
        successfulNodeAttributesRegistrationMsg
            .append(" and with following Node attribute(s) : {")
            .append(getPreviousValue()).append("}");
      } else {
        // case where provider is set but RM did not accept the node attributes
        String errorMsgFromRM = regNMResponse.getDiagnosticsMessage();
        LOG.error("Node attributes sent from NM while registration were"
            + " rejected by RM. " + ((errorMsgFromRM == null) ?
            "Seems like RM is configured with Centralized Attributes." :
            "And with message " + regNMResponse.getDiagnosticsMessage()));
      }
      return successfulNodeAttributesRegistrationMsg.toString();
    }

    @Override
    public void verifyRMHeartbeatResponseForNodeAttributes(
        NodeHeartbeatResponse response) {
      if (isValueSented()) {
        if (response.getAreNodeAttributesAcceptedByRM()) {
          LOG.debug("Node attributes {{}} were Accepted by RM ",
              getPreviousValue());
        } else {
          // case where updated node attributes from NodeAttributesProvider
          // is sent to RM and RM rejected the attributes
          LOG.error("NM node attributes {" + getPreviousValue()
              + "} were not accepted by RM and message from RM : " + response
              .getDiagnosticsMessage());
        }
      }
    }
  }

  private static interface NMNodeLabelsHandler {
    /**
     * validates nodeLabels From Provider and returns it to the caller. Also
     * ensures that if provider returns null then empty label set is considered
     */
    Set<NodeLabel> getNodeLabelsForRegistration();

    /**
     * @return RMRegistration Success message and on failure will log
     *         independently and returns empty string
     */
    String verifyRMRegistrationResponseForNodeLabels(
        RegisterNodeManagerResponse regNMResponse);

    /**
     * If nodeLabels From Provider is different previous node labels then it
     * will check the syntax correctness and throws exception if invalid. If
     * valid, returns nodeLabels From Provider. Also ensures that if provider
     * returns null then empty label set is considered and If labels are not
     * modified it returns null.
     */
    Set<NodeLabel> getNodeLabelsForHeartbeat();

    /**
     * check whether if updated labels sent to RM was accepted or not
     * @param response
     */
    void verifyRMHeartbeatResponseForNodeLabels(NodeHeartbeatResponse response);
  }

  /**
   * In centralized configuration, NM need not send Node labels or process the
   * response
   */
  private static class NMCentralizedNodeLabelsHandler
      implements NMNodeLabelsHandler {
    @Override
    public Set<NodeLabel> getNodeLabelsForHeartbeat() {
      return null;
    }

    @Override
    public Set<NodeLabel> getNodeLabelsForRegistration() {
      return null;
    }

    @Override
    public void verifyRMHeartbeatResponseForNodeLabels(
        NodeHeartbeatResponse response) {
    }

    @Override
    public String verifyRMRegistrationResponseForNodeLabels(
        RegisterNodeManagerResponse regNMResponse) {
      return "";
    }
  }

  private static class NMDistributedNodeLabelsHandler
      extends CachedNodeDescriptorHandler<Set<NodeLabel>>
      implements NMNodeLabelsHandler {

    private NMDistributedNodeLabelsHandler(
        NodeLabelsProvider nodeLabelsProvider, Configuration conf) {
      super(CommonNodeLabelsManager.EMPTY_NODELABEL_SET,
          conf.getLong(YarnConfiguration.NM_NODE_LABELS_RESYNC_INTERVAL,
              YarnConfiguration.DEFAULT_NM_NODE_LABELS_RESYNC_INTERVAL));
      this.nodeLabelsProvider = nodeLabelsProvider;
    }

    private final NodeLabelsProvider nodeLabelsProvider;

    @Override
    public Set<NodeLabel> getNodeLabelsForRegistration() {
      return getValueForRegistration();
    }

    @Override
    public String verifyRMRegistrationResponseForNodeLabels(
        RegisterNodeManagerResponse regNMResponse) {
      StringBuilder successfulNodeLabelsRegistrationMsg = new StringBuilder("");
      if (regNMResponse.getAreNodeLabelsAcceptedByRM()) {
        successfulNodeLabelsRegistrationMsg
            .append(" and with following Node label(s) : {")
            .append(StringUtils.join(",", getPreviousValue())).append("}");
      } else {
        // case where provider is set but RM did not accept the Node Labels
        String errorMsgFromRM = regNMResponse.getDiagnosticsMessage();
        LOG.error(
            "NodeLabels sent from NM while registration were rejected by RM. "
            + ((errorMsgFromRM == null)
                ? "Seems like RM is configured with Centralized Labels."
                : "And with message " + regNMResponse.getDiagnosticsMessage()));
      }
      return successfulNodeLabelsRegistrationMsg.toString();
    }

    @Override
    public Set<NodeLabel> getNodeLabelsForHeartbeat() {
      return getValueForHeartbeat();
    }

    protected void validate(Set<NodeLabel> nodeLabels)
        throws IOException {
      Iterator<NodeLabel> iterator = nodeLabels.iterator();
      boolean hasInvalidLabel = false;
      StringBuilder errorMsg = new StringBuilder();
      while (iterator.hasNext()) {
        try {
          NodeLabelUtil
              .checkAndThrowLabelName(iterator.next().getName());
        } catch (IOException e) {
          errorMsg.append(e.getMessage());
          errorMsg.append(" , ");
          hasInvalidLabel = true;
        }
      }
      if (hasInvalidLabel) {
        LOG.error("Invalid Node Label(s) from Provider : " + errorMsg);
        throw new IOException(errorMsg.toString());
      }
    }

    @Override
    public Set<NodeLabel> getValueFromProvider() {
      return this.nodeLabelsProvider.getDescriptors();
    }

    @Override
    protected boolean isValueUpdated(Set<NodeLabel> value) {
      return !Objects.equals(value, getPreviousValue());
    }

    @Override
    public void verifyRMHeartbeatResponseForNodeLabels(
        NodeHeartbeatResponse response) {
      if (isValueSented()) {
        if (response.getAreNodeLabelsAcceptedByRM()) {
          LOG.debug("Node Labels {{}} were Accepted by RM",
              StringUtils.join(",", getPreviousValue()));
        } else {
          // case where updated labels from NodeLabelsProvider is sent to RM and
          // RM rejected the labels
          LOG.error(
              "NM node labels {" + StringUtils.join(",", getPreviousValue())
                  + "} were not accepted by RM and message from RM : "
                  + response.getDiagnosticsMessage());
        }
      }
    }
  }

  private class StatusUpdaterRunnable implements Runnable {
    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      int lastHeartbeatID = 0;
      boolean missedHearbeat = false;
      while (!isStopped) {
        // Send heartbeat
        try {
          NodeHeartbeatResponse response = null;
          Set<NodeLabel> nodeLabelsForHeartbeat =
              nodeLabelsHandler.getNodeLabelsForHeartbeat();
          Set<NodeAttribute> nodeAttributesForHeartbeat =
                  nodeAttributesHandler.getNodeAttributesForHeartbeat();
          NodeStatus nodeStatus = getNodeStatus(lastHeartbeatID);
          NodeHeartbeatRequest request =
              NodeHeartbeatRequest.newInstance(nodeStatus,
                  NodeStatusUpdaterImpl.this.context
                      .getContainerTokenSecretManager().getCurrentKey(),
                  NodeStatusUpdaterImpl.this.context
                      .getNMTokenSecretManager().getCurrentKey(),
                  nodeLabelsForHeartbeat,
                  nodeAttributesForHeartbeat,
                  NodeStatusUpdaterImpl.this.context
                      .getRegisteringCollectors());

          if (logAggregationEnabled) {
            // pull log aggregation status for application running in this NM
            List<LogAggregationReport> logAggregationReports =
                getLogAggregationReportsForApps(context
                    .getLogAggregationStatusForApps());
            if (logAggregationReports != null
                && !logAggregationReports.isEmpty()) {
              request.setLogAggregationReportsForApps(logAggregationReports);
            }
          }

          request.setTokenSequenceNo(
              NodeStatusUpdaterImpl.this.tokenSequenceNo);
          response = resourceTracker.nodeHeartbeat(request);
          //get next heartbeat interval from response
          nextHeartBeatInterval = response.getNextHeartBeatInterval();
          updateMasterKeys(response);

          if (!handleShutdownOrResyncCommand(response)) {
            nodeLabelsHandler.verifyRMHeartbeatResponseForNodeLabels(
                response);
            nodeAttributesHandler
                .verifyRMHeartbeatResponseForNodeAttributes(response);

            // Explicitly put this method after checking the resync
            // response. We
            // don't want to remove the completed containers before resync
            // because these completed containers will be reported back to RM
            // when NM re-registers with RM.
            // Only remove the cleanedup containers that are acked
            removeOrTrackCompletedContainersFromContext(response
                .getContainersToBeRemovedFromNM());

            // If the last heartbeat was missed, it is possible that the
            // RM saw this one as a duplicate and did not process it.
            // If so, we can fail to notify the RM of these completed containers
            // on the next heartbeat if we clear pendingCompletedContainers.
            // If it wasn't a duplicate, the only impact is we might notify
            // the RM twice, which it can handle.
            if (!missedHearbeat) {
              pendingCompletedContainers.clear();
            } else {
              LOG.info("skipped clearing pending completed containers due to " +
                  "missed heartbeat");
              missedHearbeat = false;
            }

            logAggregationReportForAppsTempList.clear();
            lastHeartbeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanup();
            if (!containersToCleanup.isEmpty()) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup,
                      CMgrCompletedContainersEvent.Reason
                          .BY_RESOURCEMANAGER));
            }
            List<ApplicationId> appsToCleanup =
                response.getApplicationsToCleanup();
            //Only start tracking for keepAlive on FINISH_APP
            trackAppsForKeepAlive(appsToCleanup);
            if (!appsToCleanup.isEmpty()) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup,
                      CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
            }
            Map<ApplicationId, ByteBuffer> systemCredentials =
                YarnServerBuilderUtils.convertFromProtoFormat(
                    response.getSystemCredentialsForApps());
            if (systemCredentials != null && !systemCredentials.isEmpty()) {
              ((NMContext) context).setSystemCrendentialsForApps(
                  parseCredentials(systemCredentials));
              context.getContainerManager().handleCredentialUpdate();
            }
            List<org.apache.hadoop.yarn.api.records.Container>
                containersToUpdate = response.getContainersToUpdate();
            if (!containersToUpdate.isEmpty()) {
              dispatcher.getEventHandler().handle(
                  new CMgrUpdateContainersEvent(containersToUpdate));
            }

            // SignalContainer request originally comes from end users via
            // ClientRMProtocol's SignalContainer. Forward the request to
            // ContainerManager which will dispatch the event to
            // ContainerLauncher.
            List<SignalContainerRequest> containersToSignal = response
                .getContainersToSignalList();
            if (!containersToSignal.isEmpty()) {
              dispatcher.getEventHandler().handle(
                  new CMgrSignalContainersEvent(containersToSignal));
            }

            // Update QueuingLimits if ContainerManager supports queuing
            ContainerQueuingLimit queuingLimit =
                response.getContainerQueuingLimit();
            if (queuingLimit != null) {
              context.getContainerManager().updateQueuingLimit(queuingLimit);
            }
          }
          // Handling node resource update case.
          Resource newResource = response.getResource();
          if (newResource != null) {
            updateNMResource(newResource);
            LOG.debug("Node's resource is updated to {}", newResource);
            if (!totalResource.equals(newResource)) {
              LOG.info("Node's resource is updated to {}", newResource);
            }
          }
          if (timelineServiceV2Enabled) {
            updateTimelineCollectorData(response);
          }

          NodeStatusUpdaterImpl.this.tokenSequenceNo =
              response.getTokenSequenceNo();
        } catch (ConnectException e) {
          //catch and throw the exception if tried MAX wait time to connect RM
          dispatcher.getEventHandler().handle(
              new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
          // failed to connect to RM.
          failedToConnect = true;
          throw new YarnRuntimeException(e);
        } catch (Exception e) {

          // TODO Better error handling. Thread can die with the rest of the
          // NM still running.
          LOG.error("Caught exception in status-updater", e);
          missedHearbeat = true;
        } finally {
          synchronized (heartbeatMonitor) {
            nextHeartBeatInterval = nextHeartBeatInterval <= 0 ?
                YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS :
                nextHeartBeatInterval;
            try {
              heartbeatMonitor.wait(nextHeartBeatInterval);
            } catch (InterruptedException e) {
              // Do Nothing
            }
          }
        }
      }
    }

    private void updateTimelineCollectorData(
        NodeHeartbeatResponse response) {
      Map<ApplicationId, AppCollectorData> incomingCollectorsMap =
          response.getAppCollectors();
      if (incomingCollectorsMap == null) {
        LOG.debug("No collectors to update RM");
        return;
      }
      Map<ApplicationId, AppCollectorData> knownCollectors =
          context.getKnownCollectors();
      for (Map.Entry<ApplicationId, AppCollectorData> entry
          : incomingCollectorsMap.entrySet()) {
        ApplicationId appId = entry.getKey();
        AppCollectorData collectorData = entry.getValue();

        // Only handle applications running on local node.
        Application application = context.getApplications().get(appId);
        if (application != null) {
          // Update collector data if the newly received data happens after
          // the known data (updates the known data).
          AppCollectorData existingData = knownCollectors.get(appId);
          if (AppCollectorData.happensBefore(existingData, collectorData)) {
            LOG.debug("Sync a new collector address: {} for application: {}"
                + " from RM.", collectorData.getCollectorAddr(), appId);
            // Update information for clients.
            NMTimelinePublisher nmTimelinePublisher =
                context.getNMTimelinePublisher();
            if (nmTimelinePublisher != null) {
              nmTimelinePublisher.setTimelineServiceAddress(
                  application.getAppId(), collectorData.getCollectorAddr());
            }
            // Update information for the node manager itself.
            knownCollectors.put(appId, collectorData);
          }
        }
        // Remove the registering collector data
        context.getRegisteringCollectors().remove(entry.getKey());
      }
    }

    private void updateMasterKeys(NodeHeartbeatResponse response) {
      // See if the master-key has rolled over
      MasterKey updatedMasterKey = response.getContainerTokenMasterKey();
      if (updatedMasterKey != null) {
        // Will be non-null only on roll-over on RM side
        context.getContainerTokenSecretManager().setMasterKey(updatedMasterKey);
      }

      updatedMasterKey = response.getNMTokenMasterKey();
      if (updatedMasterKey != null) {
        context.getNMTokenSecretManager().setMasterKey(updatedMasterKey);
      }
    }
  }
}
