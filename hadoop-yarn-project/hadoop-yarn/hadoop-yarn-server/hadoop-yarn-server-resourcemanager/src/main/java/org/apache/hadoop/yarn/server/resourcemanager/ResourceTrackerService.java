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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections.CollectionUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;

public class ResourceTrackerService extends AbstractService implements
    ResourceTracker {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final RMContainerTokenSecretManager containerTokenSecretManager;
  private final NMTokenSecretManagerInRM nmTokenSecretManager;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private long nextHeartBeatInterval;
  private Server server;
  private InetSocketAddress resourceTrackerAddress;
  private String minimumNodeManagerVersion;

  private int minAllocMb;
  private int minAllocVcores;

  private DecommissioningNodesWatcher decommissioningWatcher;

  private boolean isDistributedNodeLabelsConf;
  private boolean isDelegatedCentralizedNodeLabelsConf;
  private DynamicResourceConfiguration drConf;

  private final AtomicLong timelineCollectorVersion = new AtomicLong(0);

  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmTokenSecretManager = nmTokenSecretManager;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.decommissioningWatcher = new DecommissioningNodesWatcher(rmContext);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    RackResolver.init(conf);
    nextHeartBeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (nextHeartBeatInterval <= 0) {
      throw new YarnRuntimeException("Invalid Configuration. "
          + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS
          + " should be larger than 0.");
    }

    minAllocMb = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    minAllocVcores = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);

    minimumNodeManagerVersion = conf.get(
        YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION);

    if (YarnConfiguration.areNodeLabelsEnabled(conf)) {
      isDistributedNodeLabelsConf =
          YarnConfiguration.isDistributedNodeLabelConfiguration(conf);
      isDelegatedCentralizedNodeLabelsConf =
          YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(conf);
    }

    loadDynamicResourceConfiguration(conf);
    decommissioningWatcher.init(conf);
    super.serviceInit(conf);
  }

  /**
   * Load DynamicResourceConfiguration from dynamic-resources.xml.
   * @param conf
   * @throws IOException
   */
  public void loadDynamicResourceConfiguration(Configuration conf)
      throws IOException {
    try {
      // load dynamic-resources.xml
      InputStream drInputStream = this.rmContext.getConfigurationProvider()
          .getConfigurationInputStream(conf,
          YarnConfiguration.DR_CONFIGURATION_FILE);
      // write lock here on drConfig is unnecessary as here get called at
      // ResourceTrackerService get initiated and other read and write
      // operations haven't started yet.
      if (drInputStream != null) {
        this.drConf = new DynamicResourceConfiguration(conf, drInputStream);
      } else {
        this.drConf = new DynamicResourceConfiguration(conf);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Update DynamicResourceConfiguration with new configuration.
   * @param conf
   */
  public void updateDynamicResourceConfiguration(
      DynamicResourceConfiguration conf) {
    this.writeLock.lock();
    try {
      this.drConf = conf;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = rpc.getServer(
        ResourceTracker.class, this, resourceTrackerAddress, conf, null,
        conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        server.getListenerAddress());
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  /**
   * Helper method to handle received ContainerStatus. If this corresponds to
   * the completion of a master-container of a managed AM,
   * we call the handler for RMAppAttemptContainerFinishedEvent.
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  void handleNMContainerStatus(NMContainerStatus containerStatus, NodeId nodeId) {
    ApplicationAttemptId appAttemptId =
        containerStatus.getContainerId().getApplicationAttemptId();
    RMApp rmApp =
        rmContext.getRMApps().get(appAttemptId.getApplicationId());
    if (rmApp == null) {
      LOG.error("Received finished container : "
          + containerStatus.getContainerId()
          + " for unknown application " + appAttemptId.getApplicationId()
          + " Skipping.");
      return;
    }

    if (rmApp.getApplicationSubmissionContext().getUnmanagedAM()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring container completion status for unmanaged AM "
            + rmApp.getApplicationId());
      }
      return;
    }

    RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
    if (rmAppAttempt == null) {
      LOG.info("Ignoring not found attempt " + appAttemptId);
      return;
    }

    Container masterContainer = rmAppAttempt.getMasterContainer();
    if (masterContainer.getId().equals(containerStatus.getContainerId())
        && containerStatus.getContainerState() == ContainerState.COMPLETE) {
      ContainerStatus status =
          ContainerStatus.newInstance(containerStatus.getContainerId(),
            containerStatus.getContainerState(), containerStatus.getDiagnostics(),
            containerStatus.getContainerExitStatus());
      // sending master container finished event.
      RMAppAttemptContainerFinishedEvent evt =
          new RMAppAttemptContainerFinishedEvent(appAttemptId, status,
              nodeId);
      rmContext.getDispatcher().getEventHandler().handle(evt);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException,
      IOException {
    NodeId nodeId = request.getNodeId();
    String host = nodeId.getHost();
    int cmPort = nodeId.getPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();
    String nodeManagerVersion = request.getNMVersion();
    Resource physicalResource = request.getPhysicalResource();

    RegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(RegisterNodeManagerResponse.class);

    if (!minimumNodeManagerVersion.equals("NONE")) {
      if (minimumNodeManagerVersion.equals("EqualToRM")) {
        minimumNodeManagerVersion = YarnVersionInfo.getVersion();
      }

      if ((nodeManagerVersion == null) ||
          (VersionUtil.compareVersions(nodeManagerVersion,minimumNodeManagerVersion)) < 0) {
        String message =
            "Disallowed NodeManager Version " + nodeManagerVersion
                + ", is less than the minimum version "
                + minimumNodeManagerVersion + " sending SHUTDOWN signal to "
                + "NodeManager.";
        LOG.info(message);
        response.setDiagnosticsMessage(message);
        response.setNodeAction(NodeAction.SHUTDOWN);
        return response;
      }
    }

    // Check if this node is a 'valid' node
    if (!this.nodesListManager.isValidNode(host) &&
        !isNodeInDecommissioning(nodeId)) {
      String message =
          "Disallowed NodeManager from  " + host
              + ", Sending SHUTDOWN signal to the NodeManager.";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    // check if node's capacity is load from dynamic-resources.xml
    String nid = nodeId.toString();

    Resource dynamicLoadCapability = loadNodeResourceFromDRConfiguration(nid);
    if (dynamicLoadCapability != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resource for node: " + nid + " is adjusted from: " +
            capability + " to: " + dynamicLoadCapability +
            " due to settings in dynamic-resources.xml.");
      }
      capability = dynamicLoadCapability;
      // sync back with new resource.
      response.setResource(capability);
    }

    // Check if this node has minimum allocations
    if (capability.getMemorySize() < minAllocMb
        || capability.getVirtualCores() < minAllocVcores) {
      String message = "NodeManager from  " + host
          + " doesn't satisfy minimum allocations, Sending SHUTDOWN"
          + " signal to the NodeManager. Node capabilities are " + capability
          + "; minimums are " + minAllocMb + "mb and " + minAllocVcores
          + " vcores";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    response.setContainerTokenMasterKey(containerTokenSecretManager
        .getCurrentKey());
    response.setNMTokenMasterKey(nmTokenSecretManager
        .getCurrentKey());

    RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort, httpPort,
        resolve(host), capability, nodeManagerVersion, physicalResource);

    RMNode oldNode = this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode);
    if (oldNode == null) {
      RMNodeStartedEvent startEvent = new RMNodeStartedEvent(nodeId,
          request.getNMContainerStatuses(),
          request.getRunningApplications());
      if (request.getLogAggregationReportsForApps() != null
          && !request.getLogAggregationReportsForApps().isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found the number of previous cached log aggregation "
              + "status from nodemanager:" + nodeId + " is :"
              + request.getLogAggregationReportsForApps().size());
        }
        startEvent.setLogAggregationReportsForApps(request
            .getLogAggregationReportsForApps());
      }
      this.rmContext.getDispatcher().getEventHandler().handle(
          startEvent);
    } else {
      LOG.info("Reconnect from the node at: " + host);
      this.nmLivelinessMonitor.unregister(nodeId);

      if (CollectionUtils.isEmpty(request.getRunningApplications())
          && rmNode.getState() != NodeState.DECOMMISSIONING
          && rmNode.getHttpPort() != oldNode.getHttpPort()) {
        // Reconnected node differs, so replace old node and start new node
        switch (rmNode.getState()) {
        case RUNNING:
          ClusterMetrics.getMetrics().decrNumActiveNodes();
          break;
        case UNHEALTHY:
          ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
          break;
        default:
          LOG.debug("Unexpected Rmnode state");
        }
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new NodeRemovedSchedulerEvent(rmNode));

        this.rmContext.getRMNodes().put(nodeId, rmNode);
        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMNodeStartedEvent(nodeId, null, null));
      } else {
        // Reset heartbeat ID since node just restarted.
        oldNode.resetLastNodeHeartBeatResponse();

        this.rmContext.getDispatcher().getEventHandler()
            .handle(new RMNodeReconnectEvent(nodeId, rmNode,
                request.getRunningApplications(),
                request.getNMContainerStatuses()));
      }
    }
    // On every node manager register we will be clearing NMToken keys if
    // present for any running application.
    this.nmTokenSecretManager.removeNodeKey(nodeId);
    this.nmLivelinessMonitor.register(nodeId);
    
    // Handle received container status, this should be processed after new
    // RMNode inserted
    if (!rmContext.isWorkPreservingRecoveryEnabled()) {
      if (!request.getNMContainerStatuses().isEmpty()) {
        LOG.info("received container statuses on node manager register :"
            + request.getNMContainerStatuses());
        for (NMContainerStatus status : request.getNMContainerStatuses()) {
          handleNMContainerStatus(status, nodeId);
        }
      }
    }

    // Update node's labels to RM's NodeLabelManager.
    Set<String> nodeLabels = NodeLabelsUtils.convertToStringSet(
        request.getNodeLabels());
    if (isDistributedNodeLabelsConf && nodeLabels != null) {
      try {
        updateNodeLabelsFromNMReport(nodeLabels, nodeId);
        response.setAreNodeLabelsAcceptedByRM(true);
      } catch (IOException ex) {
        // Ensure the exception is captured in the response
        response.setDiagnosticsMessage(ex.getMessage());
        response.setAreNodeLabelsAcceptedByRM(false);
      }
    } else if (isDelegatedCentralizedNodeLabelsConf) {
      this.rmContext.getRMDelegatedNodeLabelsUpdater().updateNodeLabels(nodeId);
    }

    StringBuilder message = new StringBuilder();
    message.append("NodeManager from node ").append(host).append("(cmPort: ")
        .append(cmPort).append(" httpPort: ");
    message.append(httpPort).append(") ")
        .append("registered with capability: ").append(capability);
    message.append(", assigned nodeId ").append(nodeId);
    if (response.getAreNodeLabelsAcceptedByRM()) {
      message.append(", node labels { ").append(
          StringUtils.join(",", nodeLabels) + " } ");
    }

    LOG.info(message.toString());
    response.setNodeAction(NodeAction.NORMAL);
    response.setRMIdentifier(ResourceManager.getClusterTimeStamp());
    response.setRMVersion(YarnVersionInfo.getVersion());
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException {

    NodeStatus remoteNodeStatus = request.getNodeStatus();
    /**
     * Here is the node heartbeat sequence...
     * 1. Check if it's a valid (i.e. not excluded) node
     * 2. Check if it's a registered node
     * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
     * 4. Send healthStatus to RMNode
     * 5. Update node's labels if distributed Node Labels configuration is enabled
     */
    NodeId nodeId = remoteNodeStatus.getNodeId();

    // 1. Check if it's a valid (i.e. not excluded) node, if not, see if it is
    // in decommissioning.
    if (!this.nodesListManager.isValidNode(nodeId.getHost())
        && !isNodeInDecommissioning(nodeId)) {
      String message =
          "Disallowed NodeManager nodeId: " + nodeId + " hostname: "
              + nodeId.getHost();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(
          NodeAction.SHUTDOWN, message);
    }

    // 2. Check if it's a registered node
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      /* node does not exist */
      String message = "Node not found resyncing " + remoteNodeStatus.getNodeId();
      LOG.info(message);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }

    // Send ping
    this.nmLivelinessMonitor.receivedPing(nodeId);
    this.decommissioningWatcher.update(rmNode, remoteNodeStatus);

    // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
    NodeHeartbeatResponse lastNodeHeartbeatResponse = rmNode.getLastNodeHeartBeatResponse();
    if (getNextResponseId(
        remoteNodeStatus.getResponseId()) == lastNodeHeartbeatResponse
            .getResponseId()) {
      LOG.info("Received duplicate heartbeat from node "
          + rmNode.getNodeAddress()+ " responseId=" + remoteNodeStatus.getResponseId());
      return lastNodeHeartbeatResponse;
    } else if (remoteNodeStatus.getResponseId() != lastNodeHeartbeatResponse
        .getResponseId()) {
      String message =
          "Too far behind rm response id:"
              + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
              + remoteNodeStatus.getResponseId();
      LOG.info(message);
      // TODO: Just sending reboot is not enough. Think more.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(NodeAction.RESYNC,
          message);
    }

    // Evaluate whether a DECOMMISSIONING node is ready to be DECOMMISSIONED.
    if (rmNode.getState() == NodeState.DECOMMISSIONING &&
        decommissioningWatcher.checkReadyToBeDecommissioned(
            rmNode.getNodeID())) {
      String message = "DECOMMISSIONING " + nodeId +
          " is ready to be decommissioned";
      LOG.info(message);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      this.nmLivelinessMonitor.unregister(nodeId);
      return YarnServerBuilderUtils.newNodeHeartbeatResponse(
          NodeAction.SHUTDOWN, message);
    }

    boolean timelineV2Enabled =
        YarnConfiguration.timelineServiceV2Enabled(getConfig());
    if (timelineV2Enabled) {
      // Check & update collectors info from request.
      updateAppCollectorsMap(request);
    }

    // Heartbeat response
    NodeHeartbeatResponse nodeHeartBeatResponse =
        YarnServerBuilderUtils.newNodeHeartbeatResponse(
            getNextResponseId(lastNodeHeartbeatResponse.getResponseId()),
            NodeAction.NORMAL, null, null, null, null, nextHeartBeatInterval);
    rmNode.setAndUpdateNodeHeartbeatResponse(nodeHeartBeatResponse);

    populateKeys(request, nodeHeartBeatResponse);

    ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
        rmContext.getSystemCredentialsForApps();
    if (!systemCredentials.isEmpty()) {
      nodeHeartBeatResponse.setSystemCredentialsForApps(systemCredentials);
    }

    if (timelineV2Enabled) {
      // Return collectors' map that NM needs to know
      setAppCollectorsMapToResponse(rmNode.getRunningApps(),
          nodeHeartBeatResponse);
    }

    // 4. Send status to RMNode, saving the latest response.
    RMNodeStatusEvent nodeStatusEvent =
        new RMNodeStatusEvent(nodeId, remoteNodeStatus);
    if (request.getLogAggregationReportsForApps() != null
        && !request.getLogAggregationReportsForApps().isEmpty()) {
      nodeStatusEvent.setLogAggregationReportsForApps(request
        .getLogAggregationReportsForApps());
    }
    this.rmContext.getDispatcher().getEventHandler().handle(nodeStatusEvent);

    // 5. Update node's labels to RM's NodeLabelManager.
    if (isDistributedNodeLabelsConf && request.getNodeLabels() != null) {
      try {
        updateNodeLabelsFromNMReport(
            NodeLabelsUtils.convertToStringSet(request.getNodeLabels()),
            nodeId);
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(true);
      } catch (IOException ex) {
        //ensure the error message is captured and sent across in response
        nodeHeartBeatResponse.setDiagnosticsMessage(ex.getMessage());
        nodeHeartBeatResponse.setAreNodeLabelsAcceptedByRM(false);
      }
    }

    // 6. check if node's capacity is load from dynamic-resources.xml
    // if so, send updated resource back to NM.
    String nid = nodeId.toString();
    Resource capability = loadNodeResourceFromDRConfiguration(nid);
    // sync back with new resource if not null.
    if (capability != null) {
      nodeHeartBeatResponse.setResource(capability);
    }

    // 7. Send Container Queuing Limits back to the Node. This will be used by
    // the node to truncate the number of Containers queued for execution.
    if (this.rmContext.getNodeManagerQueueLimitCalculator() != null) {
      nodeHeartBeatResponse.setContainerQueuingLimit(
          this.rmContext.getNodeManagerQueueLimitCalculator()
              .createContainerQueuingLimit());
    }

    // 8. Get node's attributes and update node-to-attributes mapping
    // in RMNodeAttributeManager.
    Set<NodeAttribute> nodeAttributes = request.getNodeAttributes();
    if (nodeAttributes != null && !nodeAttributes.isEmpty()) {
      nodeAttributes.forEach(nodeAttribute ->
          LOG.debug(nodeId.toString() + " ATTRIBUTE : "
              + nodeAttribute.toString()));

      // Validate attributes
      if (!nodeAttributes.stream().allMatch(
          nodeAttribute -> NodeAttribute.PREFIX_DISTRIBUTED
              .equals(nodeAttribute.getAttributeKey().getAttributePrefix()))) {
        // All attributes must be in same prefix: nm.yarn.io.
        // Since we have the checks in NM to make sure attributes reported
        // in HB are with correct prefix, so it should not reach here.
        LOG.warn("Reject invalid node attributes from host: "
            + nodeId.toString() + ", attributes in HB must have prefix "
            + NodeAttribute.PREFIX_DISTRIBUTED);
      } else {
        // Replace all distributed node attributes associated with this host
        // with the new reported attributes in node attribute manager.
        this.rmContext.getNodeAttributesManager()
            .replaceNodeAttributes(NodeAttribute.PREFIX_DISTRIBUTED,
                ImmutableMap.of(nodeId.getHost(), nodeAttributes));
      }
    }

    return nodeHeartBeatResponse;
  }

  private int getNextResponseId(int responseId) {
    // Loop between 0 and Integer.MAX_VALUE
    return (responseId + 1) & Integer.MAX_VALUE;
  }

  private void setAppCollectorsMapToResponse(
      List<ApplicationId> runningApps, NodeHeartbeatResponse response) {
    Map<ApplicationId, AppCollectorData> liveAppCollectorsMap = new
        HashMap<>();
    Map<ApplicationId, RMApp> rmApps = rmContext.getRMApps();
    // Set collectors for all running apps on this node.
    for (ApplicationId appId : runningApps) {
      RMApp app = rmApps.get(appId);
      if (app != null) {
        AppCollectorData appCollectorData = rmApps.get(appId)
            .getCollectorData();
        if (appCollectorData != null) {
          liveAppCollectorsMap.put(appId, appCollectorData);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Collector for applicaton: " + appId +
                " hasn't registered yet!");
          }
        }
      }
    }
    response.setAppCollectors(liveAppCollectorsMap);
  }

  private void updateAppCollectorsMap(NodeHeartbeatRequest request) {
    Map<ApplicationId, AppCollectorData> registeringCollectorsMap =
        request.getRegisteringCollectors();
    if (registeringCollectorsMap != null
        && !registeringCollectorsMap.isEmpty()) {
      Map<ApplicationId, RMApp> rmApps = rmContext.getRMApps();
      for (Map.Entry<ApplicationId, AppCollectorData> entry:
          registeringCollectorsMap.entrySet()) {
        ApplicationId appId = entry.getKey();
        AppCollectorData collectorData = entry.getValue();
        if (collectorData != null) {
          if (!collectorData.isStamped()) {
            // Stamp the collector if we have not done so
            collectorData.setRMIdentifier(
                ResourceManager.getClusterTimeStamp());
            collectorData.setVersion(
                timelineCollectorVersion.getAndIncrement());
          }
          RMApp rmApp = rmApps.get(appId);
          if (rmApp == null) {
            LOG.warn("Cannot update collector info because application ID: " +
                appId + " is not found in RMContext!");
          } else {
            synchronized (rmApp) {
              AppCollectorData previousCollectorData = rmApp.getCollectorData();
              if (AppCollectorData.happensBefore(previousCollectorData,
                  collectorData)) {
                // Sending collector update event.
                // Note: RM has to store the newly received collector data
                // synchronously. Otherwise, the RM may send out stale collector
                // data before this update is done, and the RM then crashes, the
                // newly updated collector data will get lost.
                LOG.info("Update collector information for application " + appId
                    + " with new address: " + collectorData.getCollectorAddr()
                    + " timestamp: " + collectorData.getRMIdentifier()
                    + ", " + collectorData.getVersion());
                ((RMAppImpl) rmApp).setCollectorData(collectorData);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Check if node in decommissioning state.
   * @param nodeId
   */
  private boolean isNodeInDecommissioning(NodeId nodeId) {
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode != null &&
        rmNode.getState().equals(NodeState.DECOMMISSIONING)) {
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException {
    UnRegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(UnRegisterNodeManagerResponse.class);
    NodeId nodeId = request.getNodeId();
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      LOG.info("Node not found, ignoring the unregister from node id : "
          + nodeId);
      return response;
    }
    LOG.info("Node with node id : " + nodeId
        + " has shutdown, hence unregistering the node.");
    this.nmLivelinessMonitor.unregister(nodeId);
    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMNodeEvent(nodeId, RMNodeEventType.SHUTDOWN));
    return response;
  }

  private void updateNodeLabelsFromNMReport(Set<String> nodeLabels,
      NodeId nodeId) throws IOException {
    try {
      Map<NodeId, Set<String>> labelsUpdate =
          new HashMap<NodeId, Set<String>>();
      labelsUpdate.put(nodeId, nodeLabels);
      this.rmContext.getNodeLabelManager().replaceLabelsOnNode(labelsUpdate);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node Labels {" + StringUtils.join(",", nodeLabels)
            + "} from Node " + nodeId + " were Accepted from RM");
      }
    } catch (IOException ex) {
      StringBuilder errorMessage = new StringBuilder();
      errorMessage.append("Node Labels {")
          .append(StringUtils.join(",", nodeLabels))
          .append("} reported from NM with ID ").append(nodeId)
          .append(" was rejected from RM with exception message as : ")
          .append(ex.getMessage());
      LOG.error(errorMessage, ex);
      throw new IOException(errorMessage.toString(), ex);
    }
  }

  private void populateKeys(NodeHeartbeatRequest request,
      NodeHeartbeatResponse nodeHeartBeatResponse) {

    // Check if node's masterKey needs to be updated and if the currentKey has
    // roller over, send it across

    // ContainerTokenMasterKey

    MasterKey nextMasterKeyForNode =
        this.containerTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownContainerTokenMasterKey().getKeyId()
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setContainerTokenMasterKey(nextMasterKeyForNode);
    }

    // NMTokenMasterKey

    nextMasterKeyForNode = this.nmTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownNMTokenMasterKey().getKeyId() 
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setNMTokenMasterKey(nextMasterKeyForNode);
    }
  }

  private Resource loadNodeResourceFromDRConfiguration(String nodeId) {
    // check if node's capacity is loaded from dynamic-resources.xml
    this.readLock.lock();
    try {
      String[] nodes = this.drConf.getNodes();
      if (nodes != null && Arrays.asList(nodes).contains(nodeId)) {
        return Resource.newInstance(this.drConf.getMemoryPerNode(nodeId),
            this.drConf.getVcoresPerNode(nodeId));
      } else {
        return null;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
}
