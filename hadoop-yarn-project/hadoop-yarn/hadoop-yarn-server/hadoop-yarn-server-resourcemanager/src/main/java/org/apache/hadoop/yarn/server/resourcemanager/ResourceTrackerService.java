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

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeReconnectEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;

public class ResourceTrackerService extends AbstractService implements
    ResourceTracker {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final RMContainerTokenSecretManager containerTokenSecretManager;

  private long nextHeartBeatInterval;
  private Server server;
  private InetSocketAddress resourceTrackerAddress;

  private static final NodeHeartbeatResponse resync = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);
  private static final NodeHeartbeatResponse shutDown = recordFactory
  .newRecordInstance(NodeHeartbeatResponse.class);
  
  private int minAllocMb;
  private int minAllocVcores;

  static {
    resync.setNodeAction(NodeAction.RESYNC);

    shutDown.setNodeAction(NodeAction.SHUTDOWN);
  }

  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
      RMContainerTokenSecretManager containerTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  @Override
  public synchronized void init(Configuration conf) {
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    RackResolver.init(conf);
    nextHeartBeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (nextHeartBeatInterval <= 0) {
      throw new YarnException("Invalid Configuration. "
          + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS
          + " should be larger than 0.");
    }

    minAllocMb = conf.getInt(
    	YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
    	YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    minAllocVcores = conf.getInt(
    	YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
    	YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
      rpc.getServer(ResourceTracker.class, this, resourceTrackerAddress,
          conf, null,
          conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT, 
              YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new RMPolicyProvider());
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
                           server.getListenerAddress());
  }

  @Override
  public synchronized void stop() {
    if (this.server != null) {
      this.server.stop();
    }
    super.stop();
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnRemoteException {

    NodeId nodeId = request.getNodeId();
    String host = nodeId.getHost();
    int cmPort = nodeId.getPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();

    RegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(RegisterNodeManagerResponse.class);

    // Check if this node is a 'valid' node
    if (!this.nodesListManager.isValidNode(host)) {
      LOG.info("Disallowed NodeManager from  " + host
          + ", Sending SHUTDOWN signal to the NodeManager.");
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    // Check if this node has minimum allocations
    if (capability.getMemory() < minAllocMb
        || capability.getVirtualCores() < minAllocVcores) {
      LOG.info("NodeManager from  " + host
          + " doesn't satisfy minimum allocations, Sending SHUTDOWN"
          + " signal to the NodeManager.");
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    if (isSecurityEnabled()) {
      MasterKey nextMasterKeyForNode =
          this.containerTokenSecretManager.getCurrentKey();
      response.setMasterKey(nextMasterKeyForNode);
    }

    RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort, httpPort,
        resolve(host), capability);

    RMNode oldNode = this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode);
    if (oldNode == null) {
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.STARTED));
    } else {
      LOG.info("Reconnect from the node at: " + host);
      this.nmLivelinessMonitor.unregister(nodeId);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeReconnectEvent(nodeId, rmNode));
    }

    this.nmLivelinessMonitor.register(nodeId);

    LOG.info("NodeManager from node " + host + "(cmPort: " + cmPort
        + " httpPort: " + httpPort + ") " + "registered with capability: "
        + capability + ", assigned nodeId " + nodeId);

    response.setNodeAction(NodeAction.NORMAL);
    response.setRMIdentifier(ResourceManager.clusterTimeStamp);
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnRemoteException {

    NodeStatus remoteNodeStatus = request.getNodeStatus();
    /**
     * Here is the node heartbeat sequence...
     * 1. Check if it's a registered node
     * 2. Check if it's a valid (i.e. not excluded) node 
     * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat 
     * 4. Send healthStatus to RMNode
     */

    NodeId nodeId = remoteNodeStatus.getNodeId();

    // 1. Check if it's a registered node
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      /* node does not exist */
      LOG.info("Node not found rebooting " + remoteNodeStatus.getNodeId());
      return resync;
    }

    // Send ping
    this.nmLivelinessMonitor.receivedPing(nodeId);

    // 2. Check if it's a valid (i.e. not excluded) node
    if (!this.nodesListManager.isValidNode(rmNode.getHostName())) {
      LOG.info("Disallowed NodeManager nodeId: " + nodeId + " hostname: "
          + rmNode.getNodeAddress());
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
      return shutDown;
    }
    
    // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
    NodeHeartbeatResponse lastNodeHeartbeatResponse = rmNode.getLastNodeHeartBeatResponse();
    if (remoteNodeStatus.getResponseId() + 1 == lastNodeHeartbeatResponse
        .getResponseId()) {
      LOG.info("Received duplicate heartbeat from node "
          + rmNode.getNodeAddress());
      return lastNodeHeartbeatResponse;
    } else if (remoteNodeStatus.getResponseId() + 1 < lastNodeHeartbeatResponse
        .getResponseId()) {
      LOG.info("Too far behind rm response id:"
          + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
          + remoteNodeStatus.getResponseId());
      // TODO: Just sending reboot is not enough. Think more.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
      return resync;
    }

    // Heartbeat response
    NodeHeartbeatResponse nodeHeartBeatResponse = YarnServerBuilderUtils
        .newNodeHeartbeatResponse(lastNodeHeartbeatResponse.
            getResponseId() + 1, NodeAction.NORMAL, null, null, null,
            nextHeartBeatInterval);
    rmNode.updateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);
    // Check if node's masterKey needs to be updated and if the currentKey has
    // roller over, send it across
    if (isSecurityEnabled()) {

      boolean shouldSendMasterKey = false;

      MasterKey nextMasterKeyForNode =
          this.containerTokenSecretManager.getNextKey();
      if (nextMasterKeyForNode != null) {
        // nextMasterKeyForNode can be null if there is no outstanding key that
        // is in the activation period.
        MasterKey nodeKnownMasterKey = request.getLastKnownMasterKey();
        if (nodeKnownMasterKey.getKeyId() != nextMasterKeyForNode.getKeyId()) {
          shouldSendMasterKey = true;
        }
      }
      if (shouldSendMasterKey) {
        nodeHeartBeatResponse.setMasterKey(nextMasterKeyForNode);
      }
    }

    // 4. Send status to RMNode, saving the latest response.
    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeStatusEvent(nodeId, remoteNodeStatus.getNodeHealthStatus(),
            remoteNodeStatus.getContainersStatuses(), 
            remoteNodeStatus.getKeepAliveApplications(), nodeHeartBeatResponse));

    return nodeHeartBeatResponse;
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
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  protected boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }
}
