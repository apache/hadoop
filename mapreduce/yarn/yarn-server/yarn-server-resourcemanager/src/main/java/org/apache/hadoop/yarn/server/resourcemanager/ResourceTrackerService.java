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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;

public class ResourceTrackerService extends AbstractService implements
    ResourceTracker, RMNodeRemovalListener {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final ContainerTokenSecretManager containerTokenSecretManager;

  /* we dont garbage collect on nodes. A node can come back up again and re register,
   * so no use garbage collecting. Though admin can break the RM by bouncing 
   * nodemanagers on different ports again and again.
   */
  private Map<String, NodeId> nodes = new ConcurrentHashMap<String, NodeId>();
  private final AtomicInteger nodeCounter = new AtomicInteger(0);

  private Server server;
  private InetSocketAddress resourceTrackerAddress;

  private static final NodeHeartbeatResponse reboot = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);
  static {
    HeartbeatResponse rebootResp = recordFactory
        .newRecordInstance(HeartbeatResponse.class);
    rebootResp.setReboot(true);
    reboot.setHeartbeatResponse(rebootResp);
  }

  private final ConcurrentMap<NodeId, HeartbeatResponse> lastHeartBeats
    = new ConcurrentHashMap<NodeId, HeartbeatResponse>();

  public ResourceTrackerService(RMContext rmContext,
      NodesListManager nodesListManager,
      NMLivelinessMonitor nmLivelinessMonitor,
      ContainerTokenSecretManager containerTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  @Override
  public synchronized void init(Configuration conf) {
    String resourceTrackerBindAddress =
      conf.get(YarnServerConfig.RESOURCETRACKER_ADDRESS,
          YarnServerConfig.DEFAULT_RESOURCETRACKER_BIND_ADDRESS);
    resourceTrackerAddress = NetUtils.createSocketAddr(resourceTrackerBindAddress);

    RackResolver.init(conf);
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration rtServerConf = new Configuration(getConfig());
    rtServerConf.setClass(
        YarnConfiguration.YARN_SECURITY_INFO,
        RMNMSecurityInfoClass.class, SecurityInfo.class);
    this.server =
      rpc.getServer(ResourceTracker.class, this, resourceTrackerAddress,
          rtServerConf, null,
          rtServerConf.getInt(RMConfig.RM_RESOURCE_TRACKER_THREADS, 
              RMConfig.DEFAULT_RM_RESOURCE_TRACKER_THREADS));
    this.server.start();

  }

  @Override
  public synchronized void stop() {
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }

  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnRemoteException {

    String host = request.getHost();
    int cmPort = request.getContainerManagerPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();

    try {
      // Check if this node is a 'valid' node
      if (!this.nodesListManager.isValidNode(host)) {
        LOG.info("Disallowed NodeManager from  " + host);
        throw new IOException("Disallowed NodeManager from  " + host); 
      }

      String node = host + ":" + cmPort;
      NodeId nodeId = mayBeCreateAndGetNodeId(node);
   
      createNewNode(nodeId, host, cmPort, httpPort, resolve(host), capability);

      this.nmLivelinessMonitor.register(nodeId);

      LOG.info("NodeManager from node " + host + 
          "(cmPort: " + cmPort + " httpPort: " + httpPort + ") "
          + "registered with capability: " + capability.getMemory()
          + ", assigned nodeId " + nodeId.getId());

      RegistrationResponse regResponse = recordFactory.newRecordInstance(
          RegistrationResponse.class);
      regResponse.setNodeId(nodeId);
      SecretKey secretKey =
        this.containerTokenSecretManager.createAndGetSecretKey(node);
      regResponse.setSecretKey(ByteBuffer.wrap(secretKey.getEncoded()));

      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setRegistrationResponse(regResponse);
      return response;
    } catch (IOException ioe) {
      LOG.info("Exception in node registration from " + request.getHost(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnRemoteException {

    NodeStatus remoteNodeStatus = request.getNodeStatus();
    try {
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
        return reboot;
      }

      // Send ping
      this.nmLivelinessMonitor.receivedPing(nodeId);

      // 2. Check if it's a valid (i.e. not excluded) node
      if (!this.nodesListManager.isValidNode(rmNode.getNodeHostName())) {
        LOG.info("Disallowed NodeManager nodeId: " + nodeId +  
            " hostname: " + rmNode.getNodeAddress());
        throw new IOException("Disallowed NodeManager nodeId: " + 
            remoteNodeStatus.getNodeId());
      }

      NodeHeartbeatResponse nodeHeartBeatResponse = recordFactory
          .newRecordInstance(NodeHeartbeatResponse.class);

      // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
      if (remoteNodeStatus.getResponseId() + 1 == this.lastHeartBeats.get(nodeId)
           .getResponseId()) {
        LOG.info("Received duplicate heartbeat from node " + 
            rmNode.getNodeAddress());
        nodeHeartBeatResponse.setHeartbeatResponse(this.lastHeartBeats
            .get(nodeId));
        return nodeHeartBeatResponse;
      } else if (remoteNodeStatus.getResponseId() + 1 < this.lastHeartBeats
          .get(nodeId).getResponseId()) {
        LOG.info("Too far behind rm response id:" +
            this.lastHeartBeats.get(nodeId).getResponseId() + " nm response id:"
            + remoteNodeStatus.getResponseId());
        // TODO: Just sending reboot is not enough. Think more.
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
        return reboot;
      }

      // 4. Send status to RMNode
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeStatusEvent(nodeId, remoteNodeStatus.getNodeHealthStatus(),
              remoteNodeStatus.getAllContainers()));

      // Heartbeat response
      HeartbeatResponse response = recordFactory
          .newRecordInstance(HeartbeatResponse.class);
      response
          .setResponseId(this.lastHeartBeats.get(nodeId).getResponseId() + 1);
      response.addAllContainersToCleanup(rmNode.pullContainersToCleanUp());
      response.addAllApplicationsToCleanup(rmNode.pullAppsToCleanup());

      // Save the response
      this.lastHeartBeats.put(nodeId, response);
      nodeHeartBeatResponse.setHeartbeatResponse(response);
      return nodeHeartBeatResponse;
    } catch (IOException ioe) {
      LOG.info("Exception in heartbeat from node " + 
          request.getNodeStatus().getNodeId(), ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  public void recover(RMState state) {
//
//    List<RMNode> nodeManagers = state.getStoredNodeManagers();
//    for (RMNode nm : nodeManagers) {
//      createNewNode(nm.getNodeID(), nm.getNodeHostName(), nm
//          .getCommandPort(), nm.getHttpPort(), nm.getNode(), nm
//          .getTotalCapability());
//    }
//    for (Map.Entry<ApplicationId, ApplicationInfo> entry : state
//        .getStoredApplications().entrySet()) {
//      List<Container> containers = entry.getValue().getContainers();
//      List<Container> containersToAdd = new ArrayList<Container>();
//      for (Container c : containers) {
//        RMNode containerNode = this.rmContext.getNodesCollection()
//            .getNodeInfo(c.getNodeId());
//        containersToAdd.add(c);
//        containerNode.allocateContainer(entry.getKey(), containersToAdd);
//        containersToAdd.clear();
//      }
//    }
  }

  private void createNewNode(NodeId nodeId, String hostName, int cmPort,
      int httpPort, Node node, Resource capability) throws IOException {

    RMNode rmNode = new RMNodeImpl(nodeId, rmContext, hostName, cmPort,
        httpPort, node, capability, this);

    if (this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode) != null) {
      throw new IOException("Duplicate registration from the node!");
    }

    // Record the new node
    synchronized (nodes) {
      LOG.info("DEBUG -- Adding  " + hostName);
      HeartbeatResponse response = recordFactory
          .newRecordInstance(HeartbeatResponse.class);
      response.setResponseId(0);
      this.lastHeartBeats.put(nodeId, response);
      nodes.put(rmNode.getNodeAddress(), nodeId);
    }
  }

  @Override
  public void RMNodeRemoved(NodeId nodeId) {
    RMNode node = null;  
    synchronized (nodes) {
      node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        nodes.remove(node.getNodeAddress());
        this.lastHeartBeats.remove(nodeId);
      } else {
        LOG.warn("Unknown node " + nodeId + " unregistered");
      }
    }
    
    if (node != null) {
      this.rmContext.getRMNodes().remove(nodeId);
    }
  }
  
  private  NodeId mayBeCreateAndGetNodeId(String node) {
    NodeId nodeId;
    nodeId = nodes.get(node);
    if (nodeId == null) {
      nodeId = recordFactory.newRecordInstance(NodeId.class);
      nodeId.setId(nodeCounter.getAndIncrement());
    }
    return nodeId;
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }

}
