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

import javax.crypto.SecretKey;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
    ResourceTracker {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final ContainerTokenSecretManager containerTokenSecretManager;

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
      conf.get(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS);
    resourceTrackerAddress = NetUtils.createSocketAddr(resourceTrackerBindAddress);

    RackResolver.init(conf);
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

    NodeId nodeId = request.getNodeId();
    String host = nodeId.getHost();
    int cmPort = nodeId.getPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();

    try {
      // Check if this node is a 'valid' node
      if (!this.nodesListManager.isValidNode(host)) {
        LOG.info("Disallowed NodeManager from  " + host);
        throw new IOException("Disallowed NodeManager from  " + host); 
      }

      RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort,
          httpPort, resolve(host), capability);

      if (this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode) != null) {
        throw new IOException("Duplicate registration from the node!");
      }

      this.nmLivelinessMonitor.register(nodeId);

      LOG.info("NodeManager from node " + host + 
          "(cmPort: " + cmPort + " httpPort: " + httpPort + ") "
          + "registered with capability: " + capability.getMemory()
          + ", assigned nodeId " + nodeId);

      RegistrationResponse regResponse = recordFactory.newRecordInstance(
          RegistrationResponse.class);
      SecretKey secretKey = this.containerTokenSecretManager
          .createAndGetSecretKey(nodeId.toString());
      regResponse.setSecretKey(ByteBuffer.wrap(secretKey.getEncoded()));

      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setRegistrationResponse(regResponse);
      return response;
    } catch (IOException ioe) {
      LOG.info("Exception in node registration from " + nodeId.getHost(), ioe);
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
      if (!this.nodesListManager.isValidNode(rmNode.getHostName())) {
        LOG.info("Disallowed NodeManager nodeId: " + nodeId +  
            " hostname: " + rmNode.getNodeAddress());
        throw new IOException("Disallowed NodeManager nodeId: " + 
            remoteNodeStatus.getNodeId());
      }

      NodeHeartbeatResponse nodeHeartBeatResponse = recordFactory
          .newRecordInstance(NodeHeartbeatResponse.class);

      // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
      HeartbeatResponse lastHeartbeatResponse = rmNode
          .getLastHeartBeatResponse();
      if (remoteNodeStatus.getResponseId() + 1 == lastHeartbeatResponse
           .getResponseId()) {
        LOG.info("Received duplicate heartbeat from node " + 
            rmNode.getNodeAddress());
        nodeHeartBeatResponse.setHeartbeatResponse(lastHeartbeatResponse);
        return nodeHeartBeatResponse;
      } else if (remoteNodeStatus.getResponseId() + 1 < lastHeartbeatResponse
          .getResponseId()) {
        LOG.info("Too far behind rm response id:" +
            lastHeartbeatResponse.getResponseId() + " nm response id:"
            + remoteNodeStatus.getResponseId());
        // TODO: Just sending reboot is not enough. Think more.
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
        return reboot;
      }

      // Heartbeat response
      HeartbeatResponse latestResponse = recordFactory
          .newRecordInstance(HeartbeatResponse.class);
      latestResponse
          .setResponseId(lastHeartbeatResponse.getResponseId() + 1);
      latestResponse.addAllContainersToCleanup(rmNode.pullContainersToCleanUp());
      latestResponse.addAllApplicationsToCleanup(rmNode.pullAppsToCleanup());

      // 4. Send status to RMNode, saving the latest response.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeStatusEvent(nodeId, remoteNodeStatus.getNodeHealthStatus(),
              remoteNodeStatus.getContainersStatuses(), latestResponse));

      nodeHeartBeatResponse.setHeartbeatResponse(latestResponse);
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

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }

}
