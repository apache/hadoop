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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NodeStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfoTracker.NodeHeartbeatStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * This class is responsible for the interaction with the NodeManagers.
 * All the interactions with the NodeManagers happens via this interface.
 *`
 */
public class RMResourceTrackerImpl extends AbstractService implements 
NodeTracker, ClusterTracker {
  private static final Log LOG = LogFactory.getLog(RMResourceTrackerImpl.class);
  /* we dont garbage collect on nodes. A node can come back up again and re register,
   * so no use garbage collecting. Though admin can break the RM by bouncing 
   * nodemanagers on different ports again and again.
   */
  private Map<String, NodeId> nodes = new ConcurrentHashMap<String, NodeId>();
  private final Map<NodeId, NodeInfoTracker> nodeManagers = 
    new ConcurrentHashMap<NodeId, NodeInfoTracker>();
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private HostsFileReader hostsReader;

  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private final TreeSet<NodeHeartbeatStatus> nmExpiryQueue =
    new TreeSet<NodeHeartbeatStatus>(
        new Comparator<NodeHeartbeatStatus>() {
          public int compare(NodeHeartbeatStatus n1,  NodeHeartbeatStatus n2) {
            long p1LastSeen = n1.getLastSeen();
            long p2LastSeen = n2.getLastSeen();
            if (p1LastSeen < p2LastSeen) {
              return -1;
            } else if (p1LastSeen > p2LastSeen) {
              return 1;
            } else {
              return (n1.getNodeId().getId() -
                  n2.getNodeId().getId());
            }
          }
        }
    );

  private ResourceListener resourceListener;
  private final ContainerTokenSecretManager containerTokenSecretManager;
  private final AtomicInteger nodeCounter = new AtomicInteger(0);
  private static final HeartbeatResponse reboot = recordFactory.newRecordInstance(HeartbeatResponse.class);
  private long nmExpiryInterval;
  private final NodeStore nodeStore;
  private final RMContext rmContext;
  
  public RMResourceTrackerImpl(
      ContainerTokenSecretManager containerTokenSecretManager, 
      RMContext context) {
    super(RMResourceTrackerImpl.class.getName());
    reboot.setReboot(true);
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmLivelinessMonitor = new NMLivelinessMonitor();
    this.rmContext = context;
    this.nodeStore = context.getNodeStore();
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void init(Configuration conf) {
    super.init(conf);
    this.nmExpiryInterval =  conf.getLong(RMConfig.NM_EXPIRY_INTERVAL, 
        RMConfig.DEFAULT_NM_EXPIRY_INTERVAL);
    this.nmLivelinessMonitor.setMonitoringInterval(conf.getLong(
        RMConfig.NMLIVELINESS_MONITORING_INTERVAL,
        RMConfig.DEFAULT_NMLIVELINESS_MONITORING_INTERVAL));
    
    // Read the hosts/exclude files to restrict access to the RM
    try {
      this.hostsReader = 
        new HostsFileReader(
            conf.get(RMConfig.RM_NODES_INCLUDE_FILE, 
                RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE),
            conf.get(RMConfig.RM_NODES_EXCLUDE_FILE, 
                RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE)
                );
      printConfiguredHosts();
    } catch (IOException ioe) {
      LOG.warn("Failed to init hostsReader, disabling", ioe);
      try {
        this.hostsReader = 
          new HostsFileReader(RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE, 
              RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE);
      } catch (IOException ioe2) {
        // Should *never* happen
        this.hostsReader = null;
      }
    }
    RackResolver.init(conf);
  }

  private void printConfiguredHosts() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    
    Configuration conf = getConfig();
    LOG.debug("hostsReader: in=" + conf.get(RMConfig.RM_NODES_INCLUDE_FILE, 
        RMConfig.DEFAULT_RM_NODES_INCLUDE_FILE) + " out=" +
        conf.get(RMConfig.RM_NODES_EXCLUDE_FILE, 
            RMConfig.DEFAULT_RM_NODES_EXCLUDE_FILE));
    for (String include : hostsReader.getHosts()) {
      LOG.debug("include: " + include);
    }
    for (String exclude : hostsReader.getExcludedHosts()) {
      LOG.debug("exclude: " + exclude);
    }
  }
  
  @Override
  @Lock(Lock.NoLock.class)
  public void addListener(ResourceListener listener) {
    this.resourceListener = listener;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void start() {
    this.nmLivelinessMonitor.start();
    LOG.info("Expiry interval of NodeManagers set to " + nmExpiryInterval);
    super.start();
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  @Lock(Lock.NoLock.class)
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }
  
  @Lock(Lock.NoLock.class)
  protected NodeInfoTracker getAndAddNodeInfoTracker(NodeId nodeId, 
      String hostName, int cmPort, int httpPort,
      Node node, Resource capability) {
    NodeInfoTracker nTracker = null;
    NodeManager nodeManager =
      new NodeManagerImpl(nodeId, hostName, cmPort, httpPort, 
          node, capability);

    // Inform listeners and nodeStore
    addNode(nodeManager);
    
    // Record the new node
    synchronized(nodeManagers) {
      LOG.info("DEBUG -- Adding  " + hostName);
      nodes.put(nodeManager.getNodeAddress(), nodeId);
      HeartbeatResponse response = 
        recordFactory.newRecordInstance(HeartbeatResponse.class);
      response.setResponseId(0);
      nTracker = new NodeInfoTracker(nodeManager, response);
      nodeManagers.put(nodeId, nTracker);
    }
    
    return nTracker;
  }

  @Lock(Lock.NoLock.class)
  private void addNode(NodeManager node) {
    // Inform the listeners
    resourceListener.addNode(node);

    // Inform the node store
    try {
      nodeStore.storeNode(node);
    } catch (IOException ioe) {
      LOG.warn("Failed to store node " + node.getNodeAddress() + " in nodestore");
    }
  }
  
  @Override
  @Lock(Lock.NoLock.class)
  public RegistrationResponse registerNodeManager(
      String host, int cmPort, int httpPort, Resource capability) 
  throws IOException {
    // Check if this node is a 'valid' node
    if (!isValidNode(host)) {
      LOG.info("Disallowed NodeManager from  " + host);
      throw new IOException("Disallowed NodeManager from  " + host); 
    }

    String node = host + ":" + cmPort;
    NodeId nodeId = getNodeId(node);
    
    NodeInfoTracker nTracker = null;
    nTracker = 
      getAndAddNodeInfoTracker(nodeId, host, cmPort, httpPort, 
          resolve(host), capability);
    addForTracking(nTracker.getlastHeartBeat());
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
    return regResponse;
  }

  /**
   * Update the listeners. This method can be inlined but are there for 
   * making testing easier
   * @param nodeManager the {@link NodeInfo} to update.
   * @param containers the containers from the status of the node manager.
   */
  @Lock(Lock.NoLock.class)
  protected void updateListener(NodeInfo nodeManager, Map<String, List<Container>>
    containers) {
  /* inform any listeners of node heartbeats */
    resourceListener.nodeUpdate(
        nodeManager, containers);
  }
  
  
  /**
   * Get a response for the nodemanager heartbeat
   * @param nodeManager the nodemanager to update
   * @param containers the containers from the status update.
   * @return the {@link NodeResponse} for the node manager.
   */
  protected NodeResponse NodeResponse(NodeManager nodeManager, Map<String, 
      List<Container>> containers) {
    return nodeManager.statusUpdate(containers);
  }
  
  @Lock(Lock.NoLock.class)
  private boolean isValidNode(String hostName) {
    synchronized (hostsReader) {
      Set<String> hostsList = hostsReader.getHosts();
      Set<String> excludeList = hostsReader.getExcludedHosts();
      return ((hostsList.isEmpty() || hostsList.contains(hostName)) && 
          !excludeList.contains(hostName));
    }
  }
  
  @Override
  @Lock(Lock.NoLock.class)
  public HeartbeatResponse nodeHeartbeat(org.apache.hadoop.yarn.server.api.records.NodeStatus remoteNodeStatus) 
  throws IOException {
    /**
     * Here is the node heartbeat sequence...
     * 1. Check if it's a registered node
     * 2. Check if it's a valid (i.e. not excluded) node
     * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
     * 4. Check if it's healthy
     *   a. If it just turned unhealthy, inform scheduler and ZK
     *   b. It it just turned healthy, inform scheduler and ZK
     * 5. If it's healthy, update node status and allow scheduler to schedule
     */
    
    NodeId nodeId = remoteNodeStatus.getNodeId();
    
    // 1. Check if it's a registered node
    NodeInfoTracker nTracker = getNodeInfoTracker(nodeId);
    if (nTracker == null) {
      /* node does not exist */
      LOG.info("Node not found rebooting " + remoteNodeStatus.getNodeId());
      return reboot;
    }
    /* update the heart beat status */
    nTracker.setLastHeartBeatTime();
    
    // 2. Check if it's a valid (i.e. not excluded) node
    if (!isValidNode(nTracker.getNodeManager().getNodeHostName())) {
      LOG.info("Disallowed NodeManager nodeId: " + nodeId +  
          " hostname: " + nTracker.getNodeManager().getNodeAddress());
      unregisterNodeManager(remoteNodeStatus.getNodeId());
      throw new IOException("Disallowed NodeManager nodeId: " + 
          remoteNodeStatus.getNodeId());
    }
    
    // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
    NodeManager nodeManager = nTracker.getNodeManager();
    if (remoteNodeStatus.getResponseId() + 1 == nTracker
        .getLastHeartBeatResponse().getResponseId()) {
      LOG.info("Received duplicate heartbeat from node " + 
          nodeManager.getNodeAddress());
      return nTracker.getLastHeartBeatResponse();
    } else if (remoteNodeStatus.getResponseId() + 1 < nTracker
        .getLastHeartBeatResponse().getResponseId()) {
      LOG.info("Too far behind rm response id:" +
          nTracker.lastHeartBeatResponse.getResponseId() + " nm response id:"
          + remoteNodeStatus.getResponseId());
      unregisterNodeManager(remoteNodeStatus.getNodeId());
      return reboot;
    }
    
    // 4. Check if it's healthy
    //   a. If it just turned unhealthy, inform scheduler and ZK
    //   b. It it just turned healthy, inform scheduler and ZK
    boolean prevHealthStatus =
      nTracker.getNodeManager().getNodeHealthStatus().getIsNodeHealthy();
    NodeHealthStatus currentNodeHealthStatus =
      remoteNodeStatus.getNodeHealthStatus();
    if (prevHealthStatus != currentNodeHealthStatus
        .getIsNodeHealthy()) {
      if (!currentNodeHealthStatus.getIsNodeHealthy()) {
        // Node turned unhealthy
        LOG.info("Node " + nodeManager.getNodeID()
            + " has become unhealthy. Health-check report: "
            + currentNodeHealthStatus.getHealthReport() + "."
            + " Removing it from the scheduler.");
        removeNode(nodeManager);
      } else {
        // Node turned healthy
        LOG.info("Node " + nodeManager.getNodeID() +
            " has become healthy back again. Health-check report: " +
            remoteNodeStatus.getNodeHealthStatus().getHealthReport()  + 
            ". Adding it to the scheduler.");
        addNode(nodeManager);
      }
    }

    // Heartbeat response
    HeartbeatResponse response = recordFactory.newRecordInstance(HeartbeatResponse.class);
    response.setResponseId(nTracker.getLastHeartBeatResponse().getResponseId() + 1);

    // 5. If it's healthy, update node status and allow scheduler to schedule
    NodeResponse nodeResponse = null;
    if (currentNodeHealthStatus.getIsNodeHealthy()) {
      nodeResponse = 
        nodeManager.statusUpdate(remoteNodeStatus.getAllContainers());
      response.addAllContainersToCleanup(nodeResponse.getContainersToCleanUp());
      response.addAllApplicationsToCleanup(nodeResponse.getFinishedApplications());
      
      /* inform any listeners of node heartbeats */
      updateListener(nodeManager, remoteNodeStatus.getAllContainers());
    }

    // For handling diagnostic information
    for (Entry<String, List<Container>> entry : remoteNodeStatus
        .getAllContainers().entrySet()) {
      
    }

    // Save the response    
    nTracker.setLastHeartBeatResponse(response);
    nTracker.getNodeManager().updateHealthStatus(currentNodeHealthStatus);

    return response;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void unregisterNodeManager(NodeId nodeId) {
    NodeManager node = null;  
    synchronized (nodeManagers) {
      node = getNodeManager(nodeId);
      if (node != null) {
        nodeManagers.remove(nodeId);
        nodes.remove(node.getNodeAddress());
      } else {
        LOG.warn("Unknown node " + nodeId + " unregistered");
      }
    }
    
    // Inform the listeners and nodeStore
    if (node != null) {
      removeNode(node);
    }
  }

  @Lock(Lock.NoLock.class)
  private void removeNode(NodeManager node) {
    resourceListener.removeNode(node);
    try {
      nodeStore.removeNode(node);
    } catch (IOException ioe) {
      LOG.warn("Failed to remove node " + node.getNodeAddress() + 
          " from nodeStore", ioe);
    }
  }
  
  @Lock(Lock.NoLock.class)
  private  NodeId getNodeId(String node) {
    NodeId nodeId;
    nodeId = nodes.get(node);
    if (nodeId == null) {
      nodeId = recordFactory.newRecordInstance(NodeId.class);
      nodeId.setId(nodeCounter.getAndIncrement());
    }
    return nodeId;
  }

  @Override
  @Lock(RMResourceTrackerImpl.class)
  public synchronized YarnClusterMetrics getClusterMetrics() {
    YarnClusterMetrics ymetrics = recordFactory.newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(nodeManagers.size());
    return ymetrics;
  }

  @Override
  @Lock(Lock.NoLock.class)
  public void stop() {
    this.nmLivelinessMonitor.interrupt();
    this.nmLivelinessMonitor.shutdown();
    try {
      this.nmLivelinessMonitor.join();
    } catch (InterruptedException ie) {
      LOG.info(this.nmLivelinessMonitor.getName() + " interrupted during join ",
          ie);
    }
    super.stop();
  }

  @Override
  @Lock(Lock.NoLock.class)
  public List<NodeInfo> getAllNodeInfo() {
    List<NodeInfo> infoList = new ArrayList<NodeInfo>();
    synchronized (nodeManagers) {
      for (NodeInfoTracker t : nodeManagers.values()) {
        infoList.add(t.getNodeManager());
      }
    }
    return infoList;
  }

  @Lock(Lock.NoLock.class)
  protected void addForTracking(NodeHeartbeatStatus nmStatus) {
    synchronized(nmExpiryQueue) {
      nmExpiryQueue.add(nmStatus);
    }
  }

  @Lock(Lock.NoLock.class)
  protected void expireNMs(List<NodeId> nodes) {
    for (NodeId id: nodes) {
      unregisterNodeManager(id);
    }
  }

  /*
   * This class runs continuosly to track the nodemanagers
   * that might be dead.
   */
  private class NMLivelinessMonitor extends Thread {
    private volatile boolean stop = false;
    private long monitoringInterval =
        RMConfig.DEFAULT_NMLIVELINESS_MONITORING_INTERVAL;

    public NMLivelinessMonitor() {
      super("RMResourceTrackerImpl:" + NMLivelinessMonitor.class.getName());
    }

    public void setMonitoringInterval(long interval) {
      this.monitoringInterval = interval;
    }

    @Override
    public void run() {
      /* the expiry queue does not need to be in sync with nodeManagers,
       * if a nodemanager in the expiry queue cannot be found in nodemanagers
       * its alright. We do not want to hold a hold on nodeManagers while going
       * through the expiry queue.
       */

      List<NodeId> expired = new ArrayList<NodeId>();
      LOG.info("Starting expiring thread with interval " + nmExpiryInterval);

      while (!stop) {
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(nmExpiryQueue) {
          NodeHeartbeatStatus leastRecent;
          while ((nmExpiryQueue.size() > 0) &&
              (leastRecent = nmExpiryQueue.first()) != null && 
              ((now - leastRecent.getLastSeen()) > 
              nmExpiryInterval)) {
            nmExpiryQueue.remove(leastRecent);
            NodeInfoTracker info = getNodeInfoTracker(leastRecent.getNodeId());
            if (info == null) {
              continue;
            }
            NodeId nodeId = leastRecent.getNodeId();
            NodeHeartbeatStatus heartBeatStatus = info.getlastHeartBeat();
            if ((now - heartBeatStatus.getLastSeen()) > nmExpiryInterval) {
              LOG.info("Going to expire the node-manager " + info.getNodeManager().getNodeAddress()
                  + " because of no updates for "
                  + (now - heartBeatStatus.getLastSeen())
                  + " seconds ( > expiry interval of " + nmExpiryInterval
                  + ").");
              expired.add(nodeId);
            } else {
              nmExpiryQueue.add(heartBeatStatus);
              break;
            }
          }
        }
        expireNMs(expired);
        try {
          Thread.sleep(this.monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }

    public void shutdown() {
      this.stop = true;
    }
  }

  @Override
  public void finishedApplication(ApplicationId applicationId,
      List<NodeInfo> nodesToNotify) {
    for (NodeInfo info: nodesToNotify) {
      NodeManager node = getNodeManager(info.getNodeID());
      if (node != null) {
        node.finishedApplication(applicationId);
      }
    } 
  }
  
  @Private
  public NodeManager getNodeManager(NodeId nodeId) {
    if (nodeId == null) {
      LOG.info("getNodeManager called with nodeId=null");
      return null;
    }
    
    NodeManager nodeManager = null;
    synchronized (nodeManagers) {
      NodeInfoTracker node = nodeManagers.get(nodeId);
      if (node != null) {
        nodeManager = node.getNodeManager();
      }
    }
    return nodeManager;
  }

  private NodeInfoTracker getNodeInfoTracker(NodeId nodeId) {
    NodeInfoTracker node = null;
    synchronized (nodeManagers) {
      node = nodeManagers.get(nodeId);
    }
    return node;
  }
  
  private NodeManager getNodeManagerForContainer(Container container) {
    NodeManager node;
    synchronized (nodeManagers) {
      LOG.info("DEBUG -- Container manager address " + 
          container.getContainerManagerAddress());
      NodeId nodeId = nodes.get(container.getContainerManagerAddress());
      node = getNodeManager(nodeId);
    }
    return node;
  }
  
  @Override
  @Lock({YarnScheduler.class})
  public  boolean releaseContainer(Container container) {
    NodeManager node = getNodeManagerForContainer(container);
    return ((node != null) && node.releaseContainer(container));
  }
  
  @Override
  public void recover(RMState state) {
    List<NodeManager> nodeManagers = state.getStoredNodeManagers();
    for (NodeManager nm: nodeManagers) {
      getAndAddNodeInfoTracker(nm.getNodeID(), nm.getNodeHostName(),
          nm.getCommandPort(), nm.getHttpPort(),
          nm.getNode(), nm.getTotalCapability());
    }
    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
      List<Container> containers = entry.getValue().getContainers();
      List<Container> containersToAdd = new ArrayList<Container>();
      for (Container c: containers) {
        NodeManager containerNode = getNodeManagerForContainer(c);
        containersToAdd.add(c);
        containerNode.allocateContainer(entry.getKey(), containersToAdd);
        containersToAdd.clear();
      }
    }
  }

  @Override
  public void refreshNodes() throws IOException {
    synchronized (hostsReader) {
      hostsReader.refresh();
      printConfiguredHosts();
    }
  }

}
