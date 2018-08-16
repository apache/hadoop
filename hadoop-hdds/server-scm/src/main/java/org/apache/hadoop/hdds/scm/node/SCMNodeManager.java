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
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto
    .ErrorCode;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.StorageContainerNodeProtocol;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Maintains information about the Datanodes on SCM side.
 * <p>
 * Heartbeats under SCM is very simple compared to HDFS heartbeatManager.
 * <p>
 * The getNode(byState) functions make copy of node maps and then creates a list
 * based on that. It should be assumed that these get functions always report
 * *stale* information. For example, getting the deadNodeCount followed by
 * getNodes(DEAD) could very well produce totally different count. Also
 * getNodeCount(HEALTHY) + getNodeCount(DEAD) + getNodeCode(STALE), is not
 * guaranteed to add up to the total nodes that we know off. Please treat all
 * get functions in this file as a snap-shot of information that is inconsistent
 * as soon as you read it.
 */
public class SCMNodeManager
    implements NodeManager, StorageContainerNodeProtocol {

  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeManager.class);


  private final NodeStateManager nodeStateManager;
  // Individual live node stats
  // TODO: NodeStat should be moved to NodeStatemanager (NodeStateMap)
  private final ConcurrentHashMap<UUID, SCMNodeStat> nodeStats;
  // Should we maintain aggregated stats? If this is not frequently used, we
  // can always calculate it from nodeStats whenever required.
  // Aggregated node stats
  private SCMNodeStat scmStat;
  // Should we create ChillModeManager and extract all the chill mode logic
  // to a new class?
  private int chillModeNodeCount;
  private final String clusterID;
  private final VersionInfo version;
  /**
   * During start up of SCM, it will enter into chill mode and will be there
   * until number of Datanodes registered reaches {@code chillModeNodeCount}.
   * This flag is for tracking startup chill mode.
   */
  private AtomicBoolean inStartupChillMode;
  /**
   * Administrator can put SCM into chill mode manually.
   * This flag is for tracking manual chill mode.
   */
  private AtomicBoolean inManualChillMode;
  private final CommandQueue commandQueue;
  // Node manager MXBean
  private ObjectName nmInfoBean;

  // Node pool manager.
  private final StorageContainerManager scmManager;

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(OzoneConfiguration conf, String clusterID,
      StorageContainerManager scmManager, EventPublisher eventPublisher)
      throws IOException {
    this.nodeStateManager = new NodeStateManager(conf, eventPublisher);
    this.nodeStats = new ConcurrentHashMap<>();
    this.scmStat = new SCMNodeStat();
    this.clusterID = clusterID;
    this.version = VersionInfo.getLatestVersion();
    this.commandQueue = new CommandQueue();
    // TODO: Support this value as a Percentage of known machines.
    this.chillModeNodeCount = 1;
    this.inStartupChillMode = new AtomicBoolean(true);
    this.inManualChillMode = new AtomicBoolean(false);
    this.scmManager = scmManager;
    LOG.info("Entering startup chill mode.");
    registerMXBean();
  }

  private void registerMXBean() {
    this.nmInfoBean = MBeans.register("SCMNodeManager",
        "SCMNodeManagerInfo", this);
  }

  private void unregisterMXBean() {
    if(this.nmInfoBean != null) {
      MBeans.unregister(this.nmInfoBean);
      this.nmInfoBean = null;
    }
  }

  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws NodeNotFoundException
   */
  @Override
  public void removeNode(DatanodeDetails node) throws NodeNotFoundException {
    nodeStateManager.removeNode(node);
  }

  /**
   * Gets all datanodes that are in a certain state. This function works by
   * taking a snapshot of the current collection and then returning the list
   * from that collection. This means that real map might have changed by the
   * time we return this list.
   *
   * @return List of Datanodes that are known to SCM in the requested state.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeState nodestate) {
    return nodeStateManager.getNodes(nodestate);
  }

  /**
   * Returns all datanodes that are known to SCM.
   *
   * @return List of DatanodeDetails
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    return nodeStateManager.getAllNodes();
  }

  /**
   * Get the minimum number of nodes to get out of Chill mode.
   *
   * @return int
   */
  @Override
  public int getMinimumChillModeNodes() {
    return chillModeNodeCount;
  }

  /**
   * Sets the Minimum chill mode nodes count, used only in testing.
   *
   * @param count - Number of nodes.
   */
  @VisibleForTesting
  public void setMinimumChillModeNodes(int count) {
    chillModeNodeCount = count;
  }

  /**
   * Returns chill mode Status string.
   * @return String
   */
  @Override
  public String getChillModeStatus() {
    if (inStartupChillMode.get()) {
      return "Still in chill mode, waiting on nodes to report in." +
          String.format(" %d nodes reported, minimal %d nodes required.",
              nodeStateManager.getTotalNodeCount(), getMinimumChillModeNodes());
    }
    if (inManualChillMode.get()) {
      return "Out of startup chill mode, but in manual chill mode." +
          String.format(" %d nodes have reported in.",
              nodeStateManager.getTotalNodeCount());
    }
    return "Out of chill mode." +
        String.format(" %d nodes have reported in.",
            nodeStateManager.getTotalNodeCount());
  }

  /**
   * Forcefully exits the chill mode even if we have not met the minimum
   * criteria of exiting the chill mode. This will exit from both startup
   * and manual chill mode.
   */
  @Override
  public void forceExitChillMode() {
    if(inStartupChillMode.get()) {
      LOG.info("Leaving startup chill mode.");
      inStartupChillMode.set(false);
    }
    if(inManualChillMode.get()) {
      LOG.info("Leaving manual chill mode.");
      inManualChillMode.set(false);
    }
  }

  /**
   * Puts the node manager into manual chill mode.
   */
  @Override
  public void enterChillMode() {
    LOG.info("Entering manual chill mode.");
    inManualChillMode.set(true);
  }

  /**
   * Brings node manager out of manual chill mode.
   */
  @Override
  public void exitChillMode() {
    LOG.info("Leaving manual chill mode.");
    inManualChillMode.set(false);
  }

  /**
   * Returns true if node manager is out of chill mode, else false.
   * @return true if out of chill mode, else false
   */
  @Override
  public boolean isOutOfChillMode() {
    return !(inStartupChillMode.get() || inManualChillMode.get());
  }

  /**
   * Returns the Number of Datanodes by State they are in.
   *
   * @return int -- count
   */
  @Override
  public int getNodeCount(NodeState nodestate) {
    return nodeStateManager.getNodeCount(nodestate);
  }

  /**
   * Returns the node state of a specific node.
   *
   * @param datanodeDetails - Datanode Details
   * @return Healthy/Stale/Dead/Unknown.
   */
  @Override
  public NodeState getNodeState(DatanodeDetails datanodeDetails) {
    try {
      return nodeStateManager.getNodeState(datanodeDetails);
    } catch (NodeNotFoundException e) {
      // TODO: should we throw NodeNotFoundException?
      return null;
    }
  }


  private void updateNodeStat(UUID dnId, NodeReportProto nodeReport) {
    SCMNodeStat stat = nodeStats.get(dnId);
    if (stat == null) {
      LOG.debug("SCM updateNodeStat based on heartbeat from previous" +
          "dead datanode {}", dnId);
      stat = new SCMNodeStat();
    }

    if (nodeReport != null && nodeReport.getStorageReportCount() > 0) {
      long totalCapacity = 0;
      long totalRemaining = 0;
      long totalScmUsed = 0;
      List<StorageReportProto> storageReports = nodeReport
          .getStorageReportList();
      for (StorageReportProto report : storageReports) {
        totalCapacity += report.getCapacity();
        totalRemaining +=  report.getRemaining();
        totalScmUsed+= report.getScmUsed();
      }
      scmStat.subtract(stat);
      stat.set(totalCapacity, totalScmUsed, totalRemaining);
      nodeStats.put(dnId, stat);
      scmStat.add(stat);
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    unregisterMXBean();
  }

  /**
   * Gets the version info from SCM.
   *
   * @param versionRequest - version Request.
   * @return - returns SCM version info and other required information needed by
   * datanode.
   */
  @Override
  public VersionResponse getVersion(SCMVersionRequestProto versionRequest) {
    return VersionResponse.newBuilder()
        .setVersion(this.version.getVersion())
        .addValue(OzoneConsts.SCM_ID,
            this.scmManager.getScmStorage().getScmId())
        .addValue(OzoneConsts.CLUSTER_ID, this.scmManager.getScmStorage()
            .getClusterID())
        .build();
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeDetails - Send datanodeDetails with Node info.
   *                   This function generates and assigns new datanode ID
   *                   for the datanode. This allows SCM to be run independent
   *                   of Namenode if required.
   * @param nodeReport NodeReport.
   *
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport) {

    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      datanodeDetails.setHostName(dnAddress.getHostName());
      datanodeDetails.setIpAddress(dnAddress.getHostAddress());
    }
    UUID dnId = datanodeDetails.getUuid();
    try {
      nodeStateManager.addNode(datanodeDetails);
      nodeStats.put(dnId, new SCMNodeStat());
      if(inStartupChillMode.get() &&
          nodeStateManager.getTotalNodeCount() >= getMinimumChillModeNodes()) {
        inStartupChillMode.getAndSet(false);
        LOG.info("Leaving startup chill mode.");
      }
      // Updating Node Report, as registration is successful
      updateNodeStat(datanodeDetails.getUuid(), nodeReport);
      LOG.info("Data node with ID: {} Registered.", datanodeDetails.getUuid());
    } catch (NodeAlreadyExistsException e) {
      LOG.trace("Datanode is already registered. Datanode: {}",
          datanodeDetails.toString());
    }
    return RegisteredCommand.newBuilder().setErrorCode(ErrorCode.success)
        .setDatanodeUUID(datanodeDetails.getUuidString())
        .setClusterID(this.clusterID)
        .setHostname(datanodeDetails.getHostName())
        .setIpAddress(datanodeDetails.getIpAddress())
        .build();
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @return SCMheartbeat response.
   * @throws IOException
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails) {
    Preconditions.checkNotNull(datanodeDetails, "Heartbeat is missing " +
        "DatanodeDetails.");
    try {
      nodeStateManager.updateLastHeartbeatTime(datanodeDetails);
    } catch (NodeNotFoundException e) {
      LOG.warn("SCM receive heartbeat from unregistered datanode {}",
          datanodeDetails);
      commandQueue.addCommand(datanodeDetails.getUuid(),
          new ReregisterCommand());
    }
    return commandQueue.getCommand(datanodeDetails.getUuid());
  }

  /**
   * Process node report.
   *
   * @param dnUuid
   * @param nodeReport
   */
  @Override
  public void processNodeReport(UUID dnUuid, NodeReportProto nodeReport) {
    this.updateNodeStat(dnUuid, nodeReport);
  }

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return new SCMNodeStat(this.scmStat);
  }

  /**
   * Return a map of node stats.
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<UUID, SCMNodeStat> getNodeStats() {
    return Collections.unmodifiableMap(nodeStats);
  }

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails - datanode ID.
   * @return node stat if it is live/stale, null if it is dead or does't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    return new SCMNodeMetric(nodeStats.get(datanodeDetails.getUuid()));
  }

  @Override
  public Map<String, Integer> getNodeCount() {
    Map<String, Integer> nodeCountMap = new HashMap<String, Integer>();
    for(NodeState state : NodeState.values()) {
      nodeCountMap.put(state.toString(), getNodeCount(state));
    }
    return nodeCountMap;
  }

  // TODO:
  // Since datanode commands are added through event queue, onMessage method
  // should take care of adding commands to command queue.
  // Refactor and remove all the usage of this method and delete this method.
  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
    this.commandQueue.addCommand(dnId, command);
  }

  /**
   * This method is called by EventQueue whenever someone adds a new
   * DATANODE_COMMAND to the Queue.
   *
   * @param commandForDatanode DatanodeCommand
   * @param ignored publisher
   */
  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
      EventPublisher ignored) {
    addDatanodeCommand(commandForDatanode.getDatanodeId(),
        commandForDatanode.getCommand());
  }
}
