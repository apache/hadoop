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
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.NodeAlreadyExistsException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.node.states.ReportResult;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
  // Should we maintain aggregated stats? If this is not frequently used, we
  // can always calculate it from nodeStats whenever required.
  // Aggregated node stats
  private SCMNodeStat scmStat;
  private final String clusterID;
  private final VersionInfo version;
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
    this.scmStat = new SCMNodeStat();
    this.clusterID = clusterID;
    this.version = VersionInfo.getLatestVersion();
    this.commandQueue = new CommandQueue();
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
    SCMNodeStat stat;
    try {
      stat = nodeStateManager.getNodeStat(dnId);
    } catch (NodeNotFoundException e) {
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
      scmStat.add(stat);
    }
    nodeStateManager.setNodeStat(dnId, stat);
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
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto) {

    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      datanodeDetails.setHostName(dnAddress.getHostName());
      datanodeDetails.setIpAddress(dnAddress.getHostAddress());
    }
    UUID dnId = datanodeDetails.getUuid();
    try {
      nodeStateManager.addNode(datanodeDetails);
      nodeStateManager.setNodeStat(dnId, new SCMNodeStat());
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
    return nodeStateManager.getNodeStatsMap();
  }

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails - datanode ID.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    try {
      return new SCMNodeMetric(
          nodeStateManager.getNodeStat(datanodeDetails.getUuid()));
    } catch (NodeNotFoundException e) {
      LOG.info("SCM getNodeStat from a decommissioned or removed datanode {}",
          datanodeDetails.getUuid());
      return null;
    }
  }

  @Override
  public Map<String, Integer> getNodeCount() {
    Map<String, Integer> nodeCountMap = new HashMap<String, Integer>();
    for(NodeState state : NodeState.values()) {
      nodeCountMap.put(state.toString(), getNodeCount(state));
    }
    return nodeCountMap;
  }

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelineByDnID(UUID dnId) {
    return nodeStateManager.getPipelineByDnID(dnId);
  }


  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    nodeStateManager.addPipeline(pipeline);
  }

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    nodeStateManager.removePipeline(pipeline);
  }

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws SCMException - if datanode is not known. For new datanode use
   *                        addDatanodeInContainerMap call.
   */
  @Override
  public void setContainersForDatanode(UUID uuid,
      Set<ContainerID> containerIds) throws SCMException {
    nodeStateManager.setContainersForDatanode(uuid, containerIds);
  }

  /**
   * Process containerReport received from datanode.
   * @param uuid - DataonodeID
   * @param containerIds - Set of containerIDs
   * @return The result after processing containerReport
   */
  @Override
  public ReportResult<ContainerID> processContainerReport(UUID uuid,
      Set<ContainerID> containerIds) {
    return nodeStateManager.processContainerReport(uuid, containerIds);
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(UUID uuid) {
    return nodeStateManager.getContainers(uuid);
  }

  /**
   * Insert a new datanode with set of containerIDs for containers available
   * on it.
   * @param uuid - DatanodeID
   * @param containerIDs - Set of ContainerIDs
   * @throws SCMException - if datanode already exists
   */
  @Override
  public void addDatanodeInContainerMap(UUID uuid,
      Set<ContainerID> containerIDs) throws SCMException {
    nodeStateManager.addDatanodeInContainerMap(uuid, containerIDs);
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

  /**
   * Update the node stats and cluster storage stats in this SCM Node Manager.
   *
   * @param dnUuid datanode uuid.
   */
  @Override
  public void processDeadNode(UUID dnUuid) {
    try {
      SCMNodeStat stat = nodeStateManager.getNodeStat(dnUuid);
      if (stat != null) {
        LOG.trace("Update stat values as Datanode {} is dead.", dnUuid);
        scmStat.subtract(stat);
        stat.set(0, 0, 0);
      }
    } catch (NodeNotFoundException e) {
      LOG.warn("Can't update stats based on message of dead Datanode {}, it"
          + " doesn't exist or decommissioned already.", dnUuid);
    }
  }
}
