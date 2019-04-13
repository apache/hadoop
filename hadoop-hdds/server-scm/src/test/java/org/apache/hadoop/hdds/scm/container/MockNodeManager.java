/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.Node2ContainerMap;
import org.apache.hadoop.hdds.scm.node.states.Node2PipelineMap;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.assertj.core.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;

/**
 * Test Helper for testing container Mapping.
 */
public class MockNodeManager implements NodeManager {
  private final static NodeData[] NODES = {
      new NodeData(10L * OzoneConsts.TB, OzoneConsts.GB),
      new NodeData(64L * OzoneConsts.TB, 100 * OzoneConsts.GB),
      new NodeData(128L * OzoneConsts.TB, 256 * OzoneConsts.GB),
      new NodeData(40L * OzoneConsts.TB, OzoneConsts.TB),
      new NodeData(256L * OzoneConsts.TB, 200 * OzoneConsts.TB),
      new NodeData(20L * OzoneConsts.TB, 10 * OzoneConsts.GB),
      new NodeData(32L * OzoneConsts.TB, 16 * OzoneConsts.TB),
      new NodeData(OzoneConsts.TB, 900 * OzoneConsts.GB),
      new NodeData(OzoneConsts.TB, 900 * OzoneConsts.GB, NodeData.STALE),
      new NodeData(OzoneConsts.TB, 200L * OzoneConsts.GB, NodeData.STALE),
      new NodeData(OzoneConsts.TB, 200L * OzoneConsts.GB, NodeData.DEAD)
  };
  private final List<DatanodeDetails> healthyNodes;
  private final List<DatanodeDetails> staleNodes;
  private final List<DatanodeDetails> deadNodes;
  private final Map<DatanodeDetails, SCMNodeStat> nodeMetricMap;
  private final SCMNodeStat aggregateStat;
  private boolean safemode;
  private final Map<UUID, List<SCMCommand>> commandMap;
  private final Node2PipelineMap node2PipelineMap;
  private final Node2ContainerMap node2ContainerMap;

  public MockNodeManager(boolean initializeFakeNodes, int nodeCount) {
    this.healthyNodes = new LinkedList<>();
    this.staleNodes = new LinkedList<>();
    this.deadNodes = new LinkedList<>();
    this.nodeMetricMap = new HashMap<>();
    this.node2PipelineMap = new Node2PipelineMap();
    this.node2ContainerMap = new Node2ContainerMap();
    aggregateStat = new SCMNodeStat();
    if (initializeFakeNodes) {
      for (int x = 0; x < nodeCount; x++) {
        DatanodeDetails dd = TestUtils.randomDatanodeDetails();
        register(dd, null, null);
        populateNodeMetric(dd, x);
      }
    }
    safemode = false;
    this.commandMap = new HashMap<>();
  }

  /**
   * Invoked from ctor to create some node Metrics.
   *
   * @param datanodeDetails - Datanode details
   */
  private void populateNodeMetric(DatanodeDetails datanodeDetails, int x) {
    SCMNodeStat newStat = new SCMNodeStat();
    long remaining =
        NODES[x % NODES.length].capacity - NODES[x % NODES.length].used;
    newStat.set(
        (NODES[x % NODES.length].capacity),
        (NODES[x % NODES.length].used), remaining);
    this.nodeMetricMap.put(datanodeDetails, newStat);
    aggregateStat.add(newStat);

    if (NODES[x % NODES.length].getCurrentState() == NodeData.HEALTHY) {
      healthyNodes.add(datanodeDetails);
    }

    if (NODES[x % NODES.length].getCurrentState() == NodeData.STALE) {
      staleNodes.add(datanodeDetails);
    }

    if (NODES[x % NODES.length].getCurrentState() == NodeData.DEAD) {
      deadNodes.add(datanodeDetails);
    }

  }

  /**
   * Sets the safe mode value.
   * @param safemode boolean
   */
  public void setSafemode(boolean safemode) {
    this.safemode = safemode;
  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(HddsProtos.NodeState nodestate) {
    if (nodestate == HEALTHY) {
      return healthyNodes;
    }

    if (nodestate == STALE) {
      return staleNodes;
    }

    if (nodestate == DEAD) {
      return deadNodes;
    }

    return null;
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(HddsProtos.NodeState nodestate) {
    List<DatanodeDetails> nodes = getNodes(nodestate);
    if (nodes != null) {
      return nodes.size();
    }
    return 0;
  }

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeDetails known to SCM.
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    return null;
  }

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return aggregateStat;
  }

  /**
   * Return a map of nodes to their stats.
   * @return a list of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {
    return nodeMetricMap;
  }

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails - datanode details.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    SCMNodeStat stat = nodeMetricMap.get(datanodeDetails);
    if (stat == null) {
      return null;
    }
    return new SCMNodeMetric(stat);
  }

  /**
   * Returns the node state of a specific node.
   *
   * @param dd - DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  @Override
  public HddsProtos.NodeState getNodeState(DatanodeDetails dd) {
    return null;
  }

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails dnId) {
    return node2PipelineMap.getPipelines(dnId.getUuid());
  }

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    node2PipelineMap.addPipeline(pipeline);
  }

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    node2PipelineMap.removePipeline(pipeline);
  }

  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
    if(commandMap.containsKey(dnId)) {
      List<SCMCommand> commandList = commandMap.get(dnId);
      Preconditions.checkNotNull(commandList);
      commandList.add(command);
    } else {
      List<SCMCommand> commandList = new LinkedList<>();
      commandList.add(command);
      commandMap.put(dnId, commandList);
    }
  }

  /**
   * Empty implementation for processNodeReport.
   *
   * @param dnUuid
   * @param nodeReport
   */
  @Override
  public void processNodeReport(DatanodeDetails dnUuid,
      NodeReportProto nodeReport) {
    // do nothing
  }

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws SCMException - if datanode is not known. For new datanode use
   *                        addDatanodeInContainerMap call.
   */
  @Override
  public void setContainers(DatanodeDetails uuid, Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    try {
      node2ContainerMap.setContainersForDatanode(uuid.getUuid(), containerIds);
    } catch (SCMException e) {
      throw new NodeNotFoundException(e.getMessage());
    }
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails uuid) {
    return node2ContainerMap.getContainers(uuid.getUuid());
  }

  // Returns the number of commands that is queued to this node manager.
  public int getCommandCount(DatanodeDetails dd) {
    List<SCMCommand> list = commandMap.get(dd.getUuid());
    return (list == null) ? 0 : list.size();
  }

  public void clearCommandQueue(UUID dnId) {
    if(commandMap.containsKey(dnId)) {
      commandMap.put(dnId, new LinkedList<>());
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {

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
    return null;
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeReport NodeReportProto
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public RegisteredCommand register(DatanodeDetails datanodeDetails,
      NodeReportProto nodeReport, PipelineReportsProto pipelineReportsProto) {
    try {
      node2ContainerMap.insertNewDatanode(datanodeDetails.getUuid(),
          Collections.emptySet());
    } catch (SCMException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - Datanode ID.
   * @return SCMheartbeat response list
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails) {
    return null;
  }

  @Override
  public Boolean isNodeRegistered(
      DatanodeDetails datanodeDetails) {
    return null;
  }

  @Override
  public Map<String, Integer> getNodeCount() {
    Map<String, Integer> nodeCountMap = new HashMap<String, Integer>();
    for (HddsProtos.NodeState state : HddsProtos.NodeState.values()) {
      nodeCountMap.put(state.toString(), getNodeCount(state));
    }
    return nodeCountMap;
  }

  @Override
  public Map<String, Long> getNodeInfo() {
    Map<String, Long> nodeInfo = new HashMap<>();
    nodeInfo.put("Capacity", aggregateStat.getCapacity().get());
    nodeInfo.put("Used", aggregateStat.getScmUsed().get());
    nodeInfo.put("Remaining", aggregateStat.getRemaining().get());
    return nodeInfo;
  }

  /**
   * Makes it easy to add a container.
   *
   * @param datanodeDetails datanode details
   * @param size number of bytes.
   */
  public void addContainer(DatanodeDetails datanodeDetails, long size) {
    SCMNodeStat stat = this.nodeMetricMap.get(datanodeDetails);
    if (stat != null) {
      aggregateStat.subtract(stat);
      stat.getCapacity().add(size);
      aggregateStat.add(stat);
      nodeMetricMap.put(datanodeDetails, stat);
    }
  }

  /**
   * Makes it easy to simulate a delete of a container.
   *
   * @param datanodeDetails datanode Details
   * @param size number of bytes.
   */
  public void delContainer(DatanodeDetails datanodeDetails, long size) {
    SCMNodeStat stat = this.nodeMetricMap.get(datanodeDetails);
    if (stat != null) {
      aggregateStat.subtract(stat);
      stat.getCapacity().subtract(size);
      aggregateStat.add(stat);
      nodeMetricMap.put(datanodeDetails, stat);
    }
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher publisher) {
    addDatanodeCommand(commandForDatanode.getDatanodeId(),
        commandForDatanode.getCommand());
  }

  @Override
  public List<SCMCommand> getCommandQueue(UUID dnID) {
    return null;
  }

  /**
   * A class to declare some values for the nodes so that our tests
   * won't fail.
   */
  private static class NodeData {
    public static final long HEALTHY = 1;
    public static final long STALE = 2;
    public static final long DEAD = 3;

    private long capacity;
    private long used;

    private long currentState;

    /**
     * By default nodes are healthy.
     * @param capacity
     * @param used
     */
    NodeData(long capacity, long used) {
      this(capacity, used, HEALTHY);
    }

    /**
     * Constructs a nodeDefinition.
     *
     * @param capacity capacity.
     * @param used used.
     * @param currentState - Healthy, Stale and DEAD nodes.
     */
    NodeData(long capacity, long used, long currentState) {
      this.capacity = capacity;
      this.used = used;
      this.currentState = currentState;
    }

    public long getCapacity() {
      return capacity;
    }

    public void setCapacity(long capacity) {
      this.capacity = capacity;
    }

    public long getUsed() {
      return used;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public long getCurrentState() {
      return currentState;
    }

    public void setCurrentState(long currentState) {
      this.currentState = currentState;
    }

  }
}
