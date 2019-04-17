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
package org.apache.hadoop.ozone.container.testutils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
        .StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.CommandQueue;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A Node Manager to test replication.
 */
public class ReplicationNodeManagerMock implements NodeManager {
  private final Map<DatanodeDetails, NodeState> nodeStateMap;
  private final CommandQueue commandQueue;

  /**
   * A list of Datanodes and current states.
   * @param nodeState A node state map.
   */
  public ReplicationNodeManagerMock(Map<DatanodeDetails, NodeState> nodeState,
                                    CommandQueue commandQueue) {
    Preconditions.checkNotNull(nodeState);
    this.nodeStateMap = nodeState;
    this.commandQueue = commandQueue;
  }

  /**
   * Get the number of data nodes that in all states.
   *
   * @return A state to number of nodes that in this state mapping
   */
  @Override
  public Map<String, Integer> getNodeCount() {
    return null;
  }

  @Override
  public Map<String, Long> getNodeInfo() {
    return null;
  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeState nodestate) {
    return null;
  }

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return int -- count
   */
  @Override
  public int getNodeCount(NodeState nodestate) {
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
   *
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return null;
  }

  /**
   * Return a map of node stats.
   *
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<DatanodeDetails, SCMNodeStat> getNodeStats() {
    return null;
  }

  /**
   * Return the node stat of the specified datanode.
   *
   * @param dd - datanode details.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails dd) {
    return null;
  }


  /**
   * Returns the node state of a specific node.
   *
   * @param dd - DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  @Override
  public NodeState getNodeState(DatanodeDetails dd) {
    return nodeStateMap.get(dd);
  }

  /**
   * Get set of pipelines a datanode is part of.
   * @param dnId - datanodeID
   * @return Set of PipelineID
   */
  @Override
  public Set<PipelineID> getPipelines(DatanodeDetails dnId) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  @Override
  public void addPipeline(Pipeline pipeline) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  @Override
  public void removePipeline(Pipeline pipeline) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Update set of containers available on a datanode.
   * @param uuid - DatanodeID
   * @param containerIds - Set of containerIDs
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                                 use addDatanodeInContainerMap call.
   */
  @Override
  public void setContainers(DatanodeDetails uuid, Set<ContainerID> containerIds)
      throws NodeNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Return set of containerIDs available on a datanode.
   * @param uuid - DatanodeID
   * @return - set of containerIDs
   */
  @Override
  public Set<ContainerID> getContainers(DatanodeDetails uuid) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
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
   * Register the node if the node finds that it is not registered with any SCM.
   *
   * @param dd DatanodeDetailsProto
   * @param nodeReport NodeReportProto
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public RegisteredCommand register(DatanodeDetails dd,
                                    NodeReportProto nodeReport,
                                    PipelineReportsProto pipelineReportsProto) {
    return null;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param dd - Datanode Details.
   * @return SCMheartbeat response list
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails dd) {
    return null;
  }

  @Override
  public Boolean isNodeRegistered(
      DatanodeDetails datanodeDetails) {
    return null;
  }

  /**
   * Clears all nodes from the node Manager.
   */
  public void clearMap() {
    this.nodeStateMap.clear();
  }

  /**
   * Adds a node to the existing Node manager. This is used only for test
   * purposes.
   * @param id DatanodeDetails
   * @param state State you want to put that node to.
   */
  public void addNode(DatanodeDetails id, NodeState state) {
    nodeStateMap.put(id, state);
  }

  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
    this.commandQueue.addCommand(dnId, command);
  }

  /**
   * Empty implementation for processNodeReport.
   * @param dnUuid
   * @param nodeReport
   */
  @Override
  public void processNodeReport(DatanodeDetails dnUuid,
                                NodeReportProto nodeReport) {
    // do nothing.
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher publisher) {
    // do nothing.
  }

  @Override
  public List<SCMCommand> getCommandQueue(UUID dnID) {
    return null;
  }
}
