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

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.ozone.protocol.StorageContainerNodeProtocol;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A node manager supports a simple interface for managing a datanode.
 * <p>
 * 1. A datanode registers with the NodeManager.
 * <p>
 * 2. If the node is allowed to register, we add that to the nodes that we need
 * to keep track of.
 * <p>
 * 3. A heartbeat is made by the node at a fixed frequency.
 * <p>
 * 4. A node can be in any of these 4 states: {HEALTHY, STALE, DEAD,
 * DECOMMISSIONED}
 * <p>
 * HEALTHY - It is a datanode that is regularly heartbeating us.
 *
 * STALE - A datanode for which we have missed few heart beats.
 *
 * DEAD - A datanode that we have not heard from for a while.
 *
 * DECOMMISSIONED - Someone told us to remove this node from the tracking
 * list, by calling removeNode. We will throw away this nodes info soon.
 */
public interface NodeManager extends StorageContainerNodeProtocol,
    EventHandler<CommandForDatanode>, NodeManagerMXBean, Closeable {

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   * @param nodeState - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  List<DatanodeDetails> getNodes(NodeState nodeState);

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   * @param nodeState - State of the node
   * @return int -- count
   */
  int getNodeCount(NodeState nodeState);

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeDetails known to SCM.
   */
  List<DatanodeDetails> getAllNodes();

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  SCMNodeStat getStats();

  /**
   * Return a map of node stats.
   * @return a map of individual node stats (live/stale but not dead).
   */
  Map<DatanodeDetails, SCMNodeStat> getNodeStats();

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails DatanodeDetails.
   * @return node stat if it is live/stale, null if it is decommissioned or
   * doesn't exist.
   */
  SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails);

  /**
   * Returns the node state of a specific node.
   * @param datanodeDetails DatanodeDetails
   * @return Healthy/Stale/Dead.
   */
  NodeState getNodeState(DatanodeDetails datanodeDetails);

  /**
   * Get set of pipelines a datanode is part of.
   * @param datanodeDetails DatanodeDetails
   * @return Set of PipelineID
   */
  Set<PipelineID> getPipelines(DatanodeDetails datanodeDetails);

  /**
   * Add pipeline information in the NodeManager.
   * @param pipeline - Pipeline to be added
   */
  void addPipeline(Pipeline pipeline);

  /**
   * Remove a pipeline information from the NodeManager.
   * @param pipeline - Pipeline to be removed
   */
  void removePipeline(Pipeline pipeline);

  /**
   * Remaps datanode to containers mapping to the new set of containers.
   * @param datanodeDetails - DatanodeDetails
   * @param containerIds - Set of containerIDs
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  void setContainers(DatanodeDetails datanodeDetails,
      Set<ContainerID> containerIds) throws NodeNotFoundException;

  /**
   * Return set of containerIDs available on a datanode.
   * @param datanodeDetails DatanodeDetails
   * @return set of containerIDs
   */
  Set<ContainerID> getContainers(DatanodeDetails datanodeDetails)
      throws NodeNotFoundException;

  /**
   * Add a {@link SCMCommand} to the command queue, which are
   * handled by HB thread asynchronously.
   * @param dnId datanode uuid
   * @param command
   */
  void addDatanodeCommand(UUID dnId, SCMCommand command);

  /**
   * Process node report.
   *
   * @param datanodeDetails
   * @param nodeReport
   */
  void processNodeReport(DatanodeDetails datanodeDetails,
                         NodeReportProto nodeReport);

  /**
   * Get list of SCMCommands in the Command Queue for a particular Datanode.
   * @param dnID - Datanode uuid.
   * @return list of commands
   */
  // TODO: We can give better name to this method!
  List<SCMCommand> getCommandQueue(UUID dnID);
}
