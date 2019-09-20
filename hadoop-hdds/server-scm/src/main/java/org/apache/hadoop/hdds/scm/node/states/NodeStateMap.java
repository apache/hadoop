/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node.states;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Maintains the state of datanodes in SCM. This class should only be used by
 * NodeStateManager to maintain the state. If anyone wants to change the
 * state of a node they should call NodeStateManager, do not directly use
 * this class.
 */
public class NodeStateMap {
  /**
   * Node id to node info map.
   */
  private final ConcurrentHashMap<UUID, DatanodeInfo> nodeMap;
  /**
   * Node to set of containers on the node.
   */
  private final ConcurrentHashMap<UUID, Set<ContainerID>> nodeToContainer;

  private final ReadWriteLock lock;

  /**
   * Creates a new instance of NodeStateMap with no nodes.
   */
  public NodeStateMap() {
    lock = new ReentrantReadWriteLock();
    nodeMap = new ConcurrentHashMap<>();
    nodeToContainer = new ConcurrentHashMap<>();
  }

  /**
   * Adds a node to NodeStateMap.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeStatus initial NodeStatus
   *
   * @throws NodeAlreadyExistsException if the node already exist
   */
  public void addNode(DatanodeDetails datanodeDetails, NodeStatus nodeStatus)
      throws NodeAlreadyExistsException {
    lock.writeLock().lock();
    try {
      UUID id = datanodeDetails.getUuid();
      if (nodeMap.containsKey(id)) {
        throw new NodeAlreadyExistsException("Node UUID: " + id);
      }
      nodeMap.put(id, new DatanodeInfo(datanodeDetails, nodeStatus));
      nodeToContainer.put(id, Collections.emptySet());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the node health state.
   *
   * @param nodeId Node Id
   * @param newHealth new health state
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public NodeStatus updateNodeHealthState(UUID nodeId, NodeState newHealth)
      throws NodeNotFoundException {
    try {
      lock.writeLock().lock();
      DatanodeInfo dn = getNodeInfo(nodeId);
      NodeStatus oldStatus = dn.getNodeStatus();
      NodeStatus newStatus = new NodeStatus(
          oldStatus.getOperationalState(), newHealth);
      dn.setNodeStatus(newStatus);
      return newStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the node operational state.
   *
   * @param nodeId Node Id
   * @param newOpState new operational state
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public NodeStatus updateNodeOperationalState(UUID nodeId,
      NodeOperationalState newOpState) throws NodeNotFoundException {
    try {
      lock.writeLock().lock();
      DatanodeInfo dn = getNodeInfo(nodeId);
      NodeStatus oldStatus = dn.getNodeStatus();
      NodeStatus newStatus = new NodeStatus(
          newOpState, oldStatus.getHealth());
      dn.setNodeStatus(newStatus);
      return newStatus;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns DatanodeInfo for the given node id.
   *
   * @param uuid Node Id
   *
   * @return DatanodeInfo of the node
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public DatanodeInfo getNodeInfo(UUID uuid) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      checkIfNodeExist(uuid);
      return nodeMap.get(uuid);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of node ids which are in the specified state.
   *
   * @param status NodeStatus
   *
   * @return list of node ids
   */
  public List<UUID> getNodes(NodeStatus status) {
    ArrayList<UUID> nodes = new ArrayList<>();
    for (DatanodeInfo dn : filterNodes(status)) {
      nodes.add(dn.getUuid());
    }
    return nodes;
  }

  /**
   * Returns the list of node ids which match the desired operational state
   * and health. Passing a null for either value is equivalent to a wild card.
   *
   * Therefore, passing opState = null, health=stale will return all stale nodes
   * regardless of their operational state.
   *
   * @param opState
   * @param health
   * @return The list of nodes matching the given states
   */
  public List<UUID> getNodes(NodeOperationalState opState, NodeState health) {
    ArrayList<UUID> nodes = new ArrayList<>();
    for (DatanodeInfo dn : filterNodes(opState, health)) {
      nodes.add(dn.getUuid());
    }
    return nodes;
  }

  /**
   * Returns the list of all the node ids.
   *
   * @return list of all the node ids
   */
  public List<UUID> getAllNodes() {
    try {
      lock.readLock().lock();
      return new ArrayList<>(nodeMap.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of all the nodes as DatanodeInfo objects.
   *
   * @return list of all the node ids
   */
  public List<DatanodeInfo> getAllDatanodeInfos() {
    try {
      lock.readLock().lock();
      return new ArrayList<>(nodeMap.values());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns a list of the nodes as DatanodeInfo objects matching the passed
   * status.
   *
   * @param status - The status of the nodes to return
   * @return List of DatanodeInfo for the matching nodes
   */
  public List<DatanodeInfo> getDatanodeInfos(NodeStatus status) {
    return filterNodes(status);
  }

  /**
   * Returns a list of the nodes as DatanodeInfo objects matching the passed
   * states. Passing null for either of the state values acts as a wildcard
   * for that state.
   *
   * @param opState - The node operational state
   * @param health - The node health
   * @return List of DatanodeInfo for the matching nodes
   */
  public List<DatanodeInfo> getDatanodeInfos(
      NodeOperationalState opState, NodeState health) {
    return filterNodes(opState, health);
  }

  /**
   * Returns the count of nodes in the specified state.
   *
   * @param state NodeStatus
   *
   * @return Number of nodes in the specified state
   */
  public int getNodeCount(NodeStatus state) {
    return getNodes(state).size();
  }

  /**
   * Returns the count of node ids which match the desired operational state
   * and health. Passing a null for either value is equivalent to a wild card.
   *
   * Therefore, passing opState=null, health=stale will count all stale nodes
   * regardless of their operational state.
   *
   * @param opState
   * @param health
   *
   * @return Number of nodes in the specified state
   */
  public int getNodeCount(NodeOperationalState opState, NodeState health) {
    return getNodes(opState, health).size();
  }

  /**
   * Returns the total node count.
   *
   * @return node count
   */
  public int getTotalNodeCount() {
    lock.readLock().lock();
    try {
      return nodeMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the current state of the node.
   *
   * @param uuid node id
   *
   * @return NodeState
   *
   * @throws NodeNotFoundException if the node is not found
   */
  public NodeStatus getNodeStatus(UUID uuid) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      DatanodeInfo dn = nodeMap.get(uuid);
      if (dn == null) {
        throw new NodeNotFoundException("Node not found in node map." +
            " UUID: " + uuid);
      }
      return dn.getNodeStatus();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Adds the given container to the specified datanode.
   *
   * @param uuid - datanode uuid
   * @param containerId - containerID
   * @throws NodeNotFoundException - if datanode is not known. For new datanode
   *                        use addDatanodeInContainerMap call.
   */
  public void addContainer(final UUID uuid,
                           final ContainerID containerId)
      throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      checkIfNodeExist(uuid);
      nodeToContainer.get(uuid).add(containerId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setContainers(UUID uuid, Set<ContainerID> containers)
      throws NodeNotFoundException{
    lock.writeLock().lock();
    try {
      checkIfNodeExist(uuid);
      nodeToContainer.put(uuid, containers);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<ContainerID> getContainers(UUID uuid)
      throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      checkIfNodeExist(uuid);
      return Collections.unmodifiableSet(nodeToContainer.get(uuid));
    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeContainer(UUID uuid, ContainerID containerID) throws
      NodeNotFoundException {
    lock.writeLock().lock();
    try {
      checkIfNodeExist(uuid);
      nodeToContainer.get(uuid).remove(containerID);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Since we don't hold a global lock while constructing this string,
   * the result might be inconsistent. If someone has changed the state of node
   * while we are constructing the string, the result will be inconsistent.
   * This should only be used for logging. We should not parse this string and
   * use it for any critical calculations.
   *
   * @return current state of NodeStateMap
   */
  @Override
  public String toString() {
    // TODO - fix this method to include the commented out values
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of nodes: ").append(getTotalNodeCount());
   // for (NodeState state : NodeState.values()) {
   //   builder.append("Number of nodes in ").append(state).append(" state: ")
   //       .append(getNodeCount(state));
   // }
    return builder.toString();
  }

  /**
   * Throws NodeNotFoundException if the Node for given id doesn't exist.
   *
   * @param uuid Node UUID
   * @throws NodeNotFoundException If the node is missing.
   */
  private void checkIfNodeExist(UUID uuid) throws NodeNotFoundException {
    if (!nodeToContainer.containsKey(uuid)) {
      throw new NodeNotFoundException("Node UUID: " + uuid);
    }
  }

  /**
   * Create a list of datanodeInfo for all nodes matching the passed states.
   * Passing null for one of the states acts like a wildcard for that state.
   *
   * @param opState
   * @param health
   * @return List of DatanodeInfo objects matching the passed state
   */
  private List<DatanodeInfo> filterNodes(
      NodeOperationalState opState, NodeState health) {
    if (opState != null && health != null) {
      return filterNodes(new NodeStatus(opState, health));
    }
    if (opState == null && health == null) {
      return getAllDatanodeInfos();
    }
    try {
      lock.readLock().lock();
      return nodeMap.values().stream()
          .filter(n -> opState == null
              || n.getNodeStatus().getOperationalState() == opState)
          .filter(n -> health == null
              || n.getNodeStatus().getHealth() == health)
          .collect(Collectors.toList());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Create a list of datanodeInfo for all nodes matching the passsed status.
   *
   * @param status
   * @return List of DatanodeInfo objects matching the passed state
   */
  private List<DatanodeInfo> filterNodes(NodeStatus status) {
    try {
      lock.readLock().lock();
      return nodeMap.values().stream()
          .filter(n -> n.getNodeStatus().equals(status))
          .collect(Collectors.toList());
    }  finally {
      lock.readLock().unlock();
    }
  }
}
