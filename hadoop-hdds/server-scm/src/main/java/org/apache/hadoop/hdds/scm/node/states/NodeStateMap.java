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
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
   * Represents the current state of node.
   */
  private final ConcurrentHashMap<NodeState, Set<UUID>> stateMap;
  /**
   * Represents the current stats of node.
   */
  private final ConcurrentHashMap<UUID, SCMNodeStat> nodeStats;

  private final ReadWriteLock lock;

  /**
   * Creates a new instance of NodeStateMap with no nodes.
   */
  public NodeStateMap() {
    lock = new ReentrantReadWriteLock();
    nodeMap = new ConcurrentHashMap<>();
    stateMap = new ConcurrentHashMap<>();
    nodeStats = new ConcurrentHashMap<>();
    initStateMap();
  }

  /**
   * Initializes the state map with available states.
   */
  private void initStateMap() {
    for (NodeState state : NodeState.values()) {
      stateMap.put(state, new HashSet<>());
    }
  }

  /**
   * Adds a node to NodeStateMap.
   *
   * @param datanodeDetails DatanodeDetails
   * @param nodeState initial NodeState
   *
   * @throws NodeAlreadyExistsException if the node already exist
   */
  public void addNode(DatanodeDetails datanodeDetails, NodeState nodeState)
      throws NodeAlreadyExistsException {
    lock.writeLock().lock();
    try {
      UUID id = datanodeDetails.getUuid();
      if (nodeMap.containsKey(id)) {
        throw new NodeAlreadyExistsException("Node UUID: " + id);
      }
      nodeMap.put(id, new DatanodeInfo(datanodeDetails));
      stateMap.get(nodeState).add(id);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Updates the node state.
   *
   * @param nodeId Node Id
   * @param currentState current state
   * @param newState new state
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public void updateNodeState(UUID nodeId, NodeState currentState,
                              NodeState newState)throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      if (stateMap.get(currentState).remove(nodeId)) {
        stateMap.get(newState).add(nodeId);
      } else {
        throw new NodeNotFoundException("Node UUID: " + nodeId +
            ", not found in state: " + currentState);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns DatanodeDetails for the given node id.
   *
   * @param uuid Node Id
   *
   * @return DatanodeDetails of the node
   *
   * @throws NodeNotFoundException if the node is not present
   */
  public DatanodeDetails getNodeDetails(UUID uuid)
      throws NodeNotFoundException {
    return getNodeInfo(uuid);
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
      if (nodeMap.containsKey(uuid)) {
        return nodeMap.get(uuid);
      }
      throw new NodeNotFoundException("Node UUID: " + uuid);
    } finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Returns the list of node ids which are in the specified state.
   *
   * @param state NodeState
   *
   * @return list of node ids
   */
  public List<UUID> getNodes(NodeState state) {
    lock.readLock().lock();
    try {
      return new LinkedList<>(stateMap.get(state));
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the list of all the node ids.
   *
   * @return list of all the node ids
   */
  public List<UUID> getAllNodes() {
    lock.readLock().lock();
    try {
      return new LinkedList<>(nodeMap.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Returns the count of nodes in the specified state.
   *
   * @param state NodeState
   *
   * @return Number of nodes in the specified state
   */
  public int getNodeCount(NodeState state) {
    lock.readLock().lock();
    try {
      return stateMap.get(state).size();
    } finally {
      lock.readLock().unlock();
    }
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
  public NodeState getNodeState(UUID uuid) throws NodeNotFoundException {
    lock.readLock().lock();
    try {
      for (Map.Entry<NodeState, Set<UUID>> entry : stateMap.entrySet()) {
        if (entry.getValue().contains(uuid)) {
          return entry.getKey();
        }
      }
      throw new NodeNotFoundException("Node UUID: " + uuid);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Removes the node from NodeStateMap.
   *
   * @param uuid node id
   *
   * @throws NodeNotFoundException if the node is not found
   */
  public void removeNode(UUID uuid) throws NodeNotFoundException {
    lock.writeLock().lock();
    try {
      if (nodeMap.containsKey(uuid)) {
        for (Map.Entry<NodeState, Set<UUID>> entry : stateMap.entrySet()) {
          if(entry.getValue().remove(uuid)) {
            break;
          }
          nodeMap.remove(uuid);
        }
        throw new NodeNotFoundException("Node UUID: " + uuid);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Returns the current stats of the node.
   *
   * @param uuid node id
   *
   * @return SCMNodeStat of the specify node.
   *
   * @throws NodeNotFoundException if the node is not found
   */
  public SCMNodeStat getNodeStat(UUID uuid) throws NodeNotFoundException {
    SCMNodeStat stat = nodeStats.get(uuid);
    if (stat == null) {
      throw new NodeNotFoundException("Node UUID: " + uuid);
    }
    return stat;
  }

  /**
   * Returns a unmodifiable copy of nodeStats.
   *
   * @return map with node stats.
   */
  public Map<UUID, SCMNodeStat> getNodeStats() {
    return Collections.unmodifiableMap(nodeStats);
  }

  /**
   * Set the current stats of the node.
   *
   * @param uuid node id
   *
   * @param newstat stat that will set to the specify node.
   */
  public void setNodeStat(UUID uuid, SCMNodeStat newstat) {
    nodeStats.put(uuid, newstat);
  }

  /**
   * Remove the current stats of the specify node.
   *
   * @param uuid node id
   *
   * @return SCMNodeStat the stat removed from the node.
   *
   * @throws NodeNotFoundException if the node is not found
   */
  public SCMNodeStat removeNodeStat(UUID uuid) throws NodeNotFoundException {
    SCMNodeStat stat = nodeStats.remove(uuid);
    if (stat == null) {
      throw new NodeNotFoundException("Node UUID: " + uuid);
    }
    return stat;
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
    StringBuilder builder = new StringBuilder();
    builder.append("Total number of nodes: ").append(getTotalNodeCount());
    for (NodeState state : NodeState.values()) {
      builder.append("Number of nodes in ").append(state).append(" state: ")
          .append(getNodeCount(state));
    }
    return builder.toString();
  }
}
