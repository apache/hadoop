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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * This abstract class provides some base methods which are inherited by
 * the DatanodeAdmin BackOff and Default Monitors, which control decommission
 * and maintenance mode.
 */
public abstract class DatanodeAdminMonitorBase
    implements DatanodeAdminMonitorInterface, Configurable {

  /**
   * Sort by lastUpdate time descending order, such that unhealthy
   * nodes are de-prioritized given they cannot be decommissioned.
   */
  static final Comparator<DatanodeDescriptor> PENDING_NODES_QUEUE_COMPARATOR =
      (dn1, dn2) -> Long.compare(dn2.getLastUpdate(), dn1.getLastUpdate());

  protected BlockManager blockManager;
  protected Namesystem namesystem;
  protected DatanodeAdminManager dnAdmin;
  protected Configuration conf;

  private final PriorityQueue<DatanodeDescriptor> pendingNodes = new PriorityQueue<>(
      PENDING_NODES_QUEUE_COMPARATOR);

  /**
   * Any nodes where decommission or maintenance has been cancelled are added
   * to this queue for later processing.
   */
  private final Queue<DatanodeDescriptor> cancelledNodes = new ArrayDeque<>();

  /**
   * The maximum number of nodes to track in outOfServiceNodeBlocks.
   * A value of 0 means no limit.
   */
  protected int maxConcurrentTrackedNodes;

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminMonitorBase.class);

  /**
   * Set the cluster namesystem.
   *
   * @param ns The namesystem for the cluster
   */
  @Override
  public void setNameSystem(Namesystem ns) {
    this.namesystem = ns;
  }

  /**
   * Set the blockmanager for the cluster.
   *
   * @param bm The cluster BlockManager
   */
  @Override
  public void setBlockManager(BlockManager bm) {
    this.blockManager = bm;
  }

  /**
   * Set the DatanodeAdminManager instance in use in the namenode.
   *
   * @param admin The current DatanodeAdminManager
   */
  @Override
  public void setDatanodeAdminManager(DatanodeAdminManager admin) {
    this.dnAdmin = admin;
  }

  /**
   * Used by the Configurable interface, which is used by ReflectionUtils
   * to create an instance of the monitor class. This method will be called to
   * pass the Configuration to the new object.
   *
   * @param conf configuration to be used
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.maxConcurrentTrackedNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        DFSConfigKeys
            .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
    if (this.maxConcurrentTrackedNodes < 0) {
      LOG.error("{} is set to an invalid value, it must be zero or greater. "+
              "Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
          DFSConfigKeys
              .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
      this.maxConcurrentTrackedNodes =
          DFSConfigKeys
              .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT;
    }

    LOG.debug("Activating DatanodeAdminMonitor with {} max concurrently tracked nodes.",
        maxConcurrentTrackedNodes);

    processConf();
  }

  /**
   * Get the current Configuration stored in this object.
   *
   * @return Configuration used when the object was created
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Abstract method which must be implemented by the sub-classes to process
   * set various instance variables from the Configuration passed at object
   * creation time.
   */
  protected abstract void processConf();

  /**
   * Start tracking a node for decommission or maintenance. The given Datanode
   * will be queued for later processing in pendingNodes. This method must be
   * called under the namenode write lock.
   * @param dn The datanode to start tracking
   */
  @Override
  public void startTrackingNode(DatanodeDescriptor dn) {
    pendingNodes.add(dn);
  }

  /**
   * Get the number of datanodes nodes in the pending queue. Ie the count of
   * nodes waiting to decommission but have not yet started the process.
   *
   * @return The count of pending nodes
   */
  @Override
  public int getPendingNodeCount() {
    return pendingNodes.size();
  }

  @Override
  public Queue<DatanodeDescriptor> getPendingNodes() {
    return pendingNodes;
  }

  @Override
  public Queue<DatanodeDescriptor> getCancelledNodes() {
    return cancelledNodes;
  }

  /**
   * If node "is dead while in Decommission In Progress", it cannot be decommissioned
   * until it becomes healthy again. If there are more pendingNodes than can be tracked
   * & some unhealthy tracked nodes, then re-queue the unhealthy tracked nodes
   * to avoid blocking decommissioning of healthy nodes.
   *
   * @param unhealthyDns The unhealthy datanodes which may be re-queued
   * @param numDecommissioningNodes The total number of nodes being decommissioned
   * @return Stream of unhealthy nodes to be re-queued
   */
  Stream<DatanodeDescriptor> getUnhealthyNodesToRequeue(
      final List<DatanodeDescriptor> unhealthyDns, int numDecommissioningNodes) {
    if (!unhealthyDns.isEmpty()) {
      // Compute the number of unhealthy nodes to re-queue
      final int numUnhealthyNodesToRequeue =
          Math.min(numDecommissioningNodes - maxConcurrentTrackedNodes, unhealthyDns.size());

      LOG.warn("{} limit has been reached, re-queueing {} "
              + "nodes which are dead while in Decommission In Progress.",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
          numUnhealthyNodesToRequeue);

      // Order unhealthy nodes by lastUpdate descending such that nodes
      // which have been unhealthy the longest are preferred to be re-queued
      return unhealthyDns.stream().sorted(PENDING_NODES_QUEUE_COMPARATOR.reversed())
          .limit(numUnhealthyNodesToRequeue);
    }
    return Stream.empty();
  }
}
