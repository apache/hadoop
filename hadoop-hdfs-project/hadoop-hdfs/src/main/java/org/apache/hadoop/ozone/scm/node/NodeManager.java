/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.blockmanagement.UnresolvedTopologyException;

import java.io.Closeable;
import java.util.List;

/**
 * A node manager supports a simple interface for managing a datanode.
 * <p/>
 * 1. A datanode registers with the NodeManager.
 * <p/>
 * 2. If the node is allowed to register, we add that to the nodes that we need
 * to keep track of.
 * <p/>
 * 3. A heartbeat is made by the node at a fixed frequency.
 * <p/>
 * 4. A node can be in any of these 4 states: {HEALTHY, STALE, DEAD,
 * DECOMMISSIONED}
 * <p/>
 * HEALTHY - It is a datanode that is regularly heartbeating us.
 *
 * STALE - A datanode for which we have missed few heart beats.
 *
 * DEAD - A datanode that we have not heard from for a while.
 *
 * DECOMMISSIONED - Someone told us to remove this node from the tracking
 * list, by calling removeNode. We will throw away this nodes info soon.
 */
public interface NodeManager extends Closeable, Runnable {

  /**
   * Update the heartbeat timestamp.
   *
   * @param datanodeID - Name of the datanode that send us heatbeat.
   */
  void updateHeartbeat(DatanodeID datanodeID);

  /**
   * Add a New Datanode to the NodeManager.
   *
   * @param nodeReg - Datanode ID.
   * @throws UnresolvedTopologyException
   */
  void registerNode(DatanodeID nodeReg)
      throws UnresolvedTopologyException;

  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws UnregisteredNodeException
   */
  void removeNode(DatanodeID node) throws UnregisteredNodeException;

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @return List of Datanodes that are Heartbeating SCM.
   */

  List<DatanodeID> getNodes(NODESTATE nodestate);

  /**
   * Returns the Number of Datanodes that are communicating with SCM.
   *
   * @return int -- count
   */
  int getNodeCount(NODESTATE nodestate);

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeIDs known to SCM.
   */
  List<DatanodeID> getAllNodes();

  /**
   * Get the minimum number of nodes to get out of safe mode.
   *
   * @return int
   */
  int getMinimumSafeModeNodes();

  /**
   * Reports if we have exited out of safe mode by discovering enough nodes.
   *
   * @return True if we are out of Node layer safe mode, false otherwise.
   */
  boolean isOutOfNodeSafeMode();

  /**
   * Enum that represents the Node State. This is used in calls to getNodeList
   * and getNodeCount. TODO: Add decommission when we support it.
   */
  enum NODESTATE {
    HEALTHY,
    STALE,
    DEAD
  }

}
