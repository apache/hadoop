/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.container;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.scm.node.NodeManager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Test Helper for testing container Mapping.
 */
public class MockNodeManager implements NodeManager {
  private final List<DatanodeID> healthyNodes;
  private static final int HEALTHY_NODE_COUNT = 10;
  private boolean chillmode;

  public MockNodeManager() {
    this.healthyNodes = new LinkedList<>();
    for (int x = 0; x < 10; x++) {
      healthyNodes.add(SCMTestUtils.getDatanodeID());
    }
    chillmode = false;
  }

  /**
   * Sets the chill mode value.
   * @param chillmode  boolean
   */
  public void setChillmode(boolean chillmode) {
    this.chillmode = chillmode;
  }

  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws UnregisteredNodeException
   */
  @Override
  public void removeNode(DatanodeID node) throws UnregisteredNodeException {

  }

  /**
   * Gets all Live Datanodes that is currently communicating with SCM.
   *
   * @param nodestate - State of the node
   * @return List of Datanodes that are Heartbeating SCM.
   */
  @Override
  public List<DatanodeID> getNodes(NODESTATE nodestate) {
    if (nodestate == NODESTATE.HEALTHY) {
      return healthyNodes;
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
  public int getNodeCount(NODESTATE nodestate) {
    if (nodestate == NODESTATE.HEALTHY) {
      return HEALTHY_NODE_COUNT;
    }
    return 0;
  }

  /**
   * Get all datanodes known to SCM.
   *
   * @return List of DatanodeIDs known to SCM.
   */
  @Override
  public List<DatanodeID> getAllNodes() {
    return null;
  }

  /**
   * Get the minimum number of nodes to get out of chill mode.
   *
   * @return int
   */
  @Override
  public int getMinimumChillModeNodes() {
    return 0;
  }

  /**
   * Reports if we have exited out of chill mode by discovering enough nodes.
   *
   * @return True if we are out of Node layer chill mode, false otherwise.
   */
  @Override
  public boolean isOutOfNodeChillMode() {
    return !chillmode;
  }

  /**
   * Chill mode is the period when node manager waits for a minimum configured
   * number of datanodes to report in. This is called chill mode to indicate the
   * period before node manager gets into action.
   * <p>
   * Forcefully exits the chill mode, even if we have not met the minimum
   * criteria of the nodes reporting in.
   */
  @Override
  public void forceExitChillMode() {

  }

  /**
   * Forcefully enters chill mode, even if all minimum node conditions are met.
   */
  @Override
  public void forceEnterChillMode() {

  }

  /**
   * Clears the manual chill mode flag.
   */
  @Override
  public void clearChillModeFlag() {

  }

  /**
   * Returns a chill mode status string.
   *
   * @return String
   */
  @Override
  public String getChillModeStatus() {
    return null;
  }

  /**
   * Returns the status of manual chill mode flag.
   *
   * @return true if forceEnterChillMode has been called, false if
   * forceExitChillMode or status is not set. eg. clearChillModeFlag.
   */
  @Override
  public boolean isInManualChillMode() {
    return false;
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
   * When an object implementing interface <code>Runnable</code> is used to
   * create a thread, starting the thread causes the object's <code>run</code>
   * method to be called in that separately executing thread.
   * <p>
   * The general contract of the method <code>run</code> is that it may take any
   * action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {

  }

  /**
   * Gets the version info from SCM.
   *
   * @param versionRequest - version Request.
   * @return - returns SCM version info and other required information needed by
   * datanode.
   */
  @Override
  public VersionResponse getVersion(StorageContainerDatanodeProtocolProtos
      .SCMVersionRequestProto versionRequest) {
    return null;
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeID - Send datanodeID with Node info, but datanode UUID is
   * empty. Server returns a datanodeID for the given node.
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public SCMCommand register(DatanodeID datanodeID) {
    return null;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeID - Datanode ID.
   * @return SCMheartbeat response list
   */
  @Override
  public List<SCMCommand> sendHeartbeat(DatanodeID datanodeID) {
    return null;
  }
}
