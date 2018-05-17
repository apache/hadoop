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

import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodePoolManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pool Manager replication mock.
 */
public class ReplicationNodePoolManagerMock implements NodePoolManager {

  private final Map<DatanodeDetails, String> nodeMemberShip;

  /**
   * A node pool manager for testing.
   */
  public ReplicationNodePoolManagerMock() {
    nodeMemberShip = new HashMap<>();
  }

  /**
   * Add a node to a node pool.
   *
   * @param pool - name of the node pool.
   * @param node - data node.
   */
  @Override
  public void addNode(String pool, DatanodeDetails node) {
    nodeMemberShip.put(node, pool);
  }

  /**
   * Remove a node from a node pool.
   *
   * @param pool - name of the node pool.
   * @param node - data node.
   * @throws SCMException
   */
  @Override
  public void removeNode(String pool, DatanodeDetails node)
      throws SCMException {
    nodeMemberShip.remove(node);

  }

  /**
   * Get a list of known node pools.
   *
   * @return a list of known node pool names or an empty list if not node pool
   * is defined.
   */
  @Override
  public List<String> getNodePools() {
    Set<String> poolSet = new HashSet<>();
    for (Map.Entry<DatanodeDetails, String> entry : nodeMemberShip.entrySet()) {
      poolSet.add(entry.getValue());
    }
    return new ArrayList<>(poolSet);

  }

  /**
   * Get all nodes of a node pool given the name of the node pool.
   *
   * @param pool - name of the node pool.
   * @return a list of datanode ids or an empty list if the node pool was not
   * found.
   */
  @Override
  public List<DatanodeDetails> getNodes(String pool) {
    Set<DatanodeDetails> datanodeSet = new HashSet<>();
    for (Map.Entry<DatanodeDetails, String> entry : nodeMemberShip.entrySet()) {
      if (entry.getValue().equals(pool)) {
        datanodeSet.add(entry.getKey());
      }
    }
    return new ArrayList<>(datanodeSet);
  }

  /**
   * Get the node pool name if the node has been added to a node pool.
   *
   * @param datanodeDetails DatanodeDetails.
   * @return node pool name if it has been assigned. null if the node has not
   * been assigned to any node pool yet.
   */
  @Override
  public String getNodePool(DatanodeDetails datanodeDetails) {
    return nodeMemberShip.get(datanodeDetails);
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
}
