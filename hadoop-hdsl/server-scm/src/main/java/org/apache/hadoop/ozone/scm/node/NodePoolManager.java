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

package org.apache.hadoop.ozone.scm.node;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface that defines SCM NodePoolManager.
 */
public interface NodePoolManager extends Closeable {

  /**
   * Add a node to a node pool.
   * @param pool - name of the node pool.
   * @param node - data node.
   */
  void addNode(String pool, DatanodeID node) throws IOException;

  /**
   * Remove a node from a node pool.
   * @param pool - name of the node pool.
   * @param node - data node.
   * @throws SCMException
   */
  void removeNode(String pool, DatanodeID node)
      throws SCMException;

  /**
   * Get a list of known node pools.
   * @return a list of known node pool names or an empty list if not node pool
   * is defined.
   */
  List<String> getNodePools();

  /**
   * Get all nodes of a node pool given the name of the node pool.
   * @param pool - name of the node pool.
   * @return a list of datanode ids or an empty list if the node pool was not
   *  found.
   */
  List<DatanodeID> getNodes(String pool);

  /**
   * Get the node pool name if the node has been added to a node pool.
   * @param datanodeID - datanode ID.
   * @return node pool name if it has been assigned.
   * null if the node has not been assigned to any node pool yet.
   */
  String getNodePool(DatanodeID datanodeID) throws SCMException;
}
